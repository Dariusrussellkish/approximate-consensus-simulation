import json
import logging, logging.handlers
import os
import socket
import sys
import threading
import time

from numpy import random, log

# General control flow for a server:
# - It begins in state DOWN and waits until the controller sends
#   all servers the UP command to start the simulation
# - Periodically broadcasts state to all servers if it is up. Only some servers if Byzantine
# - Processes incoming messages from servers if not down
#   - if p > p_end, we tell the controller we are done, but keep broadcasting and processing
# - Process state commands from the controller, UP, DOWN, BYZANTINE, and CRASH
#   - if we crash, we tell all threads we are done, they join, and we tell the controller
#     we are exiting.The controller ends the simulation when all servers are done by crashing them.

# load in parameters
with open(sys.argv[1], 'r') as fh:
    params = json.load(fh)
params["server_ips"] = ["10.0.0." + str(i+3) for i in range(params['servers'])]

# global variables
serverID = int(sys.argv[2])
K = params["K"]
nServers = int(params["servers"])
v = float(random.randint(0, K + 1))
p = 0
R = list([0 for _ in range(nServers)])
R[serverID] = 1
isByzantine = False
isDown = True
isDone = False

# ensure logs folder exists
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(filename=f"logs/server_{serverID}.log", level=logging.INFO, filemode='w',
                    format='%(asctime)s %(levelname)-8s %(message)s')

rootLogger = logging.getLogger('')
socketHandler = logging.handlers.SocketHandler(params["logging_server_ip"],
                                               9999)

rootLogger.addHandler(socketHandler)

# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# lock for atomic updates
atomic_variable_lock = threading.Lock()

# end of simulation calculation
r = (3 * params["servers"] - 2 * params["f"]) / (4 * (params["servers"] - params["f"]))
p_end = log(params["eps"] / K) / log(r)


def format_message(finished=False):
    """
    Formats internal state into utf-8 encoded JSON for sending over network

    NOTE: This MUST be called from inside the acquired lock to be meaningful
    :return: utf-8 encoded JSON padded to 1024 bytes
    """
    global serverID, v, p, isDone, isDown, isByzantine
    return json.dumps(
        {
            "id": serverID,
            "v": v,
            "p": p,
            "time": time.time_ns(),
            "done": isDone or finished,
            "isDown": isDown,
            "isByzantine": isByzantine,
        }
    ).rjust(1024).encode('utf-8')


def periodic_broadcast():
    """
    Periodically broadcasts internal state based on period in parameter file
    """
    global serverID, v, p, atomic_variable_lock, params, isDone, isDown, isByzantine
    bcastSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    bcastSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    bcastSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    try:
        while True:
            atomic_variable_lock.acquire()
            try:
                # TODO: this might be refactorable to while not isDone
    
                # break out of the infinite loop if the server is done
                if isDone:
                    break
                message = format_message()
                assert len(message) <= 1024
    
                if not isDown:
                    logging.debug(f"Server {serverID} is broadcasting and isByzantine is {isByzantine}")
                # if we are not byzantine or down, broadcast to all
                if not isDown and not isByzantine:
                    bcastSocket.sendto(message, ('<broadcast>', params["server_port"]))
    
                # if we are byzantine and not down
                elif not isDown and isByzantine:
                    for ip in params["server_ips"]:
                        # flip (biased) coin if we will send to server
                        if random.rand() > params["byzantine_send_p"]:
                            logging.debug(f"Server {serverID} is broadcasting to {ip}")
                            bcastSocket.sendto(message, (ip, params["server_port"]))
            finally:
                atomic_variable_lock.release()
    
            time.sleep(params["broadcast_period"] / 1000)
    except:
        logging.exception(f"Server {serverID} encountered exception in periodic_broadcast")
    finally:
        bcastSocket.close()
        logging.info(f"Server {serverID} is exiting periodic_broadcast")
    return True


def process_message():
    """
    Process incoming messages from other servers
    """
    global v, p, R, atomic_variable_lock, params, p_end, isDown, isDone, serverID
    controllerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    bcastListenSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    controllerSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    bcastListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    bcastListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    bcastListenSocket.bind(("", params["server_port"]))

    signaled_controller = False
    while True:
        atomic_variable_lock.acquire()
        try:
            if isDone:
                break
        finally:
            atomic_variable_lock.release()
        try:
            bcastListenSocket.settimeout(5)
            data, addr = bcastListenSocket.recvfrom(1024)
            if not data:
                continue
            message = json.loads(data.decode('utf-8'))
        except socket.timeout:
            logging.info(f"Server {serverID} timed out on BCAST read, isDone is {isDone}")
            continue
        # if we pick up our own messages, don't listen
        if message["id"] == serverID:
            continue

        atomic_variable_lock.acquire()
        logging.debug(
            f"Server {serverID} {(v, p)}, R: {R} received broadcast from {message['id']}: {(message['v'], message['p'])}")
        try:
            if isDone:
                break

            # skip if we are down
            if isDown:
                logging.debug(f"Server {serverID} is down, skipping")
                continue

            updated = False
            if int(message["p"]) > int(p):
                v = float(message["v"])
                p = int(message["p"])
                R = list([0 for _ in range(nServers)])
                R[serverID] = 1
                updated = True
                logging.info(f"Server {serverID} accepting jump update from {message['id']}, now in phase {p}")

            elif message["p"] == p and R[int(message["id"])] == 0:
                R[int(message["id"])] = 1
                logging.debug(f"Server {serverID} updating R: {R}")

                if sum(R) >= int(params["servers"]) - int(params["f"]):
                    v = float(v) / float(sum(R))
                    p += 1
                    R = list([0 for _ in range(nServers)])
                    R[serverID] = 1
                    updated = True
                    logging.info(f"Server {serverID} accepting consensus update, now in phase {p}")

        # send update to controller if we changed state
            if updated:
                message = format_message()
                assert len(message) <= 1024
                logging.debug(f"Server {serverID} is sending state update to controller")
                controllerSocket.sendto(message, (params["controller_ip"], params["controller_port"]))

            # let the controller know we are done
            if p > p_end:
                if not signaled_controller:
                    logging.info(f"Server {serverID} letting controller know they are done")
                    message = format_message(finished=True)
                    assert len(message) <= 1024
                    controllerSocket.sendto(message, (params["controller_ip"], params["controller_port"]))
                    signaled_controller = True

        finally:
            atomic_variable_lock.release()

    bcastListenSocket.close()
    controllerSocket.close()
    logging.info(f"Server {serverID} is exiting process_message")
    return True


def process_controller_messages():
    """
    Process crash state changes from the controller
    """
    global isDown, isByzantine, isDone, serverID

    controllerListenSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    controllerListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    # use TCP to force controller to wait
    connected = False
    while not connected:
        try:
            controllerListenSocket.connect((params["controller_ip"], params["controller_port"]))
            connected = True
        except ConnectionRefusedError:
            logging.info(f"Server {serverID} connection refused, retrying")

    logging.info(f"Server {serverID} connected to controller")

    while True:
        try:
            controllerListenSocket.settimeout(1)
            data, addr = controllerListenSocket.recvfrom(1024)
        except socket.timeout:
            logging.debug(f"Server {serverID} timed out on controller read, isDone is {isDone}")
            continue
        if not data:
            continue
        message = json.loads(data.decode('utf-8'))

        atomic_variable_lock.acquire()
        logging.info(f"Server {serverID} received state update from controller, now isDown is {message['isDown']}, "
                     f"isByzantine is {message['isByzantine']}, isPermanent is {message['isPermanent']}")
        try:
            # once we get the permanent down command, set isDone to true and end all threads
            if message["isPermanent"]:
                logging.info(f"Server {serverID} received CRASH from controller")
                isDown = message["isDown"]
                isDone = True
                break
            isDown = message["isDown"]
            isByzantine = message["isByzantine"]
        finally:
            atomic_variable_lock.release()

    logging.info(f"Server {serverID} is exiting process_controller_messages")
    controllerListenSocket.close()
    return True


if __name__ == "__main__":
    try:
        time.sleep(1)
        logging.info(f"Server {serverID} is beginning simulation")

        serverBCast = threading.Thread(target=periodic_broadcast)
        serverBCast.start()

        messageProcessor = threading.Thread(target=process_message)
        messageProcessor.start()

        controllerListener = threading.Thread(target=process_controller_messages)
        controllerListener.start()

        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is not main_thread:
                t.join()

        # # Tell the controller we are done in the case of a permanent failure
        # message = format_message(finished=True)
        # assert len(message) <= 1024
        # controllerSocket.sendto(message, (params["controller_ip"], params["controller_port"]))

        logging.info(f"Server {serverID} finished")

    except:
        logging.exception(f"Server {serverID} encountered error in main thread")

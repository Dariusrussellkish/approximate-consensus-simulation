import json
import logging, logging.handlers
import pickle
import select
import socket
import sys
import threading
import time
import os
import uuid
import shutil

from numpy import random, interp
from ApproximateConsensusAlgorithm.ApproximateConsensusAlgorithm import ApproximateConsensusAlgorithm

doneServersLock = threading.Lock()

# ensure logs directory exists
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(filename=f"logs/controller.log", level=logging.DEBUG, filemode='w',
                    format='%(asctime)s %(levelname)-8s %(message)s')
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# load in parameters
with open(sys.argv[1], 'r') as fh:
    params = json.load(fh)

rootLogger = logging.getLogger('')
socketHandler = logging.handlers.SocketHandler(params["logging_server_ip"],
                                               9999)

rootLogger.addHandler(socketHandler)

# list of server states for post hoc analysis
serverStates = {}
for i in range(params["servers"]):
    serverStates[i] = []

doneServers = [False for _ in range(params['servers'])]
readyServers = [False for _ in range(params['servers'])]
convergedServers = [False for _ in range(params['servers'])]


def format_message(isByzantine, isDown, isPermanent=False):
    """
    Formats message to the server about state information
    :param isByzantine:
    :param isDown:
    :return: utf-8 encoded JSON bytes
    """
    return json.dumps(
        {
            "is_down": isDown,
            "is_byzantine": isByzantine,
            "is_done": isPermanent,
        }
    ).rjust(1024).encode('utf-8')


def get_wait_time(isDown, shape=3, scale=2):
    """
    Sample wait time from a Gamma distribution

    Shape and scale (k, theta) were picked arbitrarily to
    form a distribution shape that looked good
    :param shape:
    :param scale:
    :return:
    """
    wait = random.gamma(shape, scale)
    if wait > 10:
        wait = 10
    if isDown:
        wait = interp(wait, (0, 10), (0, 1))
    else:
        wait = interp(wait, (0, 10), (0, 10))
    return wait


def downed_server(ip, server_id, connection):
    """
    Permanently downs a server
    """
    global params
    try:
        wait_time = get_wait_time(True)
        time.sleep(wait_time)
        message = format_message(False, True, isPermanent=True)
        assert len(message) <= 1024
        connection.sendall(message)
        connection.close()

        doneServersLock.acquire()
        doneServers[server_id] = True
    except:
        logging.exception(f"Controller encountered exception downing server {server_id}")
    finally:
        doneServersLock.release()
    logging.info(f"Controller sent permanent downed command to {ip}")

    return True


def unreliable_server(ip, server_id, byzantine, connection):
    """
    Periodically downs and ups a server
    """
    global params, doneServers
    try:
        isDown = False
        isByzantine = False
        done = False
        while True:
            time_slept = 0
            wait_time = get_wait_time(isDown)
            while time_slept < wait_time:
                doneServersLock.acquire()
                try:
                    done = doneServers[server_id]
                finally:
                    doneServersLock.release()

                if done:
                    # ensure the server is up before ending UP/DOWN broadcasts
                    message = format_message(isByzantine, False)
                    assert len(message) <= 1024
                    connection.sendall(message)
                    break

                time.sleep(0.5)
                time_slept += 0.5

            if done:
                break

            isDown = not isDown
            # logging.info(f"Controller sent {'down' if isDown else 'up'} command to {ip}")
            if byzantine and not isByzantine:
                isByzantine = random.rand() < params["byzantine_p"]
                if isByzantine:
                    doneServersLock.acquire()
                    try:
                        logging.info(f"Controller marking {server_id} as byzantine and done")
                        doneServers[server_id] = True
                    finally:
                        doneServersLock.release()
                message = format_message(True, isDown)
            elif isByzantine:
                message = format_message(True, isDown)
            else:
                message = format_message(False, isDown)
            assert len(message) <= 1024
            connection.sendall(message)
    except ConnectionResetError:
        doneServersLock.acquire()
        try:
            doneServers[server_id] = True
        finally:
            doneServersLock.release()

    except BrokenPipeError:
        doneServersLock.acquire()
        try:
            doneServers[server_id] = True
        finally:
            doneServersLock.release()

    except:
        logging.exception(f"Controller encountered exception in unreliable server {server_id}")
    finally:
        logging.info(f"Controller is shutting down server {server_id}")
        connection.close()
    return True


def process_server_states(faulty_servers):
    """
    Process incoming server state messages
    """
    global params, serverStates, readyServers, convergedServers
    controllerListenSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)  # UDP
    controllerListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    controllerListenSocket.bind(("", params["controller_port"]))
    signaled_servers = False
    phases_after_p_agreement = 0

    while True:
        try:
            controllerListenSocket.settimeout(5)
            data, ip = controllerListenSocket.recvfrom(1024)
            if not data:
                continue
        except socket.timeout:
            logging.debug("Controller timed out on state update read")
            continue
        message = json.loads(data.decode('utf-8'))

        # in some testing the server also picks up the UP/DOWN messages
        # so this filters them out (if server and controller share IP)
        if "id" not in message:
            continue

        if 'ready' in message:
            readyServers[message['id']] = message['ready']
            continue

        logging.info(f"Controller received state update from {message['id']}, it is now in phase {message['p']}")

        received_time = int(round(time.time() * 1000))
        serverStates[message["id"]].append({**message, 'time_received': received_time})

        # if 'converged' in message and message['converged'] and message['id'] not in faulty_servers:
        #     if not convergedServers[message['id']]:
        #         logging.info(f"Controller received converged message from {message['id']}")
        #     convergedServers[message['id']] = True

        
        values = []
        times = []
        phases = []
        for server in serverStates:
            if server not in faulty_servers and serverStates[server]:
                if server == 'p_agreement':
                    continue
                if serverStates[server][-1]['p'] == message['p']:
                    times.append(serverStates[server][-1]['time_generated'])
                    values.append(serverStates[server][-1]['v'])
                    phases.append(serverStates[server][-1]['p'])
        if len(values) >= params['servers'] - params['f'] and max(values) - min(values) <= params['eps'] and 'p_agreement' not in serverStates:
            serverStates['p_agreement'] = {'time': max(times), 'phase': max(phases)}
            phases_after_p_agreement = 0
            logging.info(f"Controller found agreement at phase {message['p']}")
        elif 'p_agreement' in serverStates and max(values) - min(values) > params['eps']:
            phases_after_p_agreement = 0
            serverStates.pop('p_agreement')
            logging.info(f"Controller resetting agreement at phase {message['p']} due to divergence")
        elif 'p_agreement' in serverStates:
            logging.info(f"Controller incrementing phases after p_agreement")
            phases_after_p_agreement += 1
            if phases_after_p_agreement > 10:
                if 'terminate_on_p_agreement' in params and params['terminate_on_p_agreement']:
                    logging.info(f"Controller is terminating servers by p agreement")
                    doneServersLock.acquire()
                    try:
                        for i, dserver in enumerate(doneServers):
                            logging.info(f"Controller is terminating {i}")
                            doneServers[i] = True
                    finally:
                        doneServersLock.release()

        doneServersLock.acquire()
        try:
            if message["is_done"]:
                logging.info(f"Controller received DONE from {message['id']}, done servers are {doneServers}")
                doneServers[message['id']] = True

            # check if all the servers are done (or permanently down)
            if all(doneServers) and not signaled_servers:
                for ip in params["server_ips"]:
                    if ip not in downedServers:
                        # send crash command to server, which will make it end
                        try:
                            logging.info(f"Controller sending CRASH to {ip}")
                            connection = sockets[ip]
                            message = format_message(False, True, isPermanent=True)
                            connection.sendall(message)
                        except:
                            break
                signaled_servers = True
                break
        finally:
            doneServersLock.release()

    controllerListenSocket.close()
    return True


if __name__ == "__main__":

    try:
        logging.info(f"Controller is starting")

        try:
            algorithm = ApproximateConsensusAlgorithm(params, -1)
        except ValueError as e:
            logging.exception(e)
            sys.exit(22)

        params["server_ips"] = ["10.0.0." + str(i + 3) for i in range(params['servers'])]
        logging.info(f"Server IPs are {set(params['server_ips'])}")

        # pick which servers will be down
        servers = params["server_ips"]
        downedServers = list(random.choice(servers, params["f"], replace=False))
        if algorithm.supports_byzantine():
            logging.info(f"Byzantine servers are: {downedServers}")
            byzantineServers = downedServers
            downedServers = []
        else:
            logging.info(f"Permanently Downed servers are: {downedServers}")
            byzantineServers = []

        faulty_servers = [*downedServers, *byzantineServers]
        sockets = {}

        controllerListener = threading.Thread(target=process_server_states, args=(faulty_servers,))
        controllerListener.start()

        logging.info(f"Controller will wait for servers to connect")

        controllerSendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        controllerSendSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        controllerSendSocket.bind(("0.0.0.0", params["controller_port"]))
        controllerSendSocket.listen(1)

        # waits for all servers to connect before beginning simulation
        for i in range(params["servers"]):
            logging.info(f"Controller is waiting for connection")

            connection, client_address = controllerSendSocket.accept()

            logging.info(f"Controller established connection with {client_address}")
            sockets[client_address[0]] = connection

        logging.info(f"Controller has connected to all servers")

        logging.info(f"Controller is waiting to all servers to mark READY")
        while not all(readyServers):
            time.sleep(0.1)
        logging.info(f"Controller has received READY from all servers")
        # send command to all servers to go up
        first_started_time = int(round(time.time() * 1000))
        for ip in params["server_ips"]:
            connection = sockets[ip]
            message = format_message(False, False)
            connection.sendall(message)
        all_started_time = int(round(time.time() * 1000))

        logging.info(f"Controller has started all servers")

        for i in range(params["servers"]):
            ip = params["server_ips"][i]

            if ip in downedServers:
                controller = threading.Thread(target=downed_server, args=(ip, i, sockets[ip]))
                controller.start()
            elif ip in byzantineServers:
                controller = threading.Thread(target=unreliable_server, args=(ip, i, True, sockets[ip]))
                controller.start()
            else:
                controller = threading.Thread(target=unreliable_server, args=(ip, i, False, sockets[ip]))
                controller.start()

        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is not main_thread:
                t.join()

        basepath = f"data/{params['algorithm']}_nServers_{params['servers']}_f_{params['f']}_eps_{params['eps']}_byzantineP_{params['byzantine_p']}_bcastPeriod_{params['broadcast_period']}"
        # ensure basepath directory exists
        if not os.path.exists(basepath):
            os.makedirs(basepath)
        unique = sys.argv[2]
        with open(f"{basepath}/{unique}.pickle", 'wb') as fh:
            pickle.dump(
                {
                    "serverStates": serverStates,
                    "params": params,
                    "first_start_time": first_started_time,
                    "all_start_time": all_started_time,
                    "faulty_servers": faulty_servers
                }, fh
            )
        # shutil.copytree("logs", f"data/{basepath}/logs_{unique}_{sys.argv[3]}")
        logging.info("Controller is finished")

    except:
        logging.exception("Controller encountered error in main thread")

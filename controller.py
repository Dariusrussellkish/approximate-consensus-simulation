import json
import logging
import pickle
import socket
import sys
import threading
import time
import os
import uuid

from numpy import random, interp

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

# list of server states for post hoc analysis
serverStates = {}
for i in range(params["servers"]):
    serverStates[i] = []

doneServers = [False for _ in range(params["servers"])]

params["server_ips"] = ["10.0.0." + str(i+2) for i in range(params['servers'])]
logging.info(f"Server IPs are {set(params['server_ips'])}")

# pick which servers will be down
servers = params["server_ips"]
downedServers = random.choice(servers, params["f"], replace=False)
logging.info(f"Downed servers are: {downedServers}")
notDownedServers = list(set(servers) - set(downedServers))
logging.info(f"Servers eligible for Byzantine are: {notDownedServers}")
byzantineServers = random.choice(notDownedServers, random.randint(0, len(notDownedServers) + 1), replace=False)
logging.info(f"Byzantine Servers are: {byzantineServers}")

sockets = {}


def format_message(isByzantine, isDown, isPermanent=False):
    """
    Formats message to the server about state information
    :param isByzantine:
    :param isDown:
    :return: utf-8 encoded JSON bytes
    """
    return json.dumps(
        {
            "isDown": isDown,
            "isByzantine": isByzantine,
            "isPermanent": isPermanent,
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
        wait = interp(wait, (0, 10), (0, 5))
    else:
        wait = interp(wait, (0, 10), (0, 200))
    return wait


def downed_server(ip, server_id, connection):
    """
    Permanently downs a server
    """
    global params

    wait_time = get_wait_time(False)
    time.sleep(wait_time + 2)
    message = format_message(False, True, isPermanent=True)
    assert len(message) <= 1024
    connection.sendall(message)
    connection.close()

    doneServersLock.acquire()
    try:
        doneServers[server_id] = True
    finally:
        doneServersLock.release()
    logging.info(f"Controller sent permanent downed command to {ip}")

    return True


def unreliable_server(ip, server_id, byzantine, connection):
    """
    Periodically downs and ups a server
    """
    global params, doneServers

    isDown = False
    isByzantine = False

    while True:
        doneServersLock.acquire()
        try:
            if doneServers[server_id]:
                # ensure the server is up before ending UP/DOWN broadcasts
                message = format_message(isByzantine, False)
                assert len(message) <= 1024
                connection.sendall(message)
                break
        except socket.error as e:
            break
        finally:
            doneServersLock.release()

        wait_time = get_wait_time(isDown)
        time.sleep(wait_time)

        isDown = not isDown
        logging.info(f"Controller sent {'down' if isDown else 'up'} command to {ip}")
        if byzantine and not isByzantine:
            isByzantine = random.rand() < params["byzantine_p"]
            message = format_message(True, isDown)
        elif isByzantine:
            message = format_message(True, isDown)
        else:
            message = format_message(False, isDown)
        assert len(message) <= 1024
        connection.sendall(message)

    connection.close()
    return True


def process_server_states():
    """
    Process incoming server state messages
    """
    global params, serverStates
    controllerListenSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)  # UDP
    controllerListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    controllerListenSocket.bind(("", params["controller_port"]))

    while True:
        try:
            data, ip = controllerListenSocket.recvfrom(1024)
            if not data:
                continue
        except socket.timeout:
            logging.info("Controller timed out on state update read")
            continue
        message = json.loads(data.decode('utf-8'))

        # in some testing the server also picks up the UP/DOWN messages
        # so this filters them out (if server and controller share IP)
        if "id" not in message:
            continue

        logging.info(f"Controller received state update from {message['id']}, it is now in phase {message['p']}")

        serverStates[message["id"]].append(message)

        doneServersLock.acquire()
        try:
            if message["done"]:
                logging.info(f"Controller received DONE from {message['id']}, done servers are {doneServers}")
                doneServers[message["id"]] = True

            # check if all the servers are done (or permanently down)
            if all(doneServers):
                for ip in params["server_ips"]:
                    if ip not in downedServers:
                        # send crash command to server, which will make it end
                        logging.info(f"Controller sending CRASH to {ip}")
                        connection = sockets[ip]
                        message = format_message(False, True, isPermanent=True)
                        connection.sendall(message)
                break
        finally:
            doneServersLock.release()

    controllerListenSocket.close()
    return True


if __name__ == "__main__":

    # TODO: refactor to make default state for servers DOWN, then wait for all connect TCP and send UP status and
    #  byzantine
    try:
        logging.info(f"Controller is starting")

        controllerListener = threading.Thread(target=process_server_states)
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

        # send command to all servers to go up
        for ip in params["server_ips"]:
            connection = sockets[ip]
            message = format_message(False, False)
            connection.sendall(message)

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

        basepath = f"{params['algorithm']}_nServers_{params['servers']}_f_{params['f']}_eps_{params['eps']}_byzantineP_{params['byzantine_p']}_bcastPeriod_{params['broadcast_period']}"
        # ensure basepath directory exists
        if not os.path.exists(basepath):
            os.makedirs(basepath)
        unique = uuid.uuid4().hex
        with open(f"{basepath}/{unique}.pickle", 'wb') as fh:
            pickle.dump(
                {
                    "serverStates": serverStates,
                    "params": params,
                }, fh
            )

        logging.info("Controller is finished")

    except:
        logging.exception("Controller encountered error in main thread")

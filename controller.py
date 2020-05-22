import json
import logging
import pickle
import socket
import sys
import threading
import time
import os

from numpy import random

# ensure logs directory exists
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(filename=f"logs/controller.log", level=logging.DEBUG, filemode='w')
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# load in parameters
with open(sys.argv[1], 'r') as fh:
    params = json.load(fh)

# list of server states for post hoc analysis
serverStates = {}
for i in range(params["servers"]):
    serverStates[i] = []

doneServers = [False for _ in range(params["servers"])]

logging.info(f"Server IPs are {set(params['server_ips'])}")

# pick which servers will be down
servers = params["server_ips"]
downedServers = random.choice(servers, params["f"], replace=False)
logging.info(f"Downed servers are: {downedServers}")
notDownedServers = list(set(servers) - set(downedServers))
logging.info(f"Servers eligible for Byzantine are: {notDownedServers}")
byzantineServers = random.choice(notDownedServers, random.randint(0, len(notDownedServers) + 1), replace=False)
logging.info(f"Byzantine Servers are: {byzantineServers}")


def format_message(isByzantine, isDown):
    """
    Formats message to the server about state information
    :param isByzantine:
    :param isDown:
    :return: utf-8 encoded JSON bytes
    """
    return json.dumps(
        {
            "isDown": isDown,
            "isByzantine": isByzantine
        }
    ).rjust(1024).encode('utf-8')


def get_wait_time(shape=3, scale=2):
    """
    Sample wait time from a Gamma distribution

    Shape and scale (k, theta) were picked arbitrarily to
    form a distribution shape that looked good
    :param shape:
    :param scale:
    :return:
    """
    return random.gamma(shape, scale)


def downed_server(ip, server_id, connection):
    """
    Permanently downs a server
    """
    global params

    wait_time = get_wait_time()
    time.sleep(wait_time + 2)
    message = format_message(False, True)
    assert len(message) <= 1024
    connection.sendall(message)
    connection.close()

    doneServers[server_id] = True

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

        if doneServers[server_id]:
            break

        for _ in range(2):
            wait_time = get_wait_time()
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
        data, ip = controllerListenSocket.recvfrom(1024)

        message = json.loads(data.decode('utf-8'))

        # in some testing the server also picks up the UP/DOWN messages
        # so this filters them out (if server and controller share IP)

        if not message.has_key("id"):
            continue

        logging.info(f"Controller received state update from {message['id']}")

        serverStates[message["id"]].append(message)
        if message["done"]:
            doneServers[message["id"]] = True

        if all(doneServers):
            break
    return True


if __name__ == "__main__":

    # TODO: refactor to make default state for servers DOWN, then wait for all connect TCP and send UP status and
    #  byzantine
    try:
        logging.info(f"Controller is starting")

        controllerListener = threading.Thread(target=process_server_states)
        controllerListener.start()

        logging.info(f"Controller will wait for servers to connect")

        # waits for all servers to connect before beginning simulation
        sockets = []
        for i in range(params["servers"]):
            ip = params["server_ips"][i]
            controllerSendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            controllerSendSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            controllerSendSocket.bind(("localhost", params["controller_port"]))
            controllerSendSocket.listen(1)

            logging.info(f"Controller is waiting for connection from {ip}")

            connection, client_address = controllerSendSocket.accept()

            logging.info(f"Controller established connection with {ip}")
            sockets.append(connection)

        logging.info(f"Controller has connected to all servers")

        # send command to all servers to go up
        for i in range(params["servers"]):
            connection = sockets[i]
            message = format_message(False, True)
            connection.sendall(message)

        logging.info(f"Controller has started all servers")

        for i in range(params["servers"]):
            ip = params["server_ips"][i]

            if ip in downedServers:
                controller = threading.Thread(target=downed_server, args=(ip, i, sockets[i]))
                controller.start()
            elif ip in byzantineServers:
                controller = threading.Thread(target=unreliable_server, args=(ip, i, True, sockets[i]))
                controller.start()
            else:
                controller = threading.Thread(target=unreliable_server, args=(ip, i, False, sockets[i]))
                controller.start()

        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is not main_thread:
                t.join()

        with open("simulation_output.pickle", 'wb') as fh:
            pickle.dump(
                {
                    "serverStates": serverStates,
                    "params": params,
                }, fh
            )

        logging.info("Controller is finished")

    except:
        logging.exception("Controller encountered error in main thread")

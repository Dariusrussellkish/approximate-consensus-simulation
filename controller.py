import json
import socket
import pickle
import threading
import time
import sys

from numpy import random

with open(sys.argv[1], 'r') as fh:
    params = json.load(fh)

serverStates = {}
for i in range(params["servers"]):
    serverStates[i] = []

doneServers = [False for _ in range(params["servers"])]

downedServers = random.choice(params["server_ips"], params["f"], replace=False)


def format_message(isByzantine, isDown):
    return json.dumps(
        {
            "isDown": isDown,
            "isByzantine": isByzantine
        }
    ).encode('utf-8')


def get_wait_time(shape=3, scale=2):
    return random.gamma(shape, scale)


def downed_server(ip):
    global params
    controllerSendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    controllerSendSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    controllerSendSocket.bind((ip, params["controller_port"]))
    controllerSendSocket.listen(1)
    connection, client_address = controllerSendSocket.accept()

    wait_time = get_wait_time()
    time.sleep(wait_time + 2)
    message = format_message(False, True)
    assert len(message) < 1024
    connection.sendall(message)
    connection.close()

    return True


def unreliable_server(ip, server_id):
    global params, doneServers

    controllerSendSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)  # UDP
    controllerSendSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    isDown = False

    while True:

        if doneServers[server_id]:
            break

        isByzantine = random.rand() < params["byzantine_p"]
        if isByzantine:
            message = format_message(isByzantine, False)
            assert len(message) < 1024
            controllerSendSocket.sendto(message, (ip, params["controller_port"]))
        for _ in range(2):
            wait_time = get_wait_time()
            time.sleep(wait_time)

            isDown = not isDown
            message = format_message(isByzantine, isDown)
            assert len(message) < 1024
            controllerSendSocket.sendto(message, (ip, params["controller_port"]))

    return True


def process_server_states():
    global params, serverStates
    controllerListenSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)  # UDP
    controllerListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    controllerListenSocket.bind(("", params["controller_socket"]))

    while True:
        data, ip = controllerListenSocket.recv(1024)
        message = json.loads(data.decode('utf-8'))
        serverStates[message["id"]].append(message)
        if message["done"]:
            doneServers[message["id"]] = True

        if all(doneServers):
            break
    return True


if __name__ == "__main__":
    controllerListener = threading.Thread(target=process_server_states)
    controllerListener.start()

    for i in range(params["servers"]):
        ip = params["server_ips"][i]

        if ip in downedServers:
            controller = threading.Thread(target=downed_server, args=(ip,))
            controller.start()

        else:
            controller = threading.Thread(target=unreliable_server, args=(ip, i))
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

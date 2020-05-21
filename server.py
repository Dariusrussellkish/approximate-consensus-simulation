import json
import logging
import socket
import sys
import threading
import time

from numpy import random, log

bcastSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
controllerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
bcastListenSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
controllerListenSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

bcastSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
controllerSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
bcastListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
controllerListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

bcastSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
bcastListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

with open(sys.argv[1], 'r') as fh:
    params = json.load(fh)

serverID = int(sys.argv[2])
K = params["K"]
nServers = int(params["servers"])
v = random.randint(0, K + 1)
p = 0
R = list([0 for _ in range(nServers)])
isByzantine = False
isDown = False
isDone = False

atomic_variable_lock = threading.Lock()

r = (3 * params["servers"] - 2 * params["f"]) / (4 * (params["servers"] - params["f"]))
p_end = log(params["eps"] / K) / log(r)

bcastListenSocket.bind(("", params["server_port"]))
controllerListenSocket.bind((params["controller_ip"], params["controller_port"]))


def format_message():
    global serverID, v, p, isDone, isDown, isByzantine
    return json.dumps(
        {
            "id": serverID,
            "v": v,
            "p": p,
            "time": time.time_ns(),
            "done": isDone,
            "isDown": isDown,
            "isByzantine": isByzantine,
        }
    ).encode('utf-8')


def periodic_broadcast():
    global serverID, v, p, atomic_variable_lock, params, isDone, isDown, isByzantine, bcastSocket
    while True:
        atomic_variable_lock.acquire()
        try:
            if isDone:
                break
            message = format_message()
            assert len(message) < 1024
            logging.info(f"Server {serverID} is broadcasting and isByzantine is {isByzantine}")
            if not isDown and not isByzantine:
                bcastSocket.sendto(message, ('<broadcast>', params["server_port"]))
            elif not isDown and isByzantine:
                for ip in params["server_ips"]:
                    if random.rand() > params["byzantine_send_p"]:
                        bcastSocket.sendto(message, (ip, params["server_port"]))
        finally:
            atomic_variable_lock.release()

        time.sleep(params["broadcast_period"] / 1000)

    return True


def process_message():
    global v, p, R, atomic_variable_lock, params, p_end, isDown, isDone, controllerSocket, serverID
    while True:
        data, addr = bcastListenSocket.recvfrom(1024)
        message = json.loads(data.decode('utf-8'))
        logging.info(f"Server {serverID} received broadcast from {message['id']}")
        if message["id"] == serverID:
            continue
        atomic_variable_lock.acquire()

        try:
            if isDown:
                continue
            updated = False
            if message["p"] > p:
                v = message["v"]
                p = message["p"]
                R = list([0 for _ in range(nServers)])
                updated = True
            elif message["p"] == p and R[int(message["id"])] != 1:
                R[int(message["id"])] = 1
                if sum(R) >= params["servers"] - params["f"]:
                    v = v / float(sum(R))
                    p += 1
                    updated = True

            if updated:
                message = format_message()
                assert len(message) < 1024
                logging.info(f"Server {serverID} is sending state update to server")
                controllerSocket.sendto(message, (params["controller_ip"], params["controller_port"]))

                if p > p_end:
                    isDone = True
                    break

        finally:
            atomic_variable_lock.release()

    return True


def process_controller_messages():
    global isDown, isByzantine, isDone, controllerListenSocket, serverID
    controllerListenSocket.connect((params["controller_ip"], params["controller_port"]))

    logging.info(f"Server {serverID} connected to controller")

    while True:
        data, addr = controllerListenSocket.recvfrom(1024)
        message = json.loads(data.decode('utf-8'))
        logging.info(f"Server {serverID} received state update from controller")

        atomic_variable_lock.acquire()
        try:
            if isDone:
                break
            isDown = message["isDown"]
            isByzantine = message["isByzantine"]
        finally:
            atomic_variable_lock.release()

    controllerListenSocket.close()
    return True


if __name__ == "__main__":

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

    message = format_message()
    assert len(message) < 1024
    controllerSocket.sendto(message, (params["controller_ip"], params["controller_port"]))

    logging.info(f"Server {serverID} finished")

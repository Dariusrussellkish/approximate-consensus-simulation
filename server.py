import json
import logging
import logging.handlers
import os
import socket
import sys
import threading
import time
import select
from ApproximateConsensusAlgorithm.ApproximateConsensusAlgorithm import ApproximateConsensusAlgorithm
from ControllerConnection.ControllerTimeoutError import ControllerTimeoutError
from ControllerConnection.DataNotPresentError import DataNotPresentError
from ControllerConnection.ControllerConnection import ControllerConnection

from numpy import random


class ServerState:
    logger = logging.getLogger('Server')
    
    def __init__(self, server_id):
        self.server_id = server_id
        self.is_down = True
        self.is_byzantine = False
        self.is_done = False
        self.lock = threading.Lock()

    def process_message(self, message):
        self.lock.acquire()
        try:
            self.is_down = message['is_down']
            self.is_byzantine = message['is_byzantine']
            self.is_done = message['is_done']
        finally:
            self.lock.release()
            ServerState.logger.info(f"Server {self.server_id} received {self.is_down} {self.is_byzantine} "
                                    f"{self.is_done} from controller")

    def get_state(self):
        self.lock.acquire()
        try:
            ret = {
                'is_down': self.is_down,
                'is_byzantine': self.is_byzantine,
                'is_done': self.is_done,
                'time_generated': int(round(time.time() * 1000)),
            }
            return ret
        finally:
            self.lock.release()

    def is_finished(self):
        self.lock.acquire()
        try:
            ret = self.is_done
            return ret
        finally:
            self.lock.release()


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

params["server_ips"] = ["10.0.0." + str(i + 3) for i in range(params['servers'])]

# global variables
serverID = int(sys.argv[2])

# ensure logs folder exists
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(filename=f"logs/server_{serverID}.log", level=logging.INFO, filemode='w',
                    format='%(asctime)s %(levelname)-8s %(message)s')

rootLogger = logging.getLogger('')
socketHandler = logging.handlers.SocketHandler(params["logging_server_ip"],
                                               9999)

rootLogger.addHandler(socketHandler)
logger = logging.getLogger('Server')


def format_message(state):
    """
    Formats internal state into utf-8 encoded JSON for sending over network

    NOTE: This MUST be called from inside the acquired lock to be meaningful
    :return: utf-8 encoded JSON padded to 1024 bytes
    """
    ret = json.dumps(state).rjust(1024).encode('utf-8')
    try:
        assert len(ret) <= 1024
    except AssertionError:
        logger.exception(ret.decode('utf-8'))
        raise AssertionError
    return ret


def broadcast(algorithm, server_state, server_id, bcastSocket):
    state = server_state.get_state()
    algo_state = algorithm.get_internal_state()
    message = format_message({**state, **algo_state})

    if not state['is_down']:
        logger.debug(f"Server {server_id} is broadcasting and isByzantine is {state['is_byzantine']}")

    if algorithm.supports_byzantine() and not state['is_down'] and state['is_byzantine']:
        for ip in params["server_ips"]:
            # flip (biased) coin if we will send to server
            if random.rand() > params["byzantine_send_p"]:
                logger.debug(f"Server {serverID} is broadcasting to {ip}")
                bcastSocket.sendto(message, (ip, params["server_port"]))

    # if we are not byzantine or down, broadcast to all
    elif not state['is_down']:
        bcastSocket.sendto(message, ('<broadcast>', params["server_port"]))


def periodic_broadcast(algorithm, server_state, server_id, bcastSocket):
    """
    Periodically broadcasts internal state based on period in parameter file
    """

    logger.info(f"Server {server_id} starting to broadcast periodically")

    try:
        while not server_state.is_finished():
            bcast = select.select([], [bcastSocket], [])[1][0]
            broadcast(algorithm, server_state, server_id, bcast)
            time.sleep(params["broadcast_period"] / 1000)
    finally:
        logger.info(f"Server {serverID} is exiting periodic_broadcast")
    return True


def process_message(algorithm, server_state, controller_connection, server_id, bcastsocket):
    bcastListenSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    bcastListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    bcastListenSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    bcastListenSocket.bind(("", params["server_port"]))
    signaled_controller = False

    logger.info(f"Server {server_id} starting to process broadcast messages")

    while not server_state.is_finished():
        try:
            bcastListenSocket.settimeout(0.5)
            data, addr = bcastListenSocket.recvfrom(1024)
            if not data or not data.decode('utf-8').strip():
                continue
            message = json.loads(data.decode('utf-8'))
        except socket.timeout:
            logger.debug(f"Server {server_id} timed out on BCAST read")
            continue
        # if we pick up our own messages, don't listen
        if message["id"] == server_id:
            continue

        if random.rand() < params['drop_rate']:
            logger.debug(f"Server {server_id} is dropping packet from {message['id']}")
            continue

        logger.debug(f"Server {server_id} received message from {message['id']}")

        updated = algorithm.process_message(message)

        if algorithm.requires_synchronous_update_broadcast and updated:
            bcast = select.select([], [bcastSocket], [])[1][0]
            broadcast(algorithm, server_state, server_id, bcast)

        if updated:
            algo_state = algorithm.get_internal_state()
            state = server_state.get_state()
            message = format_message({**state, **algo_state})
            logging.info(f"Server {serverID} is sending state update to controller")
            controller_connection.send_state(message)

        # let the controller know we are done
        if algorithm.is_done():
            if not signaled_controller:
                logging.info(f"Server {serverID} letting controller know they are done")
                state = server_state.get_state()
                algo_state = algorithm.get_internal_state()
                message = format_message({**state, **algo_state})
                controller_connection.send_state(message)
                signaled_controller = True

    bcastListenSocket.close()
    logging.info(f"Server {serverID} is exiting process_message")
    return True


def process_controller_messages(server_state, controller_connection, server_id):
    """
    Process crash state changes from the controller
    """
    logger.info(f"Server {server_id} starting to process controller messages")
    while not server_state.is_finished():
        try:
            message = controller_connection.get_data()
            server_state.process_message(message)
        except ControllerTimeoutError:
            logger.debug(f"Server {server_id} timed out on controller read")
        except DataNotPresentError:
            pass
    logger.info(f"Server {server_id} is exiting process_controller_messages")
    return True


if __name__ == "__main__":
    logger.info(f"Server {serverID} is beginning simulation")

    server_state = ServerState(serverID)
    algorithm = ApproximateConsensusAlgorithm(params, serverID)
    controller_connection = ControllerConnection(params, serverID)

    bcastSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    bcastSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    bcastSocket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    logger.info(f"Server {serverID} connected with controller")

    serverBCast = threading.Thread(target=periodic_broadcast,
                                   args=(algorithm, server_state, serverID, bcastSocket), name="serverBCast")
    serverBCast.start()

    messageProcessor = threading.Thread(target=process_message,
                                        args=(algorithm, server_state, controller_connection, serverID, bcastSocket),
                                        name="messageProcessor")
    messageProcessor.start()

    controllerListener = threading.Thread(target=process_controller_messages,
                                          args=(server_state, controller_connection, serverID),
                                          name="controllerListener")
    controllerListener.start()

    while not server_state.is_finished():
        for t in [serverBCast, messageProcessor, controllerListener]:
            if not t.is_alive() and not server_state.is_finished():
                logging.fatal(f"Server {serverID} crashed in thread {t.name}")
                server_state.lock.acquire()
                try:
                    server_state.is_done = True
                finally:
                    server_state.lock.release()
        time.sleep(1)

    main_thread = threading.currentThread()
    for t in threading.enumerate():
        if t is not main_thread:
            t.join()

    controller_connection.cleanup()
    bcastSocket.close()

    logger.info(f"Server {serverID} finished")

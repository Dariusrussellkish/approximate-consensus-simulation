import socket
import logging
import json
from ..ControllerTimeoutError import ControllerTimeoutError
from ..DataNotPresentError import DataNotPresentError


class PureNetworkImplementation:
    logger = logging.getLogger('PN-Controller')

    def __init__(self, controller_ip, controller_port, server_id, **kwargs):
        self.controller_ip = controller_ip
        self.controller_port = controller_port
        self.server_id = server_id

        self.controller_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.controller_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        # use TCP to force controller to wait
        self.controller_listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.controller_listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    def initialize(self):
        connected = False
        while not connected:
            try:
                self.controller_listen_socket.connect((self.controller_ip, self.controller_port))
                connected = True
            except ConnectionRefusedError:
                PureNetworkImplementation.logger.info(f"Server {self.server_id} connection refused, retrying")

        PureNetworkImplementation.logger.info(f"Server {self.server_id} connected to controller")

    def cleanup(self):
        try:
            self.controller_listen_socket.close()
            self.controller_socket.close()
        except OSError:
            pass

    def get_data(self):
        try:
            self.controller_listen_socket.settimeout(5)
            data = self.controller_listen_socket.recv(1024)
        except socket.timeout:
            raise ControllerTimeoutError
        if not data:
            raise DataNotPresentError
        try:
            message = json.loads(data.decode('utf-8'))
        except json.decoder.JSONDecodeError:
            PureNetworkImplementation.logger.exception(
                f"Server {self.server_id} encountered exception trying to process "
                f"JSON: '{data.decode('utf-8').strip()}'")
            raise DataNotPresentError
        return message

    def send_state(self, message):
        self.controller_socket.sendto(message, (self.controller_ip, self.controller_port))
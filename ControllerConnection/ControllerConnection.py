import logging
from .ConnectionImplementations.PureNetworkImplementation import PureNetworkImplementation
from .ConnectionImplementations.ImplementationError import ImplementationError


def __get_implementation__(implementation, params):
    if implementation == "PureNetworkImplementation":
        return PureNetworkImplementation(**params)
    else:
        raise ImplementationError


class ControllerConnection:
    logger = logging.getLogger('CC')

    def __init__(self, params, server_id, implementation_key='controller_implementation'):
        self.params = params
        self.params["server_id"] = server_id
        try:
            self.implementation = __get_implementation__(params[implementation_key], params)
        except ImplementationError:
            logging.exception(
                f"Implementation {params[implementation_key]} not found in ControllerConnection implementations")
            raise ImplementationError
        except KeyError:
            logging.exception(f"Implementation key {implementation_key} not found in params dict")
            raise KeyError(f"Implementation key {implementation_key} not found in params dict")

        self.implementation.initialize()

    def get_data(self):
        return self.implementation.get_data()

    def send_state(self, message):
        self.implementation.send_state(message)

    def cleanup(self):
        self.implementation.cleanup()

    def mark_ready(self, message):
        self.implementation.mark_ready(message)
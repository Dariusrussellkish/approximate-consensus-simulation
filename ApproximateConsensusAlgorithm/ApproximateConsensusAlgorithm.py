from threading import Lock
import logging

from .AlgorithmImplementations import *
from .AlgorithmImplementations import InvalidAlgorithmError


def __get_algorithm__(algorithm: str, params):
    """
    Get the algorithm instance

    TODO: Refactor algorithms to have an algorithm interface to ensure
     they meet requirements up front
    :param algorithm: The algorithm from params to use
    :param params: dict-like of params
    :return: Algorithm instance
    """
    if algorithm == "algorithm_1":
        return AlgorithmOne(**params)
    elif algorithm == "algorithm_2":
        return AlgorithmTwo(**params)
    elif algorithm == "algorithm_3":
        return AlgorithmThree(**params)
    elif algorithm == "algorithm_4":
        return AlgorithmFour(**params)
    elif algorithm == "BenOr":
        return AlgorithmBenOr(**params)
    elif algorithm == "JACM86":
        return AlgorithmJACM86(**params)
    else:
        raise InvalidAlgorithmError


class ApproximateConsensusAlgorithm:
    logger = logging.getLogger('AC-Algo')

    def __init__(self, params, server_id, algorithm_key="algorithm"):
        self.params = params
        self.params["server_id"] = server_id
        self.stateLock = Lock()
        try:
            self.algorithm = __get_algorithm__(params[algorithm_key], params)
        except InvalidAlgorithmError:
            logging.exception(f"Algorithm {params[algorithm_key]} not found in algorithm implementations")
            raise InvalidAlgorithmError
        except KeyError:
            logging.exception(f"Algorithm key {algorithm_key} not found in params dict")
            raise KeyError(f"Algorithm key {algorithm_key} not found in params dict")

        if not self.algorithm.has_valid_n:
            logging.fatal(f"Algorithm {params[algorithm_key]} does not have sufficient "
                          f"n={params['servers']} for f={params['f']}")
            raise ValueError(f"Algorithm {params[algorithm_key]} does not have sufficient "
                             f"n={params['servers']} for f={params['f']}")

    def get_internal_state(self):
        """
        Return algorithm internal state as Dict

        :return: Dict of internal state variables
        """
        self.stateLock.acquire()
        try:
            internal_state = {**self.algorithm.get_internal_state(),
                              'id': self.params['server_id'],
                              'is_done': bool(self.algorithm.is_done())
                              }
            return internal_state
        finally:
            self.stateLock.release()

    def process_message(self, message):
        self.stateLock.acquire()
        try:
            updated = self.algorithm.process_message(message)
            # ApproximateConsensusAlgorithm.logger.debug(f"Server {self.params['server_id']} received "
            #                                           f"message from {message['id']}, "
            #                                           f"v {message['v']} "
            #                                           f"p {message['p']} "
            #                                           f"id {message['id']} "
            #                                           f"is_done {message['is_done']}")
            return updated
        finally:
            self.stateLock.release()

    def is_done(self):
        self.stateLock.acquire()
        try:
            done = bool(self.algorithm.is_done())
            return done
        finally:
            self.stateLock.release()

    def supports_byzantine(self):
        return self.algorithm.supports_byzantine

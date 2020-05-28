import logging
from copy import deepcopy
from numpy import random, mean, floor, log
import numpy as np


def __filter_list__(to_filter, remove=None):
    if remove is None:
        remove = [None]
    return list(x for x in to_filter if x not in remove)


def __mean_trim__(l1, f):
    return mean(__trim__(l1, f))


def __trim__(l1, f):
    sorted_l1 = deepcopy(l1)
    sorted_l1.sort()
    if f != 0:
        trimmed_list = np.array(sorted_l1)[f:-f]
    else:
        trimmed_list = np.array(sorted_l1)
    return trimmed_list


def __c__(m, k):
    return floor((m-1)/float(k)) + 1


def __select__(l1, k):
    return [l1[i] for i in range(0, len(l1), k)]


def __not_none_union__(l1, l2):
    return l1 + l2


class AlgorithmJACM86:
    logger = logging.getLogger('Algo-JACM86')

    def __init__(self, K, servers, server_id, f, eps, **kwargs):
        self.K = K
        self.nServers = servers
        self.server_id = server_id
        self.v = random.uniform(0, K)
        self.has_valid_n = servers > 5 * f
        self.p = 0
        self.f = f
        self.eps = eps
        self.done_servers = [False for _ in range(servers)]
        self.done_values = [None for _ in range(servers)]
        self.converged = False
        self.supports_byzantine = servers > 5 * f
        self.p_end = log(eps / K) / log(0.5)
        self.phase = 1
        self._set()
        self.requires_synchronous_update_broadcast = True
        AlgorithmJACM86.logger.info(
            f"Server {self.server_id} will terminate after {self.p_end} phases")

    def _set(self):
        self.R = [[None for _ in range(self.nServers)] for _ in range(int(self.p_end) + 2)]
        self.R[self.p][self.server_id] = self.v

    def is_done(self):
        return bool(self.p > self.p_end)

    def process_message(self, message):
        s_id = message['id']
        if message['algorithm_is_done']:
            AlgorithmJACM86.logger.info(f"Server {self.server_id} "
                                        f"receiving done message from {s_id}")
            self.done_servers[s_id] = True
            self.done_values[s_id] = message['v']
            # add done servers value to all vectors in R
            for vector in self.R:
                vector[s_id] = message['v']

        if self.R[message['p']][s_id] is None:
            # AlgorithmJACM86.logger.info(f"Server {self.server_id} "
            #                             f"received from {s_id} in phase {message['']}")
            self.R[message['p']][s_id] = message['v']

        # AlgorithmJACM86.logger.info(f"Server {self.server_id} "
        #                             f"R[{self.p}] is {['p' if x else 'N' for x in self.R[self.p]]}")
        filtered_R = __filter_list__(self.R[self.p])
        if len(filtered_R) >= self.nServers - self.f:
            if self.p <= self.p_end:
                values = __trim__(filtered_R, self.f)
                if any([abs(self.v - v) > self.eps/2. for v in values]):
                    self.v = (max(values) + min(values)) / 2
                else:
                    self.converged = True
                self.p += 1
                self.R[self.p][self.server_id] = self.v
                AlgorithmJACM86.logger.info(
                    f"Server {self.server_id} accepting update via mean trim, phase is now {self.p}")
                return True
        return False

    def get_internal_state(self):
        return {
            "v": self.v,
            "p": self.p,
            "phase": self.phase,
            "algorithm_is_done": self.is_done(),
            "converged": self.converged
        }

import logging
from copy import deepcopy
import collections
from numpy import random, log, maximum, minimum
import numpy as np


def __filter_list__(to_filter, remove=None):
    if remove is None:
        remove = [None]
    return list(x for x in to_filter if x not in remove)


def __mean_trim__(l1, f):
    sorted_l1 = deepcopy(l1)
    sorted_l1.sort()
    if f != 0:
        trimmed_list = np.array(sorted_l1)[f:-f]
    else:
        trimmed_list = np.array(sorted_l1)
    return (max(trimmed_list) + min(trimmed_list)) / 2.0


def __not_none_union__(l1, l2):
    return l1 + l2


class AlgorithmThree:
    logger = logging.getLogger('Algo-3')

    def __init__(self, K, servers, server_id, f, eps, **kwargs):
        self.K = K
        self.nServers = servers
        self.server_id = server_id
        self.v = random.uniform(0, K)
        self.has_valid_n = servers > 5 * f
        self.p = 0
        self.f = f
        self.eps = eps
        self._reset()
        self.converged = False
        self.supports_byzantine = servers > 5 * f
        self.a = 1 - (1.0 / (2**servers))
        self.p_end = log(eps / K) / log(self.a)
        AlgorithmThree.logger.info(
            f"Server {self.server_id} will terminate after {self.p_end} phases")

    def _reset(self):
        self.R = list([None for _ in range(self.nServers)])
        self.R[self.server_id] = 1
        self.S = list([None for _ in range(self.nServers)])

    def is_done(self):
        # This is just for benchmarking
        return self.p > self.p_end / 4.

    def process_message(self, message):
        s_id = message['id']
        if message['p'] > self.p and self.S[s_id] is None:
            self.S[s_id] = message['v']
        elif message['p'] == self.p and self.R[s_id] is None:
            self.R[s_id] = message['v']

        filtered_R = __filter_list__(self.R)
        filtered_S = __filter_list__(self.S)
        should_update = False
        if len(filtered_R) + len(filtered_S) >= self.nServers - self.f:
            union = __not_none_union__(filtered_R, filtered_S)
            if any([abs(self.v - v) > self.eps/2. for v in union]):
                self.v = __mean_trim__(union, self.f)
            else:
                self.converged = True
            # AlgorithmThree.logger.info(f"Server {self.server_id} R: {self.R}")
            # AlgorithmThree.logger.info(f"Server {self.server_id} S: {self.S}")
            self.p += 1
            self._reset()
            AlgorithmThree.logger.info(f"Server {self.server_id} accepting update via mean trim, phase is now {self.p}")
            should_update = True

        if len(filtered_S) >= 2 * self.f + 1:
            if any([abs(self.v - v) > self.eps/2. for v in filtered_S]):
                self.v = __mean_trim__(filtered_S, self.f)
            else:
                self.converged = True
            # AlgorithmThree.logger.info(f"Server {self.server_id} R: {self.R}")
            # AlgorithmThree.logger.info(f"Server {self.server_id} S: {self.S}")
            self.p += 1
            self._reset()
            AlgorithmThree.logger.info(f"Server {self.server_id} accepting update S, phase is now {self.p}")
            should_update = True

        return should_update

    def get_internal_state(self):
        return {
            'v': self.v,
            'p': self.p,
            'converged': self.converged
        }

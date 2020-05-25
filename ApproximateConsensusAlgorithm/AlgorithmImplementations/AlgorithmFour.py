import logging
import random
from numpy import unique


def __flip_coin__():
    return int(random.getrandbits(1))


def __filter_list__(to_filter, remove=None):
    if remove is None:
        remove = [None]
    return list(x for x in to_filter if x not in remove)


def __check_majority__(l1, ignore=None):
    if ignore is None:
        ignore = [None]

    filtered_list = __filter_list__(l1, remove=ignore)

    for val, count in unique(filtered_list, return_counts=True):
        if count > len(l1)/2.0:
            return val
    return None


class AlgorithmFour:
    logger = logging.getLogger('Algo-4')

    def __init__(self, servers, server_id, f, eps, **kwargs):
        self.nServers = servers
        self.server_id = server_id
        self.v = __flip_coin__()
        self.w = -1
        self.p = 0
        self.f = f
        self.supports_byzantine = False
        self.eps = eps
        self._reset()
        self.isDone = False

    def _reset(self):
        self.w = None
        self.R = list([None for _ in range(self.nServers)])
        self.R[self.server_id] = 1
        self.S = list([None for _ in range(self.nServers)])

    def is_done(self):
        return self.isDone

    def process_message(self, message):
        s_id = message['id']
        if message['p'] > self.p:
            self.p = message['p']
            self.v = message['v']
            self._reset()
            AlgorithmFour.logger.info(
                f"Server {self.server_id} accepted jump update from {s_id}, phase is now {self.p}")
            return True
        elif message['p'] == self.p:
            self.R[s_id] = message['v']
            self.S[s_id] = message['w']

        filtered_R = __filter_list__(self.R)
        filtered_S = __filter_list__(self.S)
        if len(filtered_R) >= self.nServers - self.f and (self.w in [None, -1]):
            majority_value = __check_majority__(self.R)
            if majority_value is None:
                self.w = -1
            else:
                self.w = majority_value
            self.S[s_id] = self.w

        if len(filtered_S) >= self.nServers - self.f:
            values = __filter_list__(self.S, remove=[None, -1])
            if values:
                self.v = values[0]
                if len(list(x for x in self.S if x == self.v)) > self.f + 1:
                    self.isDone = True
            else:
                self.v = __flip_coin__()
            self.p += 1
            self._reset()
            AlgorithmFour.logger.info(
                f"Server {self.server_id} accepted update, phase is now {self.p}")
            return True
        return False

    def get_internal_state(self):
        return {
            'v': self.v,
            'p': self.p,
            'w': self.w
        }

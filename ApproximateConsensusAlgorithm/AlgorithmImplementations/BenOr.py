import logging
import random
from collections import defaultdict
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
    vals, counts = unique(filtered_list, return_counts=True)
    for val, count in zip(vals, counts):
        if count > len(l1)/2.0:
            return int(val)
    return None


class AlgorithmBenOr:
    logger = logging.getLogger('Algo-BenOr')

    def __init__(self, servers, server_id, f, eps, **kwargs):
        self.nServers = servers
        self.server_id = server_id
        self.v = __flip_coin__()
        self.w = None
        self.p = 0
        self.phase = 1
        self.f = f
        self.supports_byzantine = False
        self.has_valid_n = servers > 2 * f
        self.eps = eps
        self.futures = defaultdict(dict)
        self._reset()
        self.requires_synchronous_update_broadcast = True
        self.isDone = False

    def _reset(self):
        self.R = list([None for _ in range(self.nServers)])
        self.S = list([None for _ in range(self.nServers)])
        self.R[self.server_id] = self.v
        self.w = None

    def is_done(self):
        return self.isDone

    def process_message(self, message):
        s_id = message['id']
        should_update = False
        if self.futures[self.p]:
            for message in self.futures[self.p].values():
                # AlgorithmBenOr.logger.info(
                #     f"Server {self.server_id} processing future {message}")
                AlgorithmBenOr.logger.info(
                    f"Server {self.server_id} processing future p={message['p']} "
                    f"phase {message['phase']} from {message['id']}, v={message['v']}, w={message['w']}")
                if message['phase'] == 1 and self.R[message['id']] is None:
                    self.R[message['id']] = message['v']
                elif self.S[message['id']] is None:
                    self.R[message['id']] = message['v']
                    self.S[message['id']] = message['w']
            self.futures[self.p] = {}

        if message['p'] > self.p and not message['id'] in self.futures[message['p']]:
            AlgorithmBenOr.logger.info(
                f"Server {self.server_id} received future p={message['p']} phase {message['phase']} from {s_id}")
            self.futures[message['p']][message['id']] = message
        if message['p'] == self.p and self.R[s_id] is None and message['phase'] == 1:
            AlgorithmBenOr.logger.info(
                f"Server {self.server_id} received p={message['p']} phase 1 from {s_id}")
            AlgorithmBenOr.logger.info(
                f"Server {self.server_id} p={self.p} phase {self.phase}, R: {self.R}")
            AlgorithmBenOr.logger.info(
                f"Server {self.server_id} p={self.p} phase {self.phase}, S: {self.S}")
            self.R[s_id] = message['v']
        elif message['p'] == self.p and self.S[s_id] is None and message['phase'] == 2:
            AlgorithmBenOr.logger.info(
                f"Server {self.server_id} received p={message['p']} phase 2 from {s_id}")
            AlgorithmBenOr.logger.info(
                f"Server {self.server_id} p={self.p} phase {self.phase}, R: {self.R}")
            AlgorithmBenOr.logger.info(
                f"Server {self.server_id} p={self.p} phase {self.phase}, S: {self.S}")
            self.R[s_id] = message['v']
            self.S[s_id] = message['w']

        # AlgorithmBenOr.logger.info(
        #     f"Server {self.server_id} p={self.p} phase {self.phase}, R: {self.R}")
        # AlgorithmBenOr.logger.info(
        #     f"Server {self.server_id} p={self.p} phase {self.phase}, S: {self.S}")
        filtered_R = __filter_list__(self.R)
        filtered_S = __filter_list__(self.S)
        if self.phase == 1 and len(filtered_R) >= self.nServers - self.f:
            majority_value = __check_majority__(self.R)
            if majority_value is not None:
                self.w = majority_value
                self.S[self.server_id] = majority_value
            else:
                self.w = -1
                self.S[self.server_id] = -1
            should_update = True
            AlgorithmBenOr.logger.info(
                f"Server {self.server_id} moving to phase 2, phase is {self.p}")
            self.phase = 2
        if self.phase == 2 and len(filtered_S) >= self.nServers - self.f:
            values = __filter_list__(self.S, remove=[None, -1])
            AlgorithmBenOr.logger.info(
                f"Server {self.server_id} accepting phase 2, values: {values}")
            AlgorithmBenOr.logger.info(
                f"Server {self.server_id} p={self.p} phase {self.phase}, R: {self.R}")
            AlgorithmBenOr.logger.info(
                f"Server {self.server_id} p={self.p} phase {self.phase}, S: {self.S}")
            if values:
                self.v = values[0]
                if len(list(x for x in self.S if x == self.v)) > self.f:
                    self.isDone = True
            else:
                self.v = __flip_coin__()
            self.phase = 1
            self._reset()
            self.p += 1
            AlgorithmBenOr.logger.info(
                f"Server {self.server_id} accepted update, phase is now {self.p}")
            return True
        return should_update

    def get_internal_state(self):
        return {
            "phase": self.phase,
            "v": self.v,
            "p": self.p,
            "w": self.w
        }

import logging
from collections import defaultdict

from numpy import random, log, mean


class AlgorithmOne:
    logger = logging.getLogger('Algo-1')

    def __init__(self, K, servers: int, server_id: int, f, eps, **kwargs):
        self.K = K
        self.nServers = servers
        self.server_id = server_id
        self.v = random.uniform(0, K)
        self.p = 0
        self.f = f
        self.supports_byzantine = False
        self.has_valid_n = servers > 2 * f
        self.eps = eps
        self.p_end = log(eps / K) / log(0.5)
        self.R = [[None for i in range(servers)] for j in range(int(self.p_end) + 1)]
        self.R[self.p][self.server_id] = self.v
        AlgorithmOne.logger.info(
            f"Server {self.server_id} will terminate after {self.p_end} phases")

    def is_done(self):
        return self.p > self.p_end

    def process_message(self, message):
        p = message['p']
        s_id = message['id']
        if self.R[p][s_id] is None:
            self.R[p][s_id] = message['v']
            AlgorithmOne.logger.info(f"Accepting new value from {s_id} in phase {p} R[p] is {self.R[p]}")
            filtered_list = list(x for x in self.R[self.p] if x is not None)
            if len(filtered_list) >= self.nServers - self.f:
                self.v = (max(filtered_list) + min(filtered_list)) / 2.
                self.p += 1
                self.R[self.p][self.server_id] = self.v
                AlgorithmOne.logger.info(
                    f"Server {self.server_id} accepting consensus update, now in phase {self.p}")
                return True

        return False

    def get_internal_state(self):
        return {
            'v': self.v,
            'p': self.p
        }

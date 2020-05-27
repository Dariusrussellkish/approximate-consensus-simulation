import logging
from typing import Union, List
from numpy import random, log


class AlgorithmOne:
    logger = logging.getLogger('Algo-1')

    def __init__(self, K, servers: int, server_id: int, f, eps, **kwargs):
        self.K = K
        self.nServers = servers
        self.server_id: int = server_id
        self.v = random.uniform(0, K)
        self.p = 0
        self.phase = 1
        self.f = f
        self.supports_byzantine = False
        self.has_valid_n = servers > 2 * f
        self.eps = eps
        self.p_end = log(eps / K) / log(float(f) / (servers - f))
        self.requires_synchronous_update_broadcast = True
        self._reset()
        AlgorithmOne.logger.info(
            f"Server {self.server_id} will terminate after {self.p_end} phases")

    def is_done(self):
        return self.p > self.p_end

    def _reset(self):
        self.R = [0. for _ in range(self.nServers)]
        self.R[self.server_id] = self.v

    def process_message(self, message):
        s_id = message['id']
        if self.R[s_id] == 0:
            self.R[s_id] = message['v']
            if sum(self.R) >= self.nServers - self.f:
                self.v = (max(self.R) + min(self.R)) / 2.0
                self.p += 1
                self.R[self.p] = self.v
                return True
        return False

    def get_internal_state(self):
        return {
            'v': self.v,
            'p': self.p,
            'phase': self.phase
        }

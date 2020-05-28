import logging
from numpy import random, log, ceil


class AlgorithmTwo:
    logger = logging.getLogger('Algo-2')

    def __init__(self, K, servers, server_id, f, eps, **kwargs):
        self.K = K
        self.nServers = servers
        self.server_id = server_id
        self.v = random.uniform(0, K)
        self.p = 0
        self.f = f
        self.supports_byzantine = False
        self.has_valid_n = servers > 2 * f
        self.eps = eps
        self._reset()
        self.p_end = log(eps / K) / log(float(f) / (servers - f))
        AlgorithmTwo.logger.info(
            f"Server {self.server_id} will terminate after {self.p_end} phases")

    def _reset(self):
        self.R = list([0 for _ in range(self.nServers)])
        self.R[self.server_id] = 1

    def is_done(self):
        return self.p > self.p_end

    def process_message(self, message):
        s_id = message['id']
        p = message['p']
        v = message['v']
        if p > self.p:
            self.v = v
            self.p = p
            self._reset()
            return True
        elif p == self.p and self.R[s_id] == 0:
            self.R[s_id] = 1
            self.v += v
            if sum(self.R) >= self.nServers - self.f:
                self.v = self.v / float(sum(self.R))
                self.p += 1
                self._reset()
                AlgorithmTwo.logger.info(
                    f"Server {self.server_id} accepting consensus update, now in phase {self.p}")
                return True
        return False

    def get_internal_state(self):
        return {
            'v': self.v,
            'p': self.p
        }
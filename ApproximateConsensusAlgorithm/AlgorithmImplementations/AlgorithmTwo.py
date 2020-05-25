import logging
from numpy import random, log


class AlgorithmTwo:
    logger = logging.getLogger('Algo-2')

    def __init__(self, K, servers, server_id, f, eps, **kwargs):
        self.K = K
        self.nServers = servers
        self.server_id = server_id
        self.v = float(random.randint(0, K + 1))
        self.p = 0
        self.f = f
        self.supports_byzantine = False
        self.eps = eps
        self._reset()
        self.r = (3 * servers - 2 * f) / (4 * (servers - f))
        self.p_end = log(eps / K) / log(self.r)
        AlgorithmTwo.logger.info(
            f"Server {self.server_id} will terminate after {self.p_end} phases")

    def _reset(self):
        self.R = list([0 for _ in range(self.nServers)])
        self.R[self.server_id] = 1

    def is_done(self):
        return self.p > self.p_end

    def process_message(self, message):
        if message['p'] > self.p:
            self.v = message['v']
            self.p = message['p']
            self._reset()
            AlgorithmTwo.logger.info(
                f"Server {self.server_id} accepting jump update from {message['id']}, now in phase {self.p}")
            return True

        elif message['p'] == self.p and self.R[message['id']] == 0:
            self.R[message['id']] = 1
            if sum(self.R) >= self.nServers - self.f:
                self.v = float(self.v) / float(sum(self.R))
                self.p += 1
                self._reset()
                AlgorithmTwo.logger.info(
                    f"Server {self.server_id} accepting consensus update, now in phase {self.p}")
                return True
            else:
                return False
        else:
            return False

    def get_internal_state(self):
        return {
            'v': self.v,
            'p': self.p
        }
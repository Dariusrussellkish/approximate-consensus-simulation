import logging
from numpy import random, log, ceil

def __filter_list__(to_filter, remove=None):
    if remove is None:
        remove = [None]
    return list(x for x in to_filter if x not in remove)


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
        self.converged = False
        self._reset()
        AlgorithmOne.logger.info(
            f"Server {self.server_id} will terminate after {self.p_end} phases")

    def is_done(self):
        return self.p > self.p_end

    def _reset(self):
        self.R = list([None for _ in range(self.nServers)])
        self.R[self.server_id] = self.v

    def process_message(self, message):
        s_id = message['id']
        if self.R[s_id] == 0:
            self.R[s_id] = message['v']
            values = __filter_list__(self.R)
            if len(values) >= self.nServers - self.f:
                if any([abs(self.v - v) > self.eps/2. for v in values]):
                    self.v = (max(values) + min(values)) / 2.0
                else:
                    self.converged = True
                self.p += 1
                self._reset()
                AlgorithmOne.logger.info(
                    f"Server {self.server_id} accepting update, now phase {self.p}")
                return True
        return False

    def get_internal_state(self):
        return {
            'v': self.v,
            'p': self.p,
            'phase': self.phase,
            'converged': self.converged
        }

import logging
from numpy import random, log, ceil

def __filter_list__(to_filter, remove=None):
    if remove is None:
        remove = [None]
    return list(x for x in to_filter if x not in remove)


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
        self.converged = False
        # TODO: Remove, only for testing against algorithm 1
        self.requires_synchronous_update_broadcast = True
        self.phase = 1
        AlgorithmTwo.logger.info(
            f"Server {self.server_id} will terminate after {self.p_end} phases")

    def _reset(self):
        self.values = list([None for _ in range(self.nServers)])
        self.values[self.server_id] = self.v

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
        elif p == self.p and self.values[s_id] is None:
            self.values[s_id] = v
            values = __filter_list__(self.values)
            if len(values) >= self.nServers - self.f:
                if any([abs(self.v - v) > self.eps/2. for v in values]):
                    self.v = sum(values)
                    self.v = self.v / float(len(values))
                else:
                    self.converged = True
                self.p += 1
                self._reset()
                AlgorithmTwo.logger.info(
                    f"Server {self.server_id} accepting consensus update, now in phase {self.p}")
                return True
        return False

    def get_internal_state(self):
        return {
            'v': self.v,
            'p': self.p,
            'converged': self.converged
        }
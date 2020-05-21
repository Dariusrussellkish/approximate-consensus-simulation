import json
import sys

from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.topo import Topo
from mininet.util import dumpNodeConnections


class SimulationTopo(Topo):
    """
        build a Mininet topology with one switch,
        and each host is connected to the switch.
    """

    def build(self):
        with open(sys.argv[1], 'r') as fh:
            params = json.load(fh)
        switch = self.addSwitch('s%s' % 1)
        for i in range(params["servers"] + 1):
            host = self.addHost('h%s' % (i + 1))
            self.addLink(host, switch)


def start_mini():
    """
    Start Mininet with mounted directories
    :return:
    """

    with open(sys.argv[1], 'r') as fh:
        params = json.load(fh)

    setLogLevel('info')
    topo = SimulationTopo()
    net = Mininet(topo=topo)
    net.addNAT().configDefault()
    net.start()
    net.pingAll()

    hs = [net.get('h{0}'.format(i + 1)) for i in range(params["servers"] + 1)]

    dumpNodeConnections(net.hosts)

    return net, hs


def start_simulation(hs):
    with open(sys.argv[1], 'r') as fh:
        params = json.load(fh)

    print
    "Starting controller on ip: " + hs[-1].IP
    hs[-1].cmd("python3 controller.py " + sys.argv[1])

    for i in range(params["servers"]):
        print
        "Starting server " + str(i) + " on ip: " + hs[i].IP
        hs[i].cmd("python3 server.py " + sys.argv[1] + " " + str(i))


if __name__ == "__main__":
    net, hs = start_mini()
    start_simulation(hs)
    CLI(net)

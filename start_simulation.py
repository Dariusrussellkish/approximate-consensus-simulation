import json
import sys
import time
from os import system

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

    for i in range(params["n_simulations"]):
        print(f"Starting simulation {i}")
        print(f"Starting controller on ip: {hs[-1].IP}")
        hs[-1].cmd(f"python3 ~/approximate-consensus-simulation/controller.py {sys.argv[1]} &")

        for i in range(params["servers"]):
            print(f"Starting server {i} on ip: {hs[i].IP}")
            hs[i].cmd(f"python3 ~/approximate-consensus-simulation/server.py {sys.argv[1]} {i} &")

        while True:
            result = hs[-1].cmd(f"ps -fe | grep controller")
            print(result)
            if "controller" not in result:
                print(f"Simulation {i} finished")
                break
            time.sleep(0.5)


if __name__ == "__main__":
    system("mn --clean")
    net, hs = start_mini()
    start_simulation(hs)
    CLI(net)

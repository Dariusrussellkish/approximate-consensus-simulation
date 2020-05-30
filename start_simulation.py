import json
import sys
import time
import os
import uuid

from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.topo import Topo
from mininet.util import dumpNodeConnections
from mininet.node import CPULimitedHost
from mininet.link import TCLink

class SimulationTopo(Topo):
    """
        build a Mininet topology with one switch,
        and each host is connected to the switch.
    """
    def build(self):
        with open(sys.argv[1], 'r') as fh:
            params = json.load(fh)
        n = params["servers"]
        switch = self.addSwitch('s1')
        for h in range(n + 2):
            # Each host gets 50%/n of system CPU
            host = self.addHost('h%s' % (h + 1),
                                cpu=.8 / n)
            # 10 Mbps, 5ms delay, no packet loss
            self.addLink(host, switch, delay='5ms')



def start_mini(params):
    """
    Start Mininet with mounted directories
    :return:
    """

    setLogLevel('info')
    topo = SimulationTopo()
    net = Mininet(topo=topo, host=CPULimitedHost, link=TCLink,)
    net.addNAT().configDefault()
    net.start()
    # net.pingAll()

    hs = [net.get('h{0}'.format(i + 1)) for i in range(params["servers"] + 2)]
    dumpNodeConnections(net.hosts)

    return net, hs


def start_simulation(params):

    for k in range(params["n_simulations"]):
        unique = uuid.uuid4().hex
        os.system("mn --clean")
        net, hs = start_mini(params)
        time.sleep(2)
        print(f"Starting simulation {k}")
        print(f"Starting logging server on ip: {hs[0].IP}")
        hs[0].cmd(f"python3 ~/approximate-consensus-simulation/logging_server.py "
                  f"{sys.argv[1]} > logs/logging_server.out 2>&1 &")

        print(f"Starting controller on ip: {hs[1].IP}")
        hs[1].cmd(f"python3 ~/approximate-consensus-simulation/controller.py {sys.argv[1]} "
                  f"{unique} {k} > logs/controller.out 2>&1 &")

        for i in range(params["servers"]):
            time.sleep(0.05)
            print(f"Starting server {i} on ip: {hs[i+2].IP}")
            hs[i+2].cmd(f"python3 ~/approximate-consensus-simulation/server.py "
                        f"{sys.argv[1]} {i} > logs/server_{i}.out 2>&1 &")

        while True:
            result = hs[0].cmd(f"ps -fe | grep controller")
            if "python3 /root/approximate-consensus-simulation/controller.py" not in result:
                print(f"Simulation {k} finished")
                break
            time.sleep(0.5)
    print("Done, now cleaning system")
    os.system("mn --clean")


if __name__ == "__main__":
    with open(sys.argv[1], 'r') as fh:
        params = json.load(fh)
    try:
        os.system("mn --clean")
        os.system("rm logs/logging_server.log")
        if not os.path.exists('logs'):
            os.makedirs('logs')
        start_simulation(params)
    finally:
        pass
        # os.system(f"gsutil -m cp -r data {params['bucket']}")

__author__ = 'kkg'
from status import NodeStatus, Config, TransitionState
import argparse
import sys


if __name__ == '__main__':
    args = argparse.ArgumentParser()
    config = Config()

    args.add_argument("-c", "--consulconnect", type=str, default="http://192.168.160.106:8500")
    args.add_argument("-D", "--datapath", type=str, default="/var/lib/postgresql/data")

    args.add_argument("-i", "--boundiface", type=str, default="localhost")
    args.add_argument("-p", "--boundport", type=int, default=5432)

    args.add_argument("-w", "--password", type=str, default="postgres")
    args.add_argument("-u", "--user", type=str, default="postgres")

    args.add_argument("-R", "--replicationuser", type=str, default="replicator")
    args.add_argument("-P", "--replicationpassword", type=str, default="postgres")

    args.add_argument("-n", "--clustername", type=str, default="cluster")

    params = args.parse_args(sys.argv[1:])

    config.bound_iface = params.boundiface
    config.bound_port = params.boundport

    config.password = params.password
    config.user = params.user

    config.repl_user = params.replicationuser
    config.repl_password = params.replicationpassword
    config.cluster_name = params.clustername

    config.data_path = params.datapath

    if not params.consulconnect.startswith("http://") or params.consulconnect.startswith("https://"):
        params.consulconnect = "http://" + params.consulconnect

    status = NodeStatus(config, consul_connect=params.consulconnect)
    node_type = TransitionState(config)

    while True:
        try:
            node_type = status.create_node_type(node_type)
            node_type.commit()
        except Exception as e:
            print(e)
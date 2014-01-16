from execo import *
from execo_g5k import *
import itertools
jobs = oarsub([
  ( OarSubmission(resources = "/cluster=2/nodes=4"), "nancy")
])
if jobs[0][0]:
    try:
        nodes = []
        wait_oar_job_start(jobs[0][0], jobs[0][1])
        nodes = get_oar_job_nodes(jobs[0][0], jobs[0][1])
        # group nodes by cluster
        sources, targets = [ list(n) for c, n in itertools.groupby(
          sorted(nodes,
                 lambda n1, n2: cmp(
                   get_host_cluster(n1),
                   get_host_cluster(n2))),
          get_host_cluster) ]
        servers = Remote("iperf -s",
                         targets,
                         connection_params = default_oarsh_oarcp_params)
        for p in servers.processes:
            p.ignore_exit_code = p.nolog_exit_code = True
        clients = Remote("iperf -c {{[t.address for t in targets]}}",
                         sources,
                         connection_params = default_oarsh_oarcp_params)
        servers.start()
        sleep(1)
        clients.run()
        servers.kill().wait()
        print Report([ servers, clients ]).to_string()
        for index, p in enumerate(clients.processes):
            print "client %s -> server %s - stdout:" % (p.host.address,
                                                        targets[index].address)
            print p.stdout
    finally:
        oardel([(jobs[0][0], jobs[0][1])])

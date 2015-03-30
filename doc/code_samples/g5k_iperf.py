from execo import *
from execo_g5k import *
import itertools
[(jobid, site)] = oarsub([
  ( OarSubmission(resources = "/cluster=2/nodes=4"), "nancy")
])
if jobid:
    try:
        nodes = []
        wait_oar_job_start(jobid, site)
        nodes = get_oar_job_nodes(jobid, site)
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
        clients = Remote("iperf -c {{[t.address for t in targets]}}",
                         sources,
                         connection_params = default_oarsh_oarcp_params)
        with servers.start():
            sleep(1)
            clients.run()
        servers.wait()
        print Report([ servers, clients ]).to_string()
        for index, p in enumerate(clients.processes):
            print "client %s -> server %s - stdout:" % (p.host.address,
                                                        targets[index].address)
            print p.stdout
    finally:
        oardel([(jobid, site)])

from execo import *
from execo_g5k import *
from execo_engine import *
import yaml, re, itertools

class g5k_tcp_congestion(Engine):

    def run(self):
        result_file = self.result_dir + "/results"
        params = {
            "num_flows": igeom(1, 20, 5),
            "tcp_congestion_control": ["cubic", "reno"],
            "repeat": range(0, 3),
            }
        combs = sweep(params)
        sweeper = ParamSweeper(self.result_dir + "/sweeper", combs)
        logger.info("experiment plan: " + str(combs))
        logger.info("compute resources to reserve")
        planning = get_planning()
        slots = compute_slots(planning, "01:00:00")
        wanted = {'grid5000': 0}
        start_date, end_date, resources = find_first_slot(slots, wanted)
        actual_resources = dict(
            { list(clusters)[0]:1 for site,clusters in
              itertools.groupby(sorted([ cluster
                                         for cluster, n_nodes in resources.iteritems()
                                         if cluster in get_g5k_clusters() and n_nodes > 0
                                         and 1e9 ==
                                         [adapter
                                          for adapter in get_host_attributes(cluster + "-1")["network_adapters"]
                                          if adapter.get("network_address") ==
                                           cluster + "-1." + get_cluster_site(cluster) + ".grid5000.fr"][0]["rate"]],
                                       lambda c1, c2: cmp(get_cluster_site(c1),
                                                          get_cluster_site(c2))),
                                get_cluster_site) }.items()[0:2])
        if len(actual_resources) >= 2:
            logger.info("try to reserve " + str(actual_resources))
            job_specs = get_jobs_specs(actual_resources)
            for job_spec in job_specs: job_spec[0].job_type = "deploy"
            logger.info("submit job: " + str(job_specs))
            jobid, sshkey = oargridsub(job_specs, start_date,
                                       walltime = end_date - start_date)
            if jobid:
                try:
                    logger.info("wait job start")
                    wait_oargrid_job_start(jobid)
                    logger.info("get job nodes")
                    nodes = get_oargrid_job_nodes(jobid)
                    if len(nodes) != 2: raise Exception("not enough nodes")
                    logger.info("deploy %i nodes" % (len(nodes),))
                    deployed, undeployed = deploy(Deployment(nodes, env_name = "wheezy-x64-min"))
                    logger.info("%i deployed, %i undeployed" % (len(deployed), len(undeployed)))
                    if len(deployed) != 2: raise Exception("not enough deployed nodes")
                    logger.info("prepare nodes")
                    Remote("apt-get -y install iperf", nodes, connection_params = {"user": "root"}).run()
                    logger.info("start experiment campaign")
                    while len(sweeper.get_remaining()) > 0:
                        comb = sweeper.get_next()
                        destination = SshProcess("iperf -s", nodes[0],
                                                 connection_params = {"user": "root"})
                        sources = SshProcess("iperf -c %s -P %i -Z %s" % (nodes[0].address,
                                                                          comb["num_flows"],
                                                                          comb["tcp_congestion_control"]),
                                             nodes[1], connection_params = {"user": "root"})
                        with destination.start():
                            sleep(2)
                            sources.run()
                        if comb["num_flows"] > 1:
                            pattern = "^\[SUM\].*\s(\d+(\.\d+)?) (\w?)bits/sec"
                        else:
                            pattern = "^\[\s*\d+\].*\s(\d+(\.\d+)?) (\w?)bits/sec"
                        bw_mo = re.search(pattern, sources.stdout, re.MULTILINE)
                        if bw_mo:
                            bw = float(bw_mo.group(1)) * {"": 1, "K": 1e3, "M": 1e6, "G": 1e9}[bw_mo.group(3)]
                            results = { "params": comb, "bw": bw }
                            with open(result_file, "a") as f:
                                yaml.dump([results], f, width = 72)
                            logger.info("comb : %s bw = %f bits/s" % (comb, bw))
                            sweeper.done(comb)
                        else:
                            logger.info("comb failed: %s" % (comb,))
                            logger.info("sources stdout:\n" + sources.stdout)
                            logger.info("sources stderr:\n" + sources.stderr)
                            logger.info("destination stdout:\n" + destination.stdout)
                            logger.info("destination stderr:\n" + destination.stderr)
                            sweeper.skip(comb)
                finally:
                    logger.info("deleting job")
                    oargriddel([jobid])
        else:
            logger.info("not enough resources available: " + str(actual_resources))

if __name__ == "__main__":
    engine = g5k_tcp_congestion()
    engine.start()

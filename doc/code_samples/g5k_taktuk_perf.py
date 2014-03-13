from execo import *
from execo_g5k import *
from execo_g5k.oar import oarsubgrid
import itertools, random, tempfile, shutil

logger.info("compute resources to reserve")
slots = compute_slots(get_planning(), 60*15)
wanted = { "grid5000": 0 }
start_date, end_date, resources = find_first_slot(slots, wanted)
actual_resources = distribute_hosts(resources, wanted, ratio = 0.8)
job_specs = get_jobs_specs(actual_resources)
logger.info("try to reserve " + str(actual_resources))
jobs = oarsubgrid(job_specs, start_date,
                  walltime = end_date - start_date,
                  job_type = "allow_classic_ssh")
if len(jobs) > 0:
    try:
        logger.info("wait jobs start")
        for job in jobs: wait_oar_job_start(*job)
        logger.info("get jobs nodes")
        nodes = []
        for job in jobs: nodes += get_oar_job_nodes(*job)
        logger.info("got %i nodes" % (len(nodes),))
        cores = []
        nodes = sorted(nodes, key=get_host_cluster)
        for cluster, cluster_nodes in itertools.groupby(nodes, key=get_host_cluster):
            num_cores = get_host_attributes(cluster + "-1")["architecture"]["smt_size"]
            for node in cluster_nodes:
                cores += [ node ] * num_cores
        logger.info("for a total of %i cores" % (len(cores),))
        pingtargets = [ core.address for core in cores ]
        random.shuffle(pingtargets)
        ping1 = TaktukRemote("ping -c 1 {{pingtargets}}", cores)
        ping2 = Remote("ping -c 1 {{pingtargets}}", cores)
        logger.info("run taktukremote")
        ping1.run()
        logger.info("run remote (parallel ssh)")
        ping2.run()
        logger.info("summary:\n" + Report([ping1, ping2]).to_string())
    finally:
        logger.info("deleting jobs")
        oardel(jobs)

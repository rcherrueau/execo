from execo import *
from execo_g5k import *

logger.info("compute resources to reserve")
slots = compute_slots(get_planning(), 60*15)
wanted = { "grid5000": 0 }
start_date, end_date, resources = find_first_slot(slots, wanted)
actual_resources = distribute_hosts(resources, wanted)
job_specs = get_jobs_specs(actual_resources)
logger.info("try to reserve " + str(actual_resources))
jobid, sshkey = oargridsub(job_specs, start_date,
                           walltime = end_date - start_date)
if jobid:
    try:
        logger.info("generate random data")
        Process("dd if=/dev/urandom of=randomdata bs=1M count=50").run()
        logger.info("wait job start")
        wait_oargrid_job_start(jobid)
        logger.info("get job nodes")
        nodes = get_oargrid_job_nodes(jobid)
        logger.info("got %i nodes" % (len(nodes),))
        broadcast1 = ChainPut(nodes, ["randomdata"], "/tmp/",
                              connection_params = default_oarsh_oarcp_params)
        broadcast2 = Put(nodes, ["randomdata"], "/tmp/",
                         connection_params = default_oarsh_oarcp_params)
        logger.info("run chainput")
        broadcast1.run()
        logger.info("run parallel scp")
        broadcast2.run()
        logger.info("summary:\n" + Report([broadcast1, broadcast2]).to_string())
    finally:
        logger.info("deleting job")
        oargriddel([jobid])
else:
    logger.info("job submission failed")

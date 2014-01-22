from execo import *
from execo_g5k import *
import itertools, random, tempfile, shutil

logger.info("compute resources to reserve")
blacklisted = [ "graphite" ]
slots = compute_slots(get_planning(), 60*15, excluded_elements = blacklisted)
wanted = { "grid5000": 0 }
start_date, end_date, resources = find_first_slot(slots, wanted)
actual_resources = distribute_hosts(resources, wanted, excluded_elements = blacklisted)
job_specs = get_jobs_specs(actual_resources, excluded_elements = blacklisted)
logger.info("try to reserve " + str(actual_resources))
jobid, sshkey = oargridsub(job_specs, start_date,
                           walltime = end_date - start_date)
if jobid:
    try:
        logger.info("wait job start")
        wait_oargrid_job_start(jobid)
        logger.info("get job nodes")
        nodes = get_oargrid_job_nodes(jobid)
        logger.info("got %i nodes" % (len(nodes),))
        cores = []
        nodes = sorted(nodes, key=get_host_cluster)
        for cluster, cluster_nodes in itertools.groupby(nodes, key=get_host_cluster):
            num_cores = get_host_attributes(cluster + "-1")["architecture"]["smt_size"]
            for node in cluster_nodes:
                cores += [ node ] * num_cores
        logger.info("for a total of %i cores" % (len(cores),))
        tmpsshkey = tempfile.mktemp()
        shutil.copy2(sshkey, tmpsshkey)
        conn_parms = default_oarsh_oarcp_params.copy()
        # conn_parms = default_connection_params.copy()
        # conn_parms['user'] = 'oar'
        # conn_parms['port'] = 6667
        conn_parms['taktuk_options'] = ( "-s", "-S", "%s:%s" % (tmpsshkey, tmpsshkey))
        conn_parms['keyfile'] = tmpsshkey
        pingtargets = [ core.address for core in cores ]
        random.shuffle(pingtargets)
        ping1 = TaktukRemote("ping -c 1 {{pingtargets}}", cores,
                             connection_params = conn_parms)
        ping2 = Remote("ping -c 1 {{pingtargets}}", cores,
                       connection_params = conn_parms)
        logger.info("run taktukremote")
        ping1.run()
        logger.info("run remote (parallel ssh)")
        ping2.run()
        logger.info("summary:\n" + Report([ping1, ping2]).to_string())
    finally:
        logger.info("deleting job")
        oargriddel([jobid])

from execo import *
from execo_g5k import *

logger.info("compute resources to reserve")
blacklisted = [ "graphite", "reims", "helios-6.sophia.grid5000.fr",
   "helios-42.sophia.grid5000.fr", "helios-44.sophia.grid5000.fr",
   "sol-21.sophia.grid5000.fr", "suno-3.sophia.grid5000.fr" ]
planning = get_planning()
slots = compute_slots(planning, 60*10, excluded_elements = blacklisted)
wanted = {'grid5000': 0}
start_date, end_date, resources = find_first_slot(slots, wanted)
actual_resources = { cluster: 1
                     for cluster, n_nodes in resources.iteritems()
                     if cluster in get_g5k_clusters() and n_nodes > 0 }
logger.info("try to reserve " + str(actual_resources))
job_specs = get_jobs_specs(actual_resources, blacklisted)
jobid, sshkey = oargridsub(job_specs, start_date,
                           walltime = end_date - start_date)
if jobid:
    try:
        logger.info("wait job start")
        wait_oargrid_job_start(jobid)
        logger.info("get job nodes")
        nodes = get_oargrid_job_nodes(jobid)
        logger.info("got %i nodes" % (len(nodes),))
        logger.info("run cpu performance settings check")
        conn_parms = default_oarsh_oarcp_params.copy()
        conn_parms['keyfile'] = sshkey
        check = Remote("""\
find /sys/devices/system/cpu/ -name scaling_governor -exec cat {} \;

find /sys/devices/system/cpu -name thread_siblings_list -exec cat {} \;\\
 | grep , >/dev/null \\
 && echo "hyperthreading on" || echo "hyperthreading off"

find /sys/devices/system/cpu -path */cpuidle/state2/time -exec cat {} \;\\
 | grep -v 0 >/dev/null \\
 && echo "cstates on" || echo "cstates off"

if [ -e /sys/devices/system/cpu/cpufreq/boost ] ; then
  grep 1 /sys/devices/system/cpu/cpufreq/boost >/dev/null\\
   && echo "turboboost on" || echo "turboboost off"
else
  find /sys/devices/system/cpu -name scaling_available_frequencies\\
   -exec awk \'{print $1 -$2}\' {} \; | grep 1000 >/dev/null\\
   && echo "turboboost on" || echo "turboboost off"
fi
""",
                       nodes,
                       connection_params = conn_parms)
        for p in check.processes:
            p.stdout_handlers.append("%s.out" % (p.host.address,))
        check.run()
    finally:
        logger.info("deleting job")
        oargriddel([jobid])

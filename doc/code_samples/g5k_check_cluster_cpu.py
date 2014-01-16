from execo import *
from execo_g5k import *

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
job_specs = get_jobs_specs(actual_resources, blacklisted)
jobid, sshkey = oargridsub(job_specs, start_date,
                           walltime = end_date - start_date)
if jobid:
    try:
        wait_oargrid_job_start(jobid)
        nodes = get_oargrid_job_nodes(jobid)
        check = TaktukRemote('cat $(find /sys/devices/system/cpu/ '
                             '-name scaling_governor) ; '
                             'find /sys/devices/system/cpu '
                             '-name thread_siblings_list -exec cat {} \; '
                             '| grep , >/dev/null '
                             '&& echo "hyperthreading on" '
                             '|| echo "hyperthreading off"',
                             nodes,
                             connection_params = default_oarsh_oarcp_params)
        for p in check.processes:
            p.stdout_handlers.append("%s.out" % (p.host.address,))
        check.run()
    finally:
        oargriddel([jobid])

from execo import *
from execo_g5k import *

logger.info("get currently running oar jobs")
jobs = get_current_oar_jobs(get_g5k_sites())
running_jobs = [ job for job in jobs if get_oar_job_info(*job).get("state") == "Running" ]
logger.info("currently running oar jobs " + str(running_jobs))
logger.info("get job nodes")
nodes = [ job_nodes for job in running_jobs for job_nodes in get_oar_job_nodes(*job) ]
logger.info("deploying %i nodes" % (len(nodes),))
deployed, undeployed = deploy(Deployment(nodes, env_name = "wheezy-x64-min"))
logger.info("%i deployed, %i undeployed" % (len(deployed), len(undeployed)))
if len(deployed) >= 2:
    sources = list(deployed)[0:1]
    dests = list(deployed)[1:2]
    conn_params = {'user': 'root'}
    conf_nodes = Remote(
        "apt-get update ; apt-get -y install netcat-traditional tcpdump tcptrace",
        sources + dests, conn_params)
    send = Remote(
        "dd if=/dev/zero bs=5000000 count=125 | nc -q 0 {{dests}} 6543",
        sources, conn_params)
    receive = Remote(
        "nc -l -p 6543 > /dev/null",
        dests, conn_params)
    capture_if = [ [ adapter
                     for adapter in get_host_attributes(s)["network_adapters"]
                     if adapter.get("network_address") == s ][0]["device"]
                   for s in sources ]
    capture = Remote(
        "tcpdump -i {{capture_if}} -w /tmp/tmp.pcap host {{dests}} and tcp port 6543",
        sources, conn_params)
    tcptrace = Remote("tcptrace -Grlo1 /tmp/tmp.pcap", sources, conn_params)
    for p in tcptrace.processes: p.stdout_handlers.append("%s.tcptrace.out" % (p.host.address,))

    logger.info("configure nodes")
    conf_nodes.run()
    logger.info("start tcp receivers")
    receive.start()
    logger.info("start network captures")
    capture.start()
    logger.info("run tcp senders")
    send.run()
    receive.wait()
    logger.info("stop network capture")
    capture.kill().wait()
    logger.info("run tcp traffic analysis")
    tcptrace.run()

    logger.info("stdout of senders:\n" + "\n".join([ p.host.address + ":\n" + p.stdout for p in send.processes ]))
    logger.info("summary:\n" + Report([conf_nodes, receive, send, capture, tcptrace]).to_string())
else:
    logger.info("not enough deployed nodes")

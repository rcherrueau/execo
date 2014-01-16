from execo import *
from execo_g5k import *

jobs = get_current_oar_jobs(get_g5k_sites())
running_jobs = [job for job in jobs if get_oar_job_info(*job).get("state") == "Running" ]
nodes = [ job_nodes for job in running_jobs for job_nodes in get_oar_job_nodes(*job) ]
deployed, undeployed = deploy(Deployment(nodes, env_name = "wheezy-x64-min"))
if len(deployed) >= 2:
    sources = list(deployed)[0:1]
    dests = list(deployed)[1:2]
    conn_params = {'user': 'root'}
    install_tcptrace = Remote(
        "apt-get update ; apt-get -y install netcat-traditional tcpdump tcptrace",
        sources + dests, conn_params)
    send = Remote(
        "dd if=/dev/zero bs=5000000 count=125 | nc -q 0 {{[d.address for d in dests]}} 6543",
        sources, conn_params)
    receive = Remote(
        "nc -l -p 6543 > /dev/null",
        dests, conn_params)
    capture_if = [ [ adapter
                     for adapter in get_host_attributes(s.address)["network_adapters"]
                     if adapter.get("network_address") == s.address ][0]["device"]
                   for s in sources ]
    capture = Remote(
        "tcpdump -i {{capture_if}} -w /tmp/tmp.pcap host {{[d.address for d in dests]}} and tcp port 6543",
        sources, conn_params)
    for p in capture.processes: p.ignore_exit_code = p.nolog_exit_code = True
    tcptrace = execo.Remote("tcptrace -Grlo1 /tmp/tmp.pcap", sources, conn_params)
    for p in tcptrace.processes: p.stdout_handlers.append("tcptrace.out")

    install_tcptrace.run()
    receive.start()
    capture.start()
    send.run()
    receive.wait()
    capture.kill().wait()
    tcptrace.run()

    print "stdout of senders:"
    for p in send.processes: print p.stdout
    print "summary report:"
    print Report([install_tcptrace, receive, send, capture, tcptrace]).to_string()

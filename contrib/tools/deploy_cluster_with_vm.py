#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os,sys,time, libvirt
from optparse import OptionParser, OptionGroup
import execo, execo_g5k

# Functions
## utiliser logger
#execo.logger.info('coucou')

def dual_output(text,fid):
	print text
	patterns = ["\033[1m", "\033[0m", "\033[1;31m", "\033[1;m","\033[1;34m"]
	for pattern in patterns:
		text=text.replace(pattern,'')
	fid.write(text.strip())
	fid.flush()
# Parsing command line options
parser = OptionParser(description="""Allow you to deploy some nodes on a Grid5000 site, with a given number
	 of virtual machine (Xen). \n
	Based on execo : http://execo.gforge.inria.fr/doc/readme.html""")
parser.add_option("-o", 
				"--outfile", 
				dest="outfile", 
				default='vm_deployment_'+time.strftime('%Y%m%d_%H%M%S',time.localtime())+'.log', 
				help="write report to file", 
				metavar="FILE")
parser.add_option("-q", 
				"--quiet",
                action="store_true", 
                dest="quiet", 
                default=False,
                help="don't print messages to stdout")
optdeploy = OptionGroup(parser, "Deployment Options", "Customize your deployment.")
optdeploy.add_option("-s",
				"--site", 
				dest="site", 
				default="lille", 
				help="select a Grid5000 site")
optdeploy.add_option("-c",
				"--cluster", 
				dest="cluster", 
				default=None, 
				help="select a specific Grid5000 cluster")
optdeploy.add_option("-w",
				"--walltime", 
				dest="walltime", 
				default="00:30:00", 
				help="OAR time duration")
optdeploy.add_option("-n",
				"--nodes", 
				dest="n_nodes", 
				type="int", 
				default=2,	
				help="number of nodes")
optdeploy.add_option("-e",
				"--environment", 
				dest="environment", 
				default="squeeze-xen", 
				help="path to environnement file to deploy")
parser.add_option_group(optdeploy)
optvm = OptionGroup(parser, "Virtual machine options", "Define your VM parameters.")
optvm.add_option("-v",
				"--vm", 
				dest="n_vm", 
				type="int", 
				default=2, 
				help="number of vm on each node")
optvm.add_option("-d",
				"--disk_size", 
				dest="hdd_size", 
				type="int", 
				default=4, 
				help="size of VM hard drive (Gb)")
optvm.add_option("-m",
				"--memory_size", 
				dest="memory_size", 
				type="int", 
				default=256, 
				help="size of VM memory (Mb)")
parser.add_option_group(optvm)

(options, args) = parser.parse_args()
project = sys.argv[0]

if options.quiet:
	sys.stdout = open(os.devnull, 'w') 
f = open(options.outfile, 'w')

# Writing options to user
log='Starting \033[1m'+project+'\033[0m at \033[1m'+time.strftime('%Y/%m/%d %H:%M:%S')+'\033[0m\n\n'
if len(sys.argv)==1:
	log+='\033[1;31m -- WARNING : No arguments given, using default options. \n    Use -h for help --\033[1;m \n\n'

for option in parser.option_list:
	if option.dest != None:
		log+= '      - '+str(option.help)+': \033[1;34m'+str(options.__dict__[option.dest])+'\033[1;m\n'
for group in parser.option_groups:
	log+= '\n\033[1m'+group.title.upper()+'\033[0m\n'
	for option in group.option_list:
		if option.default != ("NO", "DEFAULT"):
			log+= '      - '+str(option.help)+': \033[1;34m'+str(options.__dict__[option.dest])+'\033[1;m\n'
dual_output(log,f)
		
# Launching reservation
log='\n * Reserving \033[1m'+str(options.n_nodes)+'\033[0m nodes on '
if not (options.cluster):
	log+= 'site \033[1m'+str(options.site)+'\033[0m'
	cluster=''
else:
	log+= 'cluster \033[1m'+str(options.cluster)+'\033[0m ('+str(options.site)+')'
	cluster="{cluster=\\'"+options.cluster+"\\'}"
log+=', for \033[1m'+options.walltime+'\033[0m ...\n'
sub=execo_g5k.OarSubmission(
	resources = "slash_22=2+"+cluster+"/nodes="+str(options.n_nodes), 
	job_type = "deploy", 
	walltime = options.walltime, 
	project = project)
dual_output(log,f)

job = execo_g5k.oarsub([(sub, options.site)],abort_on_error=True)
def start_prediction_changed(t):
	log='Starting date: '+execo.format_date(t)
	dual_output(log,f)
	
execo_g5k.wait_oar_job_start(job[0][0], job[0][1], prediction_callback=start_prediction_changed)
nodes=execo_g5k.get_oar_job_nodes(job[0][0], job[0][1])
log= 'Job '+str(job[0][0])+' running on '+str(job[0][1])+'\n'+"".join([ ` node `+' ' for node in nodes])
dual_output(log,f)

exit

# Getting newtork parameters for later Virtual Machine configuration
log='\n\n * Retrieving network parameters ...\n'
dual_output(log,f)
[ip_list,network_param]=execo_g5k.get_oar_job_subnets(job[0][0], job[0][1])
log = 'ip_list: \033[1m'+ip_list[0]+'\033[0m-\033[1m'+ip_list[len(ip_list)-1]+'\033[0m\n'
netmask=network_param['netmask']
broadcast=network_param['broadcast']
gateway=network_param['gateway']
log += 'netmask: \033[1m'+netmask+'\033[0m gateway: \033[1m'+gateway+'\033[0m broadcast: \033[1m'+broadcast+'\033[0m\n'
dual_output(log,f)


# Deploying operating systems on nodes
if options.environment!='squeeze-xen':
	xen_deployment = execo_g5k.Deployment(hosts=nodes, env_file=options.environment)
else:
	xen_deployment = execo_g5k.Deployment(hosts=nodes, env_name='squeeze-x64-xen')
log = '\n * Deploying \033[1m'+options.environment+'\033[0m on nodes ...'
dual_output(log,f)
Hosts=execo_g5k.deploy(xen_deployment)
DeployedHosts=Hosts[0]

log='\n * Creating VM configuration and disks ...\n'
create_vm=[]
i_host=0
for host in DeployedHosts:
	for i_vm in range(options.n_vm):
		cmd_create_vm="export http_proxy='http://proxy:3128'; "+\
			"BRIDGE=`brctl show | grep eth | cut -f1` ;"+ \
			"xen-create-image --hostname vm-"+str(i_vm+i_host*options.n_vm)+" --dir=/tmp/xen "+ \
			"--size "+str(options.hdd_size)+"Gb  --memory "+str(options.memory_size)+"Mb --swap "+str(options.memory_size)+"Mb --image full  "+ \
			"--ip "+str(ip_list[i_vm+i_host*options.n_vm])+" --netmask "+netmask+" --gateway "+gateway+" --broadcast "+broadcast+" --bridge $BRIDGE;"
		create_vm.append(execo.Remote(cmd_create_vm,[host],connexion_params={'user': 'root'}))	
		log+='      - vm-'+str(i_vm)+' on '+host.address+'\n'
	i_host+=1
dual_output(log,f)
execo.ParallelActions(create_vm).run()
log= '\n * All VM have been created, starting VM ...'

start_vm=[]
i_host=0
for host in DeployedHosts:
	for i_vm in range(options.n_vm):
		cmd_start='xm create /etc/xen/vm-'+str(i_vm+i_host*options.n_vm)+'.cfg'; 
		start_vm.append(execo.Remote(cmd_start,[host],connexion_params={'user': 'root'}))
	i_host+=1
	
dual_output(log,f)
execo.ParallelActions(start_vm).run()

# checking that everything went well
list_vm=[]
for host in DeployedHosts:
	cmd_list='xm list'
	list_vm.append(execo.Remote(cmd_list,[host],connexion_params={'user': 'root'}))

actionlist=execo.ParallelActions(list_vm).run() 
report=execo.Report()
report.add([actionlist])
print report.output()	
#f.write(report.to_string(wide=True))


f.close()


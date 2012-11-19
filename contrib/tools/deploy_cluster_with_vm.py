#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# Copyright 2009-2012 INRIA Rhone-Alpes
#
# This file is part of Execo, released under the GNU Lesser Public
# License, version 3 or later.
#SR    
# Example command lines
#
# - for deploying 10 nodes on any cluster in Lyon, with 4 vm on each nodes
#	deploy_cluster_with_vm.py -s lyon -n 10 -v 4 
#   
#
#


import os,sys,time,logging
from optparse import OptionParser, OptionGroup
import lxml.etree as ET
import execo, execo_g5k


g5k_api = False
try:
	import execo_g5k.api_utils
	g5k_api = True
except:
	pass



logger = logging.getLogger("execo")

usage = "usage: %prog"
description = """Deploy a Xen environment, with kadeploy3, to Grid5000 nodes and
create configured virtual machine on the nodes (vcpu, hdd, mem). Generate an
XML file of the deployment topology. Complex topology can be achived thanks to an XML input file. Require
execo 2.0 : http://execo.gforge.inria.fr/doc/readme.html                
"""
epilog = """Examples : deploying 10 nodes on any cluster in Lyon, with 4 vm on each nodes. 
deploy_cluster_with_vm.py -s lyon -n 10 -v 4                          
deploying 2 nodes on stremi with 3 VM having 2 Cpu, 20 Gb disk and 1024 Mb RAM
deploy_cluster_with_vm.py -s reims -c stremi -n 2 -v 3 -p 2 -d 20 -m 1024           
"""

# Parsing command line options
parser = OptionParser(usage = usage, description = description, epilog = epilog)

optinout= OptionGroup(parser, "I/O options", "Controls input and output.")
optinout.add_option("-y", 
				"--yes",
                action="store_true", 
                dest="yes", 
                default=False,
                help="Run without prompting user for confirmation (%default)")
optinout.add_option("-q", 
				"--quiet",
                action="store_true", 
                dest="quiet", 
                default=False,
                help="don't print messages to stdout  (%default)")
optinout.add_option("-i", 
				"--infile", 
				dest="infile", 
				default=None, 
				help="use XML input file for the deployment topology (%default)", 
				metavar="INFILE")
optinout.add_option("-o", 
				"--outdir", 
				dest="outdir", 
				default='deployment', 
				help="directory to store the deployment files (%default)", 
				metavar="OUTDIR")

parser.add_option_group(optinout)
optdeploy = OptionGroup(parser, "Deployment options", "Customize your grid5000 deployment and choose environment.")
optdeploy.add_option("-s",
				"--site", 
				dest="site", 
				default="lille", 
				help="select a Grid5000 site (%default)")
optdeploy.add_option("-c",
				"--cluster", 
				dest="cluster", 
				default=None, 
				help="select a specific Grid5000 cluster (%default)")
optdeploy.add_option("-w",
				"--walltime", 
				dest="walltime", 
				default="00:30:00", 
				help="OAR time duration (%default)")
optdeploy.add_option("-n",
				"--nodes", 
				dest="n_nodes", 
				type="int", 
				default=2,	
				help="number of nodes (%default)")
optdeploy.add_option("-e",
				"--environment", 
				dest="environment", 
				default="squeeze-x64-xen", 
				help="path to environnement file to deploy (%default)")
parser.add_option_group(optdeploy)
optvm = OptionGroup(parser, "Virtual machine options", "Define your VM parameters.")
optvm.add_option("-v",
				"--vm", 
				dest="n_vm", 
				type="int", 
				default=2, 
				help="number of vm on each node (%default)")
optvm.add_option("-p",
				"--cpu", 
				dest="n_cpu", 
				type="int", 
				default=1, 
				help="number of cpu for the VM (%default)")
optvm.add_option("-d",
				"--disk_size", 
				dest="hdd_size", 
				type="int", 
				default=4, 
				help="size of VM hard drive in Gb (%default)")
optvm.add_option("-m",
				"--memory_size", 
				dest="memory_size", 
				type="int", 
				default=256, 
				help="size of VM memory in Mb (%default)")
parser.add_option_group(optvm)

(options, args) = parser.parse_args()
project = sys.argv[0].replace('./','').replace('.py','')

# Setting 
if not options.quiet:
	execo.logger.setLevel(logging.INFO)
else:
	options.yes=True

# Writing options to user
log='Starting \033[1m'+project+'\033[0m  '
if len(sys.argv)==1:
	log+='\n \033[1;31m -- WARNING : No arguments given, using default options. \n     Use -h for help --\033[1;m \n'

if options.infile is None:
	if options.cluster is not None:
		options.site = execo_g5k.api_utils.get_cluster_site(options.cluster)
	for group in parser.option_groups:
		log+= '\n\033[1m'+group.title.upper()+'\033[0m\n'
		for option in group.option_list:
			align_space=20-len(str(option))
			if option.default != ("NO", "DEFAULT"):
				log+= str(option)+''.join([' ' for i in range(align_space)])+': \033[1;34m'+str(options.__dict__[option.dest])+'\033[1;m\n'
	root = ET.Element('deployment')
	site = ET.SubElement(root, 'site', attrib={'id':options.site})
	cluster= ET.SubElement(site, 'cluster', attrib={'id': options.cluster if options.cluster is not None else 'unknown' })
	for i_nodes in range(options.n_nodes):
		host = ET.SubElement(cluster, 'host', attrib={'id': 'unknown'})
		for i_vm in range(options.n_vm):
			vm = ET.SubElement(host, 'vm', attrib={'id': 'unknown', 
												'cpu': str(options.n_cpu),
												'mem': str(options.memory_size),
												'hdd': str(options.hdd_size)})
	tree = ET.ElementTree(element=root)
else:
	options.yes=True
	log+= '\nUsing a custom input file \033[1;34m'+str(options.infile)+'\033[1;m'
	tree = ET.parse(options.infile)
	root = tree.getroot()


logger.info('%s',log)

# Checking options
cont=''
if not options.yes:
	while cont != 'y' and cont != 'n':
		cont=raw_input('Are options correctly defined ? (y/n) ')
		if cont=='n':
			logger.info('Exiting program ...') 
			exit()

try:
	os.mkdir(options.outdir)
except:
	pass
	
tree.write(options.outdir+'/input-'+time.strftime('%Y%m%d_%H%M%S',time.localtime())+'.xml', pretty_print=True)

def start_prediction_changed(t):
	logger.info('%s','Starting date: '+execo.format_date(t))
subs=[]

#ET.dump(root)
# One site :

log='*** RESERVING RESOURCES:'
getlan=1
for site in root.findall('site'):	
	site_clusters=execo_g5k.api_utils.get_site_clusters(site.get('id'))
	
	if len(site.findall('cluster'))>=1:
		i_cluster=0
		for cluster in site.iter('cluster'):
			ressources="slash_22=1+" if getlan else ""
			getlan=0
			if cluster.get('id') not in site_clusters:
				for cl in site.findall('cluster'):
					if site_clusters[i_cluster] == cl.get('id'):
						i_cluster+=1
				cluster.set('id',site_clusters[i_cluster])
				
			ressources+="{cluster=\\'"+cluster.get('id')+"\\'}/nodes="+str(len(cluster.findall('host')))
			
			log+='\n - \033[1m'+str(len(list(cluster.iter("host"))))+'\033[0m nodes on '\
			+str(cluster.get('id'))+" ("+site.get('id')+")"
			
			subs.append((execo_g5k.OarSubmission(resources = ressources), site.get('id')))
		i_cluster+=1
	
log+='\n for \033[1m'+options.walltime+'\033[0m ... *** '
# Multisite deployment
# Faut que je patch execo pour avoir la fonction oargridsub.get_network
#	getlan=1
#	for site in root.findall('site'):
#		
#		i_cluster=0
#		for cluster in site.findall('cluster'):
#			resources = "slash_22=1+" if getlan else '' 
#			getlan=0
#			site_clusters=execo_g5k.api_utils.get_site_clusters(site.get('id'))
#			if cluster.get('id') not in site_clusters:
#				cluster.set('id',site_clusters[i_cluster])
#				i_cluster+=1
#			resources+="{cluster=\\'"+cluster.get('id')+"\\'}/nodes="+str(len(cluster.findall('host')))
#				
#			subs.append((execo_g5k.OarSubmission(
#				resources = resources			),site.get('id')))
#		print site.get('id')
#	
		
		
#for sub in subs:
#	print sub
#print len(subs)
logger.info('%s',log)
job = execo_g5k.oargridsub(subs,walltime=options.walltime,job_type='deploy')
print 'jobs: '+str(job)



#sub_oargrid = 'lille:rdef="{\\\\\\\"type=\'kavlan-global\'\\\\\\\"}/vlan=1+/slash_18=1
#+"{\\\\\\\"cluster in (\'chimint\',\'chinqchint','chirloute\')\\\\\\\"}/nodes=37",lyon:rdef="{\\\\\\\"cluster in (\'hercule\',\'orion\',\'taurus\')\\\\\\\"}/nodes=8",nancy:rdef="{\\\\\\\"cluster in (\'graphene\',\'griffon\')\\\\\\\"}/nodes=173"'
#	
#	
#logger.info('%s',sub_oargrid)







exit()
	

execo_g5k.wait_oar_job_start(job[0][0], job[0][1], prediction_callback=start_prediction_changed)
nodes=execo_g5k.get_oar_job_nodes(job[0][0], job[0][1])
log= 'Job '+str(job[0][0])+' running on '+str(job[0][1])+'\n'+"".join([ ` node `+' ' for node in nodes])
logger.info('%s',log)



# Getting newtork parameters for later Virtual Machine configuration
log='*** Retrieving network parameters ... ***'
logger.info('%s',log)
[ip_list,network_param]=execo_g5k.get_oar_job_subnets(job[0][0], job[0][1])
log = 'ip_list: \033[1m'+ip_list[0]+'\033[0m-\033[1m'+ip_list[len(ip_list)-1]+'\033[0m\n'
netmask=network_param['netmask']
broadcast=network_param['broadcast']
gateway=network_param['gateway']
log += 'netmask: \033[1m'+netmask+'\033[0m gateway: \033[1m'+gateway+'\033[0m broadcast: \033[1m'+broadcast+'\033[0m\n'
logger.info('%s',log)


# Deploying operating systems on nodes
if options.environment!='squeeze-x64-xen':
	xen_deployment = execo_g5k.Deployment(hosts=nodes, env_file=options.environment)
else:
	xen_deployment = execo_g5k.Deployment(hosts=nodes, env_name='squeeze-x64-xen')
log = '*** Deploying \033[1m'+options.environment+'\033[0m on nodes ... ***'
logger.info('%s',log)
Hosts=execo_g5k.deploy(xen_deployment)
DeployedHosts=Hosts[0]

log='*** Creating VM configuration and disks ... *** \n'
create_vm=[]
i_host=0
for host in DeployedHosts:
	for i_vm in range(options.n_vm):
		cmd_create_vm="export http_proxy='http://proxy:3128'; "+\
			"BRIDGE=`brctl show | grep eth | cut -f1` ;"+ \
			"xen-create-image --hostname vm-"+str(i_vm+i_host*options.n_vm)+" --dir=/tmp/xen --vcpus "+ \
			"--size "+str(options.hdd_size)+"Gb  --memory "+str(options.memory_size)+"Mb --swap "+str(options.memory_size)+"Mb --image full  "+ \
			"--ip "+str(ip_list[i_vm+i_host*options.n_vm])+" --netmask "+netmask+" --gateway "+gateway+" --broadcast "+broadcast+" --bridge $BRIDGE;"
		create_vm.append(execo.Remote(cmd_create_vm,[host],connexion_params={'user': 'root'}))	
		log+=' - vm-'+str(i_vm+i_host*options.n_vm)+' on '+host.address+'\n'
	i_host+=1
logger.info('%s',log)
execo.ParallelActions(create_vm).run()
log= '*** All VM have been created, starting VM ... ***'

start_vm=[]
i_host=0
for host in DeployedHosts:
	for i_vm in range(options.n_vm):
		cmd_start='xm create /etc/xen/vm-'+str(i_vm+i_host*options.n_vm)+'.cfg'; 
		start_vm.append(execo.Remote(cmd_start,[host],connexion_params={'user': 'root'}))
	i_host+=1
	
logger.info('%s',log)
execo.ParallelActions(start_vm).run()

log= '*** All VM have been started, writing deployment file ... ***'
list_vm=[]
for host in DeployedHosts:
	cmd_list='xm list'
	list_vm.append(execo.Remote(cmd_list,[host],connexion_params={'user': 'root'}))

actionlist=execo.ParallelActions(list_vm).run() 

for action in [actionlist]:
	for p in action.processes():
		log=p.host()+'\n'+p.stdout()
		logger.info('%s',log) 


ET.dump(root)

#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# Copyright 2009-2012 INRIA Rhone-Alpes
#
# 
#
# - for deploying 10 nodes on any cluster in Lyon, with 4 vm on each nodes
#	deploy_cluster_with_vm.py -s lyon -n 10 -v 4 
#   
#
#


import os,sys,time,logging
from optparse import OptionParser, OptionGroup
import lxml.etree as ET
import execo_g5k

logger = logging.getLogger("execo")

usage = "usage: %prog"
description = """Generate an XML file containing the deployment topology of a VM experiments on grid5000             
"""
epilog = """Examples : deploying 10 nodes on any cluster in nancy, with 4 vm on each nodes. 
 %prog -s nancy -n 10 -v 4                          
deploying 2 nodes on stremi with 3 VM having 2 Cpu, 20 Gb disk and 1024 Mb RAM
 %prog -s reims -c stremi -n 2 -v 3 -p 2 -d 20 -m 1024           
"""

# Parsing command line options
parser = OptionParser(usage = usage, description = description, epilog = epilog)

optinout= OptionGroup(parser, "I/O options", "Controls input and output.")
optinout.add_option("-o", 
				"--outdir", 
				dest="outdir", 
				default='vm_deployment', 
				help="directory to store the deployment files (%default)", 
				metavar="OUTDIR")
optinout.add_option("-a", 
				"--add", 
				dest="infile", 
				default=None, 
				help="add the deployment to an existing one (%default)", 
				metavar="INFILE")
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
parser.add_option_group(optinout)
optdeploy = OptionGroup(parser, "Deployment options", "Customize your grid5000 deployment and choose environment.")
optdeploy.add_option("-s",
				"--site", 
				dest="site", 
				default=None, 
				help="select a Grid5000 site (%default)")
optdeploy.add_option("-c",
				"--cluster", 
				dest="cluster", 
				default=None, 
				help="select a specific Grid5000 cluster (%default)")
optdeploy.add_option("-n",
				"--nodes", 
				dest="n_nodes", 
				type="int", 
				default=2,	
				help="number of nodes (%default)")
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


if not options.quiet:
	logger.setLevel(logging.INFO)
else:
	options.yes=True

logger.info('Starting \033[1m'+project+'\033[0m')

# checking consistency site <-> cluster <-> number of nodes
if options.site is None:
	if options.cluster is None:
		logger.error('No cluster or site specified, aborting ...')
		exit()
	else:
		options.site=execo_g5k.api_utils.get_cluster_site(options.cluster)
		logger.info('cluster found on site \033[1m'+options.site+'\033[0m')
else: 
	clusters=execo_g5k.api_utils.get_site_clusters(options.site)
	if len(clusters)==0:
		logger.error('No cluster found on site \033[1m'+options.site+'\033[0m, aborting ... ')
		exit()
	if options.cluster is None:
		for cl in clusters:
			if options.n_nodes <= len(execo_g5k.api_utils.get_cluster_hosts(cl)):
				options.cluster=cl
		if options.cluster is None:
			logger.error('Not enough nodes on any cluster of site \033[1m'+options.site+'\033[0m, aborting ... ')
			exit()
		logger.info('Cluster not specified, using \033[1m'+options.cluster+'\033[0m')
	elif options.cluster not in clusters:
		logger.error('cluster \033[1m'+options.cluster+'\033[0m not found on site \033[1m'+options.site+'\033[0m, aborting ...')
		exit()
if options.n_nodes > len(execo_g5k.api_utils.get_cluster_hosts(options.cluster)):
	logger.error('Not enough nodes found on cluster \033[1m'+options.cluster+'\033[0m, aborting ... ')
	exit()

try:
	os.mkdir(options.outdir)
except:
	pass

log=''
for group in parser.option_groups:
	log+= '\n\033[1m'+group.title.upper()+'\033[0m\n'
	for option in group.option_list:
		align_space=25-len(str(option))
		if option.default != ("NO", "DEFAULT"):
			log+= str(option)+''.join([' ' for i in range(align_space)])+': \033[1;34m'+str(options.__dict__[option.dest])+'\033[1;m\n'

logger.info('%s',log)
if not options.yes:
	while options.yes != 'y' and options.yes != 'n':
			options.yes=raw_input('Are options correctly defined ? (y/n) ')
			if options.yes=='n':
				logger.info('Exiting program ...') 
				exit()


if options.infile is not None:
	log='Parsing \033[1m'+options.infile+'\033[0m \n'
	parser = ET.XMLParser(remove_blank_text=True)
	tree = ET.parse(options.infile, parser)
	root = tree.getroot()
	existing_vm=len(root.findall('.//vm'))
else:
	root = ET.Element('deployment')
	existing_vm=0

if root.find("./*[@id='"+options.site+"']") is None:
	log+='Adding site \033[1m'+options.site+'\033[0m, '
	site = ET.SubElement(root, 'site', attrib={'id':options.site})
else:
	log+='Modifying site \033[1m'+options.site+'\033[0m, '
	site = root.find("./*[@id='"+options.site+"']")
	
if site.find("./*[@id='"+options.cluster+"']") is None:
	log+='adding cluster \033[1m'+options.cluster+'\033[0m\n'
	cluster= ET.SubElement(site, 'cluster', attrib={'id': options.cluster if options.cluster is not None else 'cluster' })
else:
	log+='modifying cluster \033[1m'+options.cluster+'\033[0m\n'
	cluster=site.find("./*[@id='"+options.cluster+"']")
logger.info('%s',log)

existing_hosts=len(cluster.findall('host'))

i_host=1
for i_nodes in range(options.n_nodes):
	host = ET.SubElement(cluster, 'host', attrib={'id': cluster.get('id')+'-'+str(existing_hosts+i_host)})
	for i_vm in range(options.n_vm):
		vm = ET.SubElement(host, 'vm', attrib={'id': 'vm-'+str(existing_vm+i_vm+(i_host-1)*options.n_vm), 
											'cpu': str(options.n_cpu),
											'mem': str(options.memory_size),
											'hdd': str(options.hdd_size),
											'ip': 'X.X.X.X'})
	i_host+=1
	
if (existing_hosts+i_host-1) > len(execo_g5k.api_utils.get_cluster_hosts(options.cluster)):
	logger.error('Not enough nodes on cluster \033[1m'+options.cluster+'\033[0m, aborting ... ')
	exit()


tree = ET.ElementTree(element=root)
tree.write(options.outdir+'/input-'+time.strftime('%Y%m%d_%H%M%S',time.localtime())+'.xml', pretty_print=True)
logger.info('XML VM deployment file created  %s/input-%s.xml ',options.outdir,time.strftime('%Y%m%d_%H%M%S',time.localtime()))
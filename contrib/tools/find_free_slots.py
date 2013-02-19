#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys, json
import time as T, datetime as DT
import logging as LOG, pprint as PP
import matplotlib.pyplot as PLT, matplotlib.dates as MD
from optparse import OptionParser, OptionGroup
import execo.time_utils as ET
import execo_g5k as EX5
from execo_g5k.api_utils import APIConnexion,get_g5k_sites, get_g5k_hosts, get_site_clusters, get_cluster_site, get_cluster_hosts, _get_api_password
from execo_g5k.config import default_frontend_connexion_params, g5k_configuration
from execo_g5k.oargrid import default_frontend_connexion_params,get_oargridsub_commandline


class g5k_find_free_slots(object):
	'''
	This tools determine when the resources you need are available on Grid5000 
	platform thanks to the analysis of Gantt diagram obtained form API   
	and can (optionally) make the reservation.
	
	Parameters are :
	
	walltime  = duration of your reservation
	resources = dict defining the number of nodes for a grid5000 element 
				(grid5000, site, cluster)
	weeks = the number of weeks to look in  
	reservation = if true make the reservation via oargridsub 
	log = display information in standard output
	
	Require execo 2.1, http://execo.gforge.inria.fr/doc/
	'''
	
	def __init__(self,walltime,resources,vlan=None,kavlan=None,oargridsub_opts='-t deploy',
				weeks=1,auto=False,log=True,with_plots=False,outdir='.'):
		self.restime = ET.format_date(T.time())
		self.resources = resources
		self.vlan = vlan
		self.kavlan = kavlan
		self.start_logger(log)		
		self.define_dates(walltime, weeks)
		self.compute_hosts_planning()
		
		self.compute_freeslots()
		self.get_windows_ok()
		if with_plots:
			self.draw_gantt()
		self.choose_freeslot(self.windows_ok,auto)
			
		self.create_submissions(oargridsub_opts,auto)
		self.logger.info('Done.')
	
	def start_logger(self,log):
		self.logger = LOG.getLogger('execo')
		if log:
			self.logger.setLevel(LOG.INFO)
		else: 
			self.logger.setLevel(LOG.ERROR)
		self.logger.info('Starting \033[1m%s\033[0m ...',self.__class__.__name__)
	
	def compute_hosts_planning(self):
		self.g5k_api=APIConnexion( "https://api.grid5000.fr/2.1",
                                password = _get_api_password(g5k_configuration.get('api_username')))
		self.logger.info('Retrieving Grid5000 sites, clusters, hosts and reservations from API ...')
		self.g5k_sites=get_g5k_sites()
		self.sites=[]
		if self.resources.has_key('grid5000.fr'):
			self.sites=self.g5k_sites
		else:
			for resource in self.resources:
				if resource in self.g5k_sites:
					if resource not in self.sites:
						self.sites.append(resource)
				else:
					site=get_cluster_site(resource)
					if site not in self.sites:
						self.sites.append(site)
		self.logger.info('Computing hosts planning ...')
		self.hosts_plannings={}		
		for site in self.sites:
			self.hosts_plannings[site]={}		
			for cluster in get_site_clusters(site):
				self.hosts_plannings[site][cluster]={}
				(_, content)=self.g5k_api.get('/grid5000/sites/'+site+'/clusters/'+cluster+'/status?reservations_limit=100')
				hosts = json.loads(content)
				for host in hosts['items']:
					if host['hardware_state'] != 'dead':
						planning={'busy': [], 'free': []}
						for job in host['reservations']:
							if job['queue']!='besteffort' and (job['start_time'],job['start_time']+job['walltime']) not in planning['busy']:
								job['start_time']=max(job['start_time'],self.startdate_stamp)
								planning['busy'].append((job['start_time'],job['start_time']+job['walltime']))
						planning['busy'].sort()
						if len(planning['busy'])>0:
							if planning['busy'][0][0]>self.startdate_stamp:
								planning['free'].append((self.startdate_stamp,planning['busy'][0][0])) 
							for i in range(0,len(planning['busy'])-1):
								planning['free'].append((planning['busy'][i][1],planning['busy'][i+1][0]))
							if planning['busy'][len(planning['busy'])-1][1]<self.enddate_stamp:
								planning['free'].append((planning['busy'][len(planning['busy'])-1][1],self.enddate_stamp))
						else:
							planning['free'].append((self.startdate_stamp,self.enddate_stamp))
						
						self.hosts_plannings[site][cluster][host['node_uid']]=planning
		
#		self.xml=ET.Element('platform', {'id': 'grid5000.fr'})
#		for site in sites:
#			el_site = ET.SubElement(self.xml,'site',attrib={'id':site})
#			clusters=get_site_clusters(site)
#			for cluster in clusters:
#				(_, content)=self.g5k_api.get('/grid5000/sites/'+site+'/clusters/'+cluster+'/status?reservations_limit=100')
#				hosts = json.loads(content)		
#				for host in hosts['items']:
#					if host['hardware_state'] != 'dead':
#						cluster_id, radical = host['node_uid'].split('-')
#						if el_site.find("cluster[@id='"+cluster_id+"']") is None:
#							el_cluster=ET.SubElement(el_site,'cluster', attrib={'id':cluster_id})
#						else:
#							el_cluster=el_site.find("cluster[@id='"+cluster_id+"']")
#						if el_cluster.find("host[@id='"+host['node_uid']+"']") is None:
#							el_host=ET.SubElement(el_cluster,'host', attrib={'id':host['node_uid']})
#						else:
#							el_host=el_cluster.find("host[@id='"+host['node_uid']+"']")
#						for job in host['reservations']:
#							ET.SubElement(el_host,'job', attrib={'id': str(job['batch_id']), 'start':str(job['start_time']), 'stop': str(job['start_time']+job['walltime'])})
#			self.logger.info(' - \033[1m%s\033[0m proceeded', site)
#			
#		tree = ET.ElementTree(element=self.xml)
#		tree.write('platform-'+self.restime+'.xml', pretty_print=True)
		
	def define_dates(self,walltime, weeks):
		self.startdate=(DT.datetime.now()+DT.timedelta(seconds=10))
		self.startdate_stamp=int(T.mktime(self.startdate.timetuple()))
		self.enddate=(DT.datetime.now()+ DT.timedelta(weeks=weeks,minutes=5))
		self.enddate_stamp=int(T.mktime(self.enddate.timetuple()))	
		self.walltime=walltime
		h, m, s = walltime.split(':')
		self.duration=int(DT.timedelta(hours=int(h), minutes=int(m), seconds=int(s)+10).total_seconds())
		self.logger.info('From \033[1m%s\033[0m to \033[1m%s\033[0m', 
			self.startdate.strftime("%Y-%m-%d %H:%M"), self.enddate.strftime("%Y-%m-%d %H:%M"))

	def get_hosts_element(self,element):
		element_hosts={}
		if element=='grid5000.fr':
			for site, clusters in self.hosts_plannings.iteritems():
				for hosts in clusters.itervalues():
					for host, planning in hosts.iteritems():
						element_hosts[host]=planning
		elif element in self.g5k_sites:
			for hosts in self.hosts_plannings[element].itervalues():
				for host, planning in hosts.iteritems():
					element_hosts[host]=planning
		else:
			site=get_cluster_site(element)
			for host, planning in self.hosts_plannings[site][element].iteritems():
				element_hosts[host]=planning
			
		return element_hosts
	
	def windows_limits(self,hosts,windows_limits=[]):
		for slots in hosts.itervalues():
			for freeslot in slots['free']:
				if freeslot[0] not in windows_limits:
					windows_limits.append(freeslot[0])
			for busyslot in slots['busy']:
				if busyslot[0] not in windows_limits:
					windows_limits.append(busyslot[0])
		return sorted(windows_limits)

	def merge_slots(self,slots):
		if len(slots)>1:
			for i in range(len(slots)):
				j=i+1
				if j==len(slots)-1:
					break
				while True:
					condition = slots[i][1]>=slots[j][0] 
					if condition:
						slots[i]=(slots[i][0],slots[j][1])
						slots.pop(j)
						if j==len(slots)-1:
							break
					else:
						break
				if j==len(slots)-1:
					break
			
#			for i in range(len(slots)):
#				if (slots[i][1]-slots[i][0])<1:
#					slots.pop(i)
		
	
	def compute_freeslots(self):
		self.logger.info('Computing grid, sites and clusters plannings ...')
		self.freeslots={}
		for res_id, n_nodes in self.resources.iteritems():
			self.freeslots[res_id]=[]
			hosts=self.get_hosts_element(res_id)
			windows_limits=self.windows_limits(hosts,[self.startdate_stamp,self.enddate_stamp])
			
			for i in range(len(windows_limits)):
				i_duration=0
				is_last=False
				while True:
					host_available=0
					start=windows_limits[i]+i_duration*self.duration
					if start+self.duration <self.enddate_stamp:
						stop=start+self.duration 
					else:
						stop=self.enddate_stamp
						is_last=True 
					for slots in hosts.itervalues():
						for freeslot in slots['free']:
							if start>=freeslot[0] and stop<=freeslot[1]:
								host_available+=1								
					if host_available>=n_nodes:
						self.freeslots[res_id].append((start, stop))
						if is_last:
							break
					else:
						break
					i_duration+=1
			self.freeslots[res_id].sort()
			self.merge_slots(self.freeslots[res_id])
			
	
	def get_windows_ok(self):
		self.logger.info('Determining all free slots for your reservation ...')
		n_element=len(self.resources)
		windows_limits=[self.startdate_stamp,self.enddate_stamp]
		
		for res_id in self.resources.iterkeys():
			hosts=self.get_hosts_element(res_id)
			windows_limits=self.windows_limits(hosts,windows_limits)
				
		windows_ok=[]
		for i in range(len(windows_limits)-1):
			element_ok=[]
			start=windows_limits[i]
			stop=self.enddate_stamp
			for element, freeslots in self.freeslots.iteritems():
				for freeslot in freeslots:
					if start >= freeslot[0] and start <= freeslot[1]:
						element_ok.append(element)
						stop=min(stop,freeslot[1])
			if len(element_ok)==n_element and (start,stop) not in windows_ok:				
				windows_ok.append((start,stop))
		
		self.merge_slots(windows_ok)
		
		windows_ok.sort()
		self.windows_ok=windows_ok		
		
		
		
	def draw_gantt(self):
		self.logger.info('Drawing Gantt diagram ...')
		PLT.figure(figsize=(15,10),dpi=80)
		PLT.title('Gantt diagram for the resources you ask')
		
		n_res=len(self.resources) 
		ax_all = PLT.subplot2grid((2,n_res), (1,0), colspan=n_res)
		ax_all.set_xlim(self.startdate_stamp,self.enddate_stamp)
		ax_all.set_ylim(0,n_res+1)

		
		i_res=0
		for res_id, n_nodes in  self.resources.iteritems():
			hosts=self.get_hosts_element(res_id)
			
			ax=PLT.subplot2grid((2,n_res), (0,i_res), title=res_id+': '+str(n_nodes)+'/'+str(len(hosts)))
			ax.set_xlim(self.startdate_stamp,self.enddate_stamp)
			ax.set_ylim(0,1)
			ax.xaxis.set_visible(False)
			ax.yaxis.set_visible(False)
			
			pos=0.1
			if len(hosts)!=0:
				inc=0.9/len(hosts)
			else:
				inc=0.9
			for slots in hosts.itervalues():					
				for freeslot in slots['free']:
					ax.barh(pos, (freeslot[1]-freeslot[0]), 1,
	           			color='g', left=freeslot[0],edgecolor='none' )
				for busyslot in slots['busy']:
					ax.barh(pos, (busyslot[1]-busyslot[0]), 1,
	           			color='r', left=busyslot[0],edgecolor='none' )
				pos+=inc	
				
			color=(float(i_res+1)/float(n_res)/2,float(i_res+1)/float(n_res),1)
			for window in self.freeslots[res_id]:
				ax.barh(0, (window[1]-window[0]), 0.1,
               		color=color, left=window[0], edgecolor='none' )
				ax_all.barh(i_res+1, (window[1]-window[0]), 1,
             		color=color, left=window[0], edgecolor='none' )
			ax_all.text((self.enddate_stamp+self.startdate_stamp)/2,i_res+1.5,res_id)
			ax_all.yaxis.set_visible(False)
			i_res+=1	
			
		for window in self.windows_ok:
			ax_all.barh(0, (window[1]-window[0]), 1,
             		color='y', left=window[0], edgecolor='none' )
		PLT.savefig ('platform-'+self.restime+'.png')
		PLT.show()

	def choose_freeslot(self,freeslots,autochoice):
		i_test=0
		for freeslot in freeslots:
			if freeslot[0]==freeslot[1]:
				freeslots.remove(freeslot)
			else:
				freeslots[i_test]=(freeslot[0]+90,freeslot[1])
				i_test+=1	
				
		if len(freeslots)>0:
			if not autochoice:
				log='Available freeslots :'
				i=0
				for freeslot in freeslots:
					i+=1
					log+='\n '+str(i)+') '+DT.datetime.fromtimestamp(freeslot[0]).strftime('%Y-%m-%d %H:%M:%S')\
					 +' - '+DT.datetime.fromtimestamp(freeslot[1]+self.duration).strftime('%Y-%m-%d %H:%M:%S')
				log+='\n x) Abort ...'
				self.logger.info('%s', log)	
				i_slot=raw_input('Choose your slot: ')
				if i_slot=='x':
					exit()
				i_slot=int(i_slot)-1
				self.logger.info('You have chosen slot starting at %s',DT.datetime.fromtimestamp(freeslots[i_slot][0]))
				self.chosen_slot=DT.datetime.fromtimestamp(freeslots[i_slot][0])
			else:
				self.logger.info('Slot starting at %s has been chosen',DT.datetime.fromtimestamp(freeslots[0][0]))
				self.chosen_slot=DT.datetime.fromtimestamp(freeslots[0][0])
		else:
			self.logger.error('There is not enough resources for your parameters, aborting ...')
			exit()
	

	def create_submissions(self,oargridsub_opts,auto_reservation):
		subs=[]
		getkavlan=False
		if self.kavlan is not None:
			getkavlan=True
		
		if self.resources.has_key('grid5000.fr'):
			self.logger.info('Determining which sites to use for your reservation')
			total_nodes=0
			sites_nodes={}
			for site in self.sites:
				if self.resources.has_key(site):
					sites_nodes[site]=self.resources[site]
				else:
					sites_nodes[site]=0
			while total_nodes != self.resources['grid5000.fr']:
				max_site=''
				max_nodes=0
				for site in self.sites:
					hosts=self.get_hosts_element(site)
					host_available=0
					start=int(T.mktime(self.chosen_slot.timetuple()))
					if start+self.duration <self.enddate_stamp:
						stop=start+self.duration 
					else:
						stop=self.enddate_stamp
					for slots in hosts.itervalues():
						for freeslot in slots['free']:
							if start>=freeslot[0] and stop<=freeslot[1]:
								host_available+=1
					if max_nodes<host_available-sites_nodes[site]:
						max_site=site
						max_nodes=host_available-sites_nodes[site]
				sites_nodes[max_site]+=1
				total_nodes+=1
			self.resources.clear()
			for site, n_nodes in sites_nodes.iteritems():
				if n_nodes>0:
					self.resources[site]=n_nodes
	
		PP.pprint(self.resources)
		for site in self.sites:
			sub_resources=''
			cluster_nodes=0
			
			if getkavlan:
				sub_resources="{type=\\'kavlan-global\\'}/vlan=1+"
			if self.vlan is not None:
				sub_resources+=self.vlan+'+'
				
			
			for cluster in get_site_clusters(site):
				if cluster in self.resources:
					sub_resources+="{cluster=\\'"+cluster+"\\'}/nodes="+str(self.resources[cluster])+'+'
					cluster_nodes+=self.resources[cluster]
					if site not in self.resources:
						self.resources[site]=self.resources[cluster]
					else:
						self.resources[site]+=self.resources[cluster]
				
			if site in self.resources:
				sub_resources+="nodes="+str(self.resources[site]-cluster_nodes)+'+'
				subs.append((EX5.OarSubmission(resources=sub_resources[:-1]),site))
		
		self.logger.info('Reservation command: \n\033[1m%s\033[0m',
						get_oargridsub_commandline(subs,walltime=self.walltime,additional_options=oargridsub_opts,reservation_date=self.chosen_slot))
		
		if auto_reservation:			
			(self.oargrid_job_id, self.ssh_key)=EX5.oargridsub(subs,walltime=self.walltime,additional_options=oargridsub_opts,reservation_date=self.chosen_slot)
			self.logger.info('Grid reservation done, oargridjob_id = %s',self.oargrid_job_id)
		else:			
			reservation=raw_input('Do you want me to do the reservation (y/n): ')
			if reservation=='y':
				(self.oargrid_job_id, self.ssh_key)=EX5.oargridsub(subs,walltime=self.walltime,additional_options=oargridsub_opts,reservation_date=self.chosen_slot)	
				self.logger.info('Grid reservation done, oargridjob_id = %s',self.oargrid_job_id)
	
	def check_g5k_chart(self):
		print 'coucou'
	
	
	
	
	
if __name__ == '__main__':
	if len(sys.argv)>1:
		
		usage = "usage: %prog"
		description = """
		This program allow you to determine the slots available for your experiment, defined by a resource
		combination given and a           
		"""
		epilog = """Examples : \n sdf
		"""
		
		parser = OptionParser(usage = usage, description = description, epilog = epilog)
	
		optinout= OptionGroup(parser, "I/O options", "Controls input and output.")
		optinout.add_option("-y", 
						"--yes",
		                action="store_true", 
		                dest="yes", 
		                default=False,
		                help="Run without prompting user for slot selection (%default)")
		optinout.add_option("-p", 
						"--plots",
		                action="store_true", 
		                dest="plots", 
		                default=False,
		                help="Draw Gantt chart before choosing freeslot")
						
		parser.add_option_group(optinout)
		optreservation = OptionGroup(parser, "Reservation options", "Customize your grid5000 deployment and choose environment.")
		optreservation.add_option("-r",
						"--resources", 
						dest="resources", 
						default=None, 
						help="comma separated list of 'element1:n_nodes1,element2:n_nodes2', element can be a cluster, site or grid5000.fr")
		optreservation.add_option("-l",
						"--vlan", 
						dest="vlan", 
						default=None,	
						help="Ask for a vlan (%default)")
		optreservation.add_option("-o",
						"--oargridsub_options", 
						dest="oargridsub", 
						default="-t deploy",	
						help="number of nodes (%default)")
		optreservation.add_option("-w",
						"--walltime", 
						dest="walltime", 
						default='10:00:00',	
						help="reservation walltime (%default)")
		optreservation.add_option("-t",
						"--time", 
						dest="time", 
						default=4,	
						help="time in weeks to explore (%default)")
		parser.add_option_group(optreservation)
	
		
		(options, args) = parser.parse_args()

		resources = {}
		elements = options.resources.split(',')
		for element in elements:
			element_uid, n_nodes = element.split(':')
			resources[element_uid]=int(n_nodes)
		
		test=g5k_find_free_slots(options.walltime,resources,oargridsub_opts=options.oargridsub, vlan=options.vlan,
								with_plots=options.plots, auto=options.yes, weeks=int(options.time))
		
	else:
		logger = LOG.getLogger("execo")
		logger.setLevel(LOG.INFO)
		
		resources={'chimint': 5, 'lille': 10, 'stremi':10, 'grid5000.fr':100}
		log='\n \033[1;31m -- WARNING : No arguments given, using demo parameters. -- \033[1;m \n'+PP.pformat(resources)
		logger.warning('%s',log)
		
		demo1=g5k_find_free_slots('10:00:00',resources,oargridsub_opts='-t deploy')
	

#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys, copy
import time as T, datetime as DT
import logging as LOG, pprint as PP
import lxml.etree as ET, matplotlib.pyplot as PLT
from optparse import OptionParser, OptionGroup
import execo as EX, execo_g5k as EX5, execo_g5k.api_utils as API
from execo_g5k.config import default_frontend_connexion_params
from execo_g5k.oargrid import default_frontend_connexion_params


class g5k_find_free_slots(object):
	'''
	This tools determine when the resources you need are available on Grid5000 
	platform thanks to the analysis of Gantt diagram obtained form oarstat command   
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
	
	def __init__(self,walltime,resources,job_type='deploy',lan=None,
				weeks=2,auto_choice=False,auto_reservation=False,log=True,with_plots=False):
					
		self.start_logger(log)
		self.get_g5k_topology()		
		self.create_params(resources)
		self.define_dates(walltime, weeks)
		self.generate_gantt()
		self.hosts_planning()
		self.compute_freeslots()
		self.draw_gantt()
		
		#~ freeslots={}
		#~ if (self.params.has_key('n_nodes')):
			#~ self.logger.info('** Determining free slots for site \033[1m%s\033[0m ...')
			#~ self.compute_grid_freeslots()
		#~ else:
			#~ for site, site_params in self.params['sites'].iteritems():
				#~ if (site_params.has_key('n_nodes')):
					#~ self.logger.info('** Determining free slots for site \033[1m%s\033[0m ...',site)
					#~ freeslots[site]=self.compute_site_freeslots(site)
				#~ else:
					#~ for cluster in self.params['sites'][site]['clusters'].iterkeys():
						#~ self.logger.info('** Determining free slots for cluster \033[1m%s\033[0m ...',cluster)
						#~ freeslots[cluster]=self.compute_cluster_freeslots(cluster)
						#~ if cluster=='chicon':
							#~ print freeslots['chicon']
						#~ 
				#~ 
		#~ 
		#~ self.windows_ok=self.intersection(freeslots) if type(freeslots).__name__=='dict' else freeslots
		#~ 
		#~ #if with_plots:
		#~ #	self.draw_gantt()
		#~ 
		
		
		
		#reservation_date=self.choose_freeslot(self.windows_ok)
		
		#self.create_submissions()
#		reservation=self.reservation(reservation_date)
#		if auto_reservation:
#			print 'oargrid'
#		else:
#			print 'oargrid'
			
			
		self.logger.info('Done.\n')
	
	def start_logger(self,log):
		self.logger = LOG.getLogger('execo')
		if log:
			self.logger.setLevel(LOG.INFO)
		else: 
			self.logger.setLevel(LOG.ERROR)
		self.logger.info('Starting \033[1m%s\033[0m ...',self.__class__.__name__)
	
	def get_g5k_topology(self):
		g5k_sites=API.get_g5k_sites()
		self.params={'grid5000.fr':{'n_nodes':0, 'sites': {}}}
		for site in g5k_sites:
			self.params['grid5000.fr']['sites'][site]={'n_nodes':0, 'clusters':{}}
			site_clusters=API.get_site_clusters(site)
			for cluster in site_clusters:
				self.params['grid5000.fr']['sites'][site]['clusters'][cluster]={'n_nodes':0, 'max_nodes':len(API.get_cluster_hosts(cluster))}
		
		self.logger.info('Grid5000 sites and clusters have been retrieved.')
		
	def create_params(self,resources):
		log='** Resources '
		tmp_params = copy.deepcopy(self.params)
		total_nodes=0
		missing_nodes=0
		# updating n_nodes for each cluster
		for site, s_params in self.params['grid5000.fr']['sites'].iteritems():
			site_nodes=0
			for cluster, c_params in s_params['clusters'].iteritems():
				if resources.has_key(cluster):			
					if resources[cluster] <= c_params['max_nodes']:					
						tmp_params['grid5000.fr']['sites'][site]['clusters'][cluster]['n_nodes']=resources[cluster]
						log+='\n  \033[1;32m OK \033[1;m    \033[1m'+str(resources[cluster])+'\033[0m nodes on cluster \033[1m'+cluster+'\033[0m'
						total_nodes+=resources[cluster]
					else:
						tmp_params['grid5000.fr']['sites'][site]['clusters'][cluster]['n_nodes']=c_params['max_nodes']
						log+='\n\033[1;41m WARNING \033[1;m Not enough nodes on cluster \033[1m'+cluster+'\033[0m, fixing to \033[1m'+str(c_params['max_nodes'])+'\033[1;m'
						total_nodes+=c_params['max_nodes']
						missing_nodes+=resources[cluster]-c_params['max_nodes']
					site_nodes+=resources[cluster]
			tmp_params['grid5000.fr']['sites'][site]['n_nodes']=site_nodes						
		self.params = tmp_params.copy()
		# updating n_nodes for each site 
		for site, s_params in self.params['grid5000.fr']['sites'].iteritems():
			if resources.has_key(site):
				site_nodes=0
				for cluster, c_params in s_params['clusters'].iteritems():
					site_nodes+=c_params['n_nodes']
				tmp_params['grid5000.fr']['sites'][site]['n_nodes']=max(resources[site], site_nodes)
				total_nodes+=tmp_params['grid5000.fr']['sites'][site]['n_nodes']
		self.params = tmp_params.copy()
		# updating n_nodes for grid5000.fr 
		self.params['grid5000.fr']['n_nodes']=max(resources['grid5000.fr'], total_nodes)+missing_nodes
		log+='\n \n Total nodes : \033[1;38m'+str(self.params['grid5000.fr']['n_nodes'])+'\033[1;m '
		self.logger.info('%s', log)	
	
	def define_dates(self,walltime, weeks):
		self.startdate=(DT.datetime.now()+DT.timedelta(minutes=5))
		self.startdate_stamp=int(T.mktime(self.startdate.timetuple()))
		self.enddate=(DT.datetime.now()+ DT.timedelta(weeks=weeks,minutes=5))
		self.enddate_stamp=int(T.mktime(self.enddate.timetuple()))	
		self.walltime=walltime
		h, m, s = walltime.split(':')
		self.duration=int(DT.timedelta(hours=int(h), minutes=int(m), seconds=int(s)+10).total_seconds())
		self.logger.info('** From \033[1m%s\033[0m to \033[1m%s\033[0m', 
			self.startdate.strftime("%Y-%m-%d %H:%M:%S"), self.enddate.strftime("%Y-%m-%d %H:%M:%S"))
	
	def generate_gantt(self):
		cmd_gantt='oarstat -g "'+self.startdate.strftime("%Y-%m-%d %H:%M:%S")+ \
			','+self.enddate.strftime("%Y-%m-%d %H:%M:%S")+'" -X'
		getgantt=[]
		for site, params in self.params['grid5000.fr']['sites'].iteritems():
			if params['n_nodes']>0:
				getgantt.append(EX.Remote(cmd_gantt, [EX.Host(site+'.grid5000.fr')],connexion_params=default_frontend_connexion_params))
		self.logger.info('** Getting resources via oarstat')
		getgantt=EX.ParallelActions(getgantt).run()
		self.gantt=ET.Element('gantt')
		self.logger.info('** Creating Gantt')
		for p in getgantt.processes():
			self.logger.info(' - processing \033[1m%s\033[0m data', p.host().address[0:-12])
			if not p.ok():			
				if not self.params.has_key('n_nodes'):
					self.logger.error('%s is not available, aborting',p.host().address[0:-12])
					exit()
				else:
					self.logger.warning('%s is not available, skipping ..',p.host().address[0:-12])
					del self.params['sites'][p.host().address[0:-12]]
			else:
				site=ET.SubElement(self.gantt,'site',attrib={'id':p.host().address[0:-4]})
				gantt=ET.fromstring(p.stdout())
				# finding alive hosts
				hosts=gantt.findall(".//*item[@key='host']/../..")
				for host_elem in hosts:
					state=host_elem.find("./*item[@key='state']").text
					hostname=host_elem.find("./*item[@key='host']").text
					cluster_id=host_elem.find("./*item[@key='cluster']").text
					res_id=host_elem.find(".//*[@key='resource_id']").text
					if cluster_id is not None and state!='Dead':
						if site.find("cluster[@id='"+cluster_id+"']") is None:
							cluster=ET.SubElement(site,'cluster', attrib={'id':cluster_id})
						else:
							cluster=site.find("cluster[@id='"+cluster_id+"']")
						if cluster.find("host[@id='"+hostname+"']") is None:
							host=ET.SubElement(cluster,'host', attrib={'id':hostname})
						else:
							host=cluster.find("host[@id='"+hostname+"']")
						ET.SubElement(host,'resource', attrib={'id':res_id})
				# associating jobs to host
				jobs=gantt.findall(".//*[@key='submission_time']/../..")
				for job in jobs:
					start_time=job.find("./*item[@key='start_time']").text
					stop_time=job.find("./*item[@key='stop_time']").text
					resources=job.findall('./hashref/item/arrayref/item')
					for resource in resources:
						res=self.gantt.find("./site/cluster/host/resource[@id='"+resource.text+"']")
						if res is not None:
							ET.SubElement(res,'job',attrib={'id':job.get('key'), 'start_time':start_time, 'stop_time':stop_time })
	
	def hosts_planning(self):
		self.logger.info('** Computing hosts planning ...')
		PP.pprint(self.params)
		self.hosts_plannings={}		
			
		for site,clusters in self.params['grid5000.fr']['sites'].iteritems():
			for cluster in clusters['clusters'].iterkeys():
				cl_elem=self.gantt.find(".//cluster[@id='"+cluster+"']")		
				
				if cl_elem is not None:					
					for host in cl_elem.iter('host'):
						print host.get('id')
						planning={'busy': [], 'free': []}
						for core in host.iter('resource'):
							for job in core.iter('job'):
								if (int(job.get('start_time')),int(job.get('stop_time'))) not in planning['busy']:  
									planning['busy'].append((int(job.get('start_time')),int(job.get('stop_time'))))

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
						self.hosts_plannings[host.get('id')]=planning
					else:
						print cluster
		PP.pprint(self.hosts_plannings)
		
	def compute_freeslots():
		

	def draw_gantt(self):
		print 'cocuou'
#~ 	
		
	
	
	#~ def create_submissions(self,lan):	
		#~ if self.params['sub_type']=='grid5000':
			#~ print 'grid'
		#~ elif self.params['sub_type']=='sites':
			#~ print 'sites'
		#~ elif self.params['sub_type']=='clusters':
			#~ print 'clusters'
	#~ 
	#~ def reservation(self,reservation_date):	
		#~ self.logger.info('** Making reservation ...')
		#~ #job = EX5.oargridsub(self.subs,walltime=self.walltime,job_type=self.job_type,
		#~ #					reservation_date=reservation_date)
		#~ print 'job'
		#~ 
	#~ def choose_freeslot(self,freeslots):
		#~ if len(freeslots)>0:
			#~ log='** Available freeslots :'
			#~ i=0
			#~ for freeslot in freeslots:
				#~ i+=1
				#~ log+='\n '+str(i)+') '+DT.datetime.fromtimestamp(freeslot[0]).strftime('%Y-%m-%d %H:%M:%S')\
				#~ +'-'+DT.datetime.fromtimestamp(freeslot[0]+self.duration).strftime('%Y-%m-%d %H:%M:%S')
			#~ log+='\n x) Abort ...'
			#~ self.logger.info('%s', log)	
			#~ i_slot=raw_input('Choose your slot: ')
			#~ print i_slot
			#~ if i_slot=='x':
				#~ exit()
			#~ i_slot=int(i_slot)-1
			#~ self.logger.info('You have chosen slot starting at %s',DT.datetime.fromtimestamp(freeslots[i_slot][0]))
			#~ return DT.datetime.fromtimestamp(freeslots[i_slot][0])
		#~ else:
			#~ self.logger.error('There is not enough ressources for your parameters, aborting ...')
			#~ exit()
	#~ 
	
		#~ 
	#~ def draw_cluster_gantt(self,cluster,subplot=False):
		#~ 
		#~ PLT.figure()
		#~ PLT.title(cluster)
		#~ site=API.get_cluster_site(cluster)
		#~ n_host=len(self.hosts_freeslots[site][cluster])
		#~ pos=0
		#~ for host, freeslots in self.hosts_freeslots[site][cluster].iteritems():
			#~ for freeslot in freeslots:
				#~ rects = PLT.barh(pos, (freeslot[1]-freeslot[0]), 1,
            		#~ color='g', left=freeslot[0],edgecolor='none' )	
			#~ pos+=1
		#~ pos=-round(n_host*10/100)
		#~ 
		#~ windows_ok=self.compute_cluster_freeslots(cluster)
		#~ print windows_ok
		#~ for window in windows_ok:
			#~ PLT.barh(pos, window[1]-window[0], round(n_host*10/100,0),
      		#~ color='b', left=window[0],edgecolor='none' )
			#~ 
		#~ PLT.savefig(cluster+'-test-'+self.startdate+'.png')')
		#~ 
	#~ def draw_site_gantt(self,site,subplot=False):
		#~ print site
		#~ PLT.figure()
		#~ PLT.title(site)
		#~ n_host=0
		#~ all_freeslots={}
		#~ for cluster in API.get_site_clusters(site):
			#~ n_host+=len(self.hosts_freeslots[site][cluster])
			#~ all_freeslots.update(self.hosts_freeslots[site][cluster])
		#~ 
		#~ pos=0
		#~ for host, freeslots in all_freeslots.iteritems():
			#~ for freeslot in freeslots:
				#~ rects = PLT.barh(pos, (freeslot[1]-freeslot[0]), 1,
            		#~ color='g', left=freeslot[0],edgecolor='none' )	
			#~ pos+=1
		#~ pos=-round(n_host*10/100)
		#~ 
		#~ windows_ok=self.compute_site_freeslots(site)
		#~ print windows_ok
		#~ for window in windows_ok:
			#~ PLT.barh(pos, window[1]-window[0], round(n_host*10/100,0),
      		#~ color='b', left=window[0],edgecolor='none' )
			#~ 
		#~ PLT.show()
		#~ 
	#~ def draw_grid_gantt(self,cluster,subplot=False):	
		#~ PLT.figure()
		#~ PLT.title('Grid5000')	
		#~ n_host=0
		#~ all_freeslots={}
		#~ for site, params in self.params['sites'].iteritems():
			#~ for cluster in params['clusters']:
				#~ n_host+=len(self.hosts_freeslots[site][cluster])
				#~ all_freeslots.update(self.hosts_freeslots[site][cluster])
		#~ 
		#~ pos=0
		#~ for host, freeslots in all_freeslots.iteritems():
			#~ for freeslot in freeslots:
				#~ rects = PLT.barh(pos, (freeslot[1]-freeslot[0]), 1,
            		#~ color='g', left=freeslot[0],edgecolor='none' )	
			#~ pos+=1
		#~ pos=-round(n_host*10/100)
		#~ 
		#~ windows_ok=self.compute_site_freeslots(site)
		#~ print windows_ok
		#~ for window in windows_ok:
			#~ PLT.barh(pos, window[1]-window[0], round(n_host*10/100,0),
      		#~ color='b', left=window[0],edgecolor='none' )
		#~ 
		#~ 
		#~ 
	#~ def draw_timeline(self):
		#~ self.logger.info('** Drawing gantt for your reservation ...')
		#~ PLT.Figure(figsize=(15,10),dpi=80)
		#~ 
		#~ for window in self.windows_ok:
			#~ PLT.barh(0, window[1]-window[0], 0.2,
      		#~ color='g', left=window[0],edgecolor='none' )
		#~ PLT.show()
	#~ 
	#~ def intersection(self,element_freeslots):
		#~ self.logger.info('** Determining all free slots for your reservation ...')
		#~ n_element=len(element_freeslots)
		#~ windows_limits=[self.startdate_stamp,self.enddate_stamp]
		#~ windows_limits=self.compute_windows_limits(element_freeslots,windows_limits)
		#~ 
		#~ windows_ok=[]
		#~ for i in range(len(windows_limits)-1):
			#~ element_ok=[]
			#~ start=windows_limits[i]
			#~ stop=self.enddate_stamp
			#~ 
			#~ for element, freeslots in element_freeslots.iteritems():
				#~ for freeslot in freeslots:
					#~ if start>=freeslot[0] and start <=freeslot[1] and element not in element_ok: 
						#~ element_ok.append(element)
						#~ stop=min(stop,freeslot[1])
			#~ 
			#~ if len(element_ok)==n_element:				
				#~ windows_ok.append((start,stop))
				#~ 
		#~ return windows_ok
		#~ 
#~ 
	#~ def compute_grid_freeslots(self):
		#~ windows_limits=[self.startdate_stamp,self.enddate_stamp]
		#~ for site in self.params['sites'].iterkeys():
			#~ for cluster in self.params['sites'][site]['clusters'].iterkeys():
				#~ windows_limits=self.compute_windows_limits(self.hosts_freeslots[site][cluster],windows_limits)
		#~ grid_freeslots=[]
		#~ for i in range(len(windows_limits)-1):
			#~ i_duration=0
			#~ while True:
				#~ host_available=[]
				#~ start=windows_limits[i]+i_duration*self.duration
				#~ stop=start+self.duration
				#~ for site in self.params['sites'].iterkeys():
					#~ for cluster in self.params['sites'][site]['clusters'].iterkeys():
						#~ for host, freeslots in self.hosts_freeslots[site][cluster].iteritems():
							#~ for freeslot in freeslots:
								#~ if start>=freeslot[0] and stop<freeslot[1]:
									#~ host_available.append(host)
				#~ print windows_limits[i], len(host_available), self.params['n_nodes']
				#~ i_duration+=1
				#~ if len(host_available)>=self.params['n_nodes']:					
					#~ grid_freeslots.append((start,stop))
				#~ else:	
					#~ break	
		#~ return self.merging_freeslots(grid_freeslots)					
		#~ 
	#~ def compute_site_freeslots(self, site):
		#~ windows_limits=[self.startdate_stamp,self.enddate_stamp]
		#~ for cluster in self.params['sites'][site]['clusters'].iterkeys():
			#~ windows_limits=self.compute_windows_limits(self.hosts_freeslots[site][cluster],windows_limits)
		#~ 
		#~ site_freeslots=[]
		#~ for i in range(len(windows_limits)-1):
			#~ i_duration=0
			#~ while True:
				#~ host_available=[]
				#~ start=windows_limits[i]+i_duration*self.duration
				#~ stop=start+self.duration
				#~ for cluster in self.params['sites'][site]['clusters'].iterkeys():
					#~ for host, freeslots in self.hosts_freeslots[site][cluster].iteritems():
						#~ for freeslot in freeslots:
							#~ if start>=freeslot[0] and stop<freeslot[1]:
								#~ host_available.append(host)
				#~ i_duration+=1
				#~ if len(host_available)>=self.params['sites'][site]['n_nodes']:					
					#~ site_freeslots.append((start,stop))
				#~ else:	
					#~ break	
		#~ return self.merging_freeslots(site_freeslots)
	#~ 
	#~ def compute_cluster_freeslots(self,cluster):
		#~ site=API.get_cluster_site(cluster)
		#~ windows_limits=self.compute_windows_limits(self.hosts_freeslots[site][cluster])
		#~ cluster_freeslots=[]
		#~ for i in range(len(windows_limits)-1):
			#~ i_duration=0
			#~ while True:
				#~ host_available=[]
				#~ start=windows_limits[i]+i_duration*self.duration
				#~ stop=start+self.duration
				#~ for host, freeslots in self.hosts_freeslots[site][cluster].iteritems():
					#~ for freeslot in freeslots:
						#~ if start>=freeslot[0] and stop<freeslot[1]:
							#~ host_available.append(host)
				#~ i_duration+=1
				#~ if len(host_available)>=self.params['sites'][site]['clusters'][cluster]['n_nodes']:					
					#~ cluster_freeslots.append((start,stop))
				#~ else:	
					#~ break
		#~ return self.merging_freeslots(cluster_freeslots)
	#~ 
	#~ def merging_freeslots(self,freeslots):
		#~ for i in range(len(freeslots)-1):
			#~ j=i+1
			#~ if j==len(freeslots)-1:
				#~ break
			#~ while True:
				#~ if freeslots[i][1]>=freeslots[j][0]:
					#~ freeslots[i]=(freeslots[i][0],freeslots[j][1])
					#~ freeslots.pop(j)
					#~ if j==len(freeslots)-1:
						#~ break
				#~ else:
					#~ break
			#~ if j==len(freeslots)-1:
				#~ break
		#~ for i in range(len(freeslots)-1):
			#~ if (freeslots[i][1]-freeslots[i][0])<1:
				#~ freeslots.pop(i)
		#~ return freeslots
		#~ 
	#~ def compute_windows_limits(self,elements_freeslots,windows_limits=[]):
		#~ 
		#~ for freeslots in elements_freeslots.itervalues():
			#~ for freeslot in freeslots:
				#~ if freeslot[0] not in windows_limits:
					#~ windows_limits.append(int(freeslot[0]))
		#~ return sorted(windows_limits)
						#~ 
#~ 
	#~ 
		
if __name__ == '__main__':
	if len(sys.argv)==1:
		logger = LOG.getLogger("execo")
		logger.setLevel(LOG.INFO)
		
		log='\n \033[1;31m -- WARNING : No arguments given, using demo parameters. -- \033[1;m'+\
			"\n 1 {'stremi':2, 'chimint': 2, 'chicon': 2}, with job_type besteffort, a slash_22 and Gantt graphs"
			#~ "\n 2 {'lille':20, 'sophia':10, 'lyon':5} with Gantt graphs and reservation autoselection"+\
			#~ "\n 3 {'grid5000':900}}"
		logger.warning('%s',log)
		
		demo1=g5k_find_free_slots('6:00:00',{'stremi':50, 'chimint': 5, 'chicon': 5, 'lyon':10, 'lille': 60, 'grid5000.fr':100},
									job_type='besteffort', lan='slash_22',with_plots=True)
		#demo2=g5k_find_free_slots('4:00:00',{'lille':20, 'sophia':10, 'lyon':5},with_plots=True,auto_reservation=True)
		#demo3=g5k_find_free_slots('2:00:00',{'grid5000':900})
	else:
		usage = "usage: %prog"
		description = """           
		"""
		epilog = """ 
		"""
		
		parser = OptionParser(usage = usage, description = description, epilog = epilog)
	
		optinout= OptionGroup(parser, "I/O options", "Controls input and output.")
		optinout.add_option("-i", 
						"--infile", 
						dest="infile", 
						default='vm_deployment', 
						help="directory to store the deployment files (%default)", 
						metavar="OUTDIR")
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
						"--sites", 
						dest="sites", 
						default=None, 
						help="comma separated list if site:n_nodes")
		optdeploy.add_option("-c",
						"--clusters", 
						dest="clusters", 
						default=None, 
						help="comma separated list if cluster:n_nodes")
		optdeploy.add_option("-n",
						"--nodes", 
						dest="n_nodes", 
						type="int", 
						default=2,	
						help="number of nodes (%default)")
		parser.add_option_group(optdeploy)
	
		
		(options, args) = parser.parse_args()

	
	

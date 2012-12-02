#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pprint as pp
import execo as EX 
import execo_g5k.api_utils as API
import time as T 
import datetime as DT
import lxml.etree as ET 
import matplotlib.pyplot as plt



tree = ET.parse('vm_deployment/input-20121122_181148.xml')
root=tree.getroot()

# getting sites, cluster and nodes from XML file
params={}
for site in root.findall('site'):
	params[site.get('id')]={}
	for cluster in site.findall('cluster'):		
		params[site.get('id')][cluster.get('id')]= {'n_nodes':len(cluster.findall('host'))}
walltime='1:00:00'
h, m, s = walltime.split(':')
duration=int(DT.timedelta(hours=int(h), minutes=int(m), seconds=int(s)+10).total_seconds())




EX.logger.info('%',pp.pprint(params))
# Getting gantt for all sites
startdate=(DT.datetime.now()+DT.timedelta(seconds=10))
startdate_stamp=int(T.mktime(startdate.timetuple()))
enddate=(DT.datetime.now()+ DT.timedelta(weeks=2))
enddate_stamp=int(T.mktime(enddate.timetuple()))


cmd_gantt='oarstat -g "'+startdate.strftime("%Y-%m-%d %H:%M:%S")+','+enddate.strftime("%Y-%m-%d %H:%M:%S")+'" -X'
EX.logger.info('%',cmd_gantt)
frontends=[]
for site, resources in params.iteritems():
	frontends.append(EX.Host(site+'.g5k'))
getgantt=EX.Remote(cmd_gantt, frontends,connexion_params={'user':'lpouilloux'}).run()
print DT.datetime.now()

# rewriting Gantt in a more fancy structure
fancygantt=ET.Element('gantt')
for p in getgantt.processes():
	site=ET.SubElement(fancygantt,'site',attrib={'id':p.host().address[0:-4]})
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
	count=0
	for job in jobs:
		start_time=job.find("./*item[@key='start_time']").text
		stop_time=job.find("./*item[@key='stop_time']").text
		resources=job.findall('./hashref/item/arrayref/item')
		for resource in resources:
			res=fancygantt.find("./site/cluster/host/resource[@id='"+resource.text+"']")
			if res is not None:
				ET.SubElement(res,'job',attrib={'id':job.get('key'), 'start_time':start_time, 'stop_time':stop_time })

print DT.datetime.now()

# On trouve les fenetres pour chaque cluster
i_cluster=0
n_cluster=0
for site, clusters in params.iteritems():
	for cluster, resources in clusters.iteritems():
		n_cluster+=1
fig=plt.Figure(figsize=(15,10),dpi=80)
ax2 = plt.subplot2grid((3,n_cluster), (2,0), colspan=n_cluster)	


for site, clusters in params.iteritems():
	for cluster, resources in clusters.iteritems():
		cl_elem=fancygantt.find(".//cluster[@id='"+cluster+"']")		
		# Récupération des jobs pour chaque noeuds
		hosts_jobs={}
		for host in cl_elem.iter('host'):
			host_jobs=[]
			for core in host.iter('resource'):
				for job in core.iter('job'):
					if (int(job.get('start_time')),int(job.get('stop_time'))) not in host_jobs:  
						host_jobs.append((int(job.get('start_time')),int(job.get('stop_time'))))
			hosts_jobs[host.get('id')]=sorted(host_jobs)
		
		# Computation des freeslots
		hosts_freeslots={}			
		for host in sorted(hosts_jobs.iterkeys()):
			jobs=hosts_jobs[host]
			hosts_freeslots[host]=[]
			if len(jobs)>0:
				if jobs[0][0]>startdate_stamp:
					hosts_freeslots[host].append((startdate_stamp,jobs[0][0]))	
				for i in range(0,len(jobs)-1):
					hosts_freeslots[host].append((jobs[i][1],jobs[i+1][0]))
				if jobs[len(jobs)-1][1]<enddate_stamp:
					hosts_freeslots[host].append((jobs[len(jobs)-1][1],enddate_stamp))
			else:
				hosts_freeslots[host].append((startdate_stamp,enddate_stamp))
		# On découpe la timeline 
		windows_limit=[startdate_stamp,enddate_stamp]
		for host, freeslots in hosts_freeslots.iteritems():
			for freeslot in freeslots:
				if freeslot[0] not in windows_limit:
					windows_limit.append(freeslot[0])
		windows_limit=sorted(windows_limit)
		
		# on verifie pour chaque intervalle le nombre d'hote libre
		windows_ok=[]
		for i in range(len(windows_limit)-1):
			i_duration=0
			is_last=False
			while True:
				host_available=[]
				start=windows_limit[i]+i_duration*duration
				stop=start+duration
				for host, freeslots in hosts_freeslots.iteritems():
					for freeslot in freeslots:
						if start>=freeslot[0] and stop<freeslot[1]:
							host_available.append(host)
				i_duration+=1
				if len(host_available)>=resources['n_nodes']:					
					windows_ok.append((start,stop))
				else:	
					break
		
		# On fusionne les windows et on les met dans les ressources
		for i in range(len(windows_ok)-1):
			j=i+1
			if j==len(windows_ok)-1:
				break
			while True:
				if windows_ok[i][1]>=windows_ok[j][0]:
					windows_ok[i]=(windows_ok[i][0],windows_ok[j][1])
					windows_ok.pop(j)
					if j==len(windows_ok)-1:
						break
				else:
					break
			if j==len(windows_ok)-1:
				break
			
		resources['windows']=windows_ok
		# On génère un subplot
		ax=plt.subplot2grid((2,n_cluster), (0,i_cluster), title=cluster+': '+str(resources['n_nodes']))
		ax.set_xlim(startdate_stamp,enddate_stamp)
		ax.set_ylim(0,len(hosts_jobs)*0.1)
		pos=0.2
		
		for host, jobs in hosts_jobs.iteritems():
			for job in jobs:
				rects = plt.barh(pos, (job[1]-job[0]), 0.1,
            		color='r', left=job[0],edgecolor='none' )	
			pos+=0.1	
		pos=0.2
		for host, freeslots in hosts_freeslots.iteritems():
			for freeslot in freeslots:
				rects = plt.barh(pos, (freeslot[1]-freeslot[0]), 0.1,
            		color='g', left=freeslot[0],edgecolor='none' )	
			pos+=0.1
		pos=0
		for window in resources['windows']:
			rects = ax.barh(pos, (window[1]-window[0]), 0.2,
               		color='b', left=window[0],edgecolor='none' )
			rect2 = ax2.barh( 0.1*i_cluster+0.2, (window[1]-window[0]),0.1,
               		color='b', left=window[0],edgecolor='none' )
		i_cluster+=1


# On redécoupe la timeline avec les slots de chacun des clusters
windows_limit=[startdate_stamp,enddate_stamp]
for site, clusters in params.iteritems():
	for cluster, resources in clusters.iteritems():
		for freeslot in resources['windows']:
			if freeslot[0] not in windows_limit:
				windows_limit.append(freeslot[0])
windows_limit=sorted(windows_limit)

#colors='rgb'
#i=0
#for window in windows_limit:
#	color=colors[i]
#	prout = ax2.barh(0, 5000, 0.2,
#      		color=color, left=window,edgecolor='none' )
#	i=i+1 if i<2 else 0
		

# on verifie pour chaque intervalle les clusters qui sont ok
windows_ok=[]
for i in range(len(windows_limit)-1):
	
	cluster_ok=[]
	start=windows_limit[i]
	stop=enddate_stamp
	
	for site, clusters in params.iteritems():
		for cluster, resources in clusters.iteritems():
			
			for freeslot in resources['windows']:
			
				if start>=freeslot[0] and start <=freeslot[1] and cluster not in cluster_ok: 
					cluster_ok.append(cluster)
					stop=min(stop,freeslot[1])
	
	if len(cluster_ok)==n_cluster:				
		windows_ok.append((start,stop))
		
		
	
	
for window in windows_ok:
	prout = ax2.barh(0, window[1]-window[0], 0.2,
      		color='g', left=window[0],edgecolor='none' )
	
ax2.set_ylim(0,0.1*(n_cluster)+0.2)
ax2.set_xlim(startdate_stamp,enddate_stamp)
print DT.datetime.now()	
plt.show()

#ET.dump(fancygantt)

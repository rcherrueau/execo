# Copyright 2009-2013 INRIA Rhone-Alpes, Service Experimentation et
# Developpement
#
# This file is part of Execo.
#
# Execo is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Execo is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
# License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Execo.  If not, see <http://www.gnu.org/licenses/>

"""This module provides some tools to help you to plan your experiment on grid5000"""
from pprint import pformat, pprint
from operator import itemgetter
from json import loads
from time import time, localtime, mktime
from datetime import timedelta
from math import ceil
from execo.time_utils import timedelta_to_seconds, format_seconds, format_date, get_seconds, unixts_to_datetime
#import time as T, datetime as DT, execo.time_utils as ET, math
from execo.log import set_style, logger
from execo import Host, Remote
from execo_g5k import *
from execo_g5k.config import default_frontend_connexion_params
from execo_g5k.oar import format_oar_date, oar_duration_to_seconds, OarSubmission
from execo_g5k.oargrid import get_oargridsub_commandline, oargridsub
try:
    import matplotlib.pyplot as PLT
    import matplotlib.dates as MD
    
except ImportError:    
    pass

import execo_g5k.api_utils as API

error_sites = [ 'bordeaux' ]

class Planning:
    def __init__(self, elements = None, starttime = None, endtime = None, 
                 kavlan = False):
        """:param elements: a list containing the uid of the elements
  (grid5000 or site or cluster) which planning will be compute

:param starttime: a timestamp corresponding to the planning start

:param endtime: a timestamp corresponding to the planning end

:param kavlan: a boolean to ask for the global kavlan computation
"""
        logger.debug('Initializing planning computation')
        self.sites_blacklist = [ 'bordeaux' ]
        self.elements = elements
        self.startstamp = starttime
        self.endstamp = endtime
        self.with_kavlan = kavlan
        if elements is not None and starttime is not None and endtime is not None:
            self.compute()
        

    def compute(self):
        """Compute the planning from the API data and create a dict that contains all
        hosts planning sorted by {'site': {'cluster': {'host': {'free': [(start, stop)], 'busy': [(start, stop)] }}}}
        """
        if self.elements is None:
            logger.error('No elements given, aborting')
            exit()
        logger.info('%s',set_style('Getting plannings from API', 'log_header'))
        self.planning = {}

        
            
        if 'grid5000' in self.elements:
            logger.info('for the %s', set_style('whole platform', 'emph'))
            self.planning = self.grid5000(self.startstamp, self.endstamp)
        else:
            for resource in self.elements:                
                if resource in sorted(API.get_g5k_sites()):
                    log = 'for site '+ set_style(resource, 'emph')
                    self.planning[resource] = self.site(resource, self.startstamp, self.endstamp)
                else:
                    log = 'for cluster '+ set_style(resource, 'emph')
                    site = API.get_cluster_site(resource)
                    if not self.planning.has_key(site):
                        self.planning[site] = {}
                    self.planning[site][resource] = self.cluster(resource, self.startstamp, self.endstamp)
                logger.info('%s', log)


        if self.with_kavlan:
            self.kavlan_global(self.planning.keys(), self.startstamp, self.endstamp)
            
        logger.info('%s', set_style('Merging planning elements', 'log_header'))
        
        for site, clusters in self.planning.iteritems():
            if site != 'kavlan':
                for hosts in clusters.itervalues():
                    for planning in hosts.itervalues():
                        self.merge_planning(planning)
        logger.debug(pformat(self.planning))
        

    def host(self, host_reservations, startstamp = None, endstamp = None):
        """ Compute the planning of an host from the data of the monitoring API"""
        if startstamp is None:
            startstamp = self.startstamp
        if endstamp is None:
            endstamp = self.endstamp
        
        planning = {'busy': [], 'free': []}
        for job in host_reservations:
            if job['queue']!='besteffort' and (job['start_time'],job['start_time']+job['walltime']) not in planning['busy']:
                job['start_time'] = max(job['start_time'], startstamp)
                job['end_time'] = min(job['start_time']+job['walltime']+61, endstamp)
                planning['busy'].append( (job['start_time'], job['end_time']) )
        planning['busy'].sort()

        if len(planning['busy']) > 0:
            if planning['busy'][0][0] > startstamp:
                planning['free'].append((startstamp, planning['busy'][0][0]))
            for i in range(0, len(planning['busy'])-1):
                planning['free'].append((planning['busy'][i][1], planning['busy'][i+1][0]))
            if planning['busy'][len(planning['busy'])-1][1]<endstamp:
                planning['free'].append((planning['busy'][len(planning['busy'])-1][1], endstamp))
        else:
            planning['free'].append((startstamp, endstamp))
        return planning

    def cluster(self, cluster, startstamp = None, endstamp = None):
        """ Return a dict containing all alive nodes and their planning for a given cluster"""
        if startstamp is None:
            startstamp = self.startstamp
        if endstamp is None:
            endstamp = self.endstamp
        cluster_planning = {}
        site = API.get_cluster_site(cluster)
        hosts = API.get_resource_attributes('/grid5000/sites/'+site+'/clusters/'+cluster+'/status?reservations_limit=100')
        for host in hosts['items']:
            if host['hardware_state'] != 'dead':
                cluster_planning[host['node_uid']] = self.host(host['reservations'], startstamp, endstamp)
        for planning in cluster_planning.itervalues():
            self.merge_planning(planning)
        return cluster_planning


    def site(self, site, startstamp = None, endstamp = None):
        """ Return a dict containing all alive nodes and their planning for a given site"""
        if startstamp is None:
            startstamp = self.startstamp
        if endstamp is None:
            endstamp = self.endstamp
        site_planning = {}
        try:
            hosts = API.get_resource_attributes('/grid5000/sites/'+site+'/status?reservations_limit=100')
        except:
            logger.warning('Site %s is not available', site)
            return None
        
        for host in hosts['items']:
            cluster = API.get_host_cluster(host['node_uid'])
            if not site_planning.has_key(cluster):
                site_planning[cluster] = {}
            if host['hardware_state'] != 'dead':
                site_planning[cluster][host['node_uid']] = self.host(host['reservations'], startstamp, endstamp)
        tmp_planning = site_planning.copy()
        for cluster in tmp_planning.iterkeys():
            if len(tmp_planning[cluster]) == 0:
                del site_planning[cluster]
        for cluster, cluster_planning in site_planning.iteritems():
            for planning in cluster_planning.itervalues():
                self.merge_planning(planning)
        return site_planning

    def grid5000(self, startstamp = None, endstamp = None):
        """Return a dict containing all alive nodes and their planning for a the whole platform"""
        if startstamp is None:
            startstamp = self.startstamp
        if endstamp is None:
            endstamp = self.endstamp
        grid_planning = {}
        sites = sorted(API.get_g5k_sites())        
        if 'bordeaux' in sites:
            logger.warning('Site Bordeaux is closing, no reservation can be performed')
            sites.remove('bordeaux')
        for site in sites:
            site_planning = self.site(site, startstamp, endstamp)
            if site_planning is not None: 
                grid_planning[site] = site_planning 
        return grid_planning


    def kavlan_global(self, sites, startstamp, endstamp):
        """ Compute the occupation of the global KaVLAN"""
        
        
        kavlan_planning = {}            
        get_jobs = Remote('oarstat -J -f', sites, 
                    connexion_params = default_frontend_connexion_params ).run()
        for p in get_jobs.processes():
            kavlan_planning[p.host().address] =  {'busy': [], 'free':[]}
            site_jobs = loads(p.stdout())
            site_free = True
            for job_id, info in site_jobs.iteritems():                
                if 'kavlan' in info['wanted_resources']:
                    kavlan_planning[p.host().address]['busy'] =  (info['startTime'], info['scheduledStart']+info['walltime'])
        
        for site, planning in kavlan_planning.iteritems():
            if len(planning['busy']) > 0:
                if planning['busy'][0][0] > startstamp:
                    planning['free'].append((startstamp, planning['busy'][0][0]))
                for i in range(0, len(planning['busy'])-1):
                    planning['free'].append((planning['busy'][i][1], planning['busy'][i+1][0]))
                if planning['busy'][len(planning['busy'])-1][1]<endstamp:
                    planning['free'].append((planning['busy'][len(planning['busy'])-1][1], endstamp))
            else:
                planning['free'].append((startstamp, endstamp))
                
        self.planning['kavlan'] = kavlan_planning
        


    def merge_planning(self, planning):
        """Merge the different planning elements"""
        for kind in ['free', 'busy' ]:
            slots = planning[kind]
            if len(slots) > 1:
                for i in range(len(slots)):
                    j = i+1
                    if j == len(slots)-1:
                        break
                    while True:
                        condition = slots[i][1]>=slots[j][0]
                        if condition:
                            slots[i]=(slots[i][0],slots[j][1])
                            slots.pop(j)
                            if j == len(slots) - 1:
                                break
                        else:
                            break
                    if j == len(slots) - 1:
                        break

    def slots_limits(self):
        """Compute the limits of slots (defined by a host state change)"""
        if not hasattr(self, 'planning'):
            self.compute()
        self.limits = []

        for site, clusters in self.planning.iteritems():
            if site != 'kavlan':
                for hosts in clusters.values():
                    for planning in hosts.itervalues():
                        for limits in planning['busy']:
                            if limits[0] not in self.limits and limits[0] >= self.startstamp \
                                    and limits[0] < self.endstamp:
                                self.limits.append(limits[0])
                            if limits[1] not in self.limits and limits[1] >= self.startstamp \
                                    and limits[1] < self.endstamp:
                                self.limits.append(limits[1])
                        for limits in planning['free']:
                            if limits[0] not in self.limits and limits[0] >= self.startstamp \
                                    and limits[0] < self.endstamp:
                                self.limits.append(limits[0])
                            if limits[1] not in self.limits and limits[1] >= self.startstamp \
                                    and limits[1] < self.endstamp:
                                self.limits.append(limits[1])
#        if self.endstamp not in self.limits:
#            self.limits.append(self.endstamp)
        self.limits.sort()

    def merge_slots(self, slots):
        """Merge the slots"""
        if len(slots)>1:
            list_slots = list(slots)
            
            for i in range(len(slots)):
                j = i+1
                if j == len(list_slots)-1:
                    break
                while True:
                    if list_slots[i][2] == list_slots[j][2] \
                            and list_slots[i][1][1] == list_slots[j][1][1]:
                        list_slots[i] = (i, ((list_slots[i][1][0][0], list_slots[j][1][0][1]), list_slots[i][1][1]))
                        list_slots.pop(j)
                        if j == len(list_slots)-1:
                            break
                    else:
                        break
                if j == len(list_slots)-1:
                    break
            slots.clear()
            for slot in list_slots:
                slots[(slot[1][0][0], slot[1][0][1])] = slot[1][1]

    def compute_slots(self, walltime):
        """ Determine all the slots limits and find the number of available nodes for each elements"""
        
        logger.info('%s', set_style('Computing slots', 'log_header'))
        slots = []
        if not hasattr(self, 'planning'):
            self.compute()
        if not hasattr(self, 'limits'):
            self.slots_limits()
            
        
        for limit in self.limits:
            log = ''
            free_hosts = {'grid5000': 0}
            for site, site_planning in self.planning.iteritems():
                if site != 'kavlan':
                    free_hosts[site] = 0
                    for cluster, cluster_planning in site_planning.iteritems():
                        free_hosts[cluster] = 0
                        for host, host_planning in cluster_planning.iteritems():
                            host_free = False
                            
                            for free_slot in host_planning['free']:
                                if free_slot[0]<= limit and free_slot[1] > limit + oar_duration_to_seconds(walltime):
                                    host_free = True
                            if host_free:
                                free_hosts['grid5000'] += 1
                                free_hosts[site] += 1
                                free_hosts[cluster] += 1
                                log += ', '+host
                else:
                    for kavlan_site, kavlan_planning in site_planning.iteritems():
                        for free_slot in kavlan_planning['free']:
                            free_hosts['kavlan'] = kavlan_site
            
            slots.append( [ limit, limit +oar_duration_to_seconds(walltime), free_hosts] )
        
        
        self.slots = slots        
        
    

    def find_free_slots(self, walltime, resources):
        """ Find the slots with enough resources"""
        
        if not hasattr(self, 'slots'):
            self.compute_slots()
        logger.debug(pformat(resources))
        logger.info('Filtering slots with enough resources')
        duration = oar_duration_to_seconds(walltime)
        slots_ok = []
        for slot in self.slots:
            
            slot_ok = True
            for element, n_nodes in slot[2].iteritems():
                if resources.has_key(element) and resources[element] > n_nodes:
                    slot_ok = False
            if slot_ok:
                slots_ok.append(slot)
                
        return slots_ok
        
    def find_max_slot(self, walltime, resources = None):                    
        """ Find the slots that has the maximum resources""" 
        duration = oar_duration_to_seconds(walltime)
        if not hasattr(self, 'slots'):
            self.compute_slots()
    
        logger.info('Choosing slot with max nodes')
        max_slot = {}
        max = 0
        for slot in self.slots:
            if slot[1] - slot[0] >= duration:
                if slot[2]['grid5000'] > max:
                    max = slot[2]['grid5000']
                    max_slot = slot 
        
        return max_slot

def distribute_hosts(slot, resources):
    """ Distribute the resources on the different clusters """
    
    
    all_sites = API.get_g5k_sites()
    sites = []
    for site in all_sites:
        if site in slot[2].keys():
            sites.append(site)
    
    if resources.has_key('grid5000'):
        logger.info('Determining which sites to use for your reservation')
        total_nodes = 0
        sites_nodes = {}
        cluster_nodes = {}
        for site in sites:
            if resources.has_key(site):
                sites_nodes[site] = resources[site]
            else:
                sites_nodes[site] = 0
                
            for cluster in API.get_site_clusters(site):
                if cluster in resources:
                    cluster_nodes[cluster] += resources[cluster]
                    sites_nodes[site] += cluster_nodes[cluster] 
                
        while total_nodes != resources['grid5000']:
            max_site = ''
            max_nodes = 0
            for site in sites:
                start = slot[0]
                stop = slot[1]
 
                if max_nodes < slot[2][site] - sites_nodes[site]:
                    max_site = site
                    max_nodes = slot[2][site] - sites_nodes[site]
            sites_nodes[max_site] += 1
            total_nodes += 1
        resources.clear()
        for site, n_nodes in sites_nodes.iteritems():
            if n_nodes>0:
                resources[site] = n_nodes
                
                
        for cluster in API.get_site_clusters(site):
            if cluster in resources:
                cluster_nodes += resources[cluster]
                if site not in resources:
                    resources[site] = resources[cluster]
                else:
                    resources[site] += resources[cluster]
    for site in sites:
        cluster_nodes=0
        
        for cluster in API.get_site_clusters(site):
            if cluster in resources:
                cluster_nodes += resources[cluster]
                if site not in resources:
                    resources[site] = resources[cluster]
                else:
                    resources[site] += resources[cluster]
    if slot[2].has_key('kavlan'):
        resources['kavlan'] = slot[2]['kavlan']
    return resources
    
def create_reservation(startdate, resources, walltime, oargridsub_opts = '-t deploy', 
                       auto_reservation = False, prog = None):
    """ Perform the reservation for the given slot """ 
    
    subs = []
    
    logger.debug(pformat(resources))
    sites = API.get_g5k_sites()
    sites.remove('bordeaux')
    for site in sites:
        cluster_nodes = 0
        if site in resources and resources[site] > 0:
            if resources.has_key('kavlan') and site == resources['kavlan']:
                sub_resources="{type=\\'kavlan-global\\'}/vlan=1+"
            else:
                sub_resources = ''                
            for cluster in API.get_site_clusters(site): 
                if cluster in resources and resources[cluster] > 0:
                    sub_resources += "{cluster=\\'"+cluster+"\\'}/nodes="+str(resources[cluster])+'+'
                    cluster_nodes += resources[cluster]
            if resources[site]-cluster_nodes > 0:
                sub_resources+="nodes="+str(resources[site]-cluster_nodes)+'+'
            subs.append( (OarSubmission(resources=sub_resources[:-1]),site) )    
    
    
    if prog is not None:
        oargridsub_opts += ' -p '+prog
    logger.info('Reservation command: \n\033[1m%s\033[0m',
        get_oargridsub_commandline(subs, walltime = walltime, 
            additional_options = oargridsub_opts, reservation_date = format_oar_date(startdate)) )
    
    if auto_reservation:            
        reservation = 'y'
    else:            
        reservation = raw_input('Do you want me to do the reservation (y/n): ')
        
    oargrid_job_id = None
    if reservation == 'y':
        (oargrid_job_id, ssh_key) = oargridsub(subs, walltime = walltime,
                additional_options = oargridsub_opts, reservation_date = format_oar_date(startdate))
        
        if oargrid_job_id is not None:
            logger.info('Grid reservation done, oargridjob_id = %s',oargrid_job_id)
            return oargrid_job_id
        else:
            logger.info('Error in performing the reservation ')

    
    return oargrid_job_id
    
def g5k_charter_time(t):
    # - param: a unix timestamp
    # - returns a boolean, True if the given timestamp is in a period
    #   where the g5k charter needs to be respected, False if it is in
    #   a period where charter is not applicable (night, weekends)
    l = time.localtime(t)
    if l.tm_wday in [5, 6]: return False # week-end
    if l.tm_hour < 8 or l.tm_hour >= 19: return False # nuit
    return True 

def get_first_cluster_available( clusters, walltime, n_nodes = 1):
    """Compute the planning of the clusters list and find the first one available for a given walltime
    and a given number of nodes"""

    starttime = time() + timedelta_to_seconds(timedelta(seconds = 30))
    endtime = starttime + timedelta_to_seconds(timedelta(days = 3))
    planning = Planning(clusters, starttime, endtime)
    print walltime
    planning.compute_slots(walltime)
    
    first_slots = {}
    for cluster in clusters:
        slots_ok = planning.find_free_slots( walltime, {cluster: n_nodes})
        first_slots[cluster] = slots_ok[0]
    
    first_slot = [10**20, 10**21]
    
    for cluster, slot in first_slots.iteritems():
        if slot[0] <= first_slot[0]:
            first_slot = [slot[0], slot[1]]
            first_cluster = cluster

    return first_cluster, first_slot

# def get_first_site_available( sites, walltime, n_nodes = 1):
#     """Compute the planning of the sites list and find the first one available for a given walltime and
#     a given number of node """
# 
#     starttime = T.time() + ET.timedelta_to_seconds(DT.timedelta(seconds = 30))
#     endtime = starttime + ET.timedelta_to_seconds(DT.timedelta(days = 3))
#     planning = Planning(sites, starttime, endtime)
#     planning.compute_slots()
#     first_slots = {}
#     for site in sites:
#         planning.find_slots('free', walltime, {site: n_nodes})
#         start_min = 10**20
#         for slot in planning.slots_ok:
#             if slot[0] < start_min:
#                 start_min = slot[0]
#                 first_slots[site] = (slot[0], slot[1])
# 
#     first_slot = (10**20, 10**21)
#     for site, slot in first_slots.iteritems():
#         if slot[0] <= first_slot[0]:
#             first_slot = slot
#             first_site = site
# 
#     return first_site, first_slot




def set_colors():
    colors = {}
    colors['busy'] = '#CCCCCC'
    rgb_colors = [(x[0]/255., x[1]/255., x[2]/255.) for x in \
                            [(255., 122., 122.), (255., 204., 122.), (255., 255., 122.), (255., 246., 153.), (204., 255., 122.),
                            (122., 255., 122.), (122., 255., 255.), (122., 204., 255.), (204., 188., 255.), (255., 188., 255.)]]
    i_site = 0
    for site in sorted(API.get_g5k_sites()):
        colors[site] = rgb_colors[i_site]
        i_cluster = 0
        for cluster in sorted(API.get_site_clusters(site)):
            min_index = colors[site].index(min(colors[site]))
            color = [0., 0., 0.]
            for i in range(3):
                color[i] = min(colors[site][i], 1.)
                if i == min_index:
                    color[i] += i_cluster * 0.12
            colors[cluster] = tuple(color)
            i_cluster += 1
        i_site += 1
    
    return colors


def draw_gantt(planning, colors = None, show = False, save = True):
    """ Draw the hosts planning for the elements you ask (requires Matplotlib) """
    
    if colors is None:
        colors = set_colors()
    
    n_sites = len(planning.keys())
    startstamp = int(10**20)
    endstamp = 0
    slots = planning.itervalues().next().itervalues().next().itervalues().next()['busy'] +\
        planning.itervalues().next().itervalues().next().itervalues().next()['free']
    
    for slot in slots:
        if slot[0] < startstamp:  
            startstamp = slot[0]
        if slot[1] > endstamp:
            endstamp = slot[1]
        
    n_col = 2 if n_sites > 1 else 1
    n_row = int(ceil(float(n_sites) / float(n_col)))


    if endstamp - startstamp <= timedelta_to_seconds(timedelta(days=3)):
        x_major_locator = MD.HourLocator(byhour = [9, 19])
    elif endstamp - startstamp <= timedelta_to_seconds(timedelta(days=7)):
        x_major_locator = MD.HourLocator(byhour = [9])
    else:
        x_major_locator = MD.AutoDateLocator()
    xfmt = MD.DateFormatter('%d %b, %H:%M ')

    fig = PLT.figure(figsize=(15, 5 * n_row), dpi=80)

    i_site = 1
    for site, clusters in planning.iteritems():
        ax = fig.add_subplot(n_row, n_col, i_site, title = site.title())
        ax.title.set_fontsize(18)
        ax.xaxis_date()
        ax.set_xlim(unixts_to_datetime(startstamp), unixts_to_datetime(endstamp))
        ax.xaxis.set_major_formatter(xfmt)
        ax.xaxis.set_major_locator(x_major_locator )
        ax.xaxis.grid( color = 'black', linestyle = 'dashed' )
        PLT.xticks(rotation = 15)
        ax.set_ylim(0, 1)
        ax.get_yaxis().set_ticks([])
        ax.yaxis.label.set_fontsize(16)
        n_hosts = 0
        for hosts in clusters.itervalues():
            n_hosts += len(hosts)
        pos = 0.0
        inc=1./n_hosts

        ylabel = ''
        for cluster, hosts in clusters.iteritems():
            ylabel += cluster+' '
            i_host = 0
            for slots in hosts.itervalues():
                i_host +=1
                cl_colors = {'free': colors[cluster], 'busy': colors['busy']}

                for kind in cl_colors.iterkeys():
                    for freeslot in slots[kind]:
                        edate, bdate = [MD.date2num(item) for item in
                                                        (unixts_to_datetime(freeslot[1]), unixts_to_datetime(freeslot[0]))]
                        ax.barh(pos, edate - bdate , 1, left = bdate,
                                color = cl_colors[kind],  edgecolor = 'none' )
                pos += inc
                if i_host == len(hosts):
                    ax.axhline(y = pos, color = cl_colors['busy'], linestyle ='-', linewidth = 1)
        ax.set_ylabel(ylabel)
        i_site += 1
    fig.tight_layout()
    
    if show:
        PLT.show()
    if save:
        fname = 'gantt.png'
        logger.info('Saving file %s ...', fname)
        PLT.savefig (fname, dpi=300)

def draw_slots(slots, endstamp, colors = None, show = False, save = True):
    """Draw the number of nodes available for the clusters (requires Matplotlib)"""
    
    startstamp = slots[0][0]
    
        
    if colors is None:
        colors = set_colors()
    
    xfmt = MD.DateFormatter('%d %b, %H:%M ')
    
    if endstamp - startstamp <= timedelta_to_seconds(timedelta(days=7)):
        x_major_locator = MD.HourLocator(byhour = [9, 19])
    elif endstamp - startstamp <= timedelta_to_seconds(timedelta(days=17)):
        x_major_locator = MD.HourLocator(byhour = [9])
    else:
        x_major_locator = MD.AutoDateLocator()

    max_nodes = {}
    total_nodes = 0
    slot_limits = []
    total_list = []
    i_slot = 0
    for slot in slots:
        
        
        slot_limits.append(slot[0])
        if i_slot+1 < len(slots):
            slot_limits.append(slots[i_slot+1][0])
            i_slot += 1
        
        for element, n_nodes in slot[2].iteritems():
            if element in API.get_g5k_clusters():
                if not max_nodes.has_key(element):
                    max_nodes[element] = []
                max_nodes[element].append(n_nodes)
                max_nodes[element].append(n_nodes)
            if element == 'grid5000':
                total_list.append(n_nodes)
                total_list.append(n_nodes)
                if n_nodes > total_nodes:
                    total_nodes = n_nodes
    
    
    slot_limits.append(endstamp)
    
    slot_limits.sort()                
    
    dates = [unixts_to_datetime(ts) for ts in slot_limits]

    datenums = MD.date2num(dates)

    fig = PLT.figure(figsize=(15,10), dpi=80)

    ax = PLT.subplot(111)
    ax.xaxis_date()
    box = ax.get_position()
    ax.set_position([box.x0-0.07, box.y0, box.width, box.height])
    ax.set_xlim(unixts_to_datetime(startstamp), unixts_to_datetime(endstamp))
    ax.set_xlabel('Time')
    ax.set_ylabel('Nodes available')
    ax.set_ylim(0, total_nodes*1.1)
    ax.axhline(y = total_nodes, color = '#000000', linestyle ='-', linewidth = 2, label = 'ABSOLUTE MAXIMUM')
    ax.yaxis.grid(color='gray', linestyle='dashed')
    ax.xaxis.set_major_formatter(xfmt)
    ax.xaxis.set_major_locator(x_major_locator )
    PLT.xticks(rotation = 15)


    max_nodes_list = []

    p_legend = []
    p_rects = []
    p_colors = []
    for key, value in sorted(iter(max_nodes.iteritems())):
        if key != 'grid5000':
            max_nodes_list.append(value)
            p_legend.append(key)
            p_rects.append(PLT.Rectangle((0, 0), 1, 1, fc = colors[key]))
            p_colors.append(colors[key])
            
    plots = PLT.stackplot(datenums, max_nodes_list, colors = p_colors)
    PLT.legend(p_rects, p_legend, loc='center right', ncol = 1, shadow = True, bbox_to_anchor=(1.2, 0.5))

    if show:
        PLT.show()
    if save:
        fname = 'slots.png'
        logger.info('Saving file %s ...', fname)
        PLT.savefig (fname, dpi=300)

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
from time import time
from datetime import timedelta
from math import ceil
from execo.time_utils import timedelta_to_seconds, \
    unixts_to_datetime, get_unixts, datetime_to_unixts

from execo.log import style, logger
from execo import Remote
from execo_g5k import *
from execo_g5k.oar import format_oar_date, oar_duration_to_seconds, oar_date_to_unixts
from execo_g5k.oargrid import get_oargridsub_commandline
try:
    import matplotlib.pyplot as PLT
    import matplotlib.dates as MD
    
except ImportError:    
    pass

import execo_g5k.api_utils as API


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
        self.elements = elements.copy()
        self.starttime = starttime
        self.endtime = endtime
        self.with_kavlan = kavlan
        

    def compute(self, out_of_chart = False):
        """:param out_of_chart: if True, consider all resources busy during g5k charter time periods"""
        logger.info('Compiling planning from API ...')
        planning = {}
        if 'grid5000' in self.elements:
            sites = get_g5k_sites()
            self.elements.update( {site: 0 for site in sites} )
        else:
            sites = list(set([ site for site in self.elements if site in get_g5k_sites() ]+\
                    [ get_cluster_site(cluster) for cluster in self.elements 
                     if cluster in get_g5k_clusters() ]))
            
                    
        logger.debug('Will explore the planning of '+' '.join( [site for site in sites]))
        
    
        events = get_resource_attributes('status/upcoming.json')
        for event in events:
            if 'broken' in event['tags'] and event['status'] != 'RESOLVED':
                broken_site = list(set(sites) & set(event['tags']))
                if len(broken_site) > 0:
                    logger.warn('Site '+broken_site[0]+' is broken ..')
                    sites.remove(broken_site[0])

        for site in sites:
            try:
                planning[site] = {}
                dead_nodes = [ node for node, status in \
                    get_resource_attributes('/sites/'+site+'/status')['nodes'].iteritems() if status['hard'] == 'dead' ]
                for cluster in get_site_clusters(site):
                    if cluster in self.elements or get_cluster_site(cluster) in self.elements:
                        planning[site][cluster] = {}
                        for host in sorted(get_cluster_hosts(cluster), key = lambda name: int( name.split('.',1)[0].split('-')[1] )):
                            if host not in dead_nodes:
                                planning[site][cluster][host] = {'busy': [], 'free': []}

                if self.with_kavlan:
                    vlans = [x for x in sorted(map(is_a_kavlan, get_vlans(site).itervalues() )) if x is not None]
                    planning[site]['kavlan'] = {}
                    for vlan in vlans:
                        planning[site]['kavlan'][vlan] = {'busy': [], 'free': []}

                jobs_links = [ link['href'] for job in filter(rm_besteffort, \
                       get_resource_attributes('/sites/'+site+'/jobs?state=waiting,launching,running')['items']) \
                       for link in job['links'] if link['rel'] == 'self' ]
                
                logger.info( style.emph(site.ljust(10))+str( len( jobs_links ) ).rjust(5)+" jobs" )

                for link in jobs_links:
                    attr = get_resource_attributes('/'+str(link).split('/', 2)[2])
                    logger.debug(pformat(attr))
                    try:
                        start_time = attr['started_at'] if attr['started_at'] != 0 else attr['scheduled_at']
                        end_time = start_time + attr['walltime']+timedelta_to_seconds(timedelta(minutes = 1, seconds =5))
                    except:
                        logger.warning('job '+str(attr['uid'])+' has not been scheduled')
                        pass
                    nodes = attr['assigned_nodes']
                    
                    for node in sorted(nodes, key = lambda name: int( name.split('.',1)[0].split('-')[1] )):
                        cluster = node.split('.',1)[0].split('-')[0]
                        
                        if planning[site].has_key(cluster) and planning[site][cluster].has_key(node):
                            planning[site][cluster][node]['busy'].append( (start_time, end_time))

                    if self.with_kavlan:
                        if attr['resources_by_type'].has_key('vlans'):
                            vlan = attr['resources_by_type']['vlans'][0]
                            planning[site]['kavlan']['kavlan-'+vlan]['busy'].append( (start_time, end_time))

            except APIGetException, e:
                logger.warn("API request to %s failed. uri=%r response=%s, content=%r" % (site, e.uri, e.response, e.content))
                logger.warn('Site '+site+' is broken ..')
                continue

        logger.info('Computation')
        if out_of_chart:
            charter_el_planning = get_charter_el_planning(self.starttime, self.endtime)
        for site, clusters_kavlan in planning.iteritems():
            for cluster_kavlan, elements in clusters_kavlan.iteritems():
                for elements, el_planning in elements.iteritems():
                    if out_of_chart:
                        el_planning['busy'] += charter_el_planning
                        for i in range(len(el_planning['busy'])):
                            j = i+1
                            if j == len(el_planning['busy'])-1:
                                break
                            while True:
                                condition = el_planning['busy'][i][1] >= el_planning['busy'][j][0]
                                if condition:
                                    el_planning['busy'].pop(j)
                                    if j == len(el_planning['busy']) - 1:
                                        break
                                else:
                                    break
                            if j == len(el_planning['busy']) - 1:
                                break
                    
                    el_planning['busy'].sort()
                    
                    if len(el_planning['busy']) > 0:
                        if el_planning['busy'][0][0] > self.starttime:
                            el_planning['free'].append((self.starttime, el_planning['busy'][0][0]))
                        for i in range(0, len(el_planning['busy'])-1):
                            el_planning['free'].append((el_planning['busy'][i][1], el_planning['busy'][i+1][0]))
                        if el_planning['busy'][len(el_planning['busy'])-1][1] < self.endtime:
                            el_planning['free'].append((el_planning['busy'][len(el_planning['busy'])-1][1], self.endtime))
                    else:
                        el_planning['free'].append((self.starttime, self.endtime))
                    
                    
                    for kind in ['free', 'busy' ]:
                        slots = el_planning[kind]
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
                       
        self.planning = planning

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
                            if limits[0] not in self.limits and limits[0] >= self.starttime \
                                    and limits[0] < self.endtime:
                                self.limits.append(limits[0])
                            if limits[1] not in self.limits and limits[1] >= self.starttime \
                                    and limits[1] < self.endtime:
                                self.limits.append(limits[1])
                        for limits in planning['free']:
                            if limits[0] not in self.limits and limits[0] >= self.starttime \
                                    and limits[0] < self.endtime:
                                self.limits.append(limits[0])
                            if limits[1] not in self.limits and limits[1] >= self.starttime \
                                    and limits[1] < self.endtime:
                                self.limits.append(limits[1])
#        if self.endtime not in self.limits:
#            self.limits.append(self.endtime)
        self.limits.sort()

    def merge_slots(self, slots):
        """Merge the slots"""
        if len(slots)>1:
            list_slots = list(slots)
            
            for i in range(len(slots)):
                j = i+1
                if j == len(list_slots)-1:
                    break
                logger.debug(pformat(list_slots[i])+'\n'+pformat(list_slots[j]))
                while True:
                    if list_slots[i][2] == list_slots[j][2] \
                            and list_slots[i][1] == list_slots[j][0]:
                        list_slots[i] = [list_slots[i][0], list_slots[j][1], list_slots[i][2] ]
                        list_slots.pop(j)
                        if j == len(list_slots)-1:
                            break
                    else:
                        break
                if j == len(list_slots)-1:
                    break
            
            slots = list_slots

    def compute_slots(self, walltime):
        """ Determine all the slots limits and find the number of available nodes for each elements"""
        
        logger.info('%s', style.log_header('Computing slots'))
        slots = []
        if not hasattr(self, 'planning'):
            self.compute()
        if not hasattr(self, 'limits'):
            self.slots_limits()
        
        for limit in self.limits:
            log = ''
            free_hosts = {'grid5000': 0}
            for site, site_planning in self.planning.iteritems():
                
                free_hosts[site] = 0
                for cluster, cluster_planning in site_planning.iteritems():
                    if cluster != 'kavlan':
                        free_hosts[cluster] = 0
                        for host, host_planning in cluster_planning.iteritems():
                            host_free = False
                            
                            for free_slot in host_planning['free']:
                                if free_slot[0] <= limit and free_slot[1] > limit + oar_duration_to_seconds(walltime):
                                    host_free = True
                            if host_free:
                                free_hosts['grid5000'] += 1
                                free_hosts[site] += 1
                                free_hosts[cluster] += 1
                                log += ', '+host
                    else:
                        for kavlan, kavlan_planning in cluster_planning.iteritems():
                            for free_slot in kavlan_planning['free']:
                                free_hosts['kavlan'] = kavlan
            
            slots.append( [ limit, limit +oar_duration_to_seconds(walltime), free_hosts] )
        
        slots.sort()
        logger.debug(pformat(slots))
        self.slots = slots        
        
    

    def find_free_slots(self, walltime, resources):
        """ Find the slots with enough resources"""
        
        if not hasattr(self, 'slots'):
            self.compute_slots(walltime)
        logger.debug(pformat(resources))
        logger.info('Filtering slots with enough '+style.emph('nodes') )

        slots_ok = []
        for slot in self.slots:
            slot_ok = True
            for element, n_nodes in slot[2].iteritems():
                if resources.has_key(element) and resources[element] > n_nodes:
                    slot_ok = False
            if slot_ok:
                slots_ok.append(slot)
        logger.debug(pformat(slots_ok))
        return slots_ok
        
    def find_max_slot(self, walltime, resources = None):                    
        """ Find the slots that has the maximum resources""" 
        duration = oar_duration_to_seconds(walltime)
        if not hasattr(self, 'slots'):
            self.compute_slots(walltime)
    
        logger.info('Choosing slot with max nodes')
        max_slot = {}
        max_nodes = 0
        for slot in self.slots:
            if slot[1] - slot[0] >= duration and  slot[2]['grid5000'] > max_nodes:
                    max_nodes = slot[2]['grid5000']
                    max_slot = slot 
        
        return max_slot

def self_link(link): 
    if link['rel'] == 'self': 
        return link['href'] 
def rm_besteffort(job): return job['queue'] != 'besteffort'
def get_job_link(job): return filter(self_link, job['links'])
def get_vlans(site):
    equips = get_resource_attributes('/sites/'+site+'/network_equipments/')
    for equip in equips['items']:
        if equip.has_key('vlans') and len(equip['vlans']) >2:
            return equip['vlans']
def is_a_kavlan(vlan):
    if type(vlan) == type({}) and vlan.has_key('name') and 'kavlan' in vlan['name']:
        return vlan['name']
def node_number(name): return int(name.split('.')[0].split('-')[1])


def distribute_hosts(slot, resources_wanted):
    """ Distribute the resources on the different clusters """

    resources = {}
    all_sites = API.get_g5k_sites()
    sites = []
    for site in all_sites:
        if site in slot[2].keys():
            sites.append(site)
    logger.debug('Resources wanted '+pformat(resources_wanted))        
    logger.debug('Distributing hosts on slot '+pformat(slot))

    if resources_wanted.has_key('grid5000'):
        logger.info('Determining which sites to use for your reservation')
        total_nodes = 0
        sites_nodes = {}
        cluster_nodes = {}
        for site in sites:
            if resources_wanted.has_key(site):
                sites_nodes[site] = resources_wanted[site]
            else:
                sites_nodes[site] = 0
            for cluster in API.get_site_clusters(site):
                if cluster in resources_wanted:
                    cluster_nodes[cluster] = resources_wanted[cluster]
                else:
                    cluster_nodes[cluster] = 0
                sites_nodes[site] += cluster_nodes[cluster]
                
        logger.debug('sites_nodes:\n'+pformat(sites_nodes))
        logger.debug('cluster_nodes:\n'+pformat(cluster_nodes))

        
        while total_nodes != resources_wanted['grid5000']:
            max_site = ''
            max_nodes = 0
            for site in sites:
                if max_nodes < slot[2][site] - sites_nodes[site]:
                    max_site = site
                    max_nodes = slot[2][site] - sites_nodes[site]
            sites_nodes[max_site] += 1
            total_nodes += 1
        logger.debug('sites_nodes:\n'+pformat(sites_nodes))
        logger.debug('cluster_nodes:\n'+pformat(cluster_nodes))


        for site, n_nodes in sites_nodes.iteritems():
            if n_nodes > 0:
                resources_wanted[site] = n_nodes

    for site in sites:
        if resources_wanted.has_key(site):
            resources[site] = resources_wanted[site]
        for cluster in API.get_site_clusters(site):
            if cluster in resources_wanted:
                resources[cluster] =resources_wanted[cluster]
                if get_cluster_site(cluster) not in resources:
                    resources[site] = 0
            
    if slot[2].has_key('kavlan'):
        resources['kavlan'] = slot[2]['kavlan']

    logger.debug('Resources are '+pformat(resources))
    return resources
    
def create_reservation(startdate, resources, walltime, oargridsub_opts = '',
                       auto_reservation = False, prog = None, name = ''):
    """ Perform the reservation for the given set of resources """ 
    get_kavlan = resources.has_key('kavlan')
    subs = []
    logger.debug(pformat(resources))
    sites = API.get_g5k_sites()
    n_sites = 0
    for resource in resources.iterkeys():
        if resource in sites:
            n_sites += 1
    for site in sites:
        sub_resources = ''
        if site in resources:
            clusters_nodes = 0
            if get_kavlan: 
                if n_sites > 1:
                    sub_resources="{type=\\'kavlan-global\\'}/vlan=1+"
                else:
                    sub_resources="{type=\\'kavlan\\'}/vlan=1+"
                get_kavlan = False
                                
            for cluster in API.get_site_clusters(site): 
                if cluster in resources and resources[cluster] > 0:
                    sub_resources += "{cluster=\\'"+cluster+"\\'}/nodes="+str(resources[cluster])+'+'
                    clusters_nodes += resources[cluster]
                    
            if resources[site]-clusters_nodes > 0:
                sub_resources+="nodes="+str(resources[site])+'+'
                
            logger.debug( site+': '+sub_resources)
            if sub_resources != '':
                subs.append( (OarSubmission(resources = sub_resources[:-1], name = name), site) )    
    
    logger.debug(pformat(subs))
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
        (oargrid_job_id, _) = oargridsub(subs, walltime = walltime,
                additional_options = oargridsub_opts, reservation_date = format_oar_date(startdate))
        
        if oargrid_job_id is not None:
            logger.info('Grid reservation done, oargridjob_id = %s',oargrid_job_id)
            return oargrid_job_id
        else:
            logger.error('Error in performing the reservation ')

    
    return oargrid_job_id


def g5k_charter_time(dt):
    """Is the given datetime in a g5k charter time period?

    Returns a boolean, True if the given datetime is in a period where
    the g5k charter needs to be respected, False if it is in a period
    where charter is not applicable (night, weekends)

    :param dt: a datetime.datetime
    """
    if dt.weekday() in [5, 6]: return False
    if dt.hour < 9 or dt.hour >= 19: return False
    return True

def get_next_charter_period(start, end):
    """Return the first g5k charter time period.

    :param start: datetime.datetime from which to start searching for
      the next g5k charter time period. If start is in a g5k charter
      time period, the returned g5k charter time period starts at
      start.

    :param end: datetime.datetime until which to search for the next
      g5k charter time period. If end is in the g5k charter time
      period, the returned g5k charter time period ends at end.

    :returns: a tuple (charter_start, charter_end) of
      datetime.datetime. (None, None) if no g5k charter time period
      found
    """
    if end <= start:
        return None, None
    elif g5k_charter_time(start):
        charter_end = start.replace(hour = 19, minute = 0, second = 0, microsecond = 0)
        if charter_end > end: charter_end = end
        return start, charter_end
    else:
        wd = start.weekday()
        if (wd == 4 and start.hour >= 19) or (wd > 4):
            charter_start = start.replace(hour = 9, minute = 0, second = 0, microsecond = 0) + timedelta(days = 7 - wd)
        elif start.hour < 9:
            charter_start = start.replace(hour = 9, minute = 0, second = 0, microsecond = 0)
        else:
            charter_start = start.replace(hour = 9, minute = 0, second = 0, microsecond = 0) + timedelta(days = 1)
        if charter_start > end:
            return None, None
        charter_end = charter_start.replace(hour = 19, minute = 0, second = 0, microsecond = 0)
        if charter_end > end:
            charter_end = end
        return charter_start, charter_end

def get_charter_el_planning(start_time, end_time):
    """Returns the list of tuples (start, end) of g5k charter time periods between start_time and end_time.

    :param start_time: a date in one of the types supported by
      time_utils

    :param end_time: a date in one of the types supported by
      time_utils
    """
    start_time = unixts_to_datetime(get_unixts(start_time))
    end_time = unixts_to_datetime(get_unixts(end_time))
    el_planning = []
    while True:
        charter_start, charter_end = get_next_charter_period(start_time, end_time)
        if charter_start == None: break
        el_planning.append((datetime_to_unixts(charter_start), datetime_to_unixts(charter_end)))
        start_time = charter_end
    return el_planning

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


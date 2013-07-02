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
import time as T, datetime as DT, execo.time_utils as ET, math
from execo.log import set_style, logger
from execo import Host, Remote
from execo_g5k.config import default_frontend_connexion_params
from execo_g5k.oar import format_oar_date, oar_duration_to_seconds, OarSubmission
from execo_g5k.oargrid import get_oargridsub_commandline, oargridsub

import execo_g5k.api_utils as API



class Planning:

    def __init__(self, elements = None, starttime = None, endtime = None, with_kavlan = False):
        """:param elements: a list containing the uid of the elements
  (grid5000.fr or site or cluster) which planning will be compute

:param starttime: a timestamp corresponding to the planning start

:param endtime: a timestamp corresponding to the planning end
"""
        logger.debug('Initializing planning computation')
        self.elements = elements
        self.startstamp = starttime
        self.endstamp = endtime
        self.with_kavlan = with_kavlan
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

        if 'grid5000.fr' in self.elements:
            logger.info('for the %s', set_style('whole platform \n', 'emph'))
            self.planning = self.grid5000(self.startstamp, self.endstamp)
        else:
            for resource in self.elements:
                if resource in API.get_g5k_sites():
                    log = 'for site '+ set_style(resource, 'site')
                    self.planning[resource] = self.site(resource, self.startstamp, self.endstamp)
                else:
                    log = 'for cluster '+ set_style(resource, 'emph')
                    site = API.get_cluster_site(resource)
                    if not self.planning.has_key(site):
                        self.planning[site] = {}
                    self.planning[site][resource] = self.cluster(resource, self.startstamp, self.endstamp)
                logger.info('%s', log)


        if self.with_kavlan:
            self.kavlan(self.startstamp, self.endstamp)
            
        logger.info('%s', set_style('Merging planning elements ..', 'log_header'))
        for clusters in self.planning.itervalues():
            for hosts in clusters.itervalues():
                for planning in hosts.itervalues():
                    self.merge_planning(planning)
        logger.debug(pformat(self.planning))
        

    def host(self, host_reservations, startstamp = None, endstamp = None):
        if startstamp is None:
            startstamp = self.startstamp
        if endstamp is None:
            endstamp = self.endstamp
        """ Compute the planning from the dict of reservations gathered from the API"""
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
        hosts = API.get_resource_attributes('/grid5000/sites/'+site+'/status?reservations_limit=100')
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
        sites = API.get_g5k_sites()
        for site in sites:
            grid_planning[site] = self.site(site, startstamp, endstamp)
        return grid_planning


    def kavlan(self, startstamp, endstamp):
        """ Add to the planning"""
        if self.planning is None:
            self.planning = {}
        get_jobs = Remote('oarstat -J -f', [ Host(site+'.grid5000.fr') for site in self.sites], 
                    connexion_params = default_frontend_connexion_params ).run()
        for p in get_jobs.processes():
            site_jobs = loads(p.stdout())
            site_free = True
            for job_id, info in site_jobs.iteritems():
                if 'kavlan-global' in info['wanted_resources']:
                    site_free = False 
            if site_free:
                kavlan_site = p.host().address.split('.')[0]
        


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
        """Compute the limits of slots"""
        if not hasattr(self, 'planning'):
            self.compute()
        self.limits = []

        for clusters  in self.planning.values():
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

    def compute_slots(self):
        """ Determine all the slots limits and find the number of available nodes for each elements"""
        if not hasattr(self, 'planning'):
            self.compute()
        logger.info('%s', set_style('Computings slots', 'log_header'))
        slots = []
        self.slots_limits()
        self.duration = 3600
        for i in range(len(self.limits)-1):
            slot =  [self.limits[i], self.limits[i+1], {} ] 
            for site, clusters  in self.planning.iteritems():
                slot[2][site] = 0
                for cluster, hosts in clusters.iteritems():
                    slot[2][cluster] = 0
                    for host in hosts.itervalues():
                        for freeslot in host['free']:
                            if self.limits[i] >= freeslot[0] and self.limits[i] + self.duration <= freeslot[1]:
                                slot[2][cluster] += 1
                    slot[2][site] += slot[2][cluster]
            slots.append(slot)
        
        slots.sort(key = itemgetter(0))
        
        g5k_clusters = API.get_g5k_clusters()
        for slot in slots:
            slot[2]['grid5000.fr'] = 0
            for element, n_nodes in slot[2].iteritems():
                if element in g5k_clusters:
                    slot[2]['grid5000.fr'] += n_nodes

        self.slots = slots
        

    def find_free_slots(self, walltime, resources):
        """ Find the slots with enough resources"""
        
        if not hasattr(self, 'slots'):
            self.compute_slots()
        
        
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
        """ """ 
        duration = oar_duration_to_seconds(walltime)
        if not hasattr(self, 'slots'):
            self.compute_slots()

        logger.info('Choosing slot with max nodes')
        max_slot = {}
        max = 0
        for slot in self.slots:
            if slot[1] - slot[0] >= duration:
                if slot[2]['grid5000.fr'] > max:
                    max = slot[2]['grid5000.fr']
                    max_slot = slot 
        
        return max_slot


#def create_reservation(resources, date, kavlan = None, oargridsub_opts = '-t deploy', auto_reservation = False):
#    subs=[]
#    getkavlan = False
#    if kavlan is not None:
#        getkavlan = True
#    
#    if resources.has_key('grid5000.fr'):
#        logger.info('Determining which sites to use for your reservation')
#        total_nodes = 0
#        sites_nodes = {}
#        sites = API.get_g5k_sites()
#        for site in sites:
#            if resources.has_key(site):
#                sites_nodes[site] = resources[site]
#            else:
#                sites_nodes[site]=0
#        while total_nodes != resources['grid5000.fr']:
#            max_site=''
#            max_nodes=0
#            for site in sites:
#                #hosts = [ [ host for host in cluster ] for cluster in self.get_hosts_element(site) ]
#                host_available = 0
#                start=int(T.mktime(self.chosen_slot.timetuple()))
#                if start+self.duration <self.enddate_stamp:
#                    stop=start+self.duration 
#                else:
#                    stop=self.enddate_stamp
#                for slots in hosts.itervalues():
#                    for freeslot in slots['free']:
#                        if start>=freeslot[0] and stop<=freeslot[1]:
#                            host_available+=1
#                if max_nodes<host_available-sites_nodes[site]:
#                    max_site=site
#                    max_nodes=host_available-sites_nodes[site]
#            sites_nodes[max_site]+=1
#            total_nodes+=1
#        self.resources.clear()
#        for site, n_nodes in sites_nodes.iteritems():
#            if n_nodes>0:
#                self.resources[site]=n_nodes
#
#    pprint(self.resources)
#    for site in self.sites:
#        sub_resources=''
#        cluster_nodes=0
#        
#        if getkavlan:
#            sub_resources="{type=\\'kavlan-global\\'}/vlan=1+"
#        if self.vlan is not None:
#            sub_resources+=self.vlan+'+'
#            
#        
#        for cluster in API.get_site_clusters(site):
#            if cluster in self.resources:
#                sub_resources+="{cluster=\\'"+cluster+"\\'}/nodes="+str(self.resources[cluster])+'+'
#                cluster_nodes+=self.resources[cluster]
#                if site not in self.resources:
#                    self.resources[site]=self.resources[cluster]
#                else:
#                    self.resources[site]+=self.resources[cluster]
#            
#        if site in self.resources:
#            sub_resources+="nodes="+str(self.resources[site]-cluster_nodes)+'+'
#            subs.append( (OarSubmission(resources=sub_resources[:-1]),site) )
#    
#    self.logger.info('Reservation command: \n\033[1m%s\033[0m',
#                    get_oargridsub_commandline(subs,walltime=self.walltime,additional_options=oargridsub_opts,reservation_date=self.chosen_slot))
#    
#    if auto_reservation:            
#        (self.oargrid_job_id, self.ssh_key)=EX5.oargridsub(subs,walltime=self.walltime,additional_options=oargridsub_opts,reservation_date=self.chosen_slot)
#        self.logger.info('Grid reservation done, oargridjob_id = %s',self.oargrid_job_id)
#    else:            
#        reservation=raw_input('Do you want me to do the reservation (y/n): ')
#        if reservation=='y':
#            (self.oargrid_job_id, self.ssh_key)=EX5.oargridsub(subs,walltime=self.walltime,additional_options=oargridsub_opts,reservation_date=self.chosen_slot)    
#            self.logger.info('Grid reservation done, oargridjob_id = %s',self.oargrid_job_id)


def get_first_cluster_available( clusters, walltime, n_nodes = 1):
    """Compute the planning of the clusters list and find the first one available for a given walltime
    and a given number of nodes"""

    starttime = T.time() + ET.timedelta_to_seconds(DT.timedelta(seconds = 30))
    endtime = starttime + ET.timedelta_to_seconds(DT.timedelta(days = 3))
    planning = Planning(clusters, starttime, endtime)
    planning.compute_slots()
    
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



#
#def set_colors(self):
#
#    self.outsuffix = ET.format_date( self.startstamp ).replace('-', '').replace(' ','_').replace(':','')
#
#    self.colors = {}
#    self.colors['busy'] = '#CCCCCC'
#    rgb_colors = [(x[0]/255., x[1]/255., x[2]/255.) for x in \
#                            [(255., 122., 122.), (255., 204., 122.), (255., 255., 122.), (255., 246., 153.), (204., 255., 122.),
#                            (122., 255., 122.), (122., 255., 255.), (122., 204., 255.), (204., 188., 255.), (255., 188., 255.)]]
#    i_site = 0
#    for site in sorted(API.get_g5k_sites()):
#        self.colors[site] = rgb_colors[i_site]
#        i_cluster = 0
#        for cluster in sorted(API.get_site_clusters(site)):
#            min_index = self.colors[site].index(min(self.colors[site]))
#            color = [0., 0., 0.]
#            for i in range(3):
#                color[i] = min(self.colors[site][i], 1.)
#                if i == min_index:
#                    color[i] += i_cluster * 0.12
#            self.colors[cluster] = tuple(color)
#            i_cluster += 1
#        i_site += 1


#def draw_gantt(self, show = False, save = False):
#    """ Draw the hosts planning for the element you ask (requires Matplotlib)"""
#    self.init_plots()
#    n_sites = len(self.planning.keys())
#    n_col = 2 if n_sites > 1 else 1
#    n_row = int(math.ceil(float(n_sites) / float(n_col)))
#
#    if self.endstamp - self.startstamp <= ET.timedelta_to_seconds(DT.timedelta(days=3)):
#        x_major_locator = MD.HourLocator(byhour = [9, 19])
#    elif self.endstamp - self.startstamp <= ET.timedelta_to_seconds(DT.timedelta(days=7)):
#        x_major_locator = MD.HourLocator(byhour = [9])
#    else:
#        x_major_locator = MD.AutoDateLocator()
#    xfmt = MD.DateFormatter('%d %b, %H:%M ')
#
#    fig = PLT.figure(figsize=(15, 5 * n_row), dpi=80)
#
#    i_site = 1
#    for site, clusters in self.planning.iteritems():
#        ax = fig.add_subplot(n_row, n_col, i_site, title = site.title())
#        ax.title.set_fontsize(18)
#        ax.xaxis_date()
#        ax.set_xlim(ET.unixts_to_datetime(self.startstamp), ET.unixts_to_datetime(self.endstamp))
#        ax.xaxis.set_major_formatter(xfmt)
#        ax.xaxis.set_major_locator(x_major_locator )
#        ax.xaxis.grid( color = 'black', linestyle = 'dashed' )
#        PLT.xticks(rotation = 15)
#        ax.set_ylim(0, 1)
#        ax.get_yaxis().set_ticks([])
#        ax.yaxis.label.set_fontsize(16)
#        n_hosts = 0
#        for hosts in clusters.itervalues():
#            n_hosts += len(hosts)
#        pos = 0.0
#        inc=1./n_hosts
#
#        ylabel = ''
#        for cluster, hosts in clusters.iteritems():
#            ylabel += cluster+' '
#            i_host = 0
#            for slots in hosts.itervalues():
#                i_host +=1
#                colors = {'free': self.colors[cluster], 'busy': self.colors['busy']}
#
#                for kind in colors.iterkeys():
#                    for freeslot in slots[kind]:
#                        edate, bdate = [MD.date2num(item) for item in
#                                                        (ET.unixts_to_datetime(freeslot[1]), ET.unixts_to_datetime(freeslot[0]))]
#                        ax.barh(pos, edate - bdate , 1, left = bdate,
#                                color = colors[kind],  edgecolor = 'none' )
#                pos += inc
#                if i_host == len(hosts):
#                    ax.axhline(y = pos, color = self.colors['busy'], linestyle ='-', linewidth = 1)
#        ax.set_ylabel(ylabel)
#        i_site += 1
#    fig.tight_layout()
#    if show:
#        PLT.show()
#    if save:
#        fname = 'gantt-'+self.outsuffix+'.png'
#        logger.info('Saving file %s ...', fname)
#        PLT.savefig (fname, dpi=300)
#
#def draw_max_nodes(self, show = False, save = False):
#    """Draw the number of nodes available for the clusters (requires Matplotlib)"""
#    self.init_plots()
#    xfmt = MD.DateFormatter('%d %b, %H:%M ')
#    if self.endstamp - self.startstamp <= ET.timedelta_to_seconds(DT.timedelta(days=7)):
#        x_major_locator = MD.HourLocator(byhour = [9, 19])
#    elif self.endstamp - self.startstamp <= ET.timedelta_to_seconds(DT.timedelta(days=17)):
#        x_major_locator = MD.HourLocator(byhour = [9])
#    else:
#        x_major_locator = MD.AutoDateLocator()
#
#
#    max_nodes = {}
#    total_nodes = 0
#    slot_limits = []
#    total_list = []
#    for slot, elements in iter(sorted(self.slots.iteritems())):
#        slot_limits.append(slot[0])
#        slot_limits.append(slot[1]-1)
#        for element, n_nodes in elements.iteritems():
#            if element in API.get_g5k_clusters():
#                if not max_nodes.has_key(element):
#                    max_nodes[element] = []
#                max_nodes[element].append(n_nodes)
#                max_nodes[element].append(n_nodes)
#            if element == 'grid5000.fr':
#                total_list.append(n_nodes)
#                total_list.append(n_nodes)
#                if n_nodes > total_nodes:
#                    total_nodes = n_nodes
#    slot_limits.sort()
#    dates = [ET.unixts_to_datetime(ts) for ts in slot_limits]
#
#    datenums = MD.date2num(dates)
#
#    fig = PLT.figure(figsize=(15,10), dpi=80)
##               PLT.title('Choose your slot')
#    ax = PLT.subplot(111)
#    ax.xaxis_date()
#    box = ax.get_position()
#    ax.set_position([box.x0-0.07, box.y0, box.width, box.height])
#    ax.set_xlim(ET.unixts_to_datetime(min(self.limits)), ET.unixts_to_datetime(max(self.limits)))
#    ax.set_xlabel('Time')
#    ax.set_ylabel('Nodes available')
#    ax.set_ylim(0, total_nodes*1.1)
#    ax.axhline(y = total_nodes, color = '#000000', linestyle ='-', linewidth = 2, label = 'ABSOLUTE MAXIMUM')
#    ax.yaxis.grid(color='gray', linestyle='dashed')
#    ax.xaxis.set_major_formatter(xfmt)
#    ax.xaxis.set_major_locator(x_major_locator )
#    PLT.xticks(rotation = 15)
#
#
#    max_nodes_list = []
#
#    p_legend = []
#    p_rects = []
#    p_colors = []
#    for key, value in iter(sorted(max_nodes.iteritems())):
#        if key != 'grid5000.fr':
#            max_nodes_list.append(value)
#            p_legend.append(key)
#            p_rects.append(PLT.Rectangle((0, 0), 1, 1, fc = self.colors[key]))
#            p_colors.append(self.colors[key])
#    plots = PLT.stackplot(datenums, max_nodes_list, colors = p_colors)
#    PLT.legend(p_rects, p_legend, loc='center right', ncol = 1, shadow = True, bbox_to_anchor=(1.2, 0.5))
#
#
#    if show:
#        PLT.show()
#    if save:
#        fname = 'max_nodes-'+self.outsuffix+'.png'
#        logger.info('Saving file %s ...', fname)
#        PLT.savefig (fname, dpi=300)

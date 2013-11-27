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
"""Module provides functions to help you to plan your experiment on Grid'5000.
"""
from socket import getfqdn
from pprint import pprint
from time import time
from datetime import timedelta
from math import ceil

from execo import logger
from execo_g5k import OarSubmission, oargridsub
from execo.time_utils import timedelta_to_seconds, \
    unixts_to_datetime, get_unixts, datetime_to_unixts
from execo_g5k.api_utils import get_g5k_sites, get_g5k_clusters, get_cluster_site, \
    get_site_clusters, get_resource_attributes, get_host_cluster
from execo_g5k.oar import oar_duration_to_seconds, format_oar_date
from execo_g5k.oargrid import get_oargridsub_commandline

try:
    import matplotlib as MPL
    MPL.use('Agg')  
    import matplotlib.pyplot as PLT
    import matplotlib.dates as MD  
    
except ImportError:    
    pass


planning = None

_retrieve_method = None

if 'grid5000.fr' in getfqdn():
    try:
        import MySQLdb
        _retrieve_method = 'MySQL'
    except:
        pass
        _retrieve_method = 'API'
else:
    _retrieve_method = 'API'


def get_planning(elements = ['grid5000'], vlan = False, subnet = False, storage = False, 
            out_of_chart = False, starttime = int(time()+timedelta_to_seconds(timedelta(minutes = 1))), 
            endtime = int(time()+timedelta_to_seconds(timedelta(weeks = 4, minutes = 1))) ):
    """Retrieve the planning of the elements (site, cluster) and others resources.
    Element planning structure is ``{'busy': [(123456,123457), ... ], 'free': [(123457,123460), ... ]}.`` 
    
    :param elements: a list of Grid'5000 elemenst (grid5000, site, cluster)

    :param vlan: a boolean to ask for KaVLAN computation
    
    :param subnet: a boolean to ask for subnets computation
    
    :param storage: a boolean to ask for sorage computation 
    
    :param weeks: the number of weeks in the future for the end of the planning
    
    :param out_of_chart: if True, consider that days outside weekends are busy
    
    Return a dict whose keys are sites, whose values are dict whose keys 
    are cluster, subnets, kavlan or storage, 
    whose values are planning dicts, whose keys are hosts, subnet address range, 
    vlan number or chunk id planning respectively. 
    """
    
    global planning
    
    if 'grid5000' in elements:
        sites = get_g5k_sites()
    else:
        sites = list(set([ site for site in elements if site in get_g5k_sites() ]+\
                    [ get_cluster_site(cluster) for cluster in elements 
                     if cluster in get_g5k_clusters() ]))

    planning = {site: { cluster: {} for cluster in get_site_clusters(site)} for site in sites}    
    
    for site in sites:
        if vlan:
            planning[site].update( { 'vlans': {} } )
        if subnet:
            planning[site].update( { 'subnets': {} } )
        if storage:
            planning[site].update( { 'storage': {} } )
    
    if _retrieve_method == 'API':
        _get_planning_API()
    elif _retrieve_method == 'MySQL':
        _get_planning_MySQL()
        
    if out_of_chart:
        _add_charter_to_planning(starttime, endtime)
    
    for site_pl in planning.itervalues():
        for res_pl in site_pl.itervalues():
            for el_planning in res_pl.itervalues():
                _merge_el_planning(el_planning['busy'])
                # VOIR AVEC MATT
                el_planning['busy'] = _trunc_el_planning(el_planning['busy'], starttime, endtime)
                _fill_el_planning_free(el_planning, starttime, endtime)
    return planning

def compute_slots(planning, walltime):
    """Compute the slots limits and find the number of available nodes for 
    each elements and for the given walltime. 
    
    :param planning: a dict of the resources planning, returned by ``get_planning``
    
    :param walltime: a duration at OAR duration format ``5:00:00`` where the resources are available
    
    Return the list of slots where a slot is ``[ start, stop, freehosts ]`` and
    freehosts is a dict of Grid'5000 element with number of nodes available 
    ``{'grid5000': 30, 'lyon': 20, 'reims': 10, 'stremi': 10 }``.
    """
    slots = []
    limits = _slots_limits(planning)
    
    # Checking if we need to compile vlans planning
    kavlan = False
    kavlan_global = False
    if 'vlans' in (planning.itervalues().next().keys()):
        if len(planning.keys()) >1:
            kavlan_global = True
        else:
            kavlan = True
    
    for limit in limits:
        log = ''
        free_elements = {'grid5000': 0}
        
        if kavlan_global:
            free_vlans_global = []
            
        for site, site_planning in planning.iteritems():            
            free_elements[site] = 0
            
            for cluster, cluster_planning in site_planning.iteritems():
                
                if cluster in get_g5k_clusters():
                    free_elements[cluster] = 0
                    for host, host_planning in cluster_planning.iteritems():
                        host_free = False
                        for free_slot in host_planning['free']:
                            if free_slot[0] <= limit and free_slot[1] >= limit \
                                + oar_duration_to_seconds(walltime):
                                host_free = True
                        if host_free:
                            free_elements['grid5000'] += 1
                            free_elements[site] += 1
                            free_elements[cluster] += 1
                            log += ', '+host
                
            if kavlan:
                free_vlans = 0
                for vlan, vlan_planning in site_planning['vlans'].iteritems():
                    if int(vlan.split('-')[1]) < 10:
                        kavlan_free = False
                        for free_slot in vlan_planning['free']:
                            if free_slot[0] <= limit and free_slot[1] >= limit \
                                + oar_duration_to_seconds(walltime):
                                kavlan_free = True
                        if kavlan_free:
                            free_vlans += 1
                free_elements['kavlan'] = free_vlans 
            elif kavlan_global:
                for vlan, vlan_planning in site_planning['vlans'].iteritems():
                    if int(vlan.split('-')[1]) > 10:
                        kavlan_global_free = False
                        for free_slot in vlan_planning['free']:
                        
                            if free_slot[0] <= limit and free_slot[1] >= limit \
                                + oar_duration_to_seconds(walltime):
                                kavlan_global_free = True
                        if kavlan_global_free:
                            free_vlans_global.append(site)
                free_elements['kavlan'] = free_vlans_global
                     
             
                ## MISSING OTHER RESOURCES COMPUTATION
                
        slots.append( [ limit, limit +oar_duration_to_seconds(walltime), free_elements] )
    
    slots.sort()
    return slots    

def find_first_slot( slots, resources = ['grid5000']):
    """ Return the first slot where some resources are available 
    
    :param slots: list of slots returned by ``compute_slots``
    
    :param resources: a list of elements that must have some free hosts"""

    for slot in slots:
        vlan_free = True
        if 'kavlan' in resources:
            if isinstance(slot[2]['kavlan'], int):
                if slot[2]['kavlan'] == 0:
                    vlan_free = False
            elif isinstance(slot[2]['kavlan'], list):
                if len(slot[2]['kavlan']) == 0:
                    vlan_free = False
        
        res_nodes = sum( [ nodes for element, nodes in slot[2].iteritems() if element in resources
                          and element != 'kavlan'])
        if res_nodes > 0 and vlan_free:
            return slot
        
        
    

def find_max_slot( slots, resources = ['grid5000']):                    
    """Return the slot with the maximum nodes available for the given elements
    
    :param slots: list of slots returned by ``compute_slots``
    
    :param resources: a list of elements that must be maximized"""
    max_nodes = 0
    for slot in slots:
        vlan_free = True
        if 'kavlan' in resources:
            if isinstance(slot[2]['kavlan'], int):
                if slot[2]['kavlan'] == 0:
                    vlan_free = False
            elif isinstance(slot[2]['kavlan'], list):
                if len(slot[2]['kavlan']) == 0:
                    vlan_free = False        
        res_nodes = sum( [ nodes for element, nodes in slot[2].iteritems() 
                        if element in resources and element != 'kavlan'])
        if res_nodes > max_nodes and vlan_free:
            max_nodes = res_nodes
            max_slot = slot     
    return max_slot

def find_free_slots( slots, resources):
    """Return the slots with enough resources
     
    :param slots: list of slots returned by ``compute_slots``
    
    :param resources: a dict describing the wanted ressources ``{'grid5000': 50, 'lyon': 20, 'stremi': 10 }``
    """
    free_slots = []
    for slot in slots:
        vlan_free = True
        if 'kavlan' in resources:
            if isinstance(slot[2]['kavlan'], int):
                if slot[2]['kavlan'] == 0:
                    vlan_free = False
            elif isinstance(slot[2]['kavlan'], list):
                if len(slot[2]['kavlan']) == 0:
                    vlan_free = False
        slot_ok = True
        for element, n_nodes in slot[2].iteritems():
            if resources.has_key(element) and resources[element] > n_nodes and resources != 'kavlan':
                slot_ok = False
        if slot_ok and vlan_free:
            free_slots.append(slot)
            resources['kavlan'] = slot[2]['kavlan']
    return free_slots 


def create_reservation(startdate, resources, walltime, oargridsub_opts = '',
                       auto_reservation = False, prog = None, name = ''):
    """ Perform the reservation for the given set of resources """ 
    get_kavlan = resources.has_key('kavlan')
    subs = []
    
    sites = get_g5k_sites()
    clusters = get_g5k_clusters()
    
    #Adding sites corresponding to clusters wanted
    real_resources = resources.copy()
    for resource in resources.iterkeys():
        if resource in clusters:
            site = get_cluster_site(resource)
            if not real_resources.has_key(site):
                real_resources[site] = resources[resource]
            
    n_sites = 0
    for resource in real_resources.keys():
        if resource in sites:
            n_sites +=1 
        
    
    for site in sites:
        sub_resources = ''
        if site in real_resources:
            clusters_nodes = 0
            if get_kavlan: 
                if n_sites > 1:
                    if site == resources['kavlan'][0]:
                        sub_resources="{type=\\'kavlan-global\\'}/vlan=1+"
                        get_kavlan = False
                else:
                    sub_resources="{type=\\'kavlan\\'}/vlan=1+"
                    get_kavlan = False
                
                                
            for cluster in get_site_clusters(site): 
                if cluster in resources and resources[cluster] > 0:
                    sub_resources += "{cluster=\\'"+cluster+"\\'}/nodes="+str(resources[cluster])+'+'
                    clusters_nodes += resources[cluster]
#            real_resources[site] -= clusters_nodes     
            if real_resources[site] > 0:
                sub_resources+="nodes="+str(real_resources[site])+'+'
                
            if sub_resources != '':
                subs.append( (OarSubmission(resources = sub_resources[:-1], name = name), site) )    
    
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

def distribute_hosts_grid5000(resources_available, resources_wanted):
    """ Distribute the resources on the different sites """
    
    
    resources = {}
    all_sites = get_g5k_sites()
    sites = [ site for site in all_sites if site in resources_available.keys() and site != 'kavlan' ] 
    
    total_nodes = 0
    sites_nodes = {}
    cluster_nodes = {}
    for site in sites:
        if resources_wanted.has_key(site):
            sites_nodes[site] = resources_wanted[site]
        else:
            sites_nodes[site] = 0
        for cluster in get_site_clusters(site):
            if cluster in resources_wanted:
                cluster_nodes[cluster] = resources_wanted[cluster]
            else:
                cluster_nodes[cluster] = 0
            sites_nodes[site] += cluster_nodes[cluster]
    
    while total_nodes != resources_wanted['grid5000']:
        max_site = ''
        max_nodes = 0
        for site in sites:
            if max_nodes < resources_available[site] - sites_nodes[site]:
                max_site = site
                max_nodes = resources_available[site] - sites_nodes[site]
        sites_nodes[max_site] += 1
        total_nodes += 1
    
    
    for site, n_nodes in sites_nodes.iteritems():
        if n_nodes > 0:
            resources[site] = n_nodes
    
    for cluter, n_nodes in cluster_nodes.iteritems():
        if n_nodes > 0:
            resources[cluter] = n_nodes
    
    print 'wanted'
    pprint(resources_wanted)
    if resources_wanted.has_key('kavlan'):
        resources['kavlan'] = resources_available['kavlan']
    return resources




def _get_vlans_API(site):
    """Retrieve the list of VLAN of a site from the 3.0 Grid'5000 API"""
    equips = get_resource_attributes('/sites/'+site+'/network_equipments/')
    vlans = []
    for equip in equips['items']:
        if equip.has_key('vlans') and len(equip['vlans']) >2:
            for vlan, params in equip['vlans'].iteritems():
                if type( params ) == type({}) and params.has_key('name') \
                        and int(params['name'].split('-')[1])>3:
                    # > 3 because vlans 1, 2, 3 are not routed
                    vlans.append(params['name'])
    return vlans


def _get_planning_API():
    """Retrieve the planning using the 3.0 Grid'5000 API """
    global planning
    broken_sites = []
    for site in planning.iterkeys():
        # Retrieving alive resources
        try:
            alive_nodes = [ node for node, status in \
                get_resource_attributes('/sites/'+site+'/status')['nodes'].iteritems() if status['hard'] != 'dead' ]
        except:
            logger.warn('Unable to reach '+site+', removing from computation')
            broken_sites.append(site)
            continue
        
        for host in alive_nodes:
            planning[site][get_host_cluster(str(host))].update({host: {'busy': [], 'free': []}})
        if planning[site].has_key('vlans'): 
            planning[site]['vlans'] = { vlan: {'busy': [], 'free': []} for vlan in _get_vlans_API(site) }
        # STORAGE AND SUBNETS MISSING
        # Retrieving jobs
        
        site_jobs = get_resource_attributes('/sites/'+site+'/jobs?state=waiting,launching,running')['items']
        jobs_links = [ link['href'] for job in site_jobs for link in job['links'] \
                      if link['rel'] == 'self' and job['queue'] != 'besteffort' ]
        for link in jobs_links:
            attr = get_resource_attributes('/'+str(link).split('/', 2)[2])
            try:
                start_time = attr['started_at'] if attr['started_at'] != 0 else attr['scheduled_at']
                # Add a delay as a node is never free at the end of the job
                end_time = int((start_time + attr['walltime']+\
                    timedelta_to_seconds(timedelta(minutes = 1, seconds = 5))))
                start_time = max(start_time, int(time()))
            except:
                pass
            
            nodes = attr['assigned_nodes']
            for node in nodes:
                cluster = node.split('.',1)[0].split('-')[0]
                if planning[site].has_key(cluster) and planning[site][cluster].has_key(node):
                    planning[site][cluster][node]['busy'].append( (start_time, end_time))
            if planning[site].has_key('vlans') and attr['resources_by_type'].has_key('vlans'):
                kavname ='kavlan-'+str(attr['resources_by_type']['vlans'][0])
                planning[site]['vlans'][kavname]['busy'].append( (start_time, end_time))
            if planning[site].has_key('subnets') and attr['resources_by_type'].has_key('subnets'):
                for subnet in attr['resources_by_type']['subnets']:
                    if not planning[site]['subnets'].has_key(subnet):
                        planning[site]['subnets'][subnet] = {'busy': [], 'free': []}
                    planning[site]['subnets'][subnet]['busy'].append( (start_time, end_time))                
            # STORAGE IS MISSING
    # Removing sites not reachable
    for site in broken_sites:
        del planning[site]   
                
def _get_planning_MySQL():
    """Retrieve the planning using the oar2 database"""    
    global planning
    
    broken_sites =  []
    for site in planning.iterkeys():
        try:
            db = MySQLdb.connect( host = 'mysql.'+site+'.grid5000.fr', port = 3306, user = 'oarreader', 
                        passwd = 'read', db = 'oar2', connect_timeout = 3)
        except:
            logger.warn('Unable to reach '+site+', removing from computation')
            broken_sites.append(site)
            continue
        
        # Change the group_concat_max_len to retrive long hosts lists
        db.query('SET SESSION group_concat_max_len=102400')
        
# CHUNKS IS NOT FINISHED BUT WE KEEP THE REQUEST FOR LATER                 
#        db.query("""SELECT * 
#            FROM information_schema.COLUMNS 
#            WHERE TABLE_SCHEMA = 'oar2' 
#            AND TABLE_NAME = 'resources' 
#            AND COLUMN_NAME = 'chunks' 
#            LIMIT 1""")
#        r = db.store_result()
#        print site
#        if len( r.fetch_row( maxrows = 0, how=1) )> 0:
#            has_chunks = True
#        else:
#            has_chunks = False
#        if has_chunks:
#            sql += ", IF(R.type = 'storage', R.chunks, null) as storage "

        # Retrieving alive resources
        sql = """SELECT DISTINCT IF(R.type = 'default',R.network_address,null) as host, 
            IF(R.type = 'kavlan' or R.type = 'kavlan-global', R.vlan,null) as vlans,
            IF(R.type = 'subnet', R.subnet_address, null) as subnet 
            FROM resources R 
            WHERE state <> 'Dead'; """

        
        db.query(sql)
        r = db.store_result()
        for data in r.fetch_row( maxrows = 0, how=1 ):
            if data['host'] is not None:
                cluster = data['host'].split('-')[0]
                planning[site][cluster][data['host']] = {'busy': [], 'free': []}
            if planning[site].has_key('vlans') and data['vlans'] is not None:
                planning[site]['vlans']['kavlan-'+data['vlans']] = {'busy': [], 'free': []}
            if planning[site].has_key('subnets') and data['subnet'] is not None:
                planning[site]['subnets'][data['subnet']] = {'busy': [], 'free': []}
            # STORAGE WILL BE ADDED LATER
        
        sql = """SELECT J.job_id, J.state, GJP.start_time AS start_time, J.job_user AS user, 
        GJP.start_time+MJD.moldable_walltime+TIME_TO_SEC('0:01:05') AS stop_time,  
        GROUP_CONCAT(DISTINCT R.network_address) AS hosts, 
        GROUP_CONCAT(DISTINCT  R.vlan ) AS vlan,
        GROUP_CONCAT(DISTINCT R.subnet_address) AS subnets         
        FROM jobs J
        LEFT JOIN moldable_job_descriptions MJD
            ON MJD.moldable_job_id=J.job_id
        LEFT JOIN gantt_jobs_predictions GJP
            ON GJP.moldable_job_id=MJD.moldable_id
        INNER JOIN gantt_jobs_resources AR
            ON AR.moldable_job_id=MJD.moldable_id  
        LEFT JOIN resources R
            ON AR.resource_id=R.resource_id
        WHERE ( J.state='Launching' OR J.state='Running' OR J.state='Waiting')
            AND queue_name<>'besteffort' 
        GROUP BY J.job_id 
        ORDER BY J.start_time, R.network_address, 
            CONVERT(SUBSTRING_INDEX(SUBSTRING_INDEX(R.network_address,'.',1),'-',-1), SIGNED)"""

        db.query(sql)
        r = db.store_result()    
        for job in r.fetch_row( maxrows = 0, how=1 ):
            if job['hosts'] != '':
                for host in job['hosts'].split(','):
                    if host != '':
                        cluster = host.split('-')[0]
                        if planning[site][cluster].has_key(host):
                            planning[site][cluster][host]['busy'].append( (int(job['start_time']), \
                                                                           int(job['stop_time'])))
            if planning[site].has_key('vlans') and job['vlan'] is not None:
                ##HACK TO FIX BUGS IN LILLE, SOPHIA, RENNES OAR2 DATABASE
                try:
                    vlan =  int(job['vlan'])
                    if vlan > 3:
                        planning[site]['vlans']['kavlan-'+job['vlan']]['busy'].append( (int(job['start_time']),\
                                                                                    int(job['stop_time'])) )
                except:
                    #print job['vlan']+' is not correct on site '+site
                    pass                                                                   
                    
                    
            if planning[site].has_key('subnets') and job['subnets'] is not None:
                                
                for subnet in job['subnets'].split(','):
                    planning[site]['subnets'][subnet]['busy'].append( (int(job['start_time']), \
                                                                           int(job['stop_time'])))
            # MISSING STORAGE        
        
        db.close()
    # Removing sites not reachable
    for site in broken_sites:
        del planning[site]

def _merge_el_planning(el_planning):
    """An internal function to merge the busy or free planning of an element"""
    if len(el_planning) > 1:
        el_planning.sort()
        for i in range(len(el_planning)):
            j = i+1
            if j == len(el_planning)-1:
                break
            while True:
                condition = el_planning[i][1] >= el_planning[j][0]
                if condition:
                    if el_planning[j][1] > el_planning[i][1]:
                        el_planning[i]=(el_planning[i][0], el_planning[j][1])
                    
                    el_planning.pop(j)
                    if j == len(el_planning) - 1:
                        break
                else:
                    break
            if j == len(el_planning) - 1:
                break
                        
def _trunc_el_planning(el_planning, starttime, endtime):
    """Modify (start, stop) tuple that are not within the (starttime, endtime) interval """
    if len(el_planning) > 0:        
        el_planning.sort()
        tmp_el_planning = []
        for start, stop in el_planning:            
            if start < starttime:
                if stop < endtime:
                    tmp_el_planning.append( (starttime, stop))
                else:
                    tmp_el_planning.append( (starttime, endtime))
            elif start < endtime:
                if stop < endtime:
                    tmp_el_planning.append( (start, stop))
                else:
                    tmp_el_planning.append( (start, endtime))
        return tmp_el_planning
    else:
        return el_planning
                  
def _fill_el_planning_free(el_planning, starttime, endtime):
    """An internal function to compute the planning free of all elements"""
    if len(el_planning['busy']) > 0:
        if el_planning['busy'][0][0] > starttime:
            el_planning['free'].append((starttime, el_planning['busy'][0][0]))
        for i in range(0, len(el_planning['busy'])-1):
            el_planning['free'].append((el_planning['busy'][i][1], el_planning['busy'][i+1][0]))
        if el_planning['busy'][len(el_planning['busy'])-1][1] < endtime:
            el_planning['free'].append((el_planning['busy'][len(el_planning['busy'])-1][1], endtime))
    else:
            el_planning['free'].append((starttime, endtime))
               
def _slots_limits(planning):
    """Return the limits of slots, defined by a resource state change."""
    limits = []
    
    for site in planning.itervalues():
        for res_pl in site.itervalues():
            for el_planning in res_pl.itervalues():                           
                    for start, stop in el_planning['busy']:
                        if start not in limits:
                            limits.append(start)
                        if stop not in limits:
                            limits.append(stop)
                    for start, stop in el_planning['free']:
                        if start not in limits:
                            limits.append(start)
                        if stop not in limits:
                            limits.append(stop)
                            
    limits = sorted(limits)
    limits.pop()
    return limits

   
               
def _add_charter_to_planning(starttime, endtime):
    """"""
    global planning
    charter_el_planning = get_charter_el_planning(starttime, endtime)
    
    for site in planning.itervalues():
        for res_pl in site.itervalues():
            for el_planning in res_pl.itervalues():
                el_planning['busy'] += charter_el_planning
                el_planning['busy'].sort()
                
         

def g5k_charter_time(dt):
    """Is the given datetime in a g5k charter time period ?

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
        el_planning.append((int(datetime_to_unixts(charter_start)), int(datetime_to_unixts(charter_end))))
        start_time = charter_end
    return el_planning


"""Functions to draw the Gantt chart, the slots available, and other plots """

def _set_colors():
    colors = {}
    colors['busy'] = '#666666'
    rgb_colors = [(x[0]/255., x[1]/255., x[2]/255.) for x in \
                [(255., 122., 122.), (255., 204., 122.), (255., 255., 122.), (255., 246., 153.), (204., 255., 122.),
                (122., 255., 122.), (122., 255., 255.), (122., 204., 255.), (204., 188., 255.), (255., 188., 255.)]]
    i_site = 0
    for site in sorted(get_g5k_sites()):
        colors[site] = rgb_colors[i_site]
        i_cluster = 0
        for cluster in sorted(get_site_clusters(site)):
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


def draw_gantt(planning, colors = None, show = False, save = True, outfile = None):
    """ Draw the hosts planning for the elements you ask (requires Matplotlib)
    
    :param planning: the dict of elements planning 
    
    :param colors: a dict to define element coloring ``{'element': (255., 122., 122.)}`` 
    
    :param show: display the Gantt diagram 
    
    :param save: save the Gantt diagram to outfile
    
    :param outfile: specify the output file"""
    
    
    if colors is None:
        colors = _set_colors()
    
    n_sites = len(planning.keys())
    startstamp = int(10**20)
    endstamp = 0
    slots = planning.itervalues().next().itervalues().next().itervalues().next()['busy'] +\
        planning.itervalues().next().itervalues().next().itervalues().next()['free']
    
#    pprint(slots)
    
    for slot in slots:
        if slot[0] < startstamp:  
            startstamp = slot[0]
        if slot[1] > endstamp:
            endstamp = slot[1]
        
    n_col = 2 if n_sites > 1 else 1
    n_row = int(ceil(float(n_sites) / float(n_col)))
#    if endstamp - startstamp <= timedelta_to_seconds(timedelta(days=3)):
#        x_major_locator = MD.HourLocator(byhour = [9, 19])
#    elif endstamp - startstamp <= timedelta_to_seconds(timedelta(days=7)):
#        x_major_locator = MD.HourLocator(byhour = [9])
#    else:
    x_major_locator = MD.AutoDateLocator()
    xfmt = MD.DateFormatter('%d %b, %H:%M ')
    
    PLT.ioff()
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
            for key in sorted(hosts.keys(), key = lambda name: (name.split('.',1)[0].split('-')[0], 
                                        int( name.split('.',1)[0].split('-')[1] ))):                
                slots = hosts[key]
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
        if outfile is None:
            outfile = 'gantt_'+"_".join( [site for site in planning.keys()])+'_'+format_oar_date(startstamp)
        
        PLT.savefig (outfile, dpi=300)


#
#
#    

#
#

#
#def get_first_cluster_available( clusters, walltime, n_nodes = 1):
#    """Compute the planning of the clusters list and find the first one available for a given walltime
#    and a given number of nodes"""
#
#    starttime = time() + timedelta_to_seconds(timedelta(seconds = 30))
#    endtime = starttime + timedelta_to_seconds(timedelta(days = 3))
#    planning = Planning(clusters, starttime, endtime)
#    print walltime
#    planning.compute_slots(walltime)
#    
#    first_slots = {}
#    for cluster in clusters:
#        slots_ok = planning.find_free_slots( walltime, {cluster: n_nodes})
#        first_slots[cluster] = slots_ok[0]
#    
#    first_slot = [10**20, 10**21]
#    
#    for cluster, slot in first_slots.iteritems():
#        if slot[0] <= first_slot[0]:
#            first_slot = [slot[0], slot[1]]
#            first_cluster = cluster
#
#    return first_cluster, first_slot

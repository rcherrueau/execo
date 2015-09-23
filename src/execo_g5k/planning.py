# Copyright 2009-2015 INRIA Rhone-Alpes, Service Experimentation et
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

from charter import g5k_charter_time, get_next_charter_period
from copy import deepcopy
from datetime import timedelta
from execo import logger, Host
from execo.log import style
from execo.time_utils import timedelta_to_seconds, get_seconds, \
    unixts_to_datetime, get_unixts, format_date
from execo_g5k import OarSubmission, get_current_oar_jobs, get_oar_job_info, \
    get_current_oargrid_jobs, get_oargrid_job_oar_jobs
from execo_g5k.api_utils import get_g5k_sites, get_g5k_clusters, \
    get_cluster_site, get_site_clusters, get_resource_attributes, get_host_cluster, \
    get_host_site, get_host_attributes, get_g5k_hosts, get_host_shortname, \
    get_host_longname
from execo_g5k.config import g5k_configuration
from execo_g5k.utils import G5kAutoPortForwarder
from itertools import cycle
from math import ceil, floor
from operator import itemgetter
from pprint import pformat
from threading import Thread, currentThread
from time import time
from traceback import format_exc

try:
    import matplotlib.pyplot as PLT
    import matplotlib.dates as MD
except ImportError:
    pass

try:
    import psycopg2
    _retrieve_method = 'PostgreSQL'
except:
    _retrieve_method = 'API'


def get_job_by_name(job_name, sites=None):
    """ """
    logger.detail('Looking for a job named %s', style.emph(job_name))
    if not sites:
        sites = get_g5k_sites()
    oargrid_jobs = get_current_oargrid_jobs()
    if len(oargrid_jobs) > 0:
        for g_job in oargrid_jobs:
            for job in get_oargrid_job_oar_jobs(g_job):
                info = get_oar_job_info(job[0], job[1])
                if info['name'] == job_name:
                    logger.info('Oargridjob %s found !', style.emph(g_job))
                    return g_job, None
    running_jobs = get_current_oar_jobs(sites)
    for job in running_jobs:
        info = get_oar_job_info(job[0], job[1])
        if info['name'] == job_name:
            logger.info('Job %s found on site %s !', style.emph(job[0]),
                        style.host(job[1]))
            return job
    return None, None


def get_slots(elements=['grid5000'], walltime="1:00:00", kavlan=False, subnet=False,
              out_of_chart=False, starttime=None, endtime=None, blacklisted=None):
    # Computing the planning of the ressources wanted
    logger.info('Compiling planning')
    planning = get_planning(elements=elements,
                            vlan=kavlan,
                            subnet=subnet,
                            storage=False,
                            out_of_chart=out_of_chart,
                            starttime=starttime,
                            endtime=endtime)
    logger.debug(pformat(planning.keys()))

    # Determing the slots for the given walltime, i.e. finding the
    # slice of time with constant resources, and excluding some elements
    logger.info('Calculating slots of %s ', walltime)

    slots = compute_slots(planning, walltime, excluded_elements=blacklisted)

    return slots


def get_planning(elements=['grid5000'], vlan=False, subnet=False, storage=False,
            out_of_chart=False, starttime=None, endtime=None):
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
    if not starttime: starttime = int(time() + timedelta_to_seconds(timedelta(minutes = 1)))
    starttime = get_unixts(starttime)
    if not endtime: endtime = int(starttime + timedelta_to_seconds(timedelta(weeks = 4, minutes = 1)))
    endtime = get_unixts(endtime)
    if 'grid5000' in elements:
        sites = elements = get_g5k_sites()
    else:
        sites = list(set([site for site in elements
                          if site in get_g5k_sites()] + 
                         [get_cluster_site(cluster) for cluster in elements
                          if cluster in get_g5k_clusters()] +
                         [get_host_site(host) for host in elements
                          if host in get_g5k_hosts()
                          or get_host_shortname(host) in get_g5k_hosts()]))
    if len(sites) == 0:
        logger.error('Wrong elements given, must be one of the following values \n %s \n%s', elements)
        return None
    planning = {}
    for site in sites:
        planning[site] = {}
        for cluster in get_site_clusters(site):
            planning[site][cluster] = {}

    for site in sites:
        if vlan:
            planning[site].update({'vlans': {}})
        if subnet:
            planning[site].update({'subnets': {}})
        if storage:
            planning[site].update({'storage': {}})

    if _retrieve_method == 'API':
        _get_planning_API(planning)
    elif _retrieve_method == 'PostgreSQL':
        _get_planning_PGSQL(planning)

    if out_of_chart:
        _add_charter_to_planning(planning, starttime, endtime)

    for site_pl in planning.itervalues():
        for res_pl in site_pl.itervalues():
            for el_planning in res_pl.itervalues():
                el_planning['busy'].sort()
                _merge_el_planning(el_planning['busy'])
                _trunc_el_planning(el_planning['busy'], starttime, endtime)
                _fill_el_planning_free(el_planning, starttime, endtime)

    # cleaning
    real_planning = deepcopy(planning)
    for site, site_pl in planning.iteritems():
        for cl, cl_pl in site_pl.iteritems():
            if cl in ['vlans']:
                continue
            keep_cluster = False
            for h in cl_pl:
                if not (get_host_site(h) in elements or
                        get_host_cluster(h) in elements or
                        get_host_shortname(h) in elements or
                        h in elements):
                    del real_planning[site][cl][h]
                else:
                    keep_cluster = True
            if not keep_cluster:
                del real_planning[site][cl]

    return real_planning


def compute_slots(planning, walltime, excluded_elements=None):
    """Compute the slots limits and find the number of available nodes for
    each elements and for the given walltime.

    Return the list of slots where a slot is ``[ start, stop, freehosts ]`` and
    freehosts is a dict of Grid'5000 element with number of nodes available
    ``{'grid5000': 40, 'lyon': 20, 'reims': 10, 'stremi': 10 }``.

    WARNING: slots does not includes subnets

    :param planning: a dict of the resources planning, returned by ``get_planning``

    :param walltime: a duration in a format supported by get_seconds where the resources
      are available

    :param excluded_elements: list of elements that will not be included in the slots
      computation
    """
    slots = []
    walltime = get_seconds(walltime)
    if excluded_elements is not None:
        _remove_excluded(planning, excluded_elements)
    limits = _slots_limits(planning)

    # Checking if we need to compile vlans planning
    kavlan = False
    kavlan_global = False
    if 'vlans' in (planning.itervalues().next().keys()):
        if len(planning.keys()) > 1:
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
                            if free_slot[0] <= limit and free_slot[1] >= limit + walltime:
                                host_free = True
                        if host_free:
                            free_elements['grid5000'] += 1
                            free_elements[site] += 1
                            free_elements[cluster] += 1
                            log += ', ' + host

            if kavlan:
                free_vlans = 0
                for vlan, vlan_planning in site_planning['vlans'].iteritems():
                    if int(vlan.split('-')[1]) < 10:
                        kavlan_free = False
                        for free_slot in vlan_planning['free']:
                            if free_slot[0] <= limit and free_slot[1] >= limit + walltime:
                                kavlan_free = True
                        if kavlan_free:
                            free_vlans += 1
                free_elements['kavlan'] = free_vlans
            elif kavlan_global:
                for vlan, vlan_planning in site_planning['vlans'].iteritems():
                    if int(vlan.split('-')[1]) > 10:
                        kavlan_global_free = False
                        for free_slot in vlan_planning['free']:
                            if free_slot[0] <= limit and free_slot[1] >= limit  + walltime:
                                kavlan_global_free = True
                        if kavlan_global_free:
                            free_vlans_global.append(site)
                free_elements['kavlan'] = free_vlans_global
                ## MISSING OTHER RESOURCES COMPUTATION
        logger.debug(log)
        slots.append([limit, limit + walltime, free_elements])

    slots.sort(key=itemgetter(0))
    return slots


def compute_coorm_slots(planning, excluded_elements=None):
    """ """
    slots = []
    limits = _slots_limits(planning)
    for start in limits:
        stop = 10 ** 25
        free_cores = {'grid5000': 0}
        for site, site_planning in planning.iteritems():
            free_cores[site] = 0
            for cluster, cluster_planning in site_planning.iteritems():
                free_cores[cluster] = 0
                if cluster in get_g5k_clusters():
                    for host, host_planning in cluster_planning.iteritems():
                        for free_slot in host_planning['free']:
                            if free_slot[0] <= start and free_slot[0] < stop:
                                free_cores[cluster] += get_host_attributes(host)['architecture']['smt_size']
                                free_cores[site] += get_host_attributes(host)['architecture']['smt_size']
                                free_cores['grid5000'] += get_host_attributes(host)['architecture']['smt_size']
                                if free_slot[1] < stop:
                                    stop = free_slot[1]
        slots.append((start, stop, free_cores))
    return slots


def find_first_slot(slots, resources_wanted):
    """ Return the first slot (a tuple start date, end date, resources) where some resources are available

    :param slots: list of slots returned by ``compute_slots``

    :param resources_wanted: a dict of elements that must have some free hosts
    """
    for slot in slots:
        vlan_free = True
        if 'kavlan' in resources_wanted:
            if isinstance(slot[2]['kavlan'], int):
                if slot[2]['kavlan'] == 0:
                    vlan_free = False
            elif isinstance(slot[2]['kavlan'], list):
                if len(slot[2]['kavlan']) == 0:
                    vlan_free = False
        res_nodes = sum([nodes for element, nodes in slot[2].iteritems()
                         if element in resources_wanted and element != 'kavlan'])

        if res_nodes > 0 and vlan_free:
            return slot

    return None, None, None


def find_max_slot(slots, resources_wanted):
    """Return the slot (a tuple start date, end date, resources) with the maximum nodes available for the given elements

    :param slots: list of slots returned by ``compute_slots``

    :param resources_wanted: a dict of elements that must be maximized"""
    max_nodes = 0
    max_slot = None, None, None
    for slot in slots:
        vlan_free = True
        if 'kavlan' in resources_wanted:
            if isinstance(slot[2]['kavlan'], int):
                if slot[2]['kavlan'] == 0:
                    vlan_free = False
            elif isinstance(slot[2]['kavlan'], list):
                if len(slot[2]['kavlan']) == 0:
                    vlan_free = False
        res_nodes = sum([nodes for element, nodes in slot[2].iteritems()
                        if element in resources_wanted and element != 'kavlan'])
        if res_nodes > max_nodes and vlan_free:
            max_nodes = res_nodes
            max_slot = slot
    return max_slot


def find_free_slot(slots, resources_wanted):
    """Return the first slot (a tuple start date, end date, resources) with enough resources

    :param slots: list of slots returned by ``compute_slots``

    :param resources_wanted: a dict describing the wanted ressources
      ``{'grid5000': 50, 'lyon': 20, 'stremi': 10 }``"""
    # We need to add the clusters nodes to the total nodes of a site
    real_wanted = resources_wanted.copy()
    for cluster, n_nodes in resources_wanted.iteritems():
        if cluster in get_g5k_clusters():
            site = get_cluster_site(cluster)
            if site in resources_wanted:
                real_wanted[site] += n_nodes

    for slot in slots:
        vlan_free = True
        if 'kavlan' in resources_wanted:
            if isinstance(slot[2]['kavlan'], int):
                if slot[2]['kavlan'] == 0:
                    vlan_free = False
            elif isinstance(slot[2]['kavlan'], list):
                if len(slot[2]['kavlan']) == 0:
                    vlan_free = False
        slot_ok = True
        for element, n_nodes in slot[2].iteritems():
            if element in real_wanted and real_wanted[element] > n_nodes \
                and real_wanted != 'kavlan':
                slot_ok = False

        if slot_ok and vlan_free:
            if 'kavlan' in resources_wanted:
                resources_wanted['kavlan'] = slot[2]['kavlan']
            return slot

    return None, None, None


def find_coorm_slot(slots, resources_wanted):
    """ """
    for start, stop, res in slots:
        print format_date(start), format_date(stop), res
        slot_ok = True
        for element, cpu in resources_wanted.iteritems():
            print element, cpu
            if res[element] < cpu * (stop - start) / 3600:
                slot_ok = False
        if slot_ok:
            return start, stop, res


def get_hosts_jobs(hosts, walltime, out_of_chart=False):
    """Find the first slot when the hosts are available and return a
     list of jobs_specs

    :param hosts: list of hosts

    :param walltime: duration of reservation
    """
    hosts = map(lambda x: x.address if isinstance(x, Host) else x, hosts)

    planning = get_planning(elements=hosts, out_of_chart=out_of_chart)
    limits = _slots_limits(planning)
    walltime = get_seconds(walltime)
    for limit in limits:
        all_host_free = True
        for site_planning in planning.itervalues():
            for cluster, cluster_planning in site_planning.iteritems():
                if cluster in get_g5k_clusters():
                    for host_planning in cluster_planning.itervalues():
                        host_free = False
                        for free_slot in host_planning['free']:
                            if free_slot[0] <= limit and free_slot[1] >= limit + walltime:
                                host_free = True
                        if not host_free:
                            all_host_free = False
        if all_host_free:
            startdate = limit
            break

    jobs_specs = []
    for site in planning.keys():
        site_hosts = map(get_host_longname,
                         filter(lambda h: get_host_site(h) == site,
                                hosts))
        sub_res = "{host in ('" + "','".join(site_hosts) + "')}/nodes=" + str(len(site_hosts))
        jobs_specs.append((OarSubmission(resources=sub_res,
                                         reservation_date=startdate), site))

    return jobs_specs


def show_resources(resources, msg='Resources'):
    """Print the resources in a fancy way"""
    total_hosts = 0
    log = style.log_header(msg) + '\n'

    for site in get_g5k_sites():
        site_added = False
        if site in resources.keys():
            log += style.log_header(site).ljust(20) + ' ' + str(resources[site]) + ' '
            site_added = True
        for cluster in get_site_clusters(site):
            if len(list(set(get_site_clusters(site)) & set(resources.keys()))) > 0 \
                    and not site_added:
                log += style.log_header(site).ljust(20) + ' '
                site_added = True
            if cluster in resources.keys():
                log += style.emph(cluster) + ': ' + str(resources[cluster]) + ' '
                total_hosts += resources[cluster]
        if site_added:
            log += '\n'
    if 'grid5000' in resources.keys():
        log += style.log_header('Grid5000').ljust(20) + str(resources['grid5000'])
    elif total_hosts > 0:
        log += style.log_header('Total ').ljust(20) + str(total_hosts)
    logger.info(log)


def get_jobs_specs(resources, excluded_elements=None, name=None):
    """ Generate the several job specifications from the dict of resources and
    the blacklisted elements

    :param resources: a dict, whose keys are Grid'5000 element and values the
      corresponding number of n_nodes

    :param excluded_elements: a list of elements that won't be used

    :param name: the name of the jobs that will be given
    """
    jobs_specs = []
    if excluded_elements == None:
        excluded_elements = []

    # Creating the list of sites used
    sites = []
    real_resources = resources.copy()
    for resource in resources.iterkeys():
        if resource in get_g5k_sites() and resource not in sites:
            sites.append(resource)
        if resource in get_g5k_clusters():
            if resource not in excluded_elements:
                site = get_cluster_site(resource)
                if site not in sites:
                    sites.append(site)
                if site not in real_resources:
                    real_resources[site] = 0

    # Checking if we need a Kavlan, a KaVLAN global or none
    get_kavlan = 'kavlan' in resources
    if get_kavlan:
        kavlan = 'kavlan'
        n_sites = 0
        for resource in real_resources.keys():
            if resource in sites:
                n_sites += 1
            if n_sites > 1:
                kavlan += '-global'
                break

    blacklisted_hosts = {}
    for element in excluded_elements:
        if element not in get_g5k_clusters() + get_g5k_sites():
            site = get_host_site(element)
            if not 'site' in blacklisted_hosts:
                blacklisted_hosts[site] = [element]
            else:
                blacklisted_hosts[site].append(element)

    for site in sites:
        sub_resources = ''
        # Adding a KaVLAN if needed
        if get_kavlan:
            if not 'global' in kavlan:
                sub_resources = "{type='" + kavlan + "'}/vlan=1+"
                get_kavlan = False
            elif site in resources['kavlan']:
                sub_resources = "{type='" + kavlan + "'}/vlan=1+"
                get_kavlan = False

        base_sql = '{'
        end_sql = '}/'

        # Creating blacklist SQL string for hosts
        host_blacklist = False
        str_hosts = ''
        if site in blacklisted_hosts and len(blacklisted_hosts[site]) > 0:
            str_hosts = ''.join(["host not in ('" + get_host_longname(host) + "') and "
                                for host in blacklisted_hosts[site]])
            host_blacklist = True

        #Adding the clusters blacklist
        str_clusters = str_hosts if host_blacklist else ''
        cl_blacklist = False
        clusters_nodes = 0
        for cluster in get_site_clusters(site):
            if cluster in resources and resources[cluster] > 0:
                if str_hosts == '':
                    sub_resources += "{cluster='" + cluster + "'}"
                else:
                    sub_resources += base_sql + str_hosts + "cluster='" + \
                        cluster + "'" + end_sql
                sub_resources += "/nodes=" + str(resources[cluster]) + '+'
                clusters_nodes += resources[cluster]
            if cluster in excluded_elements:
                str_clusters += "cluster not in ('" + cluster + "') and "
                cl_blacklist = True

        # Generating the site blacklist string from host and cluster blacklist
        str_site = ''
        if host_blacklist or cl_blacklist:
            str_site += base_sql
            if not cl_blacklist:
                str_site += str_hosts[:-4]
            else:
                str_site += str_clusters[:-4]
            str_site = str_site + end_sql

        if real_resources[site] > 0:
            sub_resources += str_site + "nodes=" + str(real_resources[site]) +\
                '+'

        if sub_resources != '':
            jobs_specs.append((OarSubmission(resources=sub_resources[:-1],
                                             name=name), site))

    return jobs_specs


def distribute_hosts(resources_available, resources_wanted,
                     excluded_elements=None, ratio=None):
    """ Distribute the resources on the different sites and cluster

    :param resources_available: a dict defining the resources available

    :param resources_wanted: a dict defining the resources available you really want

    :param excluded_elements: a list of elements that won't be used

    :param ratio: if not None (the default), a float between 0 and 1,
      to actually only use a fraction of the resources."""
    if excluded_elements == None: excluded_elements = []
    resources = {}
    #Defining the cluster you want
    clusters_wanted = {}
    for element, n_nodes in resources_wanted.iteritems():
        if element in get_g5k_clusters():
            clusters_wanted[element] = n_nodes
    for cluster, n_nodes in clusters_wanted.iteritems():
        nodes = n_nodes if n_nodes > 0 else resources_available[cluster]
        resources_available[get_cluster_site(cluster)] -= nodes
        resources[cluster] = nodes

    # Blacklisting clusters
    for element in excluded_elements:
        if element in get_g5k_clusters() and element in resources_available:
            resources_available['grid5000'] -= resources_available[element]
            resources_available[get_cluster_site(element)] -= resources_available[element]
            resources_available[element] = 0

    #Defining the sites you want
    sites_wanted = {}
    for element, n_nodes in resources_wanted.iteritems():
        if element in get_g5k_sites() and element not in excluded_elements:
            sites_wanted[element] = n_nodes
    for site, n_nodes in sites_wanted.iteritems():
        resources[site] = n_nodes if n_nodes > 0 else resources_available[site]

    # Blacklisting sites
    for element in excluded_elements:
        if element in get_g5k_sites() and element in resources_available:
            resources_available['grid5000'] -= resources_available[element]
            resources_available[element] = 0

    #Distributing hosts on grid5000 elements
    logger.debug(pformat(resources_wanted))
    if resources_wanted.has_key('grid5000'):
        g5k_nodes = resources_wanted['grid5000'] if resources_wanted['grid5000'] > 0 else resources_available['grid5000']

        total_nodes = 0

        sites = [element for element in resources_available.keys() if element in get_g5k_sites() ]
        iter_sites = cycle(sites)

        while total_nodes < g5k_nodes:
            site = iter_sites.next()
            if resources_available[site] == 0:
                sites.remove(site)
                iter_sites = cycle(sites)
            else:
                resources_available[site] -= 1
                if site in resources:
                    resources[site] += 1
                else:
                    resources[site] = 1
                total_nodes += 1
    logger.debug(pformat(resources))

    if resources_wanted.has_key('kavlan'):
        resources['kavlan'] = resources_available['kavlan']

    # apply optional ratio
    if ratio != None:
        resources.update((x, int(floor(y * ratio))) for x, y in resources.items())

    return resources


def _get_vlans_API(site):
    """Retrieve the list of VLAN of a site from the 3.0 Grid'5000 API"""
    equips = get_resource_attributes('/sites/'+site+'/network_equipments/')
    vlans = []
    for equip in equips['items']:
        if equip.has_key('vlans') and len(equip['vlans']) >2:
            for params in equip['vlans'].itervalues():
                if type( params ) == type({}) and params.has_key('name') \
                        and int(params['name'].split('-')[1])>3:
                    # > 3 because vlans 1, 2, 3 are not routed
                    vlans.append(params['name'])
    return vlans

def _get_job_link_attr_API(p):
    try:
        currentThread().attr = get_resource_attributes(p)
    except Exception, e:
        currentThread().broken = True
        currentThread().ex = e

def _get_site_planning_API(site, site_planning):
    try:
        alive_nodes = [ node for node, status in \
          get_resource_attributes('/sites/'+site+'/status')['nodes'].iteritems() if status['hard'] != 'dead' ]

        for host in alive_nodes:
            site_planning[get_host_cluster(str(host))].update({host: {'busy': [], 'free': []}})
        if site_planning.has_key('vlans'):
            site_planning['vlans'] = {}
            for vlan in _get_vlans_API(site):
                site_planning['vlans'][vlan] = {'busy': [], 'free': []}
        # STORAGE AND SUBNETS MISSING
        # Retrieving jobs

        site_jobs = get_resource_attributes('/sites/'+site+'/jobs?state=waiting,launching,running')['items']
        jobs_links = [ link['href'] for job in site_jobs for link in job['links'] \
                      if link['rel'] == 'self' and job['queue'] != 'besteffort' ]
        threads = []
        for link in jobs_links:
            t = Thread(target = _get_job_link_attr_API, args = ('/'+str(link).split('/', 2)[2], ))
            t.broken = False
            t.attr = None
            t.ex = None
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
            if t.broken:
                raise t.ex
            attr = t.attr
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
                if site_planning.has_key(cluster) and site_planning[cluster].has_key(node):
                    site_planning[cluster][node]['busy'].append( (start_time, end_time))
            if site_planning.has_key('vlans') and attr['resources_by_type'].has_key('vlans') \
                and int(attr['resources_by_type']['vlans'][0]) > 3:

                kavname ='kavlan-'+str(attr['resources_by_type']['vlans'][0])
                site_planning['vlans'][kavname]['busy'].append( (start_time, end_time))
            if site_planning.has_key('subnets') and attr['resources_by_type'].has_key('subnets'):
                for subnet in attr['resources_by_type']['subnets']:
                    if not site_planning['subnets'].has_key(subnet):
                        site_planning['subnets'][subnet] = {'busy': [], 'free': []}
                    site_planning['subnets'][subnet]['busy'].append( (start_time, end_time))
            # STORAGE IS MISSING
    except Exception, e:
        logger.warn('error connecting to oar database / getting planning from ' + site)
        logger.detail("exception:\n" + format_exc())
        currentThread().broken = True

def _get_planning_API(planning):
    """Retrieve the planning using the 3.0 Grid'5000 API """
    broken_sites = []
    threads = {}
    for site in planning.iterkeys():
        t = Thread(target = _get_site_planning_API, args = (site, planning[site]))
        threads[site] = t
        t.broken = False
        t.start()
    for site, t in threads.iteritems():
        t.join()
        if t.broken:
            broken_sites.append(site)
    # Removing sites not reachable
    for site in broken_sites:
        del planning[site]

def _get_site_planning_PGSQL(site, site_planning):
    with G5kAutoPortForwarder(site,
                              'oardb.' + site + '.grid5000.fr',
                              g5k_configuration['oar_pgsql_ro_port']) as (host, port):
        try:
            conn = psycopg2.connect(host=host, port=port,
                                 user=g5k_configuration['oar_pgsql_ro_user'],
                                 password=g5k_configuration['oar_pgsql_ro_password'],
                                 database=g5k_configuration['oar_pgsql_ro_db']
                                 )
            try:
                cur = conn.cursor()
                # Retrieving alive resources
                sql = """SELECT DISTINCT R.type, R.network_address, R.vlan,
                    R.subnet_address, null
                    FROM resources R
                    WHERE state <> 'Dead';"""

                cur.execute(sql)

                for data in cur.fetchall():
                    if data[0] == "default":
                        cluster = get_host_cluster(data[1])
                        if cluster in site_planning:
                            site_planning[cluster][data[1]] = {'busy': [],
                                                               'free': []}
                    if data[0] in ['kavlan', 'kavlan-global'] \
                        and 'vlans' in site_planning:
                        site_planning['vlans']['kavlan-' + data[2]] = {'busy': [],
                                                                       'free': []}
                    if data[0] == "subnet" and 'subnet' in site_planning:
                        site_planning['subnets'][data[3]] = {'busy': [],
                                                             'free': []}

                sql = """SELECT J.job_id, J.state, GJP.start_time AS start_time,
                GJP.start_time+MJD.moldable_walltime+120 AS stop_time,
                array_agg(DISTINCT R.network_address) AS hosts,
                array_agg(DISTINCT R.vlan) AS vlan,
                array_agg(DISTINCT R.subnet_address) AS subnets
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
                GROUP BY J.job_id, GJP.start_time, MJD.moldable_walltime
                ORDER BY J.start_time"""

#                    CONVERT(SUBSTRING_INDEX(SUBSTRING_INDEX(R.network_address,'.',1),'-',-1), SIGNED)"""
                cur.execute(sql)

                for job in cur.fetchall():
                    if len(job[4]) > 0:
                        for host in job[4]:
                            if host != '':
                                cluster = get_host_cluster(host)
                                if cluster in site_planning:
                                    if host in site_planning[cluster]:
                                        site_planning[cluster][host]['busy'].append((int(job[2]),
                                                                                     int(job[3])))
                    if job[5][0] and 'vlans' in site_planning:
                        for vlan in job[5]:
                            if isinstance(vlan, str) and int(vlan) > 3:
                                # only routed vlan
                                site_planning['vlans']['kavlan-' + vlan]['busy'].append((int(job[2]),\
                                                                                         int(job[3])) )

                    if len(job[6]) > 0 and 'subnet' in site_planning:
                        for subnet in job[6]:
                            site_planning['subnets'][subnet]['busy'].append((int(job[2]), \
                                                                             int(job[3])))
            finally:
                conn.close()
        except Exception, e:
            logger.warn('error connecting to oar database / getting planning from ' + site)
            logger.detail("exception:\n" + format_exc())
            currentThread().broken = True

def _get_planning_PGSQL(planning):
    """Retrieve the planning using the oar2 database"""
    broken_sites = []
    threads = {}
    for site in planning.iterkeys():
        t = Thread(target = _get_site_planning_PGSQL, args = (site, planning[site]))
        threads[site] = t
        t.broken = False
        t.start()
    for site, t in threads.iteritems():
        t.join()
        if t.broken:
            broken_sites.append(site)
    # Removing sites not reachable
    for site in broken_sites:
        del planning[site]

def _remove_excluded(planning, excluded_resources):
    """This function remove elements from planning"""
    # first removing the site
    for element in excluded_resources:
        if element in get_g5k_sites() and element in planning:
            del planning[element]
    # then removing specific clusters
    for site_pl in planning.itervalues():
        for res in site_pl.keys():
            if res in excluded_resources:
                del site_pl[res]
                continue
            for element in site_pl[res].keys():
                if element in excluded_resources:
                    del site_pl[res][element]




def _merge_el_planning(el_planning):
    """An internal function to merge the busy or free planning of an element"""
    if len(el_planning) > 1:
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
        # Truncating jobs that end before starttime
        i = 0
        while True:
            if i == len(el_planning):
                break
            start, stop = el_planning[i]
            if stop < starttime or start > endtime:
                el_planning.remove( (start, stop ))
            else:
                if start < starttime:
                    if stop < endtime:
                        el_planning.remove( (start, stop ) )
                        el_planning.append( (starttime, stop) )
                    else:
                        el_planning.remove( (start, stop ) )
                        el_planning.append( (starttime, endtime) )
                elif stop > endtime:
                    el_planning.remove( (start, stop ) )
                    el_planning.append( (start, endtime) )
                else:
                    i += 1
            if i == len(el_planning):
                break
        el_planning.sort()


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
    limits = set()
    for site in planning.itervalues():
        for res_pl in site.itervalues():
            for el_planning in res_pl.itervalues():
                    for start, stop in el_planning['busy']:
                        limits.add(start)
                        limits.add(stop)
                    for start, stop in el_planning['free']:
                        limits.add(start)
                        limits.add(stop)
    limits = sorted(limits)
    if len(limits) > 0:
        limits.pop()
    return limits


def _add_charter_to_planning(planning, starttime, endtime):
    charter_el_planning = get_charter_el_planning(starttime, endtime)

    for site in planning.itervalues():
        for res_pl in site.itervalues():
            for el_planning in res_pl.values():
                el_planning['busy'] += charter_el_planning
                el_planning['busy'].sort()

def get_charter_el_planning(start_time, end_time):
    """Returns the list of tuples (start, end) of g5k charter time periods between start_time and end_time.

    :param start_time: a date in one of the types supported by
      `execo.time_utils.get_unixts`

    :param end_time: a date in one of the types supported by
      `execo.time_utils.get_unixts`
    """
    start_time = unixts_to_datetime(get_unixts(start_time))
    end_time = unixts_to_datetime(get_unixts(end_time))
    el_planning = []
    while True:
        charter_start, charter_end = get_next_charter_period(start_time, end_time)
        if charter_start == None: break
        el_planning.append((int(charter_start), int(charter_end)))
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
    startstamp = int(10 ** 20)
    endstamp = 0

    slots = planning.itervalues().next().itervalues().next().itervalues().next()['busy'] +\
        planning.itervalues().next().itervalues().next().itervalues().next()['free']

    for slot in slots:
        if slot[0] < startstamp:
            startstamp = slot[0]
        if slot[1] > endstamp:
            endstamp = slot[1]

    if outfile is None:
        outfile = 'gantt_' + "_".join([site for site in planning]) \
            + '_' + format_date(startstamp)
    logger.info('Saving Gantt chart to %s', style.emph(outfile))

    n_col = 2 if n_sites > 1 else 1
    n_row = int(ceil(float(n_sites) / float(n_col)))
    x_major_locator = MD.AutoDateLocator()
    xfmt = MD.DateFormatter('%d %b, %H:%M ')

    PLT.ioff()
    fig = PLT.figure(figsize=(15, 5 * n_row), dpi=80)

    i_site = 1
    for site, clusters in planning.iteritems():
        ax = fig.add_subplot(n_row, n_col, i_site, title=site.title())
        ax.title.set_fontsize(18)
        ax.xaxis_date()
        ax.set_xlim(unixts_to_datetime(startstamp), unixts_to_datetime(endstamp))
        ax.xaxis.set_major_formatter(xfmt)
        ax.xaxis.set_major_locator(x_major_locator)
        ax.xaxis.grid(color='black', linestyle='dashed')
        PLT.xticks(rotation=15)
        ax.set_ylim(0, 1)
        ax.get_yaxis().set_ticks([])
        ax.yaxis.label.set_fontsize(16)
        n_hosts = 0
        for hosts in clusters.itervalues():
            n_hosts += len(hosts)
        pos = 0.0
        inc = 1. / n_hosts

        ylabel = ''
        for cluster, hosts in clusters.iteritems():
            ylabel += cluster + ' '
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
        logger.debug('Saving file %s ...', outfile)
        PLT.savefig (outfile, dpi=300)


def draw_slots(slots, colors=None, show=False, save=True, outfile=None):
    """Draw the number of nodes available for the clusters (requires Matplotlib >= 1.2.0)

    :param slots: a list of slot, as returned by ``compute_slots``

    :param colors: a dict to define element coloring ``{'element': (255., 122., 122.)}``

    :param show: display the slots versus time

    :param save: save the plot to outfile

    :param outfile: specify the output file"""

    startstamp = slots[0][0]
    endstamp = slots[-1][1]
    if outfile is None:
        outfile = 'slots_' + format_date(startstamp)

    logger.info('Saving slots diagram to %s', style.emph(outfile))

    if colors is None:
        colors = _set_colors()

    xfmt = MD.DateFormatter('%d %b, %H:%M ')

    if endstamp - startstamp <= timedelta_to_seconds(timedelta(days=7)):
        x_major_locator = MD.HourLocator(byhour=[9, 19])
    elif endstamp - startstamp <= timedelta_to_seconds(timedelta(days=17)):
        x_major_locator = MD.HourLocator(byhour=[9])
    else:
        x_major_locator = MD.AutoDateLocator()

    max_nodes = {}
    total_nodes = 0
    slot_limits = []
    total_list = []
    i_slot = 0
    for slot in slots:
        slot_limits.append(slot[0])
        if i_slot + 1 < len(slots):
            slot_limits.append(slots[i_slot + 1][0])
            i_slot += 1

        for element, n_nodes in slot[2].iteritems():
            if element in get_g5k_clusters():
                if not element in max_nodes:
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
        logger.debug('Saving file %s ...', outfile)
        PLT.savefig (outfile, dpi=300)


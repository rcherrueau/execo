# Copyright 2009-2014 INRIA Rhone-Alpes, Service Experimentation et
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

"""Module that manage the cache of the Grid'5000 Reference API (hosts and network equipments)

Data are stored in $HOME/.topo5k under pickle format

- one file for all hosts

- one file for all network equipments
"""
from os import makedirs, environ
from cPickle import load, dump
from execo import logger
from api_utils import get_resource_attributes, get_g5k_sites, get_site_clusters

_cache_dir = environ['HOME'] + '/.execo/g5k_api_cache/'
_network = None
_hosts = None


def get_api_data(cache_dir=_cache_dir):
    """Return two dicts containing the data from network,
    and hosts """
    global _network
    global _hosts

    if not _network or not _hosts:
        if _is_cache_old(cache_dir):
            network, hosts = _write_api_cache(cache_dir)
        else:
            network, hosts = _read_api_cache(cache_dir)
        _network, _hosts = network, hosts
    else:
        logger.detail('Data already loaded in memory')
        network, hosts = _network, _hosts

    return network, hosts


def _get_api_commit():
    """Retrieve the latest api commit"""
    return get_resource_attributes('')['version']


def _is_cache_old(cache_dir=_cache_dir):
    """Try to read the api_commit stored in the cache_dir and compare
    it with latest commit, return True if remote commit is different
    from cache commit"""
    cache_is_old = False
    try:
        f = open(cache_dir + 'api_commit')
        local_commit = f.readline()
        f.close()
        if local_commit != _get_api_commit():
            logger.detail('Cache is too old')
            cache_is_old = True
        else:
            logger.detail('Already at the latest commit')
    except:
        pass
        logger.detail('No commit version found')
        cache_is_old = True

    return cache_is_old


def _write_api_cache(cache_dir=_cache_dir):
    """Retrieve data from the Grid'5000 API and write it into
    the cache directory"""
    try:
        makedirs(cache_dir)
        logger.detail('No cache found, directory created.')
    except:
        logger.detail('Cache directory is present')
        pass

    network, hosts = {}, {}
    logger.detail('Retrieving topology data from API...')
    network['backbone'] = get_resource_attributes('/network_equipments')['items']

    for site in sorted(get_g5k_sites()):
        logger.detail(site)
        hosts[site] = {}
        for cluster in get_site_clusters(site):
            logger.detail('* ' + cluster)
            hosts[site][cluster] = get_resource_attributes(
                'sites/' + site + '/clusters/' + cluster + '/nodes')['items']

        network[site] = get_resource_attributes(
                    'sites/' + site + '/network_equipments')['items']
        f = open(cache_dir + site + '_equips', 'w')
        dump(network[site], f)
        f.close()

    logger.detail('Writing data to cache ...')
    f = open(cache_dir + 'network', 'w')
    dump(network, f)
    f.close()

    f = open(cache_dir + 'hosts', 'w')
    dump(hosts, f)
    f.close()

    f = open(cache_dir + 'api_commit', 'w')
    f.write(_get_api_commit())
    f.close()

    return network, hosts


def _read_api_cache(cache_dir=_cache_dir):
    """Read the picke files from cache_dir and return two dicts
    - network = the network_equipements of all sites and backbone
    - hosts = the hosts of all sites
    """
    logger.detail('Reading data from cache ...')
    f_network = open(cache_dir + 'network')
    network = load(f_network)
    f_network.close()

    f_hosts = open(cache_dir + 'hosts')
    hosts = load(f_hosts)
    f_hosts.close()

    return network, hosts

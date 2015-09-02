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

"""Execo_g5k provides easy-to-use functions to interacts with the
Grid5000 API.  All static parts of the API is stored locally, in
$HOME/.execo/g5k_api_cache, which version is checked on the first call
to a method. Beware that if API is not reachable at runtime, the local
cache will be used.

Functions for wrapping the grid5000 REST API. This module also
manage a cache of the Grid'5000 Reference API (hosts and network
equipments) (Data are stored in $HOME/.execo/g5k_api_cache/' under
pickle format)

All queries to the Grid5000 REST API are done with or without
credentials, depending on key ``api_username`` of
`execo_g5k.config.g5k_configuration`. If credentials are used, the
password is interactively asked and if the keyring module is
available, the password will then be stored in the keyring through
this module and will not be asked in subsequent executions. (the
keyring python module allows to access the system keyring services
like gnome-keyring or kwallet. If a password needs to be changed, do
it from the keyring GUI).

This module is thread-safe.

"""

from execo import logger
from execo_g5k.config import g5k_configuration
import execo
import httplib2
import json, re, itertools
import threading
from os import makedirs, environ, path
from cPickle import load, dump

_cache_dir = environ['HOME'] + '/.execo/g5k_api_cache/'
_data_lock = threading.RLock()
_data = None

_g5k_api_lock = threading.RLock()
_g5k_api = None
"""Internal singleton instance of the g5k api rest resource."""

# _g5k = None
# """cache of g5k structure.

# a dict whose keys are sites, whose values are dict whose keys are
# clusters, whose values are hosts.
# """

_api_password_lock = threading.RLock()
__api_passwords = dict()
# private dictionnary keyed by username, storing cached passwords

def _ask_password(username, message, password_check_func):
    import getpass
    for pass_try in range(3):
        password = getpass.getpass(message)
        if password_check_func(username, password):
            return password
    return None

def _get_api_password_check_func(username, password):
    try:
        http = httplib2.Http(disable_ssl_certificate_validation = True)
    except TypeError:
        http = httplib2.Http()
    http.add_credentials(username, password)
    response, content = http.request("https://api.grid5000.fr")
    return (response['status'] in ['200', '304'])

def _get_api_password(username):
    with _api_password_lock:
        if not __api_passwords.get(username):
            try:
                import keyring
                __api_passwords[username] = keyring.get_password("grid5000_api", username)
            except:
                # only use keyring if available and usable
                pass
            if not __api_passwords.get(username):
                __api_passwords[username] = _ask_password(username,
                                                          "Grid5000 API authentication password for user %s" % (username,),
                                                          _get_api_password_check_func)
                if __api_passwords[username]:
                    try:
                        import keyring
                        keyring.set_password("grid5000_api", username, __api_passwords[username])
                    except:
                        # only use keyring if available and usable
                        pass
        return __api_passwords[username]

class APIGetException(Exception):
    """Raised when an API request fails"""

    def __init__(self, uri, response, content):
        self.uri = uri
        """the HTTP URI to which the request failed"""
        self.response = response
        """HTTP response"""
        self.content = content
        """HTTP content"""

    def __str__(self):
        return "<APIGetException uri=%r response=%s content=%r>" % (self.uri, self.response, self.content)

class APIConnection(object):
    """Basic class for easily getting url contents.

    Intended to be used to get content from restfull apis, particularly the grid5000 api.
    """

    def __init__(self, base_uri = None,
                 username = None, password = None,
                 headers = None, additional_args = None,
                 timeout = 300):
        """:param base_uri: server base uri. defaults to
          ``g5k_configuration.get('api_uri')``

        :param username: username for the http connection. If None
          (default), use default from
          ``g5k_configuration.get('api_username')``. If False, don't
          use a username at all.

        :param password: password for the http connection. If None
          (default), get the password from a keyring (if available) or
          interactively.

        :param headers: http headers to use. If None (default),
          default headers accepting json answer will be used.

        :param additional_args: a list of optional arguments (strings)
          to pass at the end of the url of all http requests.

        :param timeout: timeout for the http connection.
        """
        if not base_uri:
            base_uri = g5k_configuration.get('api_uri')
        self.base_uri = base_uri.rstrip("/")
        if headers:
            self.headers = headers
        else:
            self.headers = {
                'ACCEPT': 'application/json'
            }
        self.additional_args = g5k_configuration["api_additional_args"]
        if additional_args:
            self.additional_args.extend(additional_args)
        if username:
            self.username = username
        else:
            self.username = g5k_configuration.get('api_username')
        self.password = password
        if self.username and not self.password:
            self.password = _get_api_password(self.username)
        self.timeout = timeout

    def get(self, relative_uri):
        """Get the (response, content) tuple for the given path on the server"""
        try:
            http = httplib2.Http(timeout = self.timeout,
                                 disable_ssl_certificate_validation = True)
        except TypeError:
            # probably caused by old httplib2 without option
            # disable_ssl_certificate_validation, try
            # without it
            http = httplib2.Http(timeout = self.timeout)
        if self.username and self.password:
            http.add_credentials(self.username, self.password)
        uri = self.base_uri + "/" + relative_uri.lstrip("/")
        if self.additional_args and len(self.additional_args) > 0:
            args_string = "&".join(self.additional_args)
            if "?" in uri:
                uri += "&" + args_string
            else:
                uri += "?" + args_string
        response, content = http.request(uri,
                                         headers = self.headers)
        if response['status'] not in ['200', '304']:
            raise APIGetException(uri, response, content)
        return response, content

def get_resource_attributes(path):
    """Get generic resource (path on g5k api) attributes as a dict"""
    (_, content) = _get_g5k_api().get(path)
    attributes = json.loads(content)
    return attributes

def _get_g5k_sites_uncached():
    return [site['uid'] for site in get_resource_attributes('/sites')['items']]

def _get_site_clusters_uncached(site):
    return [cluster['uid'] for cluster in get_resource_attributes('/sites/' + site + '/clusters')['items']]

def get_api_data(cache_dir=_cache_dir):
    """Return a dict containing the data from network, sites, clusters
    and hosts."""
    global _data
    if not _data:
        with _data_lock:
            if _is_cache_old_and_reachable(cache_dir):
                _data = _write_api_cache(cache_dir)
            else:
                _data = _read_api_cache(cache_dir)
    return _data

def _is_cache_old_and_reachable(cache_dir=_cache_dir):
    """Try to read the api_commit stored in the cache_dir and compare
    it with latest commit, return True if remote commit is different
    from cache commit"""
    try:
        f = open(cache_dir + 'api_commit')
    except:
        logger.detail('No commit version found')
        return True
    local_commit = f.readline()
    f.close()
    try:
        api_commit = get_resource_attributes('')['version']
    except:
        logger.warning('Unable to check API, reverting to cache')
        return False
    if local_commit != get_resource_attributes('')['version']:
        logger.info('Cache is outdated, will retrieve the latest commit')
        return True
    else:
        logger.detail('Already at the latest commit')
        return False

def __get_backbone():
    logger.detail("backbone network")
    threading.currentThread().backbone_data = get_resource_attributes('/network_equipments')['items']

def __get_site_attrs(site):
    logger.detail(site + " attrs")
    threading.currentThread().site_data = get_resource_attributes('sites/' + site)

def __get_cluster_attrs(site, cluster):
    logger.detail(cluster + " attrs")
    threading.currentThread().cluster_data = get_resource_attributes('sites/' + site
                                                                     + '/clusters/'
                                                                     + cluster)

def __get_host_attrs(site, cluster):
    logger.detail(cluster + " hosts")
    threading.currentThread().host_data = {}
    for host in get_resource_attributes('sites/' + site + '/clusters/'
                                        + cluster + '/nodes')['items']:
        threading.currentThread().host_data[host['uid']] = host

def __get_site_network(site):
    logger.detail(site + " network")
    threading.currentThread().network_data = {}
    for equip in get_resource_attributes('sites/' + site + '/network_equipments')['items']:
        threading.currentThread().network_data[equip['uid']] = equip

def __get_site(site):
    logger.detail(site)
    site_attrs_th = threading.Thread(target = __get_site_attrs, args = (site,))
    site_attrs_th.start()
    site_network_th = threading.Thread(target = __get_site_network, args = (site,))
    site_network_th.start()
    cluster_attrs_th = {}
    host_attrs_th = {}
    for cluster in _get_site_clusters_uncached(site):
        t = threading.Thread(target = __get_cluster_attrs, args = (site, cluster))
        t.start()
        cluster_attrs_th[cluster] = t
        t = threading.Thread(target = __get_host_attrs, args = (site, cluster))
        t.start()
        host_attrs_th[cluster] = t
    for t in [ site_attrs_th, site_network_th ] + cluster_attrs_th.values() + host_attrs_th.values():
        t.join()
    threading.currentThread().site_data = site_attrs_th.site_data
    threading.currentThread().network_data = site_network_th.network_data
    threading.currentThread().cluster_data = {}
    threading.currentThread().host_data = {}
    for cluster in cluster_attrs_th:
        threading.currentThread().cluster_data[cluster] = cluster_attrs_th[cluster].cluster_data
        threading.currentThread().host_data[cluster] = host_attrs_th[cluster].host_data

def _write_api_cache(cache_dir=_cache_dir):
    """Retrieve data from the Grid'5000 API and write it into
    the cache directory"""
    if not path.exists(cache_dir):
        makedirs(cache_dir)
        logger.detail('No cache found, directory created')
    else:
        logger.detail('Cache directory is present')

    logger.info('Retrieving data from API...')

    backbone_th = threading.Thread(target = __get_backbone)
    backbone_th.start()

    site_th = {}
    for site in sorted(_get_g5k_sites_uncached()):
        t = threading.Thread(target = __get_site, args = (site,))
        t.start()
        site_th[site] = t

    for t in [ backbone_th ] + site_th.values():
        t.join()

    data = {'network': {},
            'sites': {},
            'clusters': {},
            'hosts':  {},
            'hierarchy': {}}
    data['network']['backbone'] = backbone_th.backbone_data
    for site in site_th:
        data['network'][site] = site_th[site].network_data
        data['sites'][site] = site_th[site].site_data
        data['hierarchy'][site] = {}
        for cluster in site_th[site].cluster_data:
            data['clusters'][cluster] = site_th[site].cluster_data[cluster]
            data['hierarchy'][site][cluster] = []
            for host in site_th[site].host_data[cluster]:
                data['hosts'][host] = site_th[site].host_data[cluster][host]
                data['hierarchy'][site][cluster].append(host)

    logger.detail('Writing data to cache ...')
    for e, d in data.iteritems():
        f = open(cache_dir + e, 'w')
        dump(d, f)
        f.close()

    f = open(cache_dir + 'api_commit', 'w')
    f.write(data['network']['backbone'][0]['version'])
    f.close()

    return data


def _read_api_cache(cache_dir=_cache_dir):
    """Read the picke files from cache_dir and return two dicts
    - network = the network_equipements of all sites and backbone
    - hosts = the hosts of all sites
    """
    data = {}
    logger.detail('Reading data from cache ...')
    for e in ['network', 'sites', 'clusters', 'hosts', 'hierarchy']:
        f = open(cache_dir + e)
        data[e] = load(f)
        f.close()

    return data


def _get_g5k_api():
    """Get a singleton instance of a g5k api rest resource."""
    with _g5k_api_lock:
        global _g5k_api #IGNORE:W0603
        if not _g5k_api:
            _g5k_api = APIConnection()
        return _g5k_api

def get_g5k_sites():
    """Get the list of Grid5000 sites. Returns an iterable."""
    return get_api_data()['hierarchy'].keys()

def get_site_clusters(site):
    """Get the list of clusters from a site. Returns an iterable."""
    if not site in get_g5k_sites():
        raise ValueError, "unknown g5k site %s" % (site,)
    return get_api_data()['hierarchy'][site].keys()

def get_site_hosts(site):
    """Get the list of hosts from a site. Returns an iterable"""
    if not site in get_g5k_sites():
        raise ValueError, "unknown g5k site %s" % (site,)
    hosts = []
    for cluster in get_site_clusters(site):
        hosts += get_cluster_hosts(cluster)
    return hosts

def get_site_network_equipments(site):
    """Get the list of network elements from a site. Returns an iterable."""
    if not site in get_g5k_sites():
        raise ValueError, "unknown g5k site %s" % (site,)
    return get_api_data()['network'][site].keys()

def get_cluster_hosts(cluster):
    """Get the list of hosts from a cluster. Returns an iterable."""
    for site in get_g5k_sites():
        if cluster in get_site_clusters(site):
            return get_api_data()['hierarchy'][site][cluster]
    raise ValueError, "unknown g5k cluster %s" % (cluster,)

def get_cluster_network_equipments(cluster):
    """Get the list of the network equipments used by a cluster"""
    if cluster in get_g5k_clusters():
        return list(set([e for h in get_cluster_hosts(cluster)
                    for e in get_host_network_equipments(h)]))
    raise ValueError, "unknown g5k cluster %s" % (cluster,)

def get_g5k_clusters():
    """Get the list of all g5k clusters. Returns an iterable."""
    return get_api_data()['clusters'].keys()

def get_g5k_hosts():
    """Get the list of all g5k hosts. Returns an iterable."""
    return get_api_data()['hosts'].keys()

def get_cluster_site(cluster):
    """Get the site of a cluster."""
    for site in get_g5k_sites():
        if cluster in get_site_clusters(site):
            return site
    raise ValueError, "unknown g5k cluster %s" % (cluster,)

__g5k_host_group_regex = re.compile("^([a-zA-Z]+)-\d+(\.(\w+))?")

def get_host_cluster(host):
    """Get the cluster of a host.

    Works both with a bare hostname or a fqdn.
    """
    if isinstance(host, execo.Host):
        host = host.address
    host = canonical_host_name(host)
    m = __g5k_host_group_regex.match(host)
    if m: return m.group(1)
    else: return None

def get_host_site(host):
    """Get the site of a host.

    Works both with a bare hostname or a fqdn.
    """
    if isinstance(host, execo.Host):
        host = host.address
    host = canonical_host_name(host)
    m = __g5k_host_group_regex.match(host)
    if m:
        if m.group(3):
            return m.group(3)
        else:
            return get_cluster_site(m.group(1))
    else: return None 

def get_host_network_equipments(host):
    """"""
    _host = get_host_shortname(host)
    if _host in get_g5k_hosts():
        return list(set(map(lambda x: x['switch'],
                   filter(lambda n: 'switch' in n and not n['management'] and n['mountable']
                          and n['switch'] and n['interface'] == 'Ethernet',
                          get_host_attributes(_host)['network_adapters']))))
    raise ValueError, "unknown g5k host %s" % (host,)

def get_network_equipment_site(equip):
    """Return the site of a network_equipment"""
    for site in get_g5k_sites():
        if equip in get_site_network_equipments(site):
            return site 
    return None

def group_hosts(hosts):
    """Given a sequence of hosts, group them in a dict by sites and clusters"""
    grouped_hosts = {}
    for site, site_hosts in itertools.groupby(
        sorted(hosts,
               lambda h1, h2: cmp(
                   get_host_site(h1),
                   get_host_site(h2))),
        get_host_site):
        grouped_hosts[site] = {}
        for cluster, cluster_hosts in itertools.groupby(
            sorted(site_hosts,
                   lambda h1, h2: cmp(
                       get_host_cluster(h1),
                       get_host_cluster(h2))),
            get_host_cluster):
            grouped_hosts[site][cluster] = list(cluster_hosts)
    return grouped_hosts

def get_host_attributes(host):
    """Get the attributes of a host (as known to the g5k api) as a dict"""
    return get_api_data()['hosts'][get_host_shortname(host)]

def get_cluster_attributes(cluster):
    """Get the attributes of a cluster (as known to the g5k api) as a dict"""
    return get_api_data()['clusters'][cluster]

def get_site_attributes(site):
    """Get the attributes of a site (as known to the g5k api) as a dict"""
    return get_api_data()['sites'][site]

def get_network_equipment_attributes(equip):
    """Get the attributes of a network equipment of a site as a dict"""
    site = get_network_equipment_site(equip)
    return get_api_data()['network'][site][equip]

def get_g5k_measures(host, metric, startstamp, endstamp, resolution=5):
    """ Return a dict with the api values"""
    host_shortname = get_host_shortname(host)
    site = get_host_site(host)
    return get_resource_attributes('/sites/' + site
                                   + '/metrics/' + metric
                                   + '/timeseries/' + host_shortname
                                   + '?resolution=' + str(resolution)
                                   + '&from=' + str(startstamp)
                                   + '&to=' + str(endstamp))

__canonical_host_name_regex = re.compile("^([a-zA-Z]+-\d+)(-kavlan-\d+)?(\.([.\w]+))?")

def __canonical_sub_func(matchobj):
    n = matchobj.group(1)
    if matchobj.lastindex >= 3:
        n += matchobj.group(3)
    return n

def canonical_host_name(host):
    """Convert, if needed, the host name to its canonical form without kavlan part.

    Can be given a Host, will return a Host.
    Can be given a string, will return a string.
    Works with short or fqdn forms of hostnames.
    """
    h = execo.Host(host)
    h.address = __canonical_host_name_regex.sub(__canonical_sub_func, h.address)
    if isinstance(host, execo.Host):
        return h
    else:
        return h.address

def get_host_shortname(host):
    """Convert, if needed, the host name to its shortname"""
    if isinstance(host, execo.Host):
        host = host.address
    host = canonical_host_name(host)
    host_shortname, _, _ = host.partition(".")
    return host_shortname

__host_longname_regex = re.compile("^([^.]*)(\.([^.]+))?")

def get_host_longname(host):
    """Convert, if needed, the host name to a grid5000 fully qualified name"""
    if isinstance(host, execo.Host):
        host = host.address
    mo = __host_longname_regex.match(host)
    host_shortname = mo.group(1)
    if mo.group(3):
        host_site = mo.group(3)
    else:
        host_site = get_host_site(host_shortname)
    return host_shortname + "." + host_site + ".grid5000.fr"

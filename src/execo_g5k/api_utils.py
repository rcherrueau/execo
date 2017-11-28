# Copyright 2009-2017 INRIA Rhone-Alpes, Service Experimentation et
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
from execo.utils import singleton_to_collection
from execo.time_utils import get_unixts, get_seconds
import execo
import requests
import re, itertools
import threading
import logging, sys
from os import makedirs, environ, path
if sys.version_info >= (3,):
    from pickle import load, dump
else:
    from cPickle import load, dump

if 'HOME' in environ:
    _cache_dir = environ['HOME'] + "/.execo/g5k_api_cache-%s.%s/" % (sys.version_info[0], sys.version_info[1])
else:
    _cache_dir = None
_data_lock = threading.RLock()
_data = None

_g5k_api_lock = threading.RLock()
_g5k_api = None
"""Internal singleton instance of the g5k api rest resource."""

_api_password_lock = threading.RLock()
__api_passwords = dict()
# private dictionnary keyed by username, storing cached passwords

def _ask_password(username, uri, message, password_check_func):
    import getpass
    for pass_try in range(3):
        password = getpass.getpass(message)
        if password_check_func(username, uri, password):
            return password
    return None

def _get_api_password_check_func(username, uri, password):
    if g5k_configuration['api_verify_ssl_cert'] == False:
        verify = False
        requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    else:
        verify = True
    response = requests.get(uri,
                            auth=(username, password),
                            verify=verify,
                            timeout=g5k_configuration.get('api_timeout'))
    return (response.status_code in [200, 304])

def _get_api_password(username, uri):
    with _api_password_lock:
        if not __api_passwords.get(username):
            try:
                logging.getLogger("keyring").addHandler(logging.NullHandler())
                import keyring
                __api_passwords[username] = keyring.get_password("grid5000_api", username)
            except:
                # only use keyring if available and usable
                pass
            if not __api_passwords.get(username):
                __api_passwords[username] = _ask_password(username,
                                                          uri,
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

class APIException(Exception):
    """Raised when an API request fails"""

    def __init__(self, uri, method, response=None, exception=None):
        self.uri = uri
        """the HTTP URI to which the request failed"""
        self.method = method
        """the HTTP method of the failed request"""
        self.response = response
        """requests response object"""
        self.exception = exception
        """exception which occured"""

    def __str__(self):
        s = '<APIException uri=%r method=%s' % (self.uri,
                                                self.method)
        if self.response != None: s += ' status=%s/"%s" content="%s"' % (self.response.status_code,
                                                                         self.response.reason,
                                                                         self.response.text)
        if self.exception != None: s += ' exception=%s' % (self.exception,)
        s += '>'
        return s


class APIConnection(object):
    """Basic class for easily getting url contents.

    Intended to be used to get content from restfull apis, particularly the grid5000 api.
    """

    def __init__(self, base_uri=None,
                 username=None, password=None,
                 headers=None, additional_args=None,
                 timeout=g5k_configuration.get('api_timeout')):
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

        :param additional_args: a dict of optional arguments (string
          to string mappings) to pass at the end of the url of all
          http requests.

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
            self.additional_args.update(additional_args)
        if username:
            self.username = username
        else:
            self.username = g5k_configuration.get('api_username')
        self.password = password
        if self.username and not self.password:
            self.password = _get_api_password(self.username, self.base_uri)
        self.timeout = timeout

    def get(self, relative_uri):
        """Get the (response, content) tuple for the given path on the server"""
        uri = self._build_uri(relative_uri)
        auth, verify = self._get_security_conf()
        try:
            response = requests.get(uri,
                                    params=self.additional_args,
                                    headers=self.headers,
                                    auth=auth,
                                    verify=verify,
                                    timeout=self.timeout)
        except Exception as e:
            raise APIException(uri, 'GET', exception=e)
        if response.status_code not in [200, 304]:
            raise APIException(uri, 'GET', response=response)
        return response

    def post(self, relative_uri, json):
        """Submit the body to a given path on the server, returns the (response, content) tuple"""
        uri = self._build_uri(relative_uri)
        auth, verify = self._get_security_conf()
        try:
            response = requests.post(uri,
                                     params=self.additional_args,
                                     headers=self.headers,
                                     json=json,
                                     auth=auth,
                                     verify=verify,
                                     timeout=self.timeout)
        except Exception as e:
            raise APIException(uri, 'POST', exception=e)
        if response.status_code not in [200, 304]:
            raise APIException(uri, 'POST', response=response)
        return response

    def _build_uri(self, relative_uri):
        uri = self.base_uri + "/" + relative_uri.lstrip("/")
        return uri

    def _get_security_conf(self):
        if self.username and self.password:
            auth = (self.username, self.password)
        else:
            auth = None
        if auth == None or g5k_configuration['api_verify_ssl_cert'] == False:
            verify = False
            try:
                requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
            except:
                # special case for older versions of requests
                pass
        else:
            verify = True
        return (auth, verify)

def get_resource_attributes(path):
    """Get generic resource (path on g5k api) attributes as a dict"""
    response = _get_g5k_api().get(path)
    attributes = response.json()
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
            if cache_dir:
                if _is_cache_old_and_reachable(cache_dir):
                    _data = _get_api()
                    _write_api_cache(cache_dir, _data)
                else:
                    _data = _read_api_cache(cache_dir)
            else:
                _data = _get_api()
    return _data

def _is_cache_old_and_reachable(cache_dir):
    """Try to read the api_commit stored in the cache_dir and compare
    it with latest commit, return True if remote commit is different
    from cache commit"""
    try:
        with open(cache_dir + 'api_commit') as f:
            local_commit = f.readline()
    except:
        logger.detail('No commit version found')
        return True
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
    for t in [ site_attrs_th, site_network_th ] + list(cluster_attrs_th.values()) + list(host_attrs_th.values()):
        t.join()
    threading.currentThread().site_data = site_attrs_th.site_data
    threading.currentThread().network_data = site_network_th.network_data
    threading.currentThread().cluster_data = {}
    threading.currentThread().host_data = {}
    for cluster in cluster_attrs_th:
        threading.currentThread().cluster_data[cluster] = cluster_attrs_th[cluster].cluster_data
        threading.currentThread().host_data[cluster] = host_attrs_th[cluster].host_data

def _get_api():
    """Retrieve data from the Grid'5000 API"""
    logger.info('Retrieving data from API...')

    backbone_th = threading.Thread(target = __get_backbone)
    backbone_th.start()

    site_th = {}
    for site in sorted(_get_g5k_sites_uncached()):
        t = threading.Thread(target = __get_site, args = (site,))
        t.start()
        site_th[site] = t

    for t in [ backbone_th ] + list(site_th.values()):
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

    return data

def _write_api_cache(cache_dir, data):
    """write Grid'5000 API data into cache directory"""
    if not path.exists(cache_dir):
        makedirs(cache_dir)
        logger.detail('No cache found, directory created')
    else:
        logger.detail('Cache directory is present')

    logger.detail('Writing data to cache ...')
    for e, d in data.items():
        with open(cache_dir + e, 'wb') as f:
            dump(d, f)
    with open(cache_dir + 'api_commit', 'w') as f:
        f.write(data['network']['backbone'][0]['version'])

def _read_api_cache(cache_dir):
    """Read the picke files from cache_dir and return two dicts
    - network = the network_equipements of all sites and backbone
    - hosts = the hosts of all sites
    """
    data = {}
    logger.detail('Reading data from cache ...')
    for e in ['network', 'sites', 'clusters', 'hosts', 'hierarchy']:
        with open(cache_dir + e, 'rb') as f:
            data[e] = load(f)
    return data


def _get_g5k_api():
    """Get a singleton instance of a g5k api rest resource."""
    with _g5k_api_lock:
        global _g5k_api #IGNORE:W0603
        if not _g5k_api:
            _g5k_api = APIConnection()
        return _g5k_api

def filter_clusters(clusters, queues = "default"):
    """Filter a list of clusters on their queue(s).

    Given a list of clusters, return the list filtered, keeping only
    clusters that have at least one oar queue matching one in the list
    of filter queues passed in parameters. The cluster queues are
    taken from the queues attributes in the grid5000 API. If this
    attribute is missing, the cluster is considered to be in queues
    ["admin", "default", "besteffort"]

    :param clusters: list of clusters

    :param queues: a queue name or a list of queues. clusters will be
      kept in the returned filtered list only if at least one of their
      queues matches one queue of this parameter. If queues = None or
      False, no filtering at all is done. By default, keeps clusters
      in queue "default".
    """

    if queues == None or queues == False:
        return clusters
    queues = singleton_to_collection(queues)
    filtered_clusters = []
    for cluster in clusters:
        cluster_queues = get_cluster_attributes(cluster).get("queues")
        if not cluster_queues:
            cluster_queues = [ "admin", "default", "besteffort" ]
        for q in queues:
            if q in cluster_queues:
                filtered_clusters.append(cluster)
                break
    return filtered_clusters

def get_g5k_sites():

    """Get the list of Grid5000 sites. Returns an iterable."""
    return list(get_api_data()['hierarchy'])

def get_site_clusters(site, queues = "default"):
    """Get the list of clusters from a site. Returns an iterable.

    :param site: site name

    :param queues: queues filter, see
      `execo_g5k.api_utils.filter_clusters`
    """
    if not site in get_g5k_sites():
        raise ValueError("unknown g5k site %s" % (site,))
    return filter_clusters(list(get_api_data()['hierarchy'][site]), queues)

def get_site_hosts(site, queues = "default"):
    """Get the list of hosts from a site. Returns an iterable.

    :param site: site name

    :param queues: queues filter, see
      `execo_g5k.api_utils.filter_clusters`
    """
    if not site in get_g5k_sites():
        raise ValueError("unknown g5k site %s" % (site,))
    hosts = []
    for cluster in get_site_clusters(site, queues):
        hosts += get_cluster_hosts(cluster)
    return hosts

def get_site_network_equipments(site):
    """Get the list of network elements from a site. Returns an iterable."""
    if not site in get_g5k_sites():
        raise ValueError("unknown g5k site %s" % (site,))
    return list(get_api_data()['network'][site])

def get_cluster_hosts(cluster):
    """Get the list of hosts from a cluster. Returns an iterable."""
    for site in get_g5k_sites():
        if cluster in get_site_clusters(site, queues = None):
            return get_api_data()['hierarchy'][site][cluster]
    raise ValueError("unknown g5k cluster %s" % (cluster,))

def get_cluster_network_equipments(cluster):
    """Get the list of the network equipments used by a cluster"""
    if cluster in get_g5k_clusters():
        return list(set([e for h in get_cluster_hosts(cluster, queues = None)
                    for e in get_host_network_equipments(h)]))
    raise ValueError("unknown g5k cluster %s" % (cluster,))

def get_g5k_clusters(queues = "default"):
    """Get the list of all g5k clusters. Returns an iterable.

    :param queues: queues filter, see
      `execo_g5k.api_utils.filter_clusters`
    """
    return filter_clusters(list(get_api_data()['clusters']), queues)

def get_g5k_hosts(queues = "default"):
    """Get the list of all g5k hosts. Returns an iterable.

    :param queues: queues filter, see
      `execo_g5k.api_utils.filter_clusters`
    """
    hosts = []
    for cluster in get_g5k_clusters(queues):
        hosts.extend(get_cluster_hosts(cluster))
    return hosts

def get_cluster_site(cluster):
    """Get the site of a cluster."""
    for site in get_g5k_sites():
        if cluster in get_site_clusters(site, queues = None):
            return site
    raise ValueError("unknown g5k cluster %s" % (cluster,))

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
        return list(set([n['switch']
                         for n in get_host_attributes(_host)['network_adapters']
                         if 'switch' in n
                         and not n['management']
                         and n['mountable']
                         and n['switch']
                         and n['interface'] == 'Ethernet']))
    raise ValueError("unknown g5k host %s" % (host,))

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
            sorted(hosts, key = get_host_site),
            get_host_site):
        grouped_hosts[site] = {}
        for cluster, cluster_hosts in itertools.groupby(
                sorted(site_hosts, key = get_host_cluster),
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

def __get_site_metrics(site, grouped_hosts, metric, from_ts, to_ts, resolution):
    threading.currentThread().res = {}
    hosts_site = [ h for cluster in grouped_hosts[site].values() for h in cluster ]
    args = [ 'only=' + ','.join([ get_host_shortname(host) for host in hosts_site ]) ]
    if from_ts: args.append('from=' + str(from_ts))
    if to_ts: args.append('to=' + str(to_ts))
    if resolution: args.append('resolution=' + str(resolution))
    path = 'sites/%s/metrics/%s/timeseries?%s' % (site, metric, '&'.join(args))
    for apires in get_resource_attributes(path)['items']:
        for host in hosts_site:
            if get_host_shortname(host) == apires['uid']:
                # this inner loop to make sure the name under which
                # the host is returned is the same as how it was
                # called
                threading.currentThread().res[host] = {}
                host_entry = threading.currentThread().res[host]
                # output varies depending on the metric
                if 'timestamps' in apires:
                    host_entry['values'] = [ (apires['timestamps'][i], apires['values'][i])
                                             for i in range(len(apires['values'])) ]
                else:
                    host_entry['values'] = [ (ts, apires['values'][i])
                                             for i, ts in enumerate(range(apires['from'], apires['to'], apires['resolution'])) ]
                # note: for kwapi metrics, (those having a
                # 'timestamps' entry), values in from, to, resolution
                # seem to be inconsistent with the number of
                # (timestamp, value) tuples
                for k in ['from', 'to', 'resolution', 'type']:
                    if k in apires:
                        host_entry[k] = apires[k]

def get_hosts_metric(hosts, metric, from_ts=None, to_ts=None, resolution=None):
    """Get metric values from Grid'5000 metrology API

    :param hosts: List of hosts

    :param metric: Grid'5000 metrology metric to fetch (eg: "power",
      "cpu_user")

    :param from_ts: Time from which metric is collected, in any type
      supported by `execo.time_utils.get_unixts`, optional.

    :param to_ts: Time until which metric is collected, in any type
      supported by `execo.time_utils.get_unixts`, optional.

    :param resolution: time resolution, in any type supported by
      `execo.time_utils.get_seconds`, optional.

    :return: A dict of host -> dict with entries 'from' (unix
      timestamp in seconds, as returned from g5k api), 'to' (unix
      timestamp in seconds, as returned from g5k api), 'resolution'
      (in seconds, as returned from g5k api), type (the type of
      metric, as returned by g5k api), 'values': a list of tuples
      (timestamp, metric value). Some g5k metrics (the kwapi ones)
      return both the timestamps and values as separate lists, in
      which case this function only takes care of gathering them in
      tuples (note also that for these metrics, it seems that 'from',
      'to', 'resolution' returned by g5k api are inconsistent with the
      timestamps list. In this case this function makes no correction
      and returns everything 'as is'). Some other g5k metrics (the
      ganglia ones) only return the values, in which case this
      function generates the timestamps of the tuples from 'from',
      'to', 'resolution'.
    """
    if from_ts != None: from_ts = int(get_unixts(from_ts))
    if to_ts != None: to_ts = int(get_unixts(to_ts))
    if resolution != None: resolution = get_seconds(resolution)
    grouped_hosts = group_hosts(hosts)
    res = {}
    site_threads = []
    for site in grouped_hosts:
        site_th = threading.Thread(
            target=__get_site_metrics,
            args=(site, grouped_hosts, metric, from_ts, to_ts, resolution)
            )
        site_th.start()
        site_threads.append(site_th)
    for site_th in site_threads:
        site_th.join()
        res.update(site_th.res)
    return res

def set_nodes_vlan(site, hosts, interface, vlan_id):
    """Set the interface of a list of hosts in a given vlan

    :param site: Site name

    :param hosts: List of hosts

    :param interface: The interface to put in the vlan

    :param vlan_id: Id of the vlan to use
    """

    def _to_network_address(host):
        """Translate a host to a network address

        e.g:
        paranoia-20.rennes.grid5000.fr -> paranoia-20-eth2.rennes.grid5000.fr
        """
        splitted = host.address.split('.')
        splitted[0] = splitted[0] + "-" + interface
        return ".".join(splitted)

    network_addresses = map(_to_network_address, hosts)
    logger.info("Setting %s in vlan %s of site %s" % (network_addresses, vlan_id, site))
    return _get_g5k_api().post('/sites/%s/vlans/%s' % (site, str(vlan_id)), {"nodes": network_addresses})

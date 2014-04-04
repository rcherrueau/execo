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

"""Functions for wrapping the grid5000 REST API. The functions which
query the reference api cache their results for the life of the
module.

All queries to the Grid5000 REST API are done with or without
credentials, depending on key ``api_username`` of
`execo_g5k.config.g5k_configuration`. If credentials are used, the
password is interactively asked and if the keyring module is
available, the password will then be stored in the keyring through
this module and will not be asked in subsequent executions. (the
keyring python module allows to access the system keyring services
like gnome-keyring or kwallet. If a password needs to be changed, do
it from the keyring GUI).

This module is now thread-safe.
"""

from execo_g5k.config import g5k_configuration
import execo
import httplib2
import json, re, itertools
import threading

_lock = threading.RLock()

_g5k_api = None
"""Internal singleton instance of the g5k api rest resource."""
_g5k = None
"""cache of g5k structure.

a dict whose keys are sites, whose values are dict whose keys are
clusters, whose values are hosts.
"""

__api_passwords = dict()
# private dictionnary keyed by username, storing cached passwords

def _get_api_password(username):
    with _lock:
        if not __api_passwords.get(username):
            try:
                import keyring
                __api_passwords[username] = keyring.get_password("grid5000_api", username)
            except:
                # only use keyring if available and usable
                pass
            if not __api_passwords.get(username):
                import getpass
                __api_passwords[username] = getpass.getpass(
                    "Grid5000 API authentication password for user %s" % (username,))
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

def _get_g5k_api():
    """Get a singleton instance of a g5k api rest resource."""
    with _lock:
        global _g5k_api #IGNORE:W0603
        if not _g5k_api:
            _g5k_api = APIConnection()
        return _g5k_api

def get_g5k_sites():
    """Get the list of Grid5000 sites. Returns an iterable."""
    with _lock:
        global _g5k #IGNORE:W0603
        if not _g5k:
            (_, content) = _get_g5k_api().get('/sites')
            sites = json.loads(content)
            _g5k = dict()
            for site in [site['uid'] for site in sites['items']]:
                _g5k[site] = None
        return _g5k.keys()

def get_site_clusters(site):
    """Get the list of clusters from a site. Returns an iterable."""
    with _lock:
        get_g5k_sites()
        if not _g5k.has_key(site):
            raise ValueError, "unknown g5k site %s" % (site,)
        if not _g5k[site]:
            (_, content) = _get_g5k_api().get('/sites/'
                             + site
                             + '/clusters')
            clusters = json.loads(content)
            _g5k[site] = dict()
            for cluster in [cluster['uid'] for cluster in clusters['items']]:
                _g5k[site][cluster] = None
        return _g5k[site].keys()

def get_cluster_hosts(cluster):
    """Get the list of hosts from a cluster. Returns an iterable."""
    with _lock:
        _get_all_site_clusters()
        for site in _g5k.keys():
            if cluster in _g5k[site]:
                if not _g5k[site][cluster]:
                    (_, content) = _get_g5k_api().get('/sites/' + site
                                     + '/clusters/' + cluster
                                     + '/nodes')
                    hosts = json.loads(content)
                    _g5k[site][cluster] = ["%s.%s.grid5000.fr" % (host['uid'], site) for host in hosts['items']]
                return list(_g5k[site][cluster])
        raise ValueError, "unknown g5k cluster %s" % (cluster,)

def _get_all_site_clusters():
    """Trigger the querying of the list of clusters from all sites."""
    for site in get_g5k_sites():
        get_site_clusters(site)

def _get_all_clusters_hosts():
    """Trigger the querying of the list of hosts from all clusters from all sites."""
    _get_all_site_clusters()
    for site in get_g5k_sites():
        for cluster in get_site_clusters(site):
            get_cluster_hosts(cluster)

def get_g5k_clusters():
    """Get the list of all g5k clusters. Returns an iterable."""
    clusters = []
    for site in get_g5k_sites():
        clusters.extend(get_site_clusters(site))
    return clusters

def get_g5k_hosts():
    """Get the list of all g5k hosts. Returns an iterable."""
    hosts = []
    for cluster in get_g5k_clusters():
        hosts.extend(get_cluster_hosts(cluster))
    return hosts

def get_cluster_site(cluster):
    """Get the site of a cluster."""
    _get_all_site_clusters()
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

def get_resource_attributes(path):
    """Get generic resource (path on g5k api) attributes as a dict"""
    (_, content) = _get_g5k_api().get(path)
    attributes = json.loads(content)
    return attributes

def get_host_attributes(host):
    """Get the attributes of a host (as known to the g5k api) as a dict"""
    if isinstance(host, execo.Host):
        host = host.address
    host = canonical_host_name(host)
    host_shortname, _, _ = host.partition(".")
    cluster = get_host_cluster(host)
    site = get_host_site(host)
    return get_resource_attributes('/sites/' + site
                                      + '/clusters/' + cluster
                                      + '/nodes/' + host_shortname)

def get_cluster_attributes(cluster):
    """Get the attributes of a cluster (as known to the g5k api) as a dict"""
    site = get_cluster_site(cluster)
    return get_resource_attributes('/sites/' + site
                                      + '/clusters/' + cluster)

def get_site_attributes(site):
    """Get the attributes of a site (as known to the g5k api) as a dict"""
    return get_resource_attributes('/sites/' + site)


def get_g5k_measures(host, metric, startstamp, endstamp, resolution=5):
    """ Return a dict with the api values"""
    if isinstance(host, execo.Host):
        host = host.address
    host = canonical_host_name(host)
    host_shortname, _, _ = host.partition(".")
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

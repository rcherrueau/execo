import re, socket
import restclient # http://pypi.python.org/pypi/py-restclient/1.3.3
import simplejson # http://pypi.python.org/pypi/simplejson/

"""Tools for using grid5000.

- Functions for wrapping the grid5000 rest api. The functions which
  query the reference api cache their results for the life of the
  module.

- Miscellaneous functions.

This module is currently not thread-safe.
"""

# following module globals may be modified by client *before* calling any function:
_g5k_api_server = "https://api.grid5000.fr"
"""g5k api server url."""
_g5k_api_version = "2.0"
"""g5k api version to use."""
_g5k_platform_version = None
"""g5k platform version to use. If None, use the default (latest)."""

# following module globals not to be modified by client.
_g5k_api = None
"""Internal singleton instance of the g5k api rest resource."""
_g5k = None
"""cache of g5k structure.

a dict whose keys are sites, whose values are dict whose keys are
clusters, whose values are hosts.
"""

def get_local_site():
    """Get the name of the local site."""
    try:
        local_site = re.search("^[^ \t\n\r\f\v\.]+\.([^ \t\n\r\f\v\.]+)\.grid5000.fr$", socket.getfqdn()).group(1)
    except:
        raise EnvironmentError, "unable to get local site name"
    return local_site

def _get_g5k_api():
    """Get a singleton instance of a g5k api rest resource."""
    global _g5k_api
    if not _g5k_api:
        _g5k_api = restclient.Resource(_g5k_api_server,
                                       transport = restclient.transport.HTTPLib2Transport())
    return _g5k_api

def get_g5k_sites():
    """Get the list of Grid5000 sites. Returns an iterable."""
    global _g5k
    if not _g5k:
        sites = simplejson.loads(
            _get_g5k_api().get('/' + _g5k_api_version
                               + '/grid5000/sites',
                               headers = {'Accept': 'application/json'},
                               version = _g5k_platform_version))
        _g5k = dict()
        for site in [site['uid'] for site in sites['items']]:
            _g5k[site] = None
    return _g5k.keys()

def get_site_clusters(site):
    """Get the list of clusters from a site. Returns an iterable."""
    get_g5k_sites()
    if not _g5k.has_key(site):
        raise ValueError, "unknown g5k site %s" % (site,)
    if not _g5k[site]:
        clusters = simplejson.loads(
            _get_g5k_api().get('/' + _g5k_api_version
                               + '/grid5000/sites/'
                               + site
                               + '/clusters',
                               headers = {'Accept': 'application/json'},
                               version = _g5k_platform_version))
        _g5k[site] = dict()
        for cluster in [cluster['uid'] for cluster in clusters['items']]:
            _g5k[site][cluster] = None
    return _g5k[site].keys()

def get_cluster_hosts(cluster):
    """Get the list of hosts from a cluster. Returns an iterable."""
    _get_all_site_clusters()
    for site in _g5k.keys():
        if cluster in _g5k[site]:
            if not _g5k[site][cluster]:
                hosts = simplejson.loads(
                    _get_g5k_api().get('/' + _g5k_api_version
                                       + '/grid5000/sites/' + site
                                       + '/clusters/' + cluster
                                       + '/nodes',
                                       headers = {'Accept': 'application/json'},
                                       version = _g5k_platform_version))
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
    for site in _g5k.keys():
        if cluster in _g5k[site]:
            return site
    raise ValueError, "unknown g5k cluster %s" % (cluster,)

# def get_environments(site):
#     """Get the kadeploy environments for a site.

#     Returns a dict whose keys are environment names and whose values
#     are dicts of environment properties.
#     """
#     envs = simplejson.loads(
#         _get_g5k_api().get('/' + _g5k_api_version
#                            + '/grid5000/sites/' + site
#                            + '/environments',
#                            headers = {'Accept': 'application/json'},
#                            version = _g5k_platform_version))
#     environments = {}
#     for env in envs['items']:
#         environments[env['uid']] = env
#     return environments

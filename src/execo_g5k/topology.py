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

""" A module based on `networkx <http://networkx.github.io/>`_ to create a
topological graph of the Grid'5000 platform. "Nodes" are used to represent
elements (compute nodes, switch, router, renater) and "Edges" are the network
links. Nodes has a kind data (+ power and core for compute nodes)
whereas edges has bandwidth and latency information.

All information comes from the Grid'5000 reference API.
"""

from pprint import pformat
from time import time
from execo import logger, Host
from execo.log import style
from oar import format_date
from itertools import groupby
from operator import itemgetter
from api_cache import get_api_data
from api_utils import get_g5k_sites, get_host_site, canonical_host_name, \
    get_host_cluster, get_cluster_site, get_g5k_clusters, get_cluster_hosts, \
    get_site_clusters
import networkx as nx

try:
    import matplotlib.pyplot as plt
except:
    logger.warning('Matplotlib not found, no plot can be generated')
    pass
from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement, tostring

arbitrary_latency = 2.25E-3
suffix = '.grid5000.fr'


class g5k_graph(nx.Graph):
    """Main graph representing the topology of the Grid'5000 platform. All
    nodes elements are defined with their FQDN"""

    def __init__(self, sites=None):
        """Retrieve API data and initialize the Graph with api_commit
        and date of generations

        :param sites: add the topology of the given site(s)
         (can be a string or list of string)"""
        super(g5k_graph, self).__init__()
        # reading API data
        self.network, self.hosts = get_api_data()
        # initializing graph
        self.graph['api_commit'] = self.network['backbone'][0]['version']
        self.graph['date'] = format_date(time())
        self.sites = []
        if sites:
            if isinstance(sites, str):
                sites = [sites]
            for site in sites:
                self.add_site(site)

    def add_backbone(self):
        """Add the Renater backbone"""
        logger.info('Add/update %s network', style.emph('Renater'))
        backbone = self.network['backbone']
        # Adding all the elements of the backbone
        for equip in backbone:
            src = equip['uid'].replace('renater-', 'renater.') + suffix
            if not self.has_node(src):
                logger.detail('Adding ' + style.host(src))
                self.add_node(src, kind='renater')
            for lc in equip['linecards']:
                for port in lc['ports']:
                    if 'renater-' in port['uid']:
                        bandwidth = lc['rate'] if 'rate' not in port else port['rate']
                        latency = port['latency'] if 'latency' in port \
                            else arbitrary_latency
                        kind = 'renater' if 'kind' not in port else port['kind']
                        dst = port['uid'].replace('renater-', 'renater.') + \
                            suffix
                        logger.detail('* %s (%s, bw=%s, lat=%s)', dst, kind,
                                      bandwidth, latency)
                        if not self.has_node(dst):
                            self.add_node(dst, kind=kind)
                        if not self.has_edge(src, dst):
                            self.add_edge(src, dst, bandwidth=bandwidth,
                                          latency=latency)

        # Removing unused one
        if self.sites != get_g5k_sites():
            logger.detail('Removing unused Renater equipments')
            used_elements = []
            for site in self.sites:
                dests = self.sites[:]
                dests.remove(site)
                for dest in dests:
                    gw_src = 'gw-' + site + '.' + site + suffix
                    gw_dst = 'gw-' + dest + '.' + dest + suffix
                    for element in filter(lambda el: 'renater' in el,
                                          nx.shortest_path(self, gw_src, gw_dst)):
                        if element not in used_elements:
                            used_elements.append(element)

            for element, _ in filter(lambda n: n[1]['kind'] == 'renater',
                                     self.nodes_iter(data=True)):
                if element not in used_elements:
                    self.remove_node(element)

    def add_host(self, host):
        """Add the host to the graph, and its link to the equipment"""
        data = self._get_host_data(host)
        logger.info('Adding %s', style.host(host))
        site = get_host_site(data['uid'])
        power = data['performance']['core_flops']
        cores = data['architecture']['smt_size']
        attr = {'kind': 'node', 'power': power, 'cores': cores}
        host_name = data['uid'] + '.' + site + suffix
        self._add_node(host_name, attr)

        if site not in self.sites:
            self._add_site_router(site)

        # Finding the equipment
        eq_uid = filter(lambda n: n['enabled'] and not n['management'] and
                        n['mounted'] and n['interface'] == 'Ethernet',
                        data['network_adapters'])[0]['switch']
        if eq_uid is None:
            logger.warning('Unable to find the equipment for %s, removing',
                           style.host(host_name))
            self.remove_node(host_name)
            return

        eq_data = self._get_equip_data(eq_uid, site)
        eq_name = eq_uid + '.' + site + suffix

        # Adding the equipment
        self._add_node(eq_name, {'kind': eq_data['kind'],
                                 'backplane': eq_data['backplane_bps']})

        if eq_data['kind'] == 'switch':
            # we need to find how the switch is connected to the router
            path = self._switch_router_path(eq_uid, site)
            for i in range(len(path) - 1):
                eq1_data = self._get_equip_data(path[i + 1], site)
                self._add_node(path[i + 1] + '.' + site + suffix,
                               {'kind': eq1_data['kind'],
                                'backplane': eq1_data['backplane_bps']})
                attr = self._link_attr(eq_data['linecards'], path[i + 1])
                self._add_edge(path[i] + '.' + site + suffix,
                               path[i + 1] + '.' + site + suffix, attr)

        # Adding the link between node and equipment
        self._add_edge(host_name, eq_name,
                       self._link_attr(eq_data['linecards'], data['uid']))

    def remove_host(self, host):
        """Remove an host from the graph """
        if isinstance(host, Host):
            host = host.address
        logger.info('Removing %s', style.host(host))
        if host in self.nodes():
            self.remove_node(host)

    def add_cluster(self, cluster):
        """Add the cluster to the graph"""
        logger.info('Adding cluster %s', style.host(cluster))
        if cluster not in get_g5k_clusters():
            logger.error('%s is not a valid Grid\'5000 cluster',
                         style.emph(cluster))
            return False
        site = get_cluster_site(cluster)
        if site not in self.sites:
            self._add_site_router(site)

        for host in get_cluster_hosts(cluster):
            self.add_host(host)

    def remove_cluster(self, cluster):
        """Remove a cluster from the graph"""
        logger.info('Removing cluster %s', style.host(cluster))
        if cluster not in get_g5k_clusters():
            logger.error('%s is not a valid Grid\'5000 cluster',
                         style.emph(cluster))
            return
        for host in get_cluster_hosts(cluster):
            self.remove_host(host)

    def add_site(self, site):
        """Add the site to the graph"""
        logger.info('Adding site %s', style.host(site))
        if site not in get_g5k_sites():
            logger.error('%s is not a valid Grid\'5000 site', style.emph(site))
            return
        if site not in self.sites:
            self._add_site_router(site)

        for cluster in get_site_clusters(site):
            self.add_cluster(cluster)

    def remove_site(self, site):
        """Remove the site from the graph"""
        logger.info('Removing site %s', style.host(site))
        if site not in get_g5k_sites():
            logger.error('%s is not a valid Grid\'5000 site', style.emph(site))
            return
        for cluster in get_site_clusters(site):
            self.remove_cluster(cluster)
        self._remove_site_router(site)

        self.sites.remove(site)

    def _add_site_router(self, site):
        """Add the site router and it's connection to Renater"""
        data = filter(lambda n: n['kind'] == 'router', self.network[site])[0]
        router_name = data['uid'] + '.' + site + suffix
        renater_name = 'renater.' + site + suffix
        self._add_node(router_name, {'kind': 'router',
                                     'backplane': data['backplane_bps']})
        self._add_node(renater_name, {'kind': 'renater'})
        self._add_edge(router_name, renater_name,
                       self._link_attr(data['linecards'], 'renater-' + site))
        if site not in self.sites:
            self.sites.append(site)
        if len(self.sites) > 1:
            self.add_backbone()

    def _remove_site_router(self, site):
        """Remove the site router"""
        data = filter(lambda n: n['kind'] == 'router', self.network[site])[0]
        router_name = data['uid'] + '.' + site + suffix
        if router_name in self.nodes():
            self.remove_node(router_name)

    def get_hosts(self, site=None):
        """Return the compute hosts from the graph.
        :params site: if site is a valid g5k site, return only the hosts
        of this site"""
        if site in get_g5k_sites():
            return sorted(filter(lambda x: site in x[0] and
                                 x[1]['kind'] == 'router',
                                 self.nodes_iter(data=True)))
        else:
            return sorted(filter(lambda x: x[1]['kind'] == 'router',
                                 self.nodes_iter(data=True)))

    def get_backbone_graph(self):
        """Get the Renater backbone nodes and edges"""
        return self._get_subgraph_elements(lambda x: 'renater' in x)

    def get_site_graph(self, site=None):
        """Retrieve the nodes and edges of a site"""
        return self._get_subgraph_elements(lambda x: site in x)

    def get_cluster_graph(self, cluster=None):
        """Retrieve the nodes and edges of a cluster"""
        return self._get_subgraph_elements(lambda x: cluster in x)

    def get_routers(self):
        """Retrieve the routers of a graph """
        return sorted(filter(lambda x: x[1]['kind'] == 'router',
                             self.nodes_iter(data=True)))

    def get_host_switch(self, host):
        """Return the switch of an host"""
        if isinstance(host, Host):
            host = host.address
        for sw in nx.all_neighbors(self, canonical_host_name(host)):
            return sw.split('.')[0]

    def site_clusters(self, site):
        """Compute the information of the clusters of a site"""
        clusters = {}
        for host in filter(lambda n: n[1]['kind'] == 'node' and site in n[0],
                           self.nodes(True)):
            hostname, suffix = host[0].split('.', 1)
            cluster = hostname.split('-')[0]
            if cluster not in clusters:
                clusters[cluster] = {'equips': {},
                                     'suffix': '.' + suffix,
                                     'prefix': cluster + '-',
                                     'core': host[1]['cores'],
                                     'power': host[1]['power']}
            for equip in nx.all_neighbors(self, host[0]):
                if self.node[equip]['kind'] in ['switch', 'router']:
                    clusters[cluster]['latency'] = self.edge[host[0]][equip]['latency']
                    clusters[cluster]['bandwidth'] = self.edge[host[0]][equip]['bandwidth']
                    if equip not in clusters[cluster]['equips']:
                        clusters[cluster]['equips'][equip] = [host[0]]
                    else:
                        clusters[cluster]['equips'][equip].append(host[0])

        for cluster, data in clusters.iteritems():

            if len(data['equips']) == 1:
                radical_list = map(lambda x: int(x.split('.')[0].split('-')[1]),
                                   data['equips'].itervalues().next())
                radical = str(min(radical_list)) + '-' + str(max(radical_list))
                data['equips'][data['equips'].keys()[0]] = radical
            else:
                for equip, hosts in data['equips'].iteritems():
                    if 'gw-' in equip:
                        router = equip
                    else:
                        router = list(set(filter(lambda x: 'gw-' in x,
                                                 nx.all_neighbors(self, equip))))[0]
                        clusters[cluster]['bb_lat'] = self.edge[router][equip]['latency']
                        clusters[cluster]['bb_bw'] = self.edge[router][equip]['bandwidth']
                    radical_list = sorted(map(lambda x: int(x.split('.')[0].split('-')[1]),
                                              hosts))
                    radical = ''
                    for k, g in groupby(enumerate(radical_list),
                                        lambda (i, x): i - x):
                        radical_range = map(itemgetter(1), g)
                        if len(radical_range) > 1:
                            radical += str(min(radical_range)) + '-' + \
                                str(max(radical_range))
                        else:
                            radical += str(radical_range[0])
                        radical += ','
                    radical = radical[:-1]
                    data['equips'][equip] = radical
        return clusters

    def _add_node(self, name, attr):
        """A method that add a node with its attribute or update attributes
        if the node is present"""
        if not self.has_node(name):
            logger.detail('Adding %s with %s', style.host(name), pformat(attr))
            self.add_node(name, attr)
        else:
            logger.detail('Updating %s attributes with %s',
                          style.host(name), pformat(attr))
            for k, v in attr.iteritems():
                nx.set_node_attributes(self, k, {name: v})

    def _add_edge(self, src, dst, attr):
        """A method that add an edge with its attribute or update attributes
        if the edge is present"""
        if not self.has_edge(src, dst):
            logger.detail('Adding link between %s and %s with attributes %s',
                          style.host(src), style.host(dst), pformat(attr))
            self.add_edge(src, dst, attr)
        else:
            logger.detail('Updating %s<->%s attributes with %s',
                          style.host(src), style.host(dst), pformat(attr))
            for k, v in attr.iteritems():
                nx.set_edge_attributes(self, k, {(src, dst): v})

    def _switch_router_path(self, switch_uid, site):
        """Find the several elements between a switch and a router"""
        path = [switch_uid]
        router = 'gw-' + site
        lc_data = self._get_equip_data(switch_uid, site)['linecards']

        if self._link_attr(lc_data, router)['latency'] is not None:
            path.append(router)
        else:
            candidates = []
            for lc in filter(lambda n: 'ports' in n, lc_data):
                for port in sorted(filter(lambda p: 'uid' in p, lc['ports'])):
                    kind = port['kind'] if 'kind' in port else lc['kind']
                    if kind == 'switch' and port['uid'] not in candidates:
                        candidates.append(port['uid'])
            paths = {}
            for switch in candidates:
                paths[switch] = self._switch_router_path(switch, site)
            smallest = min(paths, key=lambda k: len(paths[k]))
            path += paths[smallest]

        return path

    def _link_attr(self, lc_data, uid):
        """Retrieve the bandwith and latency of a link to an element"""
        bandwidth = 0
        latency = None
        for lc in filter(lambda n: 'ports' in n, lc_data):
            for port in sorted(filter(lambda p: 'uid' in p, lc['ports'])):
                kind = port['kind'] if 'kind' in port else lc['kind']
                if port['uid'] == uid:
                    active_if = True
                    if 'port' in port and kind == 'node':
                        iface = filter(lambda a: a["device"] == port['port'],
                                       self._get_host_data(uid)['network_adapters'])[0]
                        if not iface['mounted']:
                            active_if = False
                    if active_if:
                        bandwidth += lc['rate'] if 'rate' not in port \
                            else port['rate']
                        latency = port['latency'] if 'latency' in port \
                            else arbitrary_latency

        return {'bandwidth': bandwidth, 'latency': latency}

    def _get_host_data(self, host):
        """Return the attributes of a host"""
        if isinstance(host, Host):
            host = host.address
        host = canonical_host_name(host)
        if '.' not in host:
            uid, cluster, site = host, get_host_cluster(host), \
                get_host_site(host)
        else:
            uid, site, _, _ = host.split('.')
            cluster = uid.split('-')[0]
        if not cluster:
            logger.error('Unable to find the cluster of %s', host)
            return None
        if not site:
            logger.error('Unable to find the site of %s', host)
            return None

        return filter(lambda h: h['uid'] == uid, self.hosts[site][cluster])[0]

    def _get_equip_data(self, uid, site):
        """Return the attributes of a network equipments"""
        return filter(lambda eq: eq['uid'] == uid, self.network[site])[0]

    def _get_subgraph_elements(self, my_filter):
        """Return the nodes and edges matching a filter on nodes"""
        sgr = self.subgraph(filter(my_filter, self.nodes()))
        return sgr


def treemap(gr, nodes_legend=None, edges_legend=None, nodes_labels=None,
            layout='neato', compact=False):
    """Create a treemap of the topology and return a matplotlib figure

    :param nodes_legend: a dict of dicts containing the parameter used to draw
     the nodes, such as 'myelement': {'color': '#9CF7BC', 'shape': 'p',
     'size': 200}

    :param edges_legend: a dict of dicts containing the parameter used to draw
     the edges, such as bandwidth: {'width': 0.2, 'color': '#666666'}

    :param nodes_labels: a dict of dicts containing the font parameters for
     the labels, such as 'myelement ': {'nodes': {}, 'font_size': 8,
     'font_weight': 'bold', 'str_func': lambda n: n.split('.')[1].title()}

    :param layout: the graphviz tool to be used to compute node position

    :param compact: represent only on node for a cluster/cabinet

    WARNING: This function use matplotlib.figure that by default requires a
    DISPLAY. If you want use this on a headless host, you need to change the
    matplotlib backend before to import execo_g5k.topology module.
    """

    _default_color = '#000000'
    _default_shape = 'o'
    _default_size = 100
    _default_width = 1.0
    _default_font_size = 10
    _default_font_weight = 'normal'

    def _default_str_func(n):
        return n.split('.')[0]

    def _default_nodes_legend():
        """Create a default legend for the nodes"""
        return {'renater':
                {'color': '#9CF7BC', 'shape': 'p', 'size': 200},
                'router':
                {'color': '#BFDFF2', 'shape': '8', 'size': 300},
                'switch':
                {'color': '#F5C9CD', 'shape': 's', 'size': 100},
                'node':
                {'color': '#F0F7BE', 'shape': 'o', 'size': 30},
                'cluster':
                {'color': '#F0F7BE', 'shape': 'd', 'size': 200},
                'default':
                {'color': _default_color, 'shape': _default_shape,
                 'size': _default_size}
                }

    def _default_edges_legend():
        """Defines the width and color of the edges based on bandwidth"""
        return {1000000000: {'width': 0.2, 'color': '#666666'},
                3000000000: {'width': 0.6, 'color': '#333333'},
                10000000000: {'width': 1.0, 'color': '#111111'},
                20000000000: {'width': 2.0, 'color': '#111111'},
                30000000000: {'width': 3.0, 'color': '#111111'},
                40000000000: {'width': 4.0, 'color': '#111111'},
                'default': {'width': _default_width, 'color': _default_color}}

    def _default_nodes_labels(compact=False):
        """Defines the font labels"""
        base_size = 2 if compact else 1

        def _default_str_func(n):
            return n.split('.')[0]

        return {'renater':
                {'nodes': {},
                 'font_size': base_size * 6,
                 'font_weight': 'normal',
                 'str_func': lambda n: n.split('.')[1].title()},
                'router':
                {'nodes': {},
                 'font_size': base_size * 6,
                 'font_weight': 'bold',
                 'str_func': _default_str_func},
                'switch':
                {'nodes': {},
                 'font_size': base_size * 6,
                 'font_weight': 'normal',
                 'str_func': _default_str_func},
                'cluster':
                {'nodes': {},
                 'font_size': base_size * 5,
                 'font_weight': 'normal',
                 'str_func': _default_str_func},
                'node':
                {'nodes': {},
                 'font_size': base_size * 5,
                 'font_weight': 'normal',
                 'str_func': _default_str_func},
                'default':
                {'nodes': {},
                 'font_size': _default_font_size,
                 'font_weight': _default_font_weight,
                 'str_func': _default_str_func}
                }

    # Setting legend and labels
    _nodes_legend = _default_nodes_legend()
    _edges_legend = _default_edges_legend()
    _nodes_labels = _default_nodes_labels(compact)
    if nodes_legend:
        _nodes_legend.update(nodes_legend)
    if edges_legend:
        _edges_legend.update(edges_legend)
    if nodes_labels:
        _nodes_labels.update(nodes_labels)

    if not compact:
        elements = ['renater', 'router', 'switch', 'node']
    else:
        for site in gr.sites:
            for cluster, data in gr.site_clusters(site).iteritems():
                for equip, radicals in data['equips'].items():
                    gr.add_node(cluster + '\n' + radicals,
                                {'kind': 'cluster'})
                    gr.add_edge(cluster + '\n' + radicals, equip,
                                {'bandwidth': data['bandwidth']})

        gr.remove_nodes_from(map(lambda n: n[0],
                                 filter(lambda n: n[1]['kind'] == 'node',
                                        gr.nodes(True))))

        elements = ['renater', 'router', 'switch', 'cluster']

    logger.detail('Legend and labels initialized')
    # Initializing plot
    fig = plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(111)

    logger.detail('Defining positions')
    try:
        pos = nx.graphviz_layout(gr, prog=layout)
    except:
        logger.warning('No graphviz installed, using spring layout that' +
                       ' does not scale well ...')
        pos = nx.spring_layout(gr, iterations=100)
    # Adding the nodes
    for k in elements:
        nodes = [node[0] for node in gr.nodes_iter(data=True)
                 if 'kind' in node[1] and node[1]['kind'] == k]
        nodes = nx.draw_networkx_nodes(gr, pos, nodelist=nodes,
                                       node_shape=_nodes_legend[k]['shape']
                                       if 'shape' in _nodes_legend[k] else
                                       _default_shape,
                                       node_color=_nodes_legend[k]['color']
                                       if 'color' in _nodes_legend[k] else
                                       _default_color,
                                       node_size=_nodes_legend[k]['size']
                                       if 'size' in _nodes_legend[k] else
                                       _default_size)

    # Adding the edges
    for bandwidth, params in _edges_legend.iteritems():
        if bandwidth != 'other':
            edges = [(edge[0], edge[1]) for edge in gr.edges_iter(data=True)
                     if 'bandwidth' in edge[2] and edge[2]['bandwidth'] == bandwidth]
            nx.draw_networkx_edges(gr, pos, edgelist=edges,
                                   width=params['width'] if 'width' in params
                                   else _default_width,
                                   edge_color=params['color'] if 'color' in params
                                   else _default_color)
    edges = [(edge[0], edge[1]) for edge in gr.edges_iter(data=True)
             if edge[2]['bandwidth'] not in _edges_legend.keys()]

    nx.draw_networkx_edges(gr, pos, edgelist=edges,
                           width=_edges_legend['default']['width'],
                           edge_color=_edges_legend['default']['color'])
    # Adding the labels
    for node, data in gr.nodes_iter(data=True):
        if 'nodes' not in _nodes_labels[data['kind']]:
            _nodes_labels[data['kind']]['nodes'] = {}
        if data['kind'] in _nodes_labels:
            _nodes_labels[data['kind']]['nodes'][node] = _nodes_labels[data['kind']]['str_func'](node) \
                if 'str_func' in _nodes_labels[data['kind']] else _default_str_func(node)
        else:
            _nodes_labels['default']['nodes'][node] = _nodes_labels['default']['str_func'](node)

    for data in _nodes_labels.itervalues():
        nx.draw_networkx_labels(gr, pos, labels=data['nodes'],
                                font_size=data['font_size']
                                if 'font_size' in data else _default_font_size,
                                font_weight=data['font_weight']
                                if 'font_weight' in data else _default_font_weight)

    plt.axis('off')
    plt.tight_layout()

    title = 'Created by execo_g5k.topology \n%s\nAPI commit %s' % \
        (gr.graph['date'], gr.graph['api_commit'])
    plt.text(0.1, 0, title, transform=ax.transAxes)

    return fig

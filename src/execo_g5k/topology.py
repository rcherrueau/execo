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

from pprint import pformat, pprint
from time import time
from execo import logger, Host
from execo.log import style
from oar import format_date
from itertools import groupby
from operator import itemgetter
from api_utils import get_g5k_sites, get_host_site, canonical_host_name, \
    get_host_cluster, get_cluster_site, get_g5k_clusters, get_cluster_hosts, \
    get_site_clusters, get_api_data, get_g5k_hosts
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


class g5k_graph(nx.MultiGraph):
    """Main graph representing the topology of the Grid'5000 platform. All
    nodes elements are defined with their FQDN"""

    def __init__(self, elements=None):
        """Create the :func:`~nx.MultiGraph` representing Grid'5000 network 
        topology

        :param sites: add the topology of the given site(s)"""
        logger.debug('Initializing g5k_graph')
        super(g5k_graph, self).__init__()
        self.data = get_api_data()
        self.graph['api_commit'] = self.data['network']['backbone'][0]['version']
        self.graph['date'] = format_date(time())

        if elements:
            for e in elements:
                if e in get_g5k_sites():
                    self.add_site(e, self.data['sites'][e])
                if e in get_g5k_clusters():
                    self.add_cluster(e, self.data['clusters'][e])
                if e in get_g5k_hosts():
                    self.add_host(e, self.data['hosts'][e])
            if len(self.get_sites()) > 1:
                self.add_backbone()

    # add/update/rm elements, public methods
    def add_host(self, host, data=None):
        """ """

        if data:
            power = data['performance']['core_flops']
            cores = data['architecture']['smt_size']
        else:
            power = 0
            cores = 0
        logger.debug('Adding %s', style.host(host))
        if len(self.get_host_adapters(host)) > 0:
            self.add_node(host, {'kind': 'node',
                                 'power': power,
                                 'cores': cores})
            for eq in self.get_host_adapters(host):
                self.add_equip(eq['switch'], get_host_site(host))
#            edge_name = host + '_' + eq['device'] + '_' + eq['switch']
#            if not self.has_edge(host, eq['switch'], key=edge_name):
#                self.add_edge(host, eq['switch'], key=edge_name, 
#                              mounted=eq['mounted'], bandwidth=eq['rate'],
#                              device=eq['device'])
#            if self.data['network'][get_host_site(host)][eq['switch']]['kind'] != 'router':
#                self._switch_router_path(eq['switch'], get_host_site(host))

    def rm_host(self, host):
        """ """
        logger.debug('Removing host %s', style.host(host))
        self.remove_node(host)

    def add_cluster(self, cluster, data=None):
        """ """
        for h in get_cluster_hosts(cluster):
            self.add_host(h, self.data['hosts'][h])

    def rm_cluster(self, cluster):
        """ """
        for h in get_cluster_hosts(cluster):
            self.rm_host(h)

    def add_site(self, site, data=None):
        """ """
        for c in get_site_clusters(site):
            for h in get_cluster_hosts(c):
                self.add_host(h, self.data['hosts'][h])

    def rm_site(self, site):
        """ """
        for c in get_site_clusters(site):
            for h in get_cluster_hosts(c):
                self.remove_host(h)

    def add_equip(self, equip, site):
        """ """
        if equip not in self.data['network'][site]:
            logger.warning('Equipment %s not described in API')
            return
        data = self.data['network'][site][equip]
        logger.debug('Adding equipment %s', equip)
        self.add_node(equip, kind=data['kind'], 
                      backplane=data['backplane_bps'])
        lc_data = data['linecards']
        if data['kind'] == 'router':
            i_lc = 0
            for lc in filter(lambda n: 'ports' in n, lc_data):
                lc_node = equip + '_lc' + str(i_lc)
                lc_has_element = False
                for port in sorted(filter(lambda p: 'uid' in p, lc['ports'])):
                    kind = port['kind'] if 'kind' in port else lc['kind']
                    bandwidth = lc['rate'] if 'rate' not in port else port['rate']
                    if self.has_node(port['uid']):
                        lc_has_element = True
                        if kind == 'node':
                            for e in self.get_host_adapters(port['uid']):
                                if e['switch'] == equip:
                                    key1 = lc_node + '_' + port['uid'] + '_' + e['device']
                                    logger.debug('Adding link between %s and %s',
                                                 lc_node, port['uid'])
                                    self.add_edge(lc_node, port['uid'], key1,
                                                  bandwidth=bandwidth)
                                    key2 = equip + '_' + lc_node
                                    logger.debug('Adding link between %s and %s',
                                                 equip, lc_node)
                                    self.add_edge(equip, lc_node, key2,
                                                  bandwidth=0)
                        if kind == 'switch':
                            key1 = lc_node + '_' + port['uid']
                            self.add_edge(lc_node, port['uid'], key1, bandwidth=bandwidth)
                            key2 = equip + '_' + lc_node
                            self.add_edge(equip, lc_node, key2,
                                          bandwidth=0)
                    if 'renater' in port['uid']:
                        lc_has_element = True
                        self.add_node(port['uid'], kind='renater')
                        key1 = lc_node + ' ' + port['uid']
                        self.add_edge(lc_node, port['uid'], key1,
                                      bandwidth=bandwidth)
                        key2 = equip + '_' + lc_node
                        self.add_edge(equip, lc_node, key2,
                                      bandwidth=bandwidth)
                if lc_has_element:
                    backplane = lc['backplane_bps'] if 'backplane_bps' \
                        in lc else data['backplane_bps']
                    self.add_node(lc_node, kind='linecard',
                          backplane=backplane)
                i_lc += 1
        else:
            # some switch have two linecards ?? pat, sgraphene1 => REPORT BUG
            for lc in filter(lambda n: 'ports' in n, lc_data):
                for port in sorted(filter(lambda p: 'uid' in p, lc['ports'])):
                    kind = port['kind'] if 'kind' in port else lc['kind']
                    bandwidth = lc['rate'] if 'rate' not in port else port['rate']
                    if self.has_node(port['uid']):
                        if kind == 'node':
                            for e in self.get_host_adapters(port['uid']):
                                if e['switch'] == equip:
                                    key = equip + '_' + port['uid'] + '_' + e['device']
                                    self.add_edge(equip, port['uid'], key,
                                              bandwidth=bandwidth)
                        if kind == 'switch':
                            key = equip + '_' + port['uid']
                            self.add_edge(equip, port['uid'], key,
                                              bandwidth=bandwidth)
                    if kind == 'router':
                        self.add_equip(port['uid'], site)

    def rm_equip(self, equip):
        """ """
        logger.debug('Removing equip %s', style.host(equip))
        self.remove_node(equip)

    def add_backbone(self):
        """ """
        logger.debug('Add %s network', style.emph('Renater'))
        backbone = self.data['network']['backbone']
        for equip in backbone:
            src = equip['uid']
            self.add_node(src, kind='renater')
            for lc in equip['linecards']:
                for port in lc['ports']:
                    if 'renater-' in port['uid']:
                        bandwidth = lc['rate'] if 'rate' not in port else port['rate']
                        latency = port['latency'] if 'latency' in port \
                            else arbitrary_latency
                        kind = 'renater' if 'kind' not in port else port['kind']
                        dst = port['uid']
                        logger.debug('* %s (%s, bw=%s, lat=%s)', dst, kind,
                                      bandwidth, latency)
                        self.add_node(dst, kind=kind)
                        if not self.has_edge(src, dst):
                            self.add_edge(src, dst, bandwidth=bandwidth,
                                          latency=latency)
        # Removing unused one
        if self.get_sites != get_g5k_sites():
            logger.detail('Removing unused Renater equipments')
            used_elements = []
            for site in self.get_sites():
                dests = self.get_sites()[:]
                dests.remove(site)
                for dest in dests:
                    gw_src = 'gw-' + site
                    gw_dst = 'gw-' + dest
                    for element in filter(lambda el: 'renater' in el,
                                          nx.shortest_path(self, gw_src, gw_dst)):
                        if element not in used_elements:
                            used_elements.append(element)

            for element, _ in filter(lambda n: n[1]['kind'] == 'renater',
                                     self.nodes_iter(data=True)):
                if element not in used_elements:
                    self.remove_node(element)

    def rm_backbone(self):
        """ """
        self.remove_nodes_from(self.get_backbone())

    # get elements, public methods
    def get_hosts(self):
        """ """
        return filter(lambda x: x[1]['kind'] == 'node', self.nodes(True))

    def get_clusters(self):
        """ """
        return list(set(map(lambda y: get_host_cluster(y[0]),
                   filter(lambda x: x[1]['kind'] == 'node', self.nodes(True)))))

    def get_sites(self):
        """ """
        return list(set(map(lambda y: get_host_site(y[0]),
                   filter(lambda x: x[1]['kind'] == 'node', self.nodes(True)))))

    def get_backbone(self):
        return filter(lambda x: x[1]['kind'] == 'renater', self.nodes(True))

    def get_host_adapters(self, host):
        """ """
        try:
            if host in self.data['hosts']:
                return filter(lambda n: not n['management'] and n['mountable']
                              and n['switch'] and n['interface'] == 'Ethernet',
                             filter(lambda m: 'switch' in m,
                                    self.data['hosts'][host]['network_adapters']))
        except:
            logger.warning('Wrong description for host %s', style.host(host))
            print self.data['hosts'][host]['network_adapters']
            return []


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
                 'str_func': lambda n: n.split('-')[1].title()},
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
#        if 'nodes' not in _nodes_labels[data['kind']]:
#            _nodes_labels[data['kind']]['nodes'] = {}
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


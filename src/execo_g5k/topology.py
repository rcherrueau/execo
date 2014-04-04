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
hosts (compute nodes, switch, router, renater) and "Edges" are the network
links. Nodes has a kind data (+ power and core for compute nodes) 
whereas edges has bandwidth and latency information.
\n
All information comes from the Grid'5000 reference API

"""
from execo import logger
from api_cache import get_api_data
from api_utils import get_g5k_sites
from networkx import Graph, set_edge_attributes, get_edge_attributes, \
    draw_networkx_nodes, draw_networkx_edges, draw_networkx_labels, \
    graphviz_layout, spring_layout
import matplotlib.pyplot as plt

arbitrary_latency = 2.25E-3

topo_cache = None


def backbone_graph():
    """Return a networkx undirected graph describing the Grid'5000
    backbone from the list of backbone equipements:
    - nodes data: kind (renater, gw, switch, )"""
    network, _ = get_api_data()
    backbone = network['backbone']
    gr = Graph()
    # Adding backbone equipments and links
    for equip in backbone:
        src = equip['uid'].replace('renater-', 'renater.')
        if not gr.has_node(src):
            gr.add_node(src, kind='renater')
        for lc in equip['linecards']:
            for port in lc['ports']:
                kind = 'renater' if not 'kind' in port else port['kind']
                dst = port['uid'] if not 'site_uid' in port else port['uid'] \
                + '.' + port['site_uid']
                dst = dst.replace('renater-', 'renater.')
                rate = lc['rate'] if not 'rate' in port else port['rate']
                latency = port['latency'] if 'latency' in port \
                    else arbitrary_latency
                if not gr.has_node(dst):
                    gr.add_node(dst, kind=kind)
                if not gr.has_edge(src, dst):
                    gr.add_edge(src, dst, bandwidth=rate, latency=latency)
    return gr


def site_graph(site):
    """Return a networkx undirected graph describing the site
    topology from the dict of hosts and list of site equipments"""
    network, all_hosts = get_api_data()
    equips = network[site]
    hosts = all_hosts[site]
    sgr = Graph()
    for equip in equips:
        src = equip['uid'] + '.' + site
        if not sgr.has_node(src):
            sgr.add_node(src, kind=equip['kind'])
        for lc in filter(lambda n: 'ports' in n, equip['linecards']):
            if not 'kind' in lc:
                lc['kind'] = 'unknown'
            for port in filter(lambda p: 'uid' in p, lc['ports']):
                kind = lc['kind'] if not 'kind' in port else port['kind']
                dst = port['uid'] + '.' + site
                rate = lc['rate'] if not 'rate' in port else port['rate']
                latency = port['latency'] if 'latency' in port \
                    else arbitrary_latency
                if kind in ['switch', 'router']:
                    if not sgr.has_node(dst):
                        sgr.add_node(dst, kind=kind)
                    if not sgr.has_edge(src, dst):
                        sgr.add_edge(src, dst, bandwidth=rate, latency=latency)
                    else:
                        tmp = get_edge_attributes(sgr, 'bandwidth')
                        if (src, dst) in tmp.keys():
                            set_edge_attributes(sgr, 'bandwidth',
                                        {(src, dst): rate + tmp[(src, dst)]})

    for cluster_hosts in hosts.itervalues():
        for host in cluster_hosts:
            src = host['uid'] + '.' + site
            if not sgr.has_node(src):
                sgr.add_node(src, kind='node',
                             power=host['performance']['core_flops'],
                             core=host['architecture']['smt_size'])
            for adapt in filter(lambda n: n['enabled'] and not n['management']
                                and n['interface'] == 'Ethernet',
                                host['network_adapters']):
                if adapt['switch'] is None:
                    logger.warning('%s: link between %s and %s is not correct',
                                    site, src, dst)
                else:
                    dst = adapt['switch'] + '.' + site
                if not sgr.has_edge(src, dst):
                    sgr.add_edge(src, dst,
                                 bandwidth=adapt['rate'],
                                 latency=latency)
    return sgr


def remove_non_g5k(gr):
    logger.detail('Removing stalc, infiniband, myrinet ')
    for node in gr.nodes():
        if 'ib.' in node or 'stalc' in node or 'voltaire' in node or 'ib-' in node\
            or 'summit' in node or 'ipmi' in node or 'CICT' in node or 'mxl2' in node\
            or 'grelon' in node or 'myrinet' in node or 'salome' in node or 'interco' in node:
            gr.remove_node(node)


def gr_to_map(gr, outfile=None, config=None):
    """Export a topology graph to a map"""
    sites = []
    for node in gr.nodes_iter():
        site = node.split('.')[1]
        if site not in sites and site in get_g5k_sites():
            sites.append(site)
    sites.sort()
    if outfile is None:
        outfile = '_'.join(sites) + '.png'
    if config is None:
        config = {'nodes': {
                    'renater': {'color': '#9CF7BC', 'shape': 'p', 'size': 200},
                    'router': {'color': '#BFDFF2', 'shape': '8', 'size': 300},
                    'switch': {'color': '#F5C9CD', 'shape': 's', 'size': 100},
                    'node': {'color': '#F0F7BE', 'shape': 'o', 'size': 30},
                    },
                'edges': {
                    1000000000: {'width': 0.1, 'color': '#aaaaaa'},
                    3000000000: {'width': 0.6, 'color': '#333333'},
                    10000000000: {'width': 1.0, 'color': '#111111'},
                    20000000000: {'width': 2.0, 'color': '#8FC2FF'},
                    'other': {'width': 1.0, 'color': '#FF4E5A'}
                    }
                }
    sites = []

    plt.figure(figsize=(15, 15))
    logger.detail('Defining positions')
    try:
        pos = graphviz_layout(gr, prog='neato')
    except:
        logger.warning('No graphviz installed, using spring layout that ' + \
                       ' does not scale well ...')
        pos = spring_layout(gr, iterations=100)

    for kind in ['renater', 'router', 'switch', 'node']:
        nodes = [node[0] for node in gr.nodes_iter(data=True)
                 if node[1]['kind'] == kind]
        draw_networkx_nodes(gr, pos, nodelist=nodes,
            node_shape=config['nodes'][kind]['shape'],
            node_color=config['nodes'][kind]['color'],
            node_size=config['nodes'][kind]['size'])
    for bandwidth, params in config['edges'].iteritems():
        if bandwidth != 'other':
            edges = [(edge[0], edge[1]) for edge in gr.edges_iter(data=True)
                 if edge[2]['bandwidth'] == bandwidth]
            draw_networkx_edges(gr, pos, edgelist=edges,
                    width=params['width'], edge_color=params['color'])
    edges = [(edge[0], edge[1]) for edge in gr.edges_iter(data=True)
         if edge[2]['bandwidth'] not in config['edges'].keys()]
    draw_networkx_edges(gr, pos, edgelist=edges,
            width=config['edges']['other']['width'],
            edge_color=config['edges']['other']['color'])
    labels = {}
    for node, data in gr.nodes_iter(data=True):
        if data['kind'] == 'renater':
            labels[node] = node.split('.')[1].title()
        elif data['kind'] == 'node' and '-1.' in node:
            labels[node] = node.split('-')[0]
    draw_networkx_labels(gr, pos, labels=labels,
                        font_size=16, font_weight='normal')
    plt.axis('off')
    plt.tight_layout()
    logger.detail('Saving file to %s', outfile)
    plt.savefig(outfile, bbox_inches='tight', dpi=300)


def gr_to_simgrid():
    """Produce a SimGrid platform XML file"""
    print 'not finished'

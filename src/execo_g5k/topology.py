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
from time import time
from execo import logger, Host
from execo.log import style
from execo_g5k.oar import format_date
from itertools import product
from api_cache import get_api_data
from api_utils import get_g5k_sites
from networkx import Graph, set_edge_attributes, get_edge_attributes, \
    draw_networkx_nodes, draw_networkx_edges, draw_networkx_labels, \
    graphviz_layout, spring_layout, shortest_path, all_neighbors, \
    get_node_attributes


arbitrary_latency = 2.25E-3

topo_cache = None


def backbone_graph():
    """Return a networkx undirected graph describing the Grid'5000
    backbone from the list of backbone equipements:
    - nodes data: kind (renater, gw, switch, )"""
    network, _ = get_api_data()
    backbone = network['backbone']
    gr = Graph(api_commit=backbone[0]['version'],
               date=format_date(time()))
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
    sgr = Graph(api_commit=equips[0]['version'],
                date=format_date(time()))
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


def filter_compute_nodes(gr, nodes):
    """Remove compute nodes that are not in nodes list"""

    for node, data in gr.nodes_iter(data=True):
        if data['kind'] == 'node' and Host(node + '.grid5000.fr') not in nodes:
            gr.remove_node(node)

    for node, data in gr.nodes_iter(data=True):
        if data['kind'] == 'switch':
            switch_has_node = False
            for dest in all_neighbors(gr, node):
                if get_node_attributes(gr, 'kind')[dest] == 'node':
                    switch_has_node = True
                    break
            if not switch_has_node:
                gr.remove_node(node)


def remove_non_g5k(gr):
    logger.detail('Removing stalc, infiniband, myrinet ')
    to_remove = ['ib.', 'stalc', 'voltaire', 'ib-', 'summit', 'ipmi', 'CICT',
                 'mxl2', 'grelon', 'myrinet', 'salome', 'interco']
    for node in gr.nodes():
        if any(s in node for s in to_remove):
            remove_edges = []
            for dest in all_neighbors(gr, node):
                remove_edges.append((node, dest))
                remove_edges.append((dest, node))
            gr.remove_edges_from(remove_edges)
            gr.remove_node(node)


def gr_to_image(gr, outfile=None, config=None):
    """Export a topology graph to a image"""
    import matplotlib.pyplot as plt
    import matplotlib.patches
    sites = []
    for node in gr.nodes_iter():
        site = node.split('.')[1]
        if site not in sites and site in get_g5k_sites():
            sites.append(site)
    sites.sort()
    if outfile is None:
        outfile = '_'.join(sites) + '_' + gr.graph['date'].replace(' ', '_') \
            + '.png'
    if config is None:
        config = {'nodes': {
                    'renater': {'color': '#9CF7BC', 'shape': 'p', 'size': 200},
                    'router': {'color': '#BFDFF2', 'shape': '8', 'size': 300},
                    'switch': {'color': '#F5C9CD', 'shape': 's', 'size': 100},
                    'node': {'color': '#F0F7BE', 'shape': 'o', 'size': 30},
                    },
                'edges': {
                    1000000000: {'width': 0.2, 'color': '#666666'},
                    3000000000: {'width': 0.6, 'color': '#333333'},
                    10000000000: {'width': 1.0, 'color': '#111111'},
                    20000000000: {'width': 2.0, 'color': '#8FC2FF'},
                    'other': {'width': 1.0, 'color': '#FF4E5A'}
                    }
                }
    sites = []

    fig = plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(111)
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
    # Adding labels
    labels = {'renater': {'nodes': {}, 'font_size': 16, 'font_weight': 'bold'},
          'switch': {'nodes': {}, 'font_size': 12, 'font_weight': 'normal'},
          'cluster': {'nodes': {}, 'font_size': 14, 'font_weight': 'normal'}
            }
    for node, data in gr.nodes_iter(data=True):
        if data['kind'] == 'renater':
            labels['renater']['nodes'][node] = node.split('.')[1].title()
        elif data['kind'] == 'switch':
            labels['switch']['nodes'][node] = node.split('.')[0]
        elif data['kind'] == 'node' and '-1.' in node:
            labels['cluster']['nodes'][node] = node.split('-')[0]
    for label, data in labels.iteritems():
        draw_networkx_labels(gr, pos, labels=data['nodes'],
                font_size=data['font_size'], font_weight=data['font_weight'])
    plt.axis('off')
    plt.tight_layout()

    plt.text(0, 0, 'Created by topo5k \n' + gr.graph['date'] + '\n'
             'API commit ' + gr.graph['api_commit'],
             transform=ax.transAxes)

    legend = matplotlib.patches.Rectangle((0.90, 0.91), 0.15, 0.24,
                            color='#444444', transform=ax.transAxes)
    ax.add_patch(legend)

    i = 0
    for kind, param in config['nodes'].iteritems():
        plt.text(0.91, 0.977 - i * 0.001, kind, fontdict={'color': param['color'],
                    'size': 14, 'variant': 'small-caps', 'weight': 'bold'},
                 transform=ax.transAxes)
        i += 20

    logger.info('Saving file to %s', style.emph(outfile))
    plt.savefig(outfile, bbox_inches='tight', dpi=300)


def gr_to_simgrid(gr, outfile=None, tool_signature='Generated using execo_g5k.topology',
                  compact=False):
    """Produce a SimGrid platform XML file

    :params outfile: name of the output file

    :params tool: signature added in comment of the XML file

    :params compact: use compact description, i.e. cluster instead
      of hosts
    """
    default_routing = 'Floyd'
    suffix = '.grid5000.fr'
    from xml.dom import minidom
    from xml.etree.ElementTree import Element, SubElement, tostring, parse

    def prettify(elem):
        """Return a pretty-printed XML string for the Element.  """
        rough_string = tostring(elem, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="  ").replace('<?xml version="1.0" ?>\n', '')

    # Creating the AS
    platform = Element('platform', attrib={'version': '3'})
    sites = list(set(map(lambda y: y.split('.')[-1],
                         filter(lambda x: 'gw-' in x, gr.nodes()))))
    if len(sites) > 1:
        main_as = SubElement(platform, 'AS', attrib={'id': 'grid5000.fr',
                                        'routing': default_routing})

        for site in sites:
            SubElement(main_as, 'AS', attrib={'id': site + suffix, 
                                              'routing': default_routing})
        # Creating the backbone links
        for element1, element2, attrib in sorted(gr.edges_iter(data=True)):
            element1, element2 = sorted([element1, element2])
            SubElement(main_as, 'link', attrib={'id': element1 + '_' + element2,
                                    'latency': str(attrib['latency']),
                                    'bandwidth': str(attrib['bandwidth'])})
        # Creating the backbone routes between gateways
        gws = [n for n, d in gr.nodes_iter(data=True) if 'gw' in n]
        for el in product(gws, gws):
            if el[0] != el[1]:
                p = main_as.find("./ASroute/[@gw_src='" + el[1] + \
                                 "'][@gw_dst='" + el[0] + "']")
                if p is None:
                    asroute = SubElement(main_as, 'ASroute', attrib={
                        'gw_src': el[0] + suffix,
                        'gw_dst': el[1] + suffix,
                        'src': el[0].split('.')[0].split('-')[1] + suffix,
                        'dst': el[1].split('.')[0].split('-')[1] + suffix})
                    path = shortest_path(gr, el[0], el[1])
                    for i in range(len(path) - 1):
                        el1, el2 = sorted([path[i], path[i + 1]])
                        SubElement(asroute, 'link_ctn',
                                   attrib={'id': el1 + '_' + el2})
    # Creating the elements on each site
    for site in sites:
        sgr = gr.subgraph(filter(lambda x: site in x, gr.nodes()))
        if len(sites) > 1:
            site_el = main_as.find("./AS/[@id='" + site + suffix + "']")
        else:
            site_el = SubElement(platform, 'AS', attrib={'id': site + suffix,
                                        'routing': default_routing})
        # Creating the routers
        routers = sorted([node for node in sgr.nodes_iter(data=True)
                          if node[1]['kind'] == 'router'])
        for router, attrib in routers:
            SubElement(site_el, 'router', attrib={'id': router + suffix})
        # Creating the hosts
        hosts = sorted([node for node in sgr.nodes_iter(data=True)
                    if node[1]['kind'] == 'node'],
                    key=lambda node: (node[0].split('.', 1)[0].split('-')[0],
                            int(node[0].split('.', 1)[0].split('-')[1])))
        for n, d in hosts:
            SubElement(site_el, 'host', attrib={'id': n + suffix,
                                                'power': str(d['power']),
                                                'core': str(d['core'])})
        # Creating the links
        for element1, element2, attrib in sgr.edges_iter(data=True):
            if sgr.has_node(element1) and sgr.has_node(element2):
                element1, element2 = sorted([element1 + suffix, element2 + suffix])
                if not site_el.find("./link[@id='" + element1 + '_' + \
                                    element2 + "']"):
                    SubElement(site_el, 'link', attrib={
                        'id': element1 + '_' + element2,
                        'latency': str(attrib['latency']),
                        'bandwidth': str(attrib['bandwidth'])})
        for n, d in hosts:
            route = SubElement(site_el, 'route', attrib={
                            'src': 'gw-' + site + '.' + site + suffix,
                            'dst': n + suffix})
            path = shortest_path(sgr, 'gw-' + site + '.' + site, n)
            for i in range(len(path) - 1):
                el1, el2 = sorted([path[i], path[i + 1]])
                SubElement(route, 'link_ctn', attrib={'id': el1 + suffix + \
                                                      '_' + el2 + suffix})

    if not outfile:
        outfile = 'g5k_platform'
    if not '.xml' in outfile:
        outfile += '.xml'
    logger.info('Saving file to %s', style.emph(outfile))
    f = open(outfile, 'w')
    f.write('<?xml version=\'1.0\'?>\n<!DOCTYPE platform SYSTEM ' + \
            '"http://simgrid.gforge.inria.fr/simgrid.dtd">\n' + \
            '<!-- ' + tool_signature + '\n     ' +\
            'API commit ' + gr.graph['api_commit'] + \
            '\n     ' + format_date(gr.graph['date']) + ' -->\n' + \
             prettify(platform))
    f.close()

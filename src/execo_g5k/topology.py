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

All information comes from the Grid'5000 reference API
"""

from pprint import pformat, pprint
from time import time
from execo import logger, Host
from execo.log import style
from execo_g5k import group_hosts
from oar import format_date
from itertools import product, groupby
from operator import itemgetter 
from api_cache import get_api_data
from api_utils import get_g5k_sites, get_host_site, canonical_host_name
import networkx as nx

try:
    import matplotlib.pyplot as plt
    import matplotlib.patches
except:
    pass
from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement, tostring

arbitrary_latency = 2.25E-3
suffix = '.grid5000.fr'


class g5k_graph(nx.Graph):
    """Main graph representing the topology of the Grid'5000 platform"""

    def __init__(self, api_commit=None, date=format_date(time())):
        super(g5k_graph, self).__init__()
        # reading API data
        self.network, self.hosts = get_api_data()
        # initializing graph
        self.graph['api_commit'] = self.network['backbone'][0]['version']
        self.graph['date'] = format_date(time())
        self.sites = []

    def add_backbone(self):
        logger.info('Adding Renater network to the Graph')
        backbone = self.network['backbone']

        for equip in backbone:
            src = equip['uid'].replace('renater-', 'renater.') + suffix
            if not self.has_node(src):
                logger.detail('Adding ' + src)
                self.add_node(src, kind='renater')
            for lc in equip['linecards']:
                for port in lc['ports']:
                    if 'renater-' in port['uid']:
                        dst = port['uid'].replace('renater-', 'renater.') +\
                            suffix

                    kind = 'renater' if not 'kind' in port else port['kind']
                    rate = lc['rate'] if not 'rate' in port else port['rate']
                    latency = port['latency'] if 'latency' in port \
                    else arbitrary_latency
                    if kind == 'renater':
                        logger.detail('* %s (%s, bw=%s, lat=%s)', dst, kind,
                                      rate, latency)
                        if not self.has_node(dst):
                            self.add_node(dst, kind=kind)
                        if not self.has_edge(src, dst):
                            self.add_edge(src, dst, bandwidth=rate,
                                          latency=latency)

    def add_site(self, site=None):
        """ """
        if site not in self.sites:
            self.sites.append(site)
        equips = self.network[site]
        hosts = self.hosts[site]
        # Adding the hosts
        for cluster_hosts in hosts.itervalues():
            for host in cluster_hosts:
                src = host['uid'] + '.' + site + suffix
                if not self.has_node(src):
                    self.add_node(src, kind='node',
                                 power=host['performance']['core_flops'],
                                 core=host['architecture']['smt_size'])
                for adapt in filter(lambda n: n['enabled'] and not n['management']
                                and n['mounted'] and n['interface'] == 'Ethernet',
                                    host['network_adapters']):
                    if not 'switch' in adapt or adapt['switch'] is None:
                        logger.warning('%s: link between %s and it\'s network ' +\
                                       'equipment is not correct',
                                        site, src)
                        logger.debug(pprint(adapt))
                    else:
                        dst = adapt['switch'] + '.' + site + suffix

        # Adding the router
    ## HACK TO FIX TOULOUSE R4 API DESCRIPTION AS THERE ARE ONLY ONE ROUTER PER SITE
        routers = filter(lambda n: n['kind'] == 'router', equips)
        router = routers[0]
        src = router['uid'] + '.' + site + suffix
        self.add_node(src, kind='router',
                      bandwidth=router['backplane_bps'])
        i_lc = 1
        for lc in filter(lambda n: 'ports' in n, router['linecards']):
            # Adding linecards
            bandwidth = lc['backplane_bps'] if 'backplane_bps' in lc else 0
            lc_name = 'lc' + str(i_lc) + '_' + src
            self.add_node(lc_name, kind='linecard')
            self.add_edge(src, lc_name, bandwidth=bandwidth, latency=0)
            logger.detail('Adding ' + lc_name)
            for port in sorted(filter(lambda p: 'uid' in p, lc['ports'])):
                dst = port['uid'] + '.' + site + suffix
                rate = lc['rate'] if not 'rate' in port else port['rate']
                latency = port['latency'] if 'latency' in port \
                    else arbitrary_latency
                kind = lc['kind'] if not 'kind' in port else port['kind']

                if kind == 'virtual':
                    dst = dst.replace('-' + site, '')
                #elif kind == 'switch':
        ## HACK TO FIX TOULOUSE R4 API DESCRIPTION AS THERE ARE ONLY ONE ROUTER PER SITE
                elif kind == 'switch' or kind == 'router':
                    self.add_node(dst, kind='switch')
                logger.detail('* %s (%s, bw=%s, lat=%s)', dst, kind,
                                      rate, latency)
                if self.has_node(dst):
                    self.add_edge(lc_name, dst, bandwidth=rate, latency=latency)

            i_lc += 1

        # Adding the switchs
        switchs = filter(lambda n: n['kind'] == 'switch', equips)
    ## HACK TO FIX TOULOUSE R4 API DESCRIPTION AS THERE ARE ONLY ONE ROUTER PER SITE
        if len(routers) > 1:
            switchs += routers[1:]
        for switch in switchs:
            src = switch['uid'] + '.' + site + suffix
            logger.detail('Adding ' + src)
            if not self.has_node(src):
                self.add_node(src, kind='switch')
            linecards = filter(lambda n: 'ports' in n, switch['linecards'])
            for lc in linecards:
                for port in filter(lambda p: 'uid' in p, lc['ports']):
                    kind = lc['kind'] if not 'kind' in port else port['kind']
                    dst = port['uid'] + '.' + site + suffix
                    rate = lc['rate'] if not 'rate' in port else port['rate']
                    latency = port['latency'] if 'latency' in port \
                        else arbitrary_latency

                    if kind != 'router' and self.has_node(dst):
                        self.add_edge(src, dst, bandwidth=rate, latency=latency)
                        logger.detail('* %s (%s, bw=%s, lat=%s)', dst, kind,
                                      rate, latency)

        for node in self.nodes():
            if len(self.neighbors(node)) == 0:
                logger.warning(node + ' not connected in the ' + \
                    'network_equipments, we remove it.')
                self.remove_node(node)

    
    def hadoop_topology(self, hosts):
        """A method that find the associate"""
        hadoop_topology = {}
        self.add_backbone()
        for h in hosts:
            try: 
                site = get_host_site(h)
                if site not in self.sites:
                    self.add_site(site)
                    for sw in nx.all_neighbors(self, canonical_host_name(h).address):
                        hadoop_topology[canonical_host_name(h)] = "/" + sw.split('.')[0]
                        break
            except:
                logger.warning('%s is not a valid Grid5000 host', h)
                pass
                    
        return hadoop_topology
    
    
    # Plotting functions
    def create_plot(self, legend=None, labels=None, layout='neato',
                    all_nodes=False):
        """ """
        # Setting defaults legend and labels
        if legend is None:
            legend = self._default_legend()
        if labels is None:
            labels = self._default_labels(all_nodes)
        # Initializing plot
        fig = plt.figure(figsize=(10, 10))
        ax = fig.add_subplot(111)
        logger.detail('Defining positions')
#         try:
        pos = nx.graphviz_layout(self, prog=layout)
#         except:
#             logger.warning('No graphviz installed, using spring layout that' +\
#                            ' does not scale well ...')
#             pos = nx.spring_layout(self, iterations=100)
        # Adding the nodes
        for kind in ['renater', 'router', 'switch', 'node']:
            nodes = [node[0] for node in self.nodes_iter(data=True)
                     if node[1]['kind'] == kind]
            nx.draw_networkx_nodes(self, pos, nodelist=nodes,
                node_shape=legend['nodes'][kind]['shape'],
                node_color=legend['nodes'][kind]['color'],
                node_size=legend['nodes'][kind]['size'])
        # Adding the edges
        for bandwidth, params in legend['edges'].iteritems():
            if bandwidth != 'other':
                edges = [(edge[0], edge[1]) for edge in self.edges_iter(data=True)
                     if edge[2]['bandwidth'] == bandwidth]
                nx.draw_networkx_edges(self, pos, edgelist=edges,
                        width=params['width'], edge_color=params['color'])
        edges = [(edge[0], edge[1]) for edge in self.edges_iter(data=True)
             if edge[2]['bandwidth'] not in legend['edges'].keys()]
        nx.draw_networkx_edges(self, pos, edgelist=edges,
                width=legend['edges']['other']['width'],
                edge_color=legend['edges']['other']['color'])
        # Adding the labels
        cluster_added = []
        for node, data in self.nodes_iter(data=True):
            if data['kind'] == 'renater':
                labels['renater']['nodes'][node] = node.split('.')[1].title()
            elif data['kind'] == 'switch':
                labels['switch']['nodes'][node] = node.split('.')[0]
            elif data['kind'] == 'node':
                if all_nodes:
                    labels['cluster']['nodes'][node] = node.split('.')[0]
                elif not node.split('-')[0] in cluster_added:
                    labels['cluster']['nodes'][node] = node.split('-')[0]
                    cluster_added.append(node.split('-')[0])
        for data in labels.itervalues():
            nx.draw_networkx_labels(self, pos, labels=data['nodes'],
                font_size=data['font_size'], font_weight=data['font_weight'])

        plt.axis('off')
        plt.tight_layout()

#         plt.text(0, 0, 'Created by execo_g5k.topology \n %s' +\
#             '\nAPI commit %s', self.graph['date'], self.graph['api_commit'],
#             transform=ax.transAxes)

#         legend = matplotlib.patches.Rectangle((0.90, 0.91), 0.15, 0.24,
#                                 color='#444444', transform=ax.transAxes)
#         ax.add_patch(legend)
# 
#         i = 0
#         for kind, param in legend['nodes'].iteritems():
#             plt.text(0.91, 0.977 - i * 0.001, kind,
#                 fontdict={'color': param['color'], 'size': 14,
#                           'variant': 'small-caps', 'weight': 'bold'},
#                 transform=ax.transAxes)
#             i += 20

        return fig

    def show(self, legend=None, all_nodes=False):
        """ """
        self.create_plot(legend, all_nodes=all_nodes)
        plt.show()

    def save(self, outfile=None, legend=None, dpi=300, all_nodes=False):
        """Save the graph to an image"""
        if not outfile:
            if len(self.sites) >= 1:
                self.sites.sort()
                print self.graph
                outfile = '_'.join(self.sites) + '_' + \
                 self.graph['date'].replace(' ', '_') \
                + '.png'
            else:
                outfile = 'g5k_backbone.png'

        self.create_plot(legend, all_nodes=all_nodes)
        logger.info('Saving file to %s', style.emph(outfile))
        plt.savefig(outfile, bbox_inches='tight', dpi=dpi)

    def _default_legend(self):
        legend = {'nodes': {
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
        return legend

    def _default_labels(self, all_nodes=False):
        if all_nodes or len(self.nodes()) > 100:
            base_size = 1
        else:
            base_size = 2
        labels = {'renater':
              {'nodes': {}, 'font_size': base_size * 8, 'font_weight': 'bold'},
              'switch':
              {'nodes': {}, 'font_size': base_size * 6, 'font_weight': 'normal'},
              'cluster':
              {'nodes': {}, 'font_size': base_size * 7, 'font_weight': 'normal'}
            }
        return labels

    # SimGrid exports
    def simgrid(self, outfile=None, compact=False, routing=None,
        tool_signature='Generated using execo_g5k.topology'):
        """Produce a SimGrid platform XML file
        :params gr: a graph object representing the topology

        :param compact: a boolean to use compact formulation (clusters
        instead of hosts)

        :params outfile: name of the output file

        :params tool: signature added in comment of the XML file
        """

        if routing is None:
            routing = 'Floyd'

        # Creating the AS
        platform = Element('platform', attrib={'version': '3'})

        if len(self.sites) > 1:
            logger.debug(pformat(self.sites))
            main_as = self._sg_add_AS(platform, 'grid5000.fr', routing)

            # Adding an AS per site
            for site in self.sites:
                AS_site = self._sg_add_AS(main_as, site + suffix, routing)
                if not compact:
                    self._sg_full(AS_site)
                else:
                    self._sg_compact(AS_site)

            # Create the backbone links
            for element1, element2, attrib in get_backbone_edges(self):
                element1, element2 = sorted([element1, element2])
                link = self._sg_add_link(main_as, element1, element2,
                                attrib['bandwidth'], attrib['latency'])
            # Creating the backbone routes between gateways
            gws = [n for n in self.nodes() if 'gw' in n]
            for el in  (gws, gws):
                if el[0] != el[1]:
                    p = main_as.find("./ASroute/[@gw_src='" + el[1] + \
                                      "'][@gw_dst='" + el[0] + "']")
                    if p is None:
                        src = '.'.join(el[0].split('.')[1:])
                        dst = '.'.join(el[1].split('.')[1:])
                        self._sg_add_ASroute(main_as, src, dst,
                                el[0], el[1], self)

        else:
            main_as = self._sg_add_AS(platform, self.sites[0] + suffix, routing)
            if not compact:
                self._sg_full(main_as)
            else:
                self._sg_compact(main_as)

        if not outfile:
            outfile = 'g5k_platform'
        if not '.xml' in outfile:
            outfile += '.xml'
        logger.info('Saving file to %s', style.emph(outfile))
        f = open(outfile, 'w')
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE platform SYSTEM ' + \
                '"http://simgrid.gforge.inria.fr/simgrid.dtd">\n' + \
                '<!-- ' + tool_signature + '\n     ' +\
                'API commit ' + self.graph['api_commit'] + \
                '\n     ' + format_date(self.graph['date']) + ' -->\n' + \
                 prettify(platform))
        f.close()

    def _sg_full(self, AS_site):
        """Create all the elements of a site AS in expanded description """
        site = AS_site.get('id').split('.')[0]
        sgr = get_site_graph(self, site)
        # Adding the router
        for router in get_routers(sgr):
            self._sg_add_router(AS_site, router[0])
            if 'gw-' in router[0]:
                gateway = router[0]
        # Adding the hosts
        hosts = get_compute_hosts(get_site_graph(self, site))
        for host in hosts:
            self._sg_add_host(AS_site, host[0], host[1]['core'],
                              host[1]['power'])
        # Adding the links
        for edge in filter(lambda x: 'renater' not in x[0]
                           and 'renater' not in x[1],
                      sgr.edges_iter(data=True)):
            self._sg_add_link(AS_site, edge[0], edge[1],
                        edge[2]['bandwidth'], edge[2]['latency'])
        # Adding the routes between compute hosts and gateway
        for n, d in hosts:
            self._sg_add_route(AS_site, n, gateway, sgr)

    def _sg_compact(self, AS_site):
        """Create all the element of a site AS in compact description """
        site = AS_site.get('id').split('.')[0]
        sgr = get_site_graph(self, site)
        # Adding the router as an AS
        for router in get_routers(sgr):
            if 'gw' in router[0]:
                gateway = router[0]
                AS_router = self._sg_add_AS(AS_site, 'gw_' + site)
                self._sg_add_router(AS_router, gateway)
        # Adding the cluster (splitted if needed)
        clusters = get_clusters(sgr)
        for cluster, data in clusters.iteritems():
            if len(data['equips']) == 1:
                # We check that the switch or linecard does not contains other nodes
                if len(list(filter(lambda x: cluster not in x,
                        nx.all_neighbors(sgr, data['equips'].keys()[0])))) > 0:
                    logger.warning('Other elements on the same network ' +
                            'equipements for cluster ' + cluster
                            + ', switching to full mode')
                    hosts = filter(lambda x: cluster in x[0],
                                   get_compute_hosts(get_site_graph(self, site)))
                    for host in hosts:
                        self._sg_add_host(AS_site, host[0], host[1]['core'],
                              host[1]['power'])

                else:
                    self._sg_add_cluster(AS_site, cluster, data['prefix'],
                        data['suffix'],
                        data['equips'][data['equips'].keys()[0]],
                        data['core'], data['power'],
                        data['bandwidth'], data['latency'])

            else:
                AS_cabinet = self._sg_add_AS(AS_site, 'AS_' + cluster)
                for cabinet, values in data['equips'].iteritems():
                    self._sg_add_cluster(AS_cabinet, 'AS_' + \
                        cabinet.split('.')[0], data['prefix'], data['suffix'],
                        values,
                        data['core'], data['power'],
                        data['bandwidth'], data['latency'],
                        data['bb_bw'], data['bb_lat'])

    def _sg_add_AS(self, elem, AS_id, routing="Floyd"):
        """Add an AS Element to an element"""
        return SubElement(elem, 'AS', attrib={'id': AS_id, 'routing': routing})

    def _sg_add_router(self, elem, router_id):
        """Create a router Element"""
        return SubElement(elem, 'router', attrib={'id': router_id})

    def _sg_add_host(self, elem, host_id, core, power):
        """Create a host Element """
        return SubElement(elem, 'host', attrib={
                'id': host_id,
                'core': str(core),
                'power': str(power)})

    def _sg_add_cluster(self, elem, cluster_id, prefix, suffix, radical,
                core, power, bw, lat, bb_bw=None, bb_lat=None):
        """Create a cluster Element """
        attrib = {
                'id': cluster_id,
                'prefix': prefix,
                'suffix': suffix,
                'radical': radical,
                'power': "%.2E" % power,
                'core': str(core),
                'bw':  "%.2E" % bw,
                'lat': "%.2E" % lat}
        if bb_bw:
            attrib['bb_bw'] = "%.2E" % bb_bw
        if bb_lat:
            attrib['bb_lat'] = "%.2E" % bb_lat
        return SubElement(elem, 'cluster', attrib=attrib)

    def _sg_add_link(self, elem, src, dst, bandwidth, latency):
        """Create a link Element"""
        src, dst = sorted([src, dst])
        return SubElement(elem, 'link', attrib={
                'id': src + '_' + dst,
                'latency': "%.2E" % latency,
                'bandwidth': "%.2E" % bandwidth})

    def _sg_add_route(self, elem, src, dst, gr):
        """Create a route Element between nodes"""
        route = SubElement(elem, 'route', attrib={
                'src': src,
                'dst': dst})
        path = nx.shortest_path(gr, src, dst)
        for i in range(len(path) - 1):
            el1, el2 = sorted([path[i], path[i + 1]])
            SubElement(route, 'link_ctn',
                       attrib={'id': el1 + '_' + el2})

    def _sg_add_ASroute(self, elem, src, dst, gw_src, gw_dst, gr):
        """Create a route elem.SubElement between nodes"""
        AS_route = SubElement(elem, 'ASroute', attrib={
                        'gw_src': gw_src,
                        'gw_dst': gw_dst,
                        'src': src,
                        'dst': dst})
        path = nx.shortest_path(gr, gw_src, gw_dst)
        for i in range(len(path) - 1):
            el1, el2 = sorted([path[i], path[i + 1]])
            SubElement(AS_route, 'link_ctn',
                   attrib={'id': el1 + '_' + el2})


def get_backbone_edges(gr):
    """Return the edges corresponding to the backbone,
    i.e. contains renater and renater/gw """
    return sorted(filter(lambda x: any(s in x[0] for s in ['gw', 'renater'])
                and any(s in x[1] for s in ['gw', 'renater']),
                gr.edges_iter(data=True)))


def get_site_graph(gr, site):
    """ """
    return gr.subgraph(filter(lambda x: site in x, gr.nodes()))


def get_routers(gr):
    """Retrieve the routers of a graph """
    return sorted(filter(lambda x: x[1]['kind'] == 'router',
                         gr.nodes_iter(data=True)))


def get_switchs(gr):
    """Retrieve the routers of a graph """
    return sorted(filter(lambda x: x[1]['kind'] == 'switch',
                         gr.nodes_iter(data=True)))


def get_compute_hosts(gr):
    """Retrieve the compute nodes """
    return sorted(filter(lambda x: x[1]['kind'] == 'node',
                gr.nodes_iter(data=True)),
            key=lambda node: (node[0].split('.', 1)[0].split('-')[0],
                int(node[0].split('.', 1)[0].split('-')[1])))


def get_clusters(gr):
    """Retrieve the clusters of a graph """
    clusters = {}
    for host in get_compute_hosts(gr):
        hostname, suffix = host[0].split('.', 1)
        cluster = hostname.split('-')[0]
        if not cluster in clusters:
            clusters[cluster] = {'equips': {},
                                 'suffix': '.' + suffix,
                                 'prefix': cluster + '-',
                                 'core': host[1]['core'],
                                 'power': host[1]['power']}
        for equip in nx.all_neighbors(gr, host[0]):
            clusters[cluster]['latency'] = gr.edge[host[0]][equip]['latency']
            clusters[cluster]['bandwidth'] = gr.edge[host[0]][equip]['bandwidth']
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
                router = list(set(filter(lambda x: 'gw-' in x,
                                         nx.all_neighbors(gr, equip))))[0]
                clusters[cluster]['bb_lat'] = gr.edge[router][equip]['latency']
                clusters[cluster]['bb_bw'] = gr.edge[router][equip]['bandwidth']
                radical_list = map(lambda x: int(x.split('.')[0].split('-')[1]),
                                hosts)
                radical = ''
                for k, g in groupby(enumerate(radical_list), lambda (i, x): i - x):
                    radical_range = map(itemgetter(1), g)
                    if len(radical_range) > 1:
                        radical += str(min(radical_range)) + '-' + \
                        str(max(radical_range))
                    else:
                        radical += radical_range[0]
                    radical += ','
                radical = radical[:-1]
                data['equips'][equip] = radical
    return clusters


def filter_nodes(gr, nodes):
    """Remove compute graph nodes that are not in compute nodes list"""
    for node, data in gr.nodes_iter(data=True):
        if data['kind'] == 'node' and Host(node + '.grid5000.fr') not in nodes:
            gr.remove_node(node)
    for node, data in gr.nodes_iter(data=True):
        if data['kind'] == 'switch':
            switch_has_node = False
            for dest in nx.all_neighbors(gr, node):
                if nx.get_node_attributes(gr, 'kind')[dest] == 'node':
                    switch_has_node = True
                    break
            if not switch_has_node:
                gr.remove_node(node)


def remove_non_g5k(gr):
    """Remove nodes that are not real Grid'5000 elements or
    are infiniband element"""
    logger.detail('Removing infiniband and not Grid\'5000 elements')
    to_remove = ['ib.', 'stalc', 'voltaire', 'ib-', 'summit', 'ipmi', 'CICT',
         'mxl2', 'grelon', 'myrinet', 'salome', 'interco']
    for node in gr.nodes():
        if any(s in node for s in to_remove):
            logger.detail('* %s', node)
            remove_edges = []
            for dest in nx.all_neighbors(gr, node):
                remove_edges.append((node, dest))
                remove_edges.append((dest, node))
            gr.remove_edges_from(remove_edges)
            gr.remove_node(node)


    


def prettify(elem):
    """Return a pretty-printed XML string for the Element"""
    rough_string = tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ").replace(
            '<?xml version="1.0" ?>\n', '')







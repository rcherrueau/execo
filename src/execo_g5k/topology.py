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
from api_cache import get_api_data
from api_utils import get_g5k_sites, get_host_site, canonical_host_name, \
    get_host_cluster, get_cluster_site, get_g5k_clusters, get_cluster_hosts, \
    get_site_clusters

try:
    import networkx as nx
except:
    logger.error('Networkx not found, topology module cannot be used')
    pass
try:
    import matplotlib.pyplot as plt
    import matplotlib.patches
except:
    logger.error('Matplotlib not found, no plot can be generated')
    pass
from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement, tostring

arbitrary_latency = 2.25E-3
suffix = '.grid5000.fr'


class g5k_graph(nx.Graph):
    """Main graph representing the topology of the Grid'5000 platform. All nodes elements
    are defined with their FQDN"""

    def __init__(self, sites=None):
        """Retrieve API data and initialize the Graph with api_commit
        and date of generations"""
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
            self.add_site_router(site)

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

    def add_cluster(self, cluster):
        """Add the cluster to the graph"""
        logger.info('Adding cluster %s', style.host(cluster))
        if cluster not in get_g5k_clusters():
            logger.error('%s is not a valid Grid\'5000 cluster', 
                         style.emph(cluster))
            return False
        site = get_cluster_site(cluster)
        if site not in self.sites:
            self.add_site_router(site)

        for host in get_cluster_hosts(cluster):
            self.add_host(host)

    def add_site(self, site):
        """Add the site to the graph"""
        logger.info('Adding site %s', style.host(site))
        if site not in get_g5k_sites():
            logger.error('%s is not a valid Grid\'5000 site', style.emph(site))
            return False
        if site not in self.sites:
            self.add_site_router(site)

        for cluster in get_site_clusters(site):
            self.add_cluster(cluster)

        return True

    def add_site_router(self, site):
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
                    router = list(set(filter(lambda x: 'gw-' in x,
                                             nx.all_neighbors(self, equip))))[0]
                    clusters[cluster]['bb_lat'] = self.edge[router][equip]['latency']
                    clusters[cluster]['bb_bw'] = self.edge[router][equip]['bandwidth']
                    radical_list = map(lambda x: int(x.split('.')[0].split('-')[1]),
                                       hosts)
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
        return sgr.nodes(data=True), sgr.edges(data=True)


def treemap(gr, legend=None, labels=None, layout='neato',
            all_nodes=False, compact=False):
    """Create a treemap of the topology and return a matplotlib figure"""

    def _default_legend():
        """Create a default legend for the several treemap elements"""
        legend = {'nodes': {'renater':
                            {'color': '#9CF7BC', 'shape': 'p', 'size': 200},
                            'router':
                            {'color': '#BFDFF2', 'shape': '8', 'size': 300},
                            'switch':
                            {'color': '#F5C9CD', 'shape': 's', 'size': 100},
                            'node':
                            {'color': '#F0F7BE', 'shape': 'o', 'size': 30},
                            'cluster':
                            {'color': '#F0F7BE', 'shape': 'd', 'size': 200},
                            },
                  'edges': {1000000000:
                            {'width': 0.2, 'color': '#666666'},
                            3000000000:
                            {'width': 0.6, 'color': '#333333'},
                            10000000000:
                            {'width': 1.0, 'color': '#111111'},
                            20000000000:
                            {'width': 2.0, 'color': '#111111'},
                            30000000000:
                            {'width': 3.0, 'color': '#111111'},
                            40000000000:
                            {'width': 4.0, 'color': '#111111'},
                            'other':
                            {'width': 1.0, 'color': '#FF4E5A'}
                            }
                  }
        return legend

    def _default_labels(all_nodes=False):
        """Define the font for the several treemap elements"""
        if all_nodes:
            base_size = 1
        else:
            base_size = 2
        labels = {'renater':
                  {'nodes': {},
                   'font_size': base_size * 8,
                   'font_weight': 'bold'},
                  'switch':
                  {'nodes': {},
                   'font_size': base_size * 6,
                   'font_weight': 'normal'},
                  'cluster':
                  {'nodes': {},
                   'font_size': base_size * 7,
                   'font_weight': 'normal'}
                  }
        return labels

    # Setting defaults legend and labels
    if legend is None:
        legend = _default_legend()
    if labels is None:
        labels = _default_labels(all_nodes)
    if not compact:
        elements = ['renater', 'router', 'switch', 'node']
    else:
        for site in gr.sites:
            for cluster, data in gr.site_clusters(site).iteritems():
                for equip, radicals in data['equips'].items():
                    gr.add_node(cluster + ' (' + radicals + ')',
                                {'kind': 'cluster'})
                    gr.add_edge(cluster + ' (' + radicals + ')', equip,
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
                 if node[1]['kind'] == k]
        nodes = nx.draw_networkx_nodes(gr, pos, nodelist=nodes,
                                       node_shape=legend['nodes'][k]['shape'],
                                       node_color=legend['nodes'][k]['color'],
                                       node_size=legend['nodes'][k]['size'])

    # Adding the edges
    for bandwidth, params in legend['edges'].iteritems():
        if bandwidth != 'other':
            edges = [(edge[0], edge[1]) for edge in gr.edges_iter(data=True)
                     if edge[2]['bandwidth'] == bandwidth]
            nx.draw_networkx_edges(gr, pos, edgelist=edges,
                                   width=params['width'],
                                   edge_color=params['color'])
    edges = [(edge[0], edge[1]) for edge in gr.edges_iter(data=True)
             if edge[2]['bandwidth'] not in legend['edges'].keys()]
    nx.draw_networkx_edges(gr, pos, edgelist=edges,
                           width=legend['edges']['other']['width'],
                           edge_color=legend['edges']['other']['color'])
    # Adding the labels
    cluster_added = []
    for node, data in gr.nodes_iter(data=True):
        if data['kind'] == 'renater':
            labels['renater']['nodes'][node] = node.split('.')[1].title()
        elif data['kind'] == 'switch':
            labels['switch']['nodes'][node] = node.split('.')[0]
        elif data['kind'] == 'cluster':
            labels['cluster']['nodes'][node] = node.split('.')[0]
        elif data['kind'] == 'node':
            if all_nodes:
                labels['cluster']['nodes'][node] = node.split('.')[0]
            elif not node.split('-')[0] in cluster_added:
                labels['cluster']['nodes'][node] = node.split('-')[0]
                cluster_added.append(node.split('-')[0])
    for data in labels.itervalues():
        nx.draw_networkx_labels(gr, pos, labels=data['nodes'],
                                font_size=data['font_size'],
                                font_weight=data['font_weight'])

    plt.axis('off')
    plt.tight_layout()

    title = 'Created by execo_g5k.topology \n%s\nAPI commit %s' % \
        (gr.graph['date'], gr.graph['api_commit'])
    plt.text(0.1, 0, title, transform=ax.transAxes)

    return fig


def hadoop(gr, hosts):
    """A method that find the associate"""
    hadoop_topology = {}
    for h in hosts:
        print h
        for sw in nx.all_neighbors(gr, canonical_host_name(h).address):
            print canonical_host_name
            hadoop_topology[canonical_host_name(h)] = "/" + \
                sw.split('.')[0]

    return hadoop_topology


def simgrid(gr, outfile=None, compact=False, routing=None,
            tool_signature='Generated using execo_g5k.topology'):
    """Produce a SimGrid platform XML file
    :params gr: a graph object representing the topology

    :param compact: a boolean to use compact formulation (clusters
    instead of hosts)

    :params outfile: name of the output file

    :params tool: signature added in comment of the XML file
    """

    def AS_full(gr, AS_site):
        """Create all the elements of a site AS in expanded description """
        site = AS_site.get('id').split('.')[0]
        nodes, edges = gr.get_site_graph(site)
        # Adding the router
        for router in filter(lambda n: n[1]['kind'] == 'router', nodes):
            add_router(AS_site, router[0])
            if 'gw-' in router[0]:
                gateway = router[0]
        # Adding the hosts
        hosts = filter(lambda n: n[1]['kind'] == 'node', nodes)
        for host in hosts:
            add_host(AS_site, host[0], host[1]['cores'],
                     host[1]['power'])
        # Adding the links
        for edge in filter(lambda x: 'renater' not in x[0]
                           and 'renater' not in x[1],
                           edges):
            add_link(AS_site, edge[0], edge[1],
                     edge[2]['bandwidth'], edge[2]['latency'])
        # Adding the routes between compute hosts and gateway
        for n, d in hosts:
            add_route(gr, AS_site, n, gateway)

    def AS_compact(gr, AS_site):
        """Create all the element of a site AS in compact description """
        site = AS_site.get('id').split('.')[0]
        nodes, edges = gr.get_site_graph(site)

        # Adding the router as an AS
        for router in filter(lambda n: n[1]['kind'] == 'router', nodes):
            if 'gw' in router[0]:
                gateway = router[0]
                AS_router = add_AS(AS_site, 'gw_' + site)
                add_router(AS_router, gateway)
        # Adding the cluster (splitted if needed)
        clusters = gr.site_clusters(site)
        for cluster, data in clusters.iteritems():
            if len(data['equips']) == 1:
                # We check that the switch or linecard does not contains other nodes
                add_cluster(AS_site, cluster, data['prefix'],
                            data['suffix'],
                            data['equips'][data['equips'].keys()[0]],
                            data['core'], data['power'],
                            data['bandwidth'], data['latency'])

            else:
                AS_cabinet = add_AS(AS_site, 'AS_' + cluster)
                for cabinet, values in data['equips'].iteritems():
                    add_cluster(AS_cabinet, 'AS_' +
                                cabinet.split('.')[0], data['prefix'], data['suffix'],
                                values,
                                data['core'], data['power'],
                                data['bandwidth'], data['latency'],
                                data['bb_bw'], data['bb_lat'])

    def add_AS(elem, AS_id, routing="Floyd"):
        """Add an AS Element to an element"""
        return SubElement(elem, 'AS', attrib={'id': AS_id, 'routing': routing})

    def add_router(elem, router_id):
        """Create a router Element"""
        return SubElement(elem, 'router', attrib={'id': router_id})

    def add_host(elem, host_id, core, power):
        """Create a host Element """
        return SubElement(elem, 'host', attrib={'id': host_id,
                                                'core': str(core),
                                                'power': str(power)})

    def add_cluster(elem, cluster_id, prefix, suffix, radical,
                    core, power, bw, lat, bb_bw=None, bb_lat=None):
        """Create a cluster Element """
        attrib = {'id': cluster_id,
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

    def add_link(elem, src, dst, bandwidth, latency):
        """Create a link Element"""
        src, dst = sorted([src, dst])
        return SubElement(elem, 'link',
                          attrib={'id': src + '_' + dst,
                                  'latency': "%.2E" % latency,
                                  'bandwidth': "%.2E" % bandwidth})

    def add_route(gr, elem, src, dst):
        """Create a route Element between nodes"""
        route = SubElement(elem, 'route',
                           attrib={'src': src, 'dst': dst})
        path = nx.shortest_path(gr, src, dst)
        for i in range(len(path) - 1):
            el1, el2 = sorted([path[i], path[i + 1]])
            SubElement(route, 'link_ctn',
                       attrib={'id': el1 + '_' + el2})

    def add_ASroute(gr, elem, src, dst, gw_src, gw_dst):
        """Create a route elem.SubElement between nodes"""
        AS_route = SubElement(elem, 'ASroute',
                              attrib={'gw_src': gw_src,
                                      'gw_dst': gw_dst,
                                      'src': src,
                                      'dst': dst})
        path = nx.shortest_path(gr, gw_src, gw_dst)
        for i in range(len(path) - 1):
            el1, el2 = sorted([path[i], path[i + 1]])
            SubElement(AS_route, 'link_ctn',
                       attrib={'id': el1 + '_' + el2})

    logger.info('Generating SimGrid topology')
    if routing is None:
        routing = 'Full'

    # Creating the AS
    platform = Element('platform', attrib={'version': '3'})

    if len(gr.sites) == 1:
        main_as = add_AS(platform, 'AS_' + gr.sites[0], routing)
        if compact:
            print gr.site_clusters(gr.sites[0])
        else:
            print 'full'
    else:
        main_as = add_AS(platform, 'AS_grid5000', routing)


        # Adding an AS per site
#         for site in gr.sites:
#             AS_site = add_AS(main_as, 'AS_' + site, routing)
#             if not compact:
#                 AS_full(gr, AS_site)
#             else:
#                 AS_compact(gr, AS_site)
# 
#         # Create the backbone links
#         backbone = gr.get_backbone_graph()
#         for element1, element2, attrib in backbone[1]:
#             element1, element2 = sorted([element1, element2])
#             add_link(main_as, element1, element2,
#                      attrib['bandwidth'], attrib['latency'])
#         # Creating the backbone routes between gateways
#         gws = [n for n in gr.nodes() if 'gw' in n]
#         for el in (gws, gws):
#             if el[0] != el[1]:
#                 p = main_as.find("./ASroute/[@gw_src='" + el[0] +
#                                  "'][@gw_dst='" + el[1] + "']")
#                 if p is None:
#                     src = '.'.join(el[0].split('.')[1:])
#                     dst = '.'.join(el[1].split('.')[1:])
#                     add_ASroute(gr, main_as, src, dst,
#                                 el[0], el[1])
# 
#     else:
#         main_as = add_AS(platform, gr.sites[0] + suffix, routing)
#         if not compact:
#             AS_full(gr, main_as)
#         else:
#             AS_compact(gr, main_as)

    if not outfile:
        outfile = 'g5k_platform'
    if '.xml' not in outfile:
        outfile += '.xml'
    logger.info('Saving file to %s', style.emph(outfile))
    f = open(outfile, 'w')
    f.write('<?xml version="1.0" encoding="UTF-8"?>\n' +
            '<!DOCTYPE platform SYSTEM ' +
            '"http://simgrid.gforge.inria.fr/simgrid.dtd">\n' +
            '<!-- ' + tool_signature + '\n     ' +
            'API commit ' + gr.graph['api_commit'] +
            '\n     ' + format_date(gr.graph['date']) + ' -->\n' +
            prettify(platform))
    f.close()

# 
# 
# 
# 
# 
# 
# def get_routers(gr):
#     """Retrieve the routers of a graph """
# 
# 
# def get_switchs(gr):
#     """Retrieve the routers of a graph """
#     return sorted(filter(lambda x: x[1]['kind'] == 'switch',
#                          gr.nodes_iter(data=True)))
# 
# 
# def get_compute_hosts(gr):
#     """Retrieve the compute nodes """
#     return sorted(filter(lambda x: x[1]['kind'] == 'node',
#                 gr.nodes_iter(data=True)),
#             key=lambda node: (node[0].split('.', 1)[0].split('-')[0],
#                 int(node[0].split('.', 1)[0].split('-')[1])))
# 
# 
# def get_clusters(gr):
#     """Retrieve the clusters of a graph """
#     
# 
# 
# def filter_nodes(gr, nodes):
#     """Remove compute graph nodes that are not in compute nodes list"""
#     for node, data in gr.nodes_iter(data=True):
#         if data['kind'] == 'node' and Host(node + '.grid5000.fr') not in nodes:
#             gr.remove_node(node)
#     for node, data in gr.nodes_iter(data=True):
#         if data['kind'] == 'switch':
#             switch_has_node = False
#             for dest in nx.all_neighbors(gr, node):
#                 if nx.get_node_attributes(gr, 'kind')[dest] == 'node':
#                     switch_has_node = True
#                     break
#             if not switch_has_node:
#                 gr.remove_node(node)
# 
# 
# def remove_non_g5k(gr):
#     """Remove nodes that are not real Grid'5000 elements or
#     are infiniband element"""
#     logger.detail('Removing infiniband and not Grid\'5000 elements')
#     to_remove = ['ib.', 'stalc', 'voltaire', 'ib-', 'summit', 'ipmi', 'CICT',
#          'mxl2', 'grelon', 'myrinet', 'salome', 'interco']
#     for node in gr.nodes():
#         if any(s in node for s in to_remove):
#             logger.detail('* %s', node)
#             remove_edges = []
#             for dest in nx.all_neighbors(gr, node):
#                 remove_edges.append((node, dest))
#                 remove_edges.append((dest, node))
#             gr.remove_edges_from(remove_edges)
#             gr.remove_node(node)
#
#

def prettify(elem):
    """Return a pretty-printed XML string for the Element"""
    rough_string = tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ").replace('<?xml version="1.0" ?>\n',
                                                     '')







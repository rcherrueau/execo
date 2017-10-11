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

""" A module based on `networkx <http://networkx.github.io/>`_ to create a
topological graph of the Grid'5000 platform. "Nodes" are used to represent
elements (compute nodes, switch, router, renater) and "Edges" are the network
links. Nodes has a kind data (+ power and core for compute nodes)
whereas edges has bandwidth and latency information.

All information comes from the Grid'5000 reference API.
"""

try:
    from networkx.drawing.nx_pydot import graphviz_layout
except:
    from networkx import graphviz_layout

from time import time
from execo import logger, Host
from execo.log import style
from execo.utils import do_once
from .oar import format_date
from .api_utils import get_g5k_sites, get_host_site, \
    get_host_cluster, get_g5k_clusters, get_cluster_hosts, \
    get_site_clusters, get_api_data, get_g5k_hosts, \
    get_network_equipment_attributes, get_host_shortname, \
    get_site_hosts
import networkx as nx
import re
from execo.utils import is_string

try:
    import matplotlib.pyplot as plt
except:
    pass


arbitrary_latency = 2.25E-3
suffix = '.grid5000.fr'

def _get_linecard_name(equip, lc_index):
    return str(equip) + '-lc' + str(lc_index)

_parse_port_port_regex = re.compile('[^0-9]*([0-9]+)(:|/)([0-9]+)')
def _parse_port_port(p):
    mo = _parse_port_port_regex.match(p)
    if mo:
        target_lc = mo.group(1)
        target_port = mo.group(3)
        return target_lc, target_port
    else:
        return None, None

def _parse_port_uid(uid):
    prefix, _, uid = uid.rpartition(' ')
    if prefix:
        logger.warn('uid %s prefixed with %s' % (uid, prefix))
    return uid

def _unique_link_key(*args):
    return '_'.join(sorted(args))

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
            if is_string(elements):
                elements = [elements]
            for e in elements:
                if e in get_g5k_sites():
                    self.add_site(e, self.data['sites'][e])
                elif e in get_g5k_clusters():
                    self.add_cluster(e, self.data['clusters'][e])
                else:
                    e = get_host_shortname(e)
                    if e in get_g5k_hosts():
                        self.add_host(e, self.data['hosts'][e])
            print('self.get_sites() before adding backbone: %s %s' % (len(self.get_sites()), self.get_sites()))
            if len(self.get_sites()) > 1:
                self.add_backbone()
            print('self.get_sites() after adding backbone: %s %s' % (len(self.get_sites()), self.get_sites()))

    # add/update/rm elements, public methods
    def add_host(self, host, data=None):
        """Add a host in the graph

        :param host: a string corresponding to the node name

        :param data: a dict containing the Grid'5000 host attributes"""
        if isinstance(host, Host):
            _host = get_host_shortname(host.address)
        else:
            _host = host
        if data:
            power = data['performance']['core_flops']
            cores = data['architecture']['nb_cores']
        else:
            power = 0
            cores = 0

        if len(self.get_host_adapters(_host)) == 0:
            logger.warning('Node %s has no valid network connection',
                           _host)
        logger.debug('Adding %s', style.host(_host))
        self.add_node(_host, {'kind': 'node',
                              'power': power,
                              'cores': cores})
        for eq in self.get_host_adapters(_host):
            if eq['mounted']:
                self.add_equip(eq['switch'], get_host_site(_host))
                self._filter_equip_leaves()

    def rm_host(self, host):
        """Remove the host from the graph"""
        logger.debug('Removing host %s', style.host(host))
        self.remove_node(host)
        for eq in self.get_host_adapters(host):
            if not self._equip_has_nodes(eq['switch']):
                logger.debug('Removing equip %s', eq['switch'])
                self.rm_equip(eq['switch'])

    def add_cluster(self, cluster, data=None):
        """Add the cluster to the graph"""
        for h in get_cluster_hosts(cluster):
            self.add_host(h, self.data['hosts'][h])

    def rm_cluster(self, cluster):
        """Remove the cluster from the graph"""
        for h in get_cluster_hosts(cluster):
            self.rm_host(h)

    def add_site(self, site, data=None):
        """Add a site to the graph"""
        for h in get_site_hosts(site):
            self.add_host(h, self.data['hosts'][h])

    def rm_site(self, site):
        """Remove the site from the graph"""
        for h in get_site_hosts(site):
            self.rm_host(h)

    def _filter_equip_leaves(self):
        finished = False
        while not finished:
            finished = True
            for n in self.nodes():
                if self.node[n]['kind'] in ['switch', 'router', 'virtual']:
                    if len(self.neighbors(n)) < 2:
                        logger.debug('removing %s' % (n,))
                        self.remove_node(n)
                        finished = False
                        break
                    to_remove = True
                    for nb in self.neighbors(n):
                        if self.node[nb]['kind'] != 'linecard' or not nb.startswith(n + '-lc'):
                            to_remove = False
                        else:
                            if len(self.neighbors(nb)) >= 2:
                                to_remove = False
                    if to_remove:
                        removed = [ x for x in self.neighbors(n) if self.node[x]['kind'] == 'linecard' and x.startswith(n+'-lc') ]
                        removed.append(n)
                        for r in removed:
                            logger.debug('removing %s' % (r,))
                            self.remove_node(r)
                        finished = False
                        break

    def _checked_add_edge(self, node1, node2, key, **kwargs):
        if not self.has_edge(node1, node2, key):
            logger.debug('Adding link %s' % (key,))
            self.add_edge(node1, node2, key, **kwargs)

    def _checked_add_linecard(self, lc_node, backplane):
        if not self.has_node(lc_node):
            logger.debug('Adding linecard %s', lc_node)
            self.add_node(lc_node, kind='linecard', backplane=backplane)

    def _equip_uses_multiple_linecards(self, equip, site):
        num_linecards_with_ports = 0
        for lc in self.data['network'][site][equip]['linecards']:
            if 'ports' in lc:
                num_linecards_with_ports += 1
        if num_linecards_with_ports > 1:
            return True
        else:
            return False

    def _get_target_linecard_and_port_from_api(self, equip, linecard_index, port_index, site):
        # given the port <linecard_index>:<port_index> on equip/site
        # find (linecard_index, port_index) to which it is connected to on the target equipment
        # by searching in the target equipment network description
        # returns the tuple (linecard_index, port_index)
        port = self.data['network'][site][equip]['linecards'][linecard_index]['ports'][port_index]
        uid = _parse_port_uid(port['uid'])
        if uid not in self.data['network'][site]:
            raise Exception('trying to find a linecard of equipment %s which is not in the network description of %s' % (uid, site))
        possible_targets = []
        for i_lc, lc in enumerate(self.data['network'][site][uid]['linecards']):
            if 'ports' in lc:
                for i_p, p in enumerate(lc['ports']):
                    if 'uid' in p and _parse_port_uid(p['uid']) == equip:
                        possible_targets.append((i_lc, i_p))
        targets = []
        if len(possible_targets) > 1:
            # need to disambiguate
            for target in possible_targets:
                target_port_data = self.data['network'][site][uid]['linecards'][target[0]]['ports'][target[1]]
                if 'port' in target_port_data:
                    target_lc, target_port = _parse_port_port(target_port_data['port'])
                    if target_lc is None or target_port is None:
                        logger.warn('unable to parse port spec %s of port %s:%s on %s of link from between %s(%s:%s %s)' % (target_port_data['port'], target[0], target[1], uid, equip, linecard_index, port_index, port))
                    if target_lc == linecard_index and target_port == port_index:
                        targets.append(target)
                else:
                    logger.warn('no "port" entry in api network/%s/%s/linecards[%s]/ports[%s]' % (site, uid, target[0], target[1]))
        else:
            targets = possible_targets
        if len(targets) == 0:
            logger.warn('unable to find the target linecard on %s of link from %s(%s:%s %s)' % (uid, equip, linecard_index, port_index, port))
            if len(possible_targets) > 0:
                logger.warn('there are %s candidates %s, use the first possible one: %s' % (len(possible_targets), possible_targets, possible_targets[0]))
                return possible_targets[0]
            else:
                return (None, None)
        if len(targets) > 1:
            logger.warn('unable to disambiguate between multiple links to %s from %s(%s:%s %s). candidate linecards:ports are %s. Using the first possible one %s' % (uid, equip, linecard_index, port_index, port, targets, targets[0]))
            return targets[0]
        return targets[0]

    def _get_target_lc_and_port(self, equip, linecard_index, port_index, site):
        # given the port <linecard_index>:<port_index> on equip/site
        # find (linecard_index, port_index) to which is it connected to on the target equipment
        # by looking at the (optional) port specification and by searching in the target equipment network description, and comparing / complementing this informations
        # returns the tuple (linecard_index, port_index)
        port = self.data['network'][site][equip]['linecards'][linecard_index]['ports'][port_index]
        uid = _parse_port_uid(port['uid'])
        if 'port' in port:
            target_lc1, target_port1 = _parse_port_port(port['port'])
            if target_lc1 is None:
                logger.warn('unable to get from the port spec %s the target linecard of link between %s(%s:%s %s) and %s' % (port['port'], equip, linecard_index, port_index, port, uid))
            target_lc2, target_port2 = self._get_target_linecard_and_port_from_api(equip, linecard_index, port_index, site)
            if target_lc2 is None:
                logger.warn('unable to get from %s the target linecard of link between %s(%s:%s %s) and %s' % (uid, equip, linecard_index, port_index, port, uid))
            if (target_lc1 != target_lc2) or (target_port1 != target_port2):
                if not target_lc1 is None:
                    target_lc, target_port = target_lc1, target_port1
                else:
                    target_lc, target_port = target_lc2, target_port2
                logger.warn('mismatch between the linecards of link between %s(%s:%s %s) and %s: %s:%s vs %s:%s. Using the "less unlikely one" %s:%s' % (equip, linecard_index, port_index, port, uid, target_lc1, target_port1, target_lc2, target_port2, target_lc, target_port))
                return (target_lc, target_port)
        else:
            return self._get_target_linecard_and_port_from_api(equip, linecard_index, port_index, site)

    def _is_in_api(self, site, uid):
        if uid in self.data['hosts']:
            return True
        elif uid in self.data['network'][site]:
            return True
        elif 'renater' in uid:
            return True
        else:
            return False

    def _get_node_kind(self, site, uid):
        if uid in self.data['hosts']:
            return 'node'
        elif uid in self.data['network'][site]:
            return self.data['network'][site][uid]['kind']
        elif 'renater' in uid:
            #logger.warn('no kind found for %s. Guessing it is virtual only based on its name' % (uid,))
            return 'virtual'
        else:
            return None

    def add_equip(self, equip, site):
        """Add a network equipment """
        if equip not in self.data['network'][site]:
            logger.warn('Equipment %s not described in API' % (equip,))
            return
        data = self.data['network'][site][equip]
        if self.has_node(equip):
            recurse = False
        else:
            logger.debug('Adding equipment %s', equip)
            self.add_node(equip, kind=data['kind'],
                          backplane=data['backplane_bps'])
            recurse = True
        lc_data = data['linecards']
        multiple_linecards = self._equip_uses_multiple_linecards(equip, site)
        equip_bw = data['backplane_bps']
        for i_lc, lc in enumerate(lc_data):
            lc_node = _get_linecard_name(equip, i_lc)
            if 'ports' in lc:
                for i_port, port in enumerate(lc['ports']):
                    if 'uid' in port:
                        uid = _parse_port_uid(port['uid'])
                        if not self._is_in_api(site, uid):
                            do_once((site, uid), logger.warn, 'unable to get kind of %s in %s, is it in g5k api?' % (uid, site))
                            continue
                        kind = port.get('kind')
                        kind2 = self._get_node_kind(site, uid)
                        if not kind:
                            kind = kind2
                            if kind != 'node':
                                do_once((equip, i_lc, i_port), logger.warn, 'missing kind in port %s:%s %s of %s, using %s from %s' % (i_lc, i_port, port, equip, kind, uid))
                        elif not kind2:
                            logger.warn('missing kind in %s' % (uid,))
                        elif kind != kind2:
                            logger.warn('mismatching kind %s in port %s:%s %s of %s and kind %s from %s. Using %s' % (kind, i_lc, i_port, port, equip, kind2, uid, kind2))
                            kind = kind2
                        if not kind:
                            logger.error('unable to find kind of %s' % (uid, ))
                        port_bw = lc['rate'] if 'rate' not in port else port['rate']
                        if kind == 'virtual':
                            # in this situation, we don't know what
                            # kind is the target equipment, we need to
                            # discover it
                            if uid in self.data['network'][site]:
                                pass
                            elif uid in self.data['hosts']:
                                kind = 'virtual-node'
                                logger.warn('virtual link from %s(%s:%s %s) to node %s' % (equip, i_lc, i_port, port, uid))
                            else:
                                pass
                        if self.has_node(uid):
                            if kind in ['node', 'virtual-node']:
                                for e in self.get_host_adapters(uid):
                                    if e['switch'] == equip:
                                        if multiple_linecards:
                                            self._checked_add_linecard(lc_node, lc.get('backplane_bps', data['backplane_bps']))
                                            self._checked_add_edge(equip, lc_node,
                                                                   _unique_link_key(equip, lc_node),
                                                                   bandwidth=equip_bw, active=True)
                                            self._checked_add_edge(lc_node, uid,
                                                                   _unique_link_key(lc_node, uid + '-' + e['device']),
                                                                   bandwidth=port_bw, active=e['mounted'])
                                        else:
                                            self._checked_add_edge(equip, uid,
                                                                   _unique_link_key(equip, uid + '-' + e['device']),
                                                                   bandwidth=min(port_bw, equip_bw), active=e['mounted'])
                            elif kind in ['switch', 'router'] and recurse:
                                if multiple_linecards:
                                    self._checked_add_linecard(lc_node, lc.get('backplane_bps', data['backplane_bps']))
                                    self._checked_add_edge(equip, lc_node,
                                                           _unique_link_key(equip, lc_node),
                                                           bandwidth=equip_bw, active=True)
                                    target_lc, target_port = self._get_target_lc_and_port(equip, i_lc, i_port, site)
                                    if not target_lc is None:
                                        if self._equip_uses_multiple_linecards(uid, site):
                                            self._checked_add_edge(lc_node, _get_linecard_name(uid, target_lc),
                                                                   _unique_link_key(lc_node, _get_linecard_name(uid, target_lc)),
                                                                   bandwidth=port_bw, active=True)
                                        else:
                                            self._checked_add_edge(lc_node, uid,
                                                                   _unique_link_key(lc_node, uid),
                                                                   bandwidth=port_bw, active=True)
                                    else:
                                        logger.error('unable to find the target linecard of link between %s(%s:%s %s) and %s. Skipping this link!' % (equip, i_lc, i_port, port, uid))
                                else:
                                    target_lc, target_port = self._get_target_lc_and_port(equip, i_lc, i_port, site)
                                    if not target_lc is None:
                                        if self._equip_uses_multiple_linecards(uid, site):
                                            self._checked_add_edge(equip, _get_linecard_name(uid, target_lc),
                                                                   _unique_link_key(equip, _get_linecard_name(uid, target_lc)),
                                                                   bandwidth=min(port_bw, equip_bw), active=True)
                                        else:
                                            self._checked_add_edge(equip, uid,
                                                                   _unique_link_key(equip, uid),
                                                                   bandwidth=min(port_bw, equip_bw), active=True)
                                    else:
                                        logger.error('unable to find the target linecard of link between %s(%s:%s %s) and %s. Skipping this link!' % (equip, i_lc, i_port, port, uid))
                        if 'renater' in uid:
                            # if uid != 'renater-' + site:
                            #     logger.error('renater node in %s has name %s which is not of the form renater-%s. Forcing to renater-%s' % (site, uid, site, site))
                            #     uid = 'renater-' + site
                            self.add_node(uid, kind='renater')
                            if multiple_linecards:
                                self._checked_add_linecard(lc_node, lc.get('backplane_bps', data['backplane_bps']))
                                self._checked_add_edge(equip, lc_node,
                                                       _unique_link_key(equip, lc_node),
                                                       bandwidth=equip_bw, active=True)
                                self._checked_add_edge(lc_node, uid,
                                                       _unique_link_key(lc_node, uid),
                                                       bandwidth=port_bw, active=True)
                            else:
                                self._checked_add_edge(equip, uid,
                                                       _unique_link_key(equip, uid),
                                                       bandwidth=min(port_bw, equip_bw), active=True)

                        elif kind in ['switch', 'router']:
                            if multiple_linecards:
                                self._checked_add_linecard(lc_node, lc.get('backplane_bps', data['backplane_bps']))
                                self._checked_add_edge(equip, lc_node,
                                                       _unique_link_key(equip, lc_node),
                                                       bandwidth=equip_bw, active=True)
                            if recurse:
                                self.add_equip(uid, site)

    def rm_equip(self, equip):
        """Remove an equipment from the node"""
        logger.debug('Removing equip %s', style.host(equip))
        self.remove_node(equip)
        if get_network_equipment_attributes(equip)['kind'] == 'router':
            lc_nodes = [x for x in self.nodes() if equip in x]
            logger.debug('Removing router linecard %s', ' '.join(lc_nodes))
            self.remove_nodes_from(lc_nodes)

    def add_backbone(self):
        """Add the nodes corresponding to Renater equipments"""
        logger.debug('Add %s network', style.emph('Renater'))
        backbone = self.data['network']['backbone']
        for equip in backbone:
            src = equip['uid']
            self.add_node(src, kind='renater')
            for lc in equip['linecards']:
                for port in lc['ports']:
                    if 'uid' in port and 'renater-' in _parse_port_uid(port['uid']):
                        port_bw = lc['rate'] if 'rate' not in port else port['rate']
                        latency = port['latency'] if 'latency' in port \
                            else arbitrary_latency
                        kind = 'renater' if 'kind' not in port else port['kind']
                        dst = _parse_port_uid(port['uid'])
                        logger.debug('* %s (%s, bw=%s, lat=%s)', dst, kind,
                                      port_bw, latency)
                        self.add_node(dst, kind=kind)
                        if not self.has_edge(src, dst):
                            self.add_edge(src, dst,
                                          _unique_link_key(src, dst),
                                          bandwidth=port_bw,
                                          latency=latency, active=True)
        # Removing unused one
        logger.debug('Removing unused Renater equipments')
        used_elements = []
        for site in self.get_sites():
            dests = [ s for s in self.get_sites() if s != site ]
            for dest in dests:
                gw_src = self.get_site_router(site)[0]
                gw_dst = self.get_site_router(dest)[0]
                if not gw_src is None and not gw_dst is None:
                    for element in [el for el in nx.shortest_path(self, gw_src, gw_dst) if 'renater' in el]:
                        if element not in used_elements:
                            used_elements.append(element)

        for element, _ in [n for n in self.nodes_iter(data=True) if n[1]['kind'] == 'renater']:
            if element not in used_elements:
                logger.debug('removing %s' % (element,))
                self.remove_node(element)

    def rm_backbone(self):
        """Remove all elements from the backbone"""
        self.remove_nodes_from(self.get_backbone())

    # get elements, public methods
    def get_hosts(self, cluster=None, site=None):
        """Return the list of nodes corresponding to hosts"""
        if cluster:
            return [x for x in self.nodes(True) if cluster in x[0] and
                          x[1]['kind'] == 'node']
        elif site:
            return [x for x in self.nodes(True) if site == get_host_site(x[0]) and
                          x[1]['kind'] == 'node']
        else:
            return [x for x in self.nodes(True) if x[1]['kind'] == 'node']

    def get_clusters(self, site=None):
        """Return the list of clusters"""
        if site:
            return list(set([get_host_cluster(x[0]) for x in self.nodes(True) if get_host_site(x[0]) and x[1]['kind'] == 'node']))
        else:
            return list(set([get_host_cluster(x[0]) for x in self.nodes(True) if x[1]['kind'] == 'node']))

    def get_host_neighbours(self, host):
        """Return the compute nodes that are connected to the same switch,
        router or linecard"""

        switch = self.get_host_adapters(host)[0]['switch']
        return [x for x in self.get_equip_hosts(switch) if x != host]

    def get_equip_hosts(self, equip):
        """Return the nodes which are connected to the equipment"""
        hosts = []
        if self.node[equip]['kind'] == 'router':
            lcs = self.neighbors(equip)
        else:
            lcs = ['equip']
        for lc in lcs:
            for n in self.neighbors(lc):
                if self.node[n]['kind'] == 'node':
                    hosts.append(n)

        return hosts

    def get_site_router(self, site):
        """Return the node corresponding to the router of a site"""
        site_routers = [n for n in self.nodes(True) if n[1]['kind'] == 'router' and site in n[0]]
        if len(site_routers) > 0:
            return site_routers[0]
        else:
            return (None, {})

    def get_sites(self):
        """Return the list of sites"""
        return list(set([get_host_site(x[0]) for x in self.nodes(True) if x[1]['kind'] == 'node']))

    def get_backbone(self):
        """Return """
        return [x for x in self.nodes(True) if x[1]['kind'] == 'renater']

    def get_host_adapters(self, host):
        """Return the mountable network interfaces from a host"""
        try:
            if host in self.data['hosts']:
                return [m for m in self.data['hosts'][host]['network_adapters']
                        if 'switch' in m
                        and not m['management']
                        and m['mountable']
                        and m['switch']
                        and m['interface'] == 'Ethernet']
        except:
            logger.warning("Wrong description for host %s" % style.host(host))
            logger.debug("host's network_adapters = %s" % (self.data['hosts'][host]['network_adapters'],))
            return []

    def _equip_has_nodes(self, equip):
        """ """
        data = get_network_equipment_attributes(equip)
        if data['kind'] == 'router':
            return True
        for lc in [n for n in data['linecards'] if 'ports' in n]:
            for port in sorted([p for p in lc['ports'] if 'uid' in p]):
                kind = port['kind'] if 'kind' in port else lc['kind']
                if kind == 'node' and self.has_node(_parse_port_uid(port['uid'])):
                    return True
        return False


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

    base_size = 2

    _default_color = '#000000'
    _default_shape = 'o'
    _default_size = 100
    _default_width = 0.8
    _default_font_size = 10
    _default_font_weight = 'normal'

    def _default_str_func(n):
        return n.split('.')[0]

    def _default_nodes_legend():
        """Create a default legend for the nodes"""
        return {'renater':
                {'color': '#9CF7BC', 'shape': 'p', 'size': 200},
                'router':
                {'color': '#BFDFF2', 'shape': '8', 'size': 300,
                 'width': 0.5},
                'switch':
                {'color': '#F5C9CD', 'shape': 's', 'size': 100,
                 'width': 0.2},
                'host':
                {'color': '#F0F7BE', 'shape': 'o', 'size': 30,
                 'width': 0.2},
                'cluster':
                {'color': '#F0F7BE', 'shape': 'd', 'size': 200,
                 'width': _default_width},
                'default':
                {'color': _default_color, 'shape': _default_shape,
                 'size': _default_size},
                'linecard':
                {'size': 10, 'shape': '^', 'color': 'w', 'width': 0.1},
                }

    def _default_edges_legend():
        """Defines the width and color of the edges based on bandwidth"""
        return {100000000: {'width': 0.2, 'color': '#666666'},
                1000000000: {'width': 0.4, 'color': '#666666'},
                3000000000: {'width': 0.6, 'color': '#333333'},
                10000000000: {'width': 1.0, 'color': '#111111'},
                20000000000: {'width': 2.0, 'color': '#111111'},
                30000000000: {'width': 3.0, 'color': '#111111'},
                40000000000: {'width': 4.0, 'color': '#111111'},
                'default': {'width': _default_width, 'color': _default_color}}

    def _default_nodes_labels(compact=False):
        """Defines the font labels"""

        def _default_str_func(n):
            return n.split('.')[0]

        return {'renater':
                {'nodes': {},
                 'font_size': base_size * 4,
                 'font_weight': 'normal',
                 'str_func': _default_str_func},
                'router':
                {'nodes': {},
                 'font_size': base_size * 4,
                 'font_weight': 'bold'},
                'switch':
                {'nodes': {},
                 'font_size': base_size * 4,
                 'font_weight': 'normal'},
                'cluster':
                {'nodes': {},
                 'font_size': base_size * 4,
                 'font_weight': 'normal'},
                'host':
                {'nodes': {},
                 'font_size': base_size * 3,
                 'font_weight': 'normal'},
                'default':
                {'nodes': {},
                 'font_size': _default_font_size,
                 'font_weight': _default_font_weight,
                 'str_func': _default_str_func},
                'linecard':
                {'nodes': {},
                 'font_size': base_size * 3,
                 'str_func': _default_str_func}
                }

    grdot = gr.copy()

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
        elements = ['renater', 'router', 'switch', 'node', 'linecard']
    else:
        for site in grdot.get_sites():
            for cluster, data in grdot.get_clusters().items():
                for equip, radicals in data['equips'].items():
                    grdot.add_node(cluster + '\n' + radicals,
                                {'kind': 'cluster'})
                    grdot.add_edge(cluster + '\n' + radicals, equip,
                                {'bandwidth': data['bandwidth']})

        grdot.remove_nodes_from([n[0] for n in grdot.nodes(True) if n[1]['kind'] == 'node'])

        elements = ['renater', 'router', 'switch', 'cluster']

    logger.debug('Legend and labels initialized')

    # substitute kind host to kind node in all the graph, as node is a reserved dot word
    for n in grdot.nodes_iter(True):
        if n[1].get('kind') == 'node':
            n[1]['kind'] = 'host'

    # Initializing plot
    fig = plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(111)

    logger.debug('Defining positions')
    try:
        pos = graphviz_layout(grdot, prog=layout)
    except:
        logger.warning('Error in generating graphviz layout, will use ' +
                       'spring layout that does not scale well ...')
        raise
        pos = nx.spring_layout(grdot, iterations=100)
    # Adding the nodes
    for k in elements:
        nodes = [node[0] for node in grdot.nodes_iter(data=True)
                 if 'kind' in node[1] and node[1]['kind'] == k]
        if k not in _nodes_legend:
            _nodes_legend[k] = _nodes_legend['default']
        nodes = nx.draw_networkx_nodes(grdot, pos, nodelist=nodes,
                                       node_shape=_nodes_legend[k]['shape']
                                       if 'shape' in _nodes_legend[k] else
                                       _default_shape,
                                       node_color=_nodes_legend[k]['color']
                                       if 'color' in _nodes_legend[k] else
                                       _default_color,
                                       node_size=_nodes_legend[k]['size']
                                       if 'size' in _nodes_legend[k] else
                                       _default_size,
                                       linewidths=_nodes_legend[k]['width']
                                       if 'width' in _nodes_legend[k] else
                                       _default_width)

    # Adding the edges
    for bandwidth, params in _edges_legend.items():
        if bandwidth != 'other':
            edges = [(edge[0], edge[1]) for edge in grdot.edges_iter(data=True)
                     if 'bandwidth' in edge[2] and edge[2]['bandwidth'] == bandwidth]
            nx.draw_networkx_edges(grdot, pos, edgelist=edges,
                                   width=params['width'] if 'width' in params
                                   else _default_width,
                                   edge_color=params['color'] if 'color' in params
                                   else _default_color)
    edges = [(edge[0], edge[1]) for edge in grdot.edges_iter(data=True)
             if edge[2]['bandwidth'] not in _edges_legend]

    nx.draw_networkx_edges(grdot, pos, edgelist=edges,
                           width=_edges_legend['default']['width'],
                           edge_color=_edges_legend['default']['color'])
    # Adding the labels
    for node, data in grdot.nodes_iter(data=True):
        if 'nodes' not in _nodes_labels[data['kind']]:
            _nodes_labels[data['kind']]['nodes'] = {}
        if data['kind'] in _nodes_labels:
            _nodes_labels[data['kind']]['nodes'][node] = _nodes_labels[data['kind']]['str_func'](node) \
                if 'str_func' in _nodes_labels[data['kind']] else _default_str_func(node)
        else:
            _nodes_labels['default']['nodes'][node] = _nodes_labels['default']['str_func'](node)

    for data in _nodes_labels.values():
        nx.draw_networkx_labels(grdot, pos, labels=data['nodes'],
                                font_size=data['font_size']
                                if 'font_size' in data else _default_font_size,
                                font_weight=data['font_weight']
                                if 'font_weight' in data else _default_font_weight)

    plt.axis('off')
    plt.tight_layout()

    title = 'Created by execo_g5k.topology \n%s\nAPI commit %s' % \
        (grdot.graph['date'], grdot.graph['api_commit'])
    plt.text(0.1, 0, title, transform=ax.transAxes)

    return fig


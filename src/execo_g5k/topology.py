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

""" A module based on networkx to create a topological graph of the
Grid'5000 platform """
from execo import logger
from api_cache import get_api_data
from networkx import Graph, set_edge_attributes, get_edge_attributes

arbitrary_latency = 2.25E-3


def backbone_graph(backbone):
    """Return a networkx undirected graph describing the Grid'5000
    backbone from the list of backbone equipements"""
    if backbone is None:
        backbone, _, _ = get_api_data()
    gr = Graph()
    # Adding backbone equipments and links
    for equip in backbone:
        src = equip['uid']
        if not gr.has_node(src):
            gr.add_node(src, kind='renater')
        for lc in equip['linecards']:
            for port in lc['ports']:
                kind = 'renater' if not 'kind' in port else port['kind']
                dst = port['uid'] if not 'site_uid' in port else port['uid'] \
                + '.' + port['site_uid']
                rate = lc['rate'] if not 'rate' in port else port['rate']
                latency = port['latency'] if 'latency' in port \
                    else arbitrary_latency
                if not gr.has_node(dst):
                    gr.add_node(dst, kind=kind)
                if not gr.has_edge(src, dst):
                    gr.add_edge(src, dst, bandwidth=rate, latency=latency)
    return gr


def site_graph(site, hosts, equips):
    """Return a networkx undirected graph describing the site
    topology from the dict of hosts and list of site equipments"""
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
                    sgr.add_edge(src, dst, bandwidth=adapt['rate'],
                                 latency=latency, weight=0.5)
    return sgr

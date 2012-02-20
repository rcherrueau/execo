# Copyright 2009-2012 INRIA Rhone-Alpes, Service Experimentation et
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

import itertools, re, execo

class HashableDict(dict):

    """Hashable dictionnary. Beware: must not mutate it after its first use as a key."""

    def __key(self):
        return tuple((k,self[k]) for k in sorted(self))

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        return self.__key() == other.__key()

class ParamSweeper(object):
    
    """Iterable over all possible combinations of parameters.
    
    Given a a directory associating parameters and the list of their
    values, will iterate over the cartesian product of all parameters
    combinations. For example:
    
    >>> ps = ParamSweeper({
    ...   "param1": [ 0, 1 ],
    ...   "param2": [ "a", "b", "c" ]
    ...   })
    >>> list(ps)
    [{'param2': 'a', 'param1': 0}, {'param2': 'a', 'param1': 1}, {'param2': 'b', 'param1': 0}, {'param2': 'b', 'param1': 1}, {'param2': 'c', 'param1': 0}, {'param2': 'c', 'param1': 1}]
    """

    def __init__(self, parameters):
        self.parameters = parameters

    def __iter__(self):
        return ( HashableDict(zip(self.parameters.keys(), values)) for
                 values in itertools.product(*self.parameters.values()) )

__g5k_host_group_regex = re.compile("^([a-zA-Z]+)-\d+\.([a-zA-Z]+)\.grid5000\.fr$")

def g5k_host_get_cluster(host):
    if isinstance(host, execo.Host):
        host = host.address
    m = __g5k_host_group_regex.match(host)
    return m.group(1)

def g5k_host_get_site(host):
    if isinstance(host, execo.Host):
        host = host.address
    m = __g5k_host_group_regex.match(host)
    return m.group(2)

def group_hosts(nodes):
    grouped_nodes = {}
    for site, site_nodes in itertools.groupby(
        sorted(nodes,
               lambda n1, n2: cmp(
                   g5k_host_get_site(n1),
                   g5k_host_get_site(n2))),
        g5k_host_get_site):
        grouped_nodes[site] = {}
        for cluster, cluster_nodes in itertools.groupby(
            sorted(site_nodes,
                   lambda n1, n2: cmp(
                       g5k_host_get_cluster(n1),
                       g5k_host_get_cluster(n2))),
            g5k_host_get_cluster):
            grouped_nodes[site][cluster] = list(cluster_nodes)
    return grouped_nodes

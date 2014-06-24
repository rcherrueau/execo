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

from execo.action import ActionFactory
from execo.config import SSH, SCP, make_connection_params
from execo.host import Host
from execo_g5k.config import g5k_configuration, default_frontend_connection_params
from execo_g5k.api_utils import get_resource_attributes
from execo.process import get_port_forwarder
import re
import socket

frontend_factory = ActionFactory(remote_tool = SSH,
                                 fileput_tool = SCP,
                                 fileget_tool = SCP)

__default_frontend = None
__default_frontend_cached = False
def get_default_frontend():
    """Return the name of the default frontend."""
    global __default_frontend, __default_frontend_cached #IGNORE:W0603
    if not __default_frontend_cached:
        __default_frontend_cached = True
        if g5k_configuration.get("default_frontend"):
            __default_frontend = g5k_configuration["default_frontend"]
        else:
            try:
                localhost = socket.gethostname()
            except socket.error:
                localhost = ""
            mo = re.search("^[^ \t\n\r\f\v\.]+\.([^ \t\n\r\f\v\.]+)\.grid5000.fr$", localhost)
            if mo:
                __default_frontend = mo.group(1)
            else:
                __default_frontend = None
    return __default_frontend

def get_frontend_host(frontend = None):
    """Given a frontend name, or None, and based on the global configuration, returns the frontend to connect to or None."""
    if frontend == None:
        frontend = get_default_frontend()
    if g5k_configuration.get('no_ssh_for_local_frontend') == True and frontend == get_default_frontend():
        frontend = None
    if frontend:
        frontend = Host(frontend)
    return frontend

def get_kavlan_host_name(host, vlanid):
    """Returns the DNS hostname of a host once switched in a kavlan."""
    if isinstance(host, Host):
        host = host.address
    host_shortname, sep, fqdn = host.partition(".")
    vlan_host_name = host_shortname + "-kavlan-" + str(vlanid) + sep + fqdn
    return vlan_host_name

class G5kAutoPortForwarder():
    """Context manager for automatically opening a port forwarder if outside grid5000"""

    def __init__(self, site, host, port):
        """
        :param site: the grid5000 site to connect to

        :param host: the host to connect to in the site

        :param port: the port to connect to on the host

        It automatically decides if a port forwarding is needed,
        depending if run from inside or outside grid5000.

        When entering the context, it returns a tuple actual host,
        actual port to connect to. It is the real host/port if inside
        grid5000, or the forwarded port if outside. The forwarded
        port, if any, is guaranteed to be operational.

        When leaving the context, it kills the port forwarder
        background process.
        """
        self.__site = site
        self.__host = host
        self.__port = port
        self.__port_forwarder = None
        self.__local_port = None

    def __enter__(self):
        if 'grid5000.fr' in socket.getfqdn():
            return self.__host, self.__port
        else:
            self.__port_forwarder, self.__local_port = get_port_forwarder(
                get_frontend_host(self.__site),
                make_connection_params(default_frontend_connection_params),
                self.__host,
                self.__port)
            self.__port_forwarder.start()
            self.__port_forwarder.forwarding.wait()
            return "127.0.0.1", self.__local_port

    def __exit__(self, t, v, traceback):
        if self.__port_forwarder:
            self.__port_forwarder.kill()
        return False

def get_kavlan_network(kavlan, site):
    """Retrieve the network parameters for a given kavlan from the API"""
    network, mask_size = None, None
    equips = get_resource_attributes('/sites/' + site + '/network_equipments/')
    for equip in equips['items']:
        if 'vlans' in equip and len(equip['vlans']) > 2:
            all_vlans = equip['vlans']
    for info in all_vlans.itervalues():
        if type(info) == type({}) and 'name' in info \
            and info['name'] == 'kavlan-' + str(kavlan):
            network, _, mask_size = info['addresses'][0].partition('/',)
    return network, mask_size

def get_kavlan_ip_mac(kavlan, site):
    """Retrieve the network parameters for a given kavlan from the API"""
    network, mask_size = get_kavlan_network(kavlan, site)
    min_2 = (kavlan - 4) * 64 + 2 if kavlan < 8 \
            else (kavlan - 8) * 64 + 2 if kavlan < 10 \
            else 216
    ips = [".".join([str(part) for part in ip]) for ip in
           [ip for ip in get_ipv4_range(tuple([int(part)
                for part in network.split('.')]), int(mask_size))
           if ip[3] not in [0, 254, 255] and ip[2] >= min_2]]
    return ips

def get_ipv4_range(network, mask_size):
    """Get the ipv4 range from a network and a mask_size"""
    net = (network[0] << 24
            | network[1] << 16
            | network[2] << 8
            | network[3])
    mask = ~(2 ** (32 - mask_size) - 1)
    ip_start = net & mask
    ip_end = net | ~mask
    return [((ip & 0xff000000) >> 24,
              (ip & 0xff0000) >> 16,
              (ip & 0xff00) >> 8,
              ip & 0xff)
             for ip in xrange(ip_start, ip_end + 1)]

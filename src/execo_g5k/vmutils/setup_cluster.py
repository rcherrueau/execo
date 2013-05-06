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

"""Some classes to configure a cluster with libvirt on Grid5000 and various virtualization
technology
"""
from pprint import pprint, pformat
import time as T, logging as LOG, xml.etree.ElementTree as ET
import execo as EX, execo_g5k as EX5
from execo import logger
from execo_g5k.api_utils import get_g5k_sites, get_site_clusters, get_cluster_attributes, get_host_attributes, get_resource_attributes
import execo.time_utils as EXT
from collections import deque

def get_kavlan_sites():
    """Function that retrieve a list of sites where KAVLAN is enable"""
    kavlan_sites = []
    for site in get_g5k_sites():
        cluster_attr = get_cluster_attributes(get_site_clusters(site)[0])
        if cluster_attr['kavlan']:
            kavlan_sites.append(site)
    return kavlan_sites


def get_virt_clusters(sites = get_g5k_sites()):
    """Function that returns the list of clusters with virtualization capacities"""
    virt_clusters = []
    for site in sites:
        for cluster in get_site_clusters(site):
            if get_host_attributes(cluster+'-1.'+site+'.grid5000.fr')['supported_job_types']['virtual'] in [ 'ivt', 'amd-v']:
                virt_clusters.append(cluster)

    logger.info('Clusters with virtualization capacities \n%s', pformat( virt_clusters ))
    return virt_clusters

def get_big_clusters(sites = get_g5k_sites(), n_nodes = 20):
    """Function that returns the list of clusters that have more than n_nodes, default is 20"""
    big_clusters = []
    for site in sites:
        for cluster in get_site_clusters(site):
            if get_resource_attributes('grid5000/sites/'+site+'/clusters/'+cluster+'/nodes')['total'] >= n_nodes:
                big_clusters.append(cluster)
    logger.info('Clusters with more than '+str(n_nodes)+' nodes \n%s', pformat(big_clusters))
    return big_clusters

def get_fast_clusters(sites = get_g5k_sites(), node_flops = 10**12, n_nodes = 20):
    """Function that returns the list of clusters which node have a power >= node_flops and
    a number of nodes >= n_nodes"""
    fast_clusters = []
    for site in sites:
        for cluster in get_site_clusters(site):
            if get_resource_attributes('grid5000/sites/'+site+'/clusters/'+cluster+'/nodes')['total'] >= n_nodes\
                    and get_host_attributes(cluster+'-1')['performance'] >= node_flops:
                fast_clusters.append(cluster)
    logger.info('Clusters with a power greater than '+str(node_flops)+' and more than '+str(n_nodes)+' nodes\n%s',
                    pformat(fast_clusters))
    return fast_clusters

class VirshCluster(object):
    """Base class to deploy and configure hosts virtualization technology"""
    def __init__(self, hosts, kavlan = None, env_name = None, env_file = None):
        """ """
        logger.setLevel('INFO')
        self.hosts = hosts
        self.kavlan = kavlan
        self.packages_list = ' qemu-kvm nmap virtinst libvirt-bin'
        if env_file is None:
            self.env_name = 'squeeze-x64-prod' if env_name is None else env_name
        else:
            self.env_file = env_file
            self.env_name = None
        self.bridge = 'br0'
        self.frontend = 'reims.grid5000.fr'
        self.state =['initialized']
        self.hack_cmd = "source /etc/profile; "

    def run(self):
        """Sequentially execute deploy_hosts, rename_hosts, setup_packages,
        configure_libvirt, create_disk_image, copy_ssh_keys"""
        self.deploy_hosts()
        self.rename_hosts()
        self.setup_packages()
        self.configure_libvirt()
        self.create_disk_image()
        self.copy_ssh_keys()

    def deploy_hosts(self, out=False):
        if self.env_name is not None:
            logger.info('Deploying environment %s ...', self.env_name)
            deployment = EX5.Deployment(
                                                    hosts = self.hosts,
                                                    env_name = self.env_name,
                                                    vlan = self.kavlan)
        elif self.env_file is not None:
            logger.info('Deploying environment %s ...', self.env_file)
            deployment = EX5.Deployment(
                                                    hosts = self.hosts,
                                                    env_file = self.env_file,
                                                    vlan = self.kavlan)

        logger.debug('Deployment command ')
        Hosts = EX5.deploy(deployment, num_tries = 3, out = out)
        self.hosts = Hosts[0]
        logger.info('%s deployed',' '.join([node.address for node in self.hosts]))
        self.state.append('deployed')

    def setup_packages(self):
        logger.info('Upgrading hosts')
        cmd = self.hack_cmd+" echo 'debconf debconf/frontend select noninteractive' | debconf-set-selections; \
                echo 'debconf debconf/priority select critical' | debconf-set-selections ;      \
                apt-get update ; export DEBIAN_MASTER=noninteractive ; apt-get dist-upgrade -y;"
        upgrade = EX.Remote(cmd, self.hosts).run()
        if upgrade.ok():
            logger.debug('Upgrade finished')
        else:
            logger.error('Unable to perform dist-upgrade on the nodes ..')

        cmd = self.hack_cmd+'apt-get update && apt-get install  -y --force-yes '+self.packages_list
        logger.info('Installing packages ...')
        install = EX.Remote(cmd, self.hosts).run()
        if not install.ok():
            logger.error('Unable to install packages')
        else:
            logger.info('Packages installed')
        self.state.append('installed')

    def reboot(self):
        logger.info('Rebooting nodes ...')
        reboot=EX.Remote('reboot',self.hosts).run()
        if reboot.ok():
            while True:
                T.sleep(5)
                ping=[]
                for host in self.hosts:
                    logger.info('Waiting for node %s',host.address)
                    ping.append(EX.Remote('ping -c 4 '+host.address,[self.frontend],connexion_params={'user':'lpouilloux'}, log_exit_code=False))
                all_ping=EX.ParallelActions(ping).run()

                if all_ping.ok():
                    break;
                logger.info('Nodes down, sleeping for 5 seconds ...')
            logger.info('All hosts have been rebooted !')
        else:
            logger.error('Not able to connect to the hosts ...')
        self.state.append('rebooted')

    def rename_hosts(self):
        '''Rename hosts with the kavlan suffix '''
        if self.kavlan is not None:
            logger.info('Using KaVLAN, renaming hosts')
            for host in self.hosts:
                part_host = host.address.partition('.')
                host.address = part_host[0]+'-kavlan-'+str(self.kavlan)+part_host[1]+ part_host[2]
            logger.info('Hosts name have been changed :\n %s',pformat(self.hosts))
            self.state.append('renamed')

    def create_disk_image(self):
        '''Create a base image for the virtualization technology using qcow2 '''
        logger.info("Creating disk image on /tmp/vm-base.img")
        cmd = self.hack_cmd+'qemu-img convert -O raw /grid5000/images/KVM/squeeze-x64-base.qcow2 /tmp/vm-base.img'
        EX.Remote(cmd,self.hosts).run()
        self.state.append('disk_created')

    def copy_ssh_keys(self, ssh_key = None):
        '''Copying your ssh_keys on hosts and vms for automatic connexion'''
        logger.info('Copying ssh key on hosts ...')
        ssh_key = '~/.ssh/id_rsa' if ssh_key is None else ssh_key
        EX.Put(self.hosts,[ssh_key, ssh_key+'.pub'],remote_location='.ssh/').run()
        EX.Remote('cat '+ssh_key+'.pub >> .ssh/authorized_keys; '+ \
                          'echo "Host * \n StrictHostKeyChecking no" >> .ssh/config; ', self.hosts).run()

        if 'disk_created' not in self.state:
            self.create_disk_image()

        logger.info('Copying ssh key on vm-base ...')


        cmd = self.hack_cmd+'modprobe nbd max_part=1; '+ \
                'qemu-nbd --connect=/dev/nbd0 /tmp/vm-base.img; sleep 5; '+ \
                'mount /dev/nbd0p1 /mnt; mkdir /mnt/root/.ssh; '+ \
                'cat '+ssh_key+'.pub >> /mnt/root/.ssh/authorized_keys; '+ \
                'echo "Host * \n StrictHostKeyChecking no" >> /mnt/root/.ssh/config; '+ \
                'umount /mnt'
        logger.debug(cmd)
        copy_on_vm_base = EX.Remote(cmd, self.hosts).run()
        logger.debug('%s', copy_on_vm_base.ok())
        self.state.append('ssh_keys')

#       def setup_stress(self):
#               '''Install the stress program on the base vm '''
#               cmd = 'modprobe nbd max_part=1; '+ \
#                        'qemu-nbd --connect=/dev/nbd0 /tmp/vm-base.img; sleep 3; '+ \
#                         'mount /dev/nbd0p1 /mnt; mkdir /mnt/root/.ssh; '+ \
#                         'apt-get install stress '+ \
#                         'umount /mnt'
#               #EX.Remote(cmd, self.hosts).run()

    def configure_libvirt(self, network_xml = None):
        '''Configure the default network used by libvirt '''
        logger.info('Configuring libvirt network ...')
        if network_xml is None:
            root=ET.Element('network')
            name=ET.SubElement(root,'name')
            name.text='default'
            ET.SubElement(root, 'forward', attrib={'mode':'bridge'})
            ET.SubElement(root, 'bridge', attrib={'name': self.bridge})
        else:
            logger.info('Using custom file for network...')
            root = network_xml
        self.tree = ET.ElementTree(element=root)
        self.tree.write('default.xml')
        logger.info('Pushing default.xml on all nodes ...')

        EX.Remote('virsh net-destroy default; virsh net-undefine default', self.hosts).run()
        EX.Put(self.hosts, 'default.xml', remote_location = '/etc/libvirt/qemu/networks/').run()
        EX.Remote('virsh net-define /etc/libvirt/qemu/networks/default.xml ; virsh net-start default; virsh net-autostart default; ', self.hosts).run()
        self.state.append('virsh_network')

        logger.info('Making libvirt host unique ...')
        cmd = self.hack_cmd+'uuid=`uuidgen` && sed -i "s/00000000-0000-0000-0000-000000000000/${uuid}/g" /etc/libvirt/libvirtd.conf '\
                +'&& sed -i "s/#host_uuid/host_uuid/g" /etc/libvirt/libvirtd.conf && service libvirt-bin restart'
        EX.Remote(cmd, self.hosts).run()




#class KVMCluster(VirshCluster):
#       def __init__(self, hosts, kavlan = None):
#               VirshCluster.__init__(self, hosts, kavlan)
#               self.packages_list = ' qemu-kvm'
#
#
#
#
#class XenCluster(VirshCluster):
#       def __init__(self, hosts):
#               VirshCluster.__init__(hosts)
#               self.env_name = 'squeeze-x64-xen'
#               self.packages_list = ' xen-qemu-dm-4.0 nfs-common'
#
#       def adding_migration(self):
#               cmd='echo "(xend-unix-server yes)" >> /etc/xen/xend-config.sxp ; '+ \
#                       'echo "(xend-relocation-server yes)" >> /etc/xen/xend-config.sxp ; '+ \
#                       'service xend restart'
#               EX.Remote(cmd,self.hosts).run()
#
#       def run(self):
#               self.deploy_hosts()
#               self.adding_migration()
#               self.setup_packages()

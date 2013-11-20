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
"""A class to manage a deployment of hosts configured with libvirt-KVM"""

from logging import DEBUG
from pprint import pprint, pformat
import time, xml.etree.ElementTree as ETree, re, os, tempfile
from itertools import cycle
import random
import execo as EX, execo_g5k as EX5
from xml.dom import minidom
from execo import logger, Host, SshProcess, default_connection_params
from execo.time_utils import sleep
from execo.action import ActionFactory
from execo.log import style
from execo.config import TAKTUK, SSH, SCP
from execo_g5k.config import g5k_configuration, default_frontend_connection_params
from execo_g5k.utils import get_kavlan_host_name
from execo_g5k.api_utils import get_host_cluster, get_cluster_site, get_g5k_sites, get_site_clusters, get_cluster_attributes, get_host_attributes, get_resource_attributes, get_host_site
from execo.exception import ActionsFailed

   

class Virsh_Deployment(object):
    """ A object that allow you to deploy some wheezy-based hosts with an up-to-date version of libvirt with
    a pool of ip-MAC addresses for your virtual machines """
    def __init__(self, hosts = None, env_name = None, env_file = None, kavlan = None, oarjob_id = None, outdir = None):
        """ Initialization of deployment"""
        logger.info('Initializing Virsh_Deployment on %s hosts', len(hosts))
        self.max_vms = 10227
        self.fact = ActionFactory(remote_tool = SSH,
                                fileput_tool = SCP,
                                fileget_tool = SCP)
        self.hosts      = sorted(hosts, key = lambda x: x.address)
        self.clusters   = list(set([ get_host_cluster(host) for host in self.hosts ]))
        self.sites      = list(set([ get_cluster_site(cluster) for cluster in self.clusters ]))
        self.kavlan     = kavlan
        self.oarjob_id  = oarjob_id
        self.env_name   = env_name if env_name is not None else 'wheezy-x64-base'
        self.env_file   = env_file
        self.outdir     = tempfile.mkdtemp(prefix = 'deploy_' + time.strftime("%Y%m%d_%H%M%S_%z") + '_') if outdir is None else outdir
        try:
            os.mkdir(self.outdir)
        except:
            pass 
        self.get_hosts_attr()
        self.service_node = self.get_fastest_host()
        
        logger.debug('hosts %s',    pformat (self.hosts))
        logger.debug('clusters %s', pformat (self.clusters))
        logger.debug('sites %s',    pformat (self.sites))
        logger.debug('sites %s',    pformat (self.hosts_attr))
        
    def deploy_hosts(self, out = False, max_tries = 2, check_deployed_command = True):
        """ Deploy the environment specified by env_name or env_file """
        
        if self.env_file is not None:
            logger.info('Deploying environment %s ...', style.emph( self.env_file ) )
            deployment = EX5.Deployment( hosts = self.hosts, env_file = self.env_file,
                                        vlan = self.kavlan)
        elif self.env_name is not None:
            logger.info('Deploying environment %s ...',style.emph( self.env_name ) )
            deployment = EX5.Deployment( hosts = self.hosts, env_name = self.env_name,
                                        vlan = self.kavlan)
            
        deployed_hosts, undeployed_hosts = EX5.deploy(deployment, out = out, num_tries = max_tries, check_deployed_command = check_deployed_command)
        
        if len(list(undeployed_hosts)) > 0 :
            logger.warning('Hosts %s haven\'t been deployed', ', '.join( [node.address for node in undeployed_hosts] ))
        
        if self.kavlan is not None:
            self.hosts = [ Host(get_kavlan_host_name(host, self.kavlan)) for host in deployed_hosts ]
        else: 
            self.hosts = list(deployed_hosts)    
        self.hosts.sort(key = lambda x: x.address)
                
        logger.info('%s deployed', ' '.join([host.address for host in self.hosts]))
   
    def configure_apt(self):
        """ Add testing and unstable to /etc/apt/sources.list and set the priority wheezy > testing > unstable """
        logger.info('Configuring APT')
        f = open(self.outdir + '/sources.list', 'w')
        f.write('deb http://ftp.debian.org/debian stable main contrib non-free\n'+\
                'deb http://ftp.debian.org/debian testing main \n'+\
                'deb http://ftp.debian.org/debian unstable main \n')
        f.close()
        f = open(self.outdir + '/preferences', 'w')
        f.write('Package: * \nPin: release a=stable \nPin-Priority: 900\n\n'+\
                'Package: * \nPin: release a=testing \nPin-Priority: 850\n\n'+\
                'Package: * \nPin: release a=unstable \nPin-Priority: 800\n\n')
        f.close()
        
        
        apt_conf = self.fact.get_fileput(self.hosts, [self.outdir + '/sources.list', self.outdir + '/preferences'], 
                                         remote_location = '/etc/apt/', connection_params = {'user': 'root'}).run()
        
        if apt_conf.ok:
            logger.debug('apt configured successfully')
        else:
            logger.error('Error in configuring apt')
            raise ActionsFailed, [apt_conf]
        

    def upgrade_hosts(self):
        """ Perform apt-get update && apt-get dist-upgrade in noninteractive mode """
        logger.info('Upgrading hosts')
        cmd = " echo 'debconf debconf/frontend select noninteractive' | debconf-set-selections; \
                echo 'debconf debconf/priority select critical' | debconf-set-selections ;      \
                apt-get update ; export DEBIAN_MASTER=noninteractive ; apt-get dist-upgrade -y --force-yes "+\
                '-o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" '
        upgrade = self.fact.get_remote( cmd, self.hosts, connection_params = {'user': 'root'}).run()
        if upgrade.ok:
            logger.debug('Upgrade finished')
        else:
            logger.error('Unable to perform dist-upgrade on the nodes ..')
            raise ActionsFailed, [upgrade]


    def install_packages(self, packages_list = None):
        """ Installation of required packages on the nodes """
    
        base_packages = 'uuid-runtime bash-completion taktuk locate htop init-system-helpers'
        
        logger.info('Installing usefull packages %s', style.emph(base_packages))
        cmd = 'export DEBIAN_MASTER=noninteractive ; apt-get update && apt-get install -y --force-yes '+ base_packages
        install_base = self.fact.get_remote(cmd, self.hosts, connection_params = {'user': 'root'}).run()        
        if install_base.ok:
            logger.debug('Packages installed')
        else:
            logger.error('Unable to install packages on the nodes ..')
            raise ActionsFailed, [install_base]
        
        libvirt_packages = 'libvirt-bin virtinst python2.7 python-pycurl python-libxml2 qemu-kvm nmap'
        logger.info('Installing libvirt updated packages %s', style.emph(libvirt_packages))
        cmd = 'export DEBIAN_MASTER=noninteractive ; apt-get update && apt-get install -y --force-yes '+\
            '-o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" -t unstable '+\
            libvirt_packages
        install_libvirt = self.fact.get_remote(cmd, self.hosts, connection_params = {'user': 'root'}).run()
            
        if install_libvirt.ok:
            logger.debug('Packages installed')
        else:
            logger.error('Unable to install packages on the nodes ..')
            raise ActionsFailed, [install_libvirt]
        
        if packages_list is not None:
            logger.info('Installing extra packages %s', style.emph(packages_list))
            cmd = 'export DEBIAN_MASTER=noninteractive ; apt-get update && apt-get install -y --force-yes '+\
            packages_list
            install_extra = self.fact.get_remote(cmd, self.hosts, connection_params = {'user': 'root'}).run()
            
            if install_extra.ok:
                logger.debug('Packages installed')
            else:
                logger.error('Unable to install packages on the nodes ..')
                raise ActionsFailed, [install_extra]
            
    def check_nodes(self):
        """ Install g5kchecks on nodes and check that there are correct"""
        self.fact.get_remote('echo deb http://apt.grid5000.fr/debian sid main >> /etc/apt/sources.list ; '+\
                             'export DEBIAN_MASTER=noninteractive ; apt-get update && apt-get install -y --force-yes'+\
                             'grid5000-keyring g5kchecks', self.hosts).run()
        
        

    def reboot_nodes(self):
        """ Reboot the nodes to load the new kernel """
        logger.info('Rebooting nodes')
        self.fact.get_remote('shutdown -r now ', self.hosts, connection_params = {'user': 'root'}).run()
        n_host = len(self.hosts)
        hosts_list = ' '.join( [host.address for host in self.hosts ])
        
        hosts_down = False
        nmap_tries = 0
        while (not hosts_down) and nmap_tries < 20:
            sleep(10)
            nmap_tries += 1 
            nmap = SshProcess('nmap '+hosts_list+' -p 22', Host('rennes'),
                              connection_params = default_frontend_connection_params ).run()
            for line in nmap.stdout.split('\n'):
                if 'Nmap done' in line:
                    hosts_down = line.split()[5].replace('(','') == str(0)
        
        
        logger.info('Hosts have been shutdown, wait hosts reboot')
        hosts_up = False
        nmap_tries = 0
        while (not hosts_up) and nmap_tries < 20:
            sleep(20)
            nmap_tries += 1 
            nmap = SshProcess('nmap '+hosts_list+' -p 22', Host('rennes'),
                              connection_params = default_frontend_connection_params ).run()
            for line in nmap.stdout.split('\n'):
                if 'Nmap done' in line:
                    hosts_up = line.split()[2] == line.split()[5].replace('(','')
            
        sleep(5)
        if hosts_up:
            logger.info('Hosts have been successfully rebooted')
        else:
            logger.error('Fail to reboot all hosts ...')
        

    def configure_libvirt(self, n_vms = 10000, network_xml = None, bridge = 'br0'):
        """Configure libvirt: make host unique, configure and restart the network """
        
        logger.info('Making libvirt host unique ...')
        cmd = 'uuid=`uuidgen` && sed -i "s/00000000-0000-0000-0000-000000000000/${uuid}/g" /etc/libvirt/libvirtd.conf '\
                +'&& sed -i "s/#host_uuid/host_uuid/g" /etc/libvirt/libvirtd.conf && service libvirt-bin restart'
        self.fact.get_remote(cmd, self.hosts, connection_params = {'user': 'root'}).run()
        
        self.create_bridge()
        
        logger.info('Configuring libvirt network ...')
        if network_xml is None:
            root = ETree.Element('network')
            name = ETree.SubElement(root,'name')
            name.text = 'default'
            ETree.SubElement(root, 'forward', attrib={'mode':'bridge'})
            ETree.SubElement(root, 'bridge', attrib={'name': bridge})
        else:
            logger.info('Using custom file for network... \n%s', network_xml)
            root = ETree.fromstring( network_xml )
            
        self.tree = ETree.ElementTree(element=root)

        self.tree.write('default.xml')
        
        r = self.fact.get_remote('virsh net-destroy default; virsh net-undefine default', self.hosts,
                    connection_params = {'user': 'root'})
        r.log_exit_code = False
        r.run()
        
        self.fact.get_fileput(self.hosts, ['default.xml'], remote_location = '/etc/libvirt/qemu/networks/',
                      connection_params = {'user': 'root'}).run()
              
        self.fact.get_remote('virsh net-define /etc/libvirt/qemu/networks/default.xml ; virsh net-start default; virsh net-autostart default; ', 
                        self.hosts, connection_params = {'user': 'root'}).run()
        
        logger.info('Restarting libvirt ...')        
        self.fact.get_remote('service libvirt-bin restart', self.hosts, connection_params = {'user': 'root'}).run()
        
        


    def create_bridge(self, bridge_name = 'br0'):
        """ Creation of a bridge to be used for the virtual network """
        logger.info('Configuring the bridge')
        
        
        bridge_exists = self.fact.get_remote("brctl show |grep -v 'bridge name' | awk '{ print $1 }' |head -1", self.hosts,
                         connection_params = {'user': 'root'})
        bridge_exists.log_exit_code = False
        bridge_exists.run()
        nobr_hosts = []
        for p in bridge_exists.processes:
            stdout = p.stdout.strip()            
            if len(stdout) == 0:
                nobr_hosts.append(p.host)
            else:
                if stdout != bridge_name:
                    self.fact.get_remote('ip link set '+stdout+' down ; brctl delbr '+stdout, [p.host],
                              connection_params = {'user': 'root'}).run()
                    nobr_hosts.append(p.host)
        
        if len(nobr_hosts) > 0:
            cmd = 'sed -i "s/dhcp/manual/g" /etc/network/interfaces ;  export br_if=`ip route |grep default |cut -f 5 -d " "`;  echo " " >> /etc/network/interfaces ; echo "auto '+bridge_name+'" >> /etc/network/interfaces ; '+\
                'echo "iface '+bridge_name+' inet dhcp" >> /etc/network/interfaces ; echo "bridge_ports $br_if" >> /etc/network/interfaces ;'+\
                ' echo "bridge_stp off" >> /etc/network/interfaces ; echo "bridge_maxwait 0" >> /etc/network/interfaces ;'+\
                ' echo "bridge_fd 0" >> /etc/network/interfaces ; nohup service networking restart &'
            
            create_br = self.fact.get_remote(cmd, nobr_hosts, connection_params = {'user': 'root'}).run()
            
            if create_br.ok:
                logger.info('Bridge has been created')
            else:
                logger.error('Unable to setup the bridge')
                raise ActionsFailed, [create_br]
        else:
            logger.info('Bridge is already present')

    def configure_service_node(self, dhcp_range = None, dhcp_router = None, dhcp_hosts = None, apt_cacher = False):
        """ Generate the hosts lists, the vms list, the dnsmasq configuration and setup a DNS/DHCP server """
        service_node = self.get_fastest_host()
        
        f = open(self.outdir+'/hosts.list', 'w')
        for host in self.hosts:
            f.write(host.address+'\n')
        f.close()
        f = open(self.outdir+'/vms.list', 'w')
        f.write('\n')
        for idx, val in enumerate(self.ip_mac):
            f.write(val[0]+'         '+'vm-'+str(idx)+'\n')
        f.close()
        get_ip = SshProcess('host '+service_node.address+' |cut -d \' \' -f 4', 'rennes', 
                            connection_params = default_frontend_connection_params).run()
        ip = get_ip.stdout.strip()
        f = open(self.outdir+'/resolv.conf', 'w')
        f.write('domain grid5000.fr\nsearch grid5000.fr '+' '.join( [site+'.grid5000.fr' for site in self.sites] )+\
                ' \nnameserver '+ip+ '\n')
        f.close()
        f = open(self.outdir+'/dnsmasq.conf', 'w')
        f.write(dhcp_range+dhcp_router+dhcp_hosts+"\ndhcp-option=option:domain-search,grid5000.fr,"+\
                ','.join( [site+'.grid5000.fr' for site in self.sites]) )
        f.close()
        
        logger.info('Configuring %s as a %s server', style.host(service_node.address.split('.')[0])
                    , style.emph('DNS/DCHP'))
        
        EX.Remote('export DEBIAN_MASTER=noninteractive ; apt-get update ; apt-get -y purge dnsmasq-base  ; apt-get install -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confnew" -y dnsmasq nmap', [service_node],
                  connection_params = {'user': 'root'}).run()
        EX.Put([service_node], [self.outdir+'/dnsmasq.conf'], remote_location='/etc/', connection_params = { 'user': 'root' }).run()
        
        logger.info('Adding the VM in /etc/hosts ...')
        EX.Remote('[ -f /etc/hosts.bak ] && cp /etc/hosts.bak /etc/hosts || cp /etc/hosts /etc/hosts.bak', [service_node],
                  connection_params = {'user': 'root'}).run()
        EX.Put([service_node], [self.outdir+'/vms.list'], remote_location= '/root/', connection_params = { 'user': 'root' }).run()
        EX.Remote('cat /root/vms.list >> /etc/hosts', [service_node],
                     connection_params = {'user': 'root'}).run()
        
        logger.info('Restarting service ...')
        EX.Remote('service dnsmasq stop ; rm /var/lib/misc/dnsmasq.leases ; service dnsmasq start', [service_node],
                     connection_params = {'user': 'root'}).run()
        
        logger.info('Configuring resolv.conf on all hosts')
        clients = list(self.hosts)
        clients.remove(service_node)
       
        self.fact.get_fileput(clients, [self.outdir+'/resolv.conf'], remote_location = '/etc/',
                     connection_params = {'user': 'root'}).run()
                     
        if apt_cacher:
            self.configure_apt_cacher(service_node)
        
                     
        self.service_node = service_node


    def setup_apt_cacher(self, host):
        """ Install and configure apt-cacher on one host"""
        logger.info('Installing apt-cacher on '+style.host(host))
        base_dir  = '/tmp/apt-cacher-ng'
        log_dir   = base_dir+'/log'
        cache_dir = base_dir+'/cache'
                
        EX.Remote('export DEBIAN_MASTER=noninteractive ; apt-get update ; '+\
                  'apt-get install -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confnew" -y apt-cacher-ng', 
                  [host], connection_params = {'user': 'root'}).run()
        EX.Remote('mkdir -p '+log_dir+'; mkdir -p '+cache_dir+'; chown -R apt-cacher-ng:apt-cacher-ng '+base_dir).run()
        EX.Remote('sed -i "s/\/var\/cache\/apt-cacher-ng/'+cache_dir+'/g" /etc/apt-cacher-ng/acng.conf ;'+\
                  'sed -i "s/\/var\/log\/apt-cacher-ng/'+log_dir+'/g" /etc/apt-cacher-ng/acng.conf ;'+\
                  'sed -i "s/3142/9999/g" /etc/apt-cacher-ng/acng.conf ; service apt-cacher-ng restart').run()
         
        logger.info('apt-cacher-ng up and running on '+style.host(host))
        

    def setup_munin(self):
        """ Installing the monitoring service munin """
        logger.info('Configuring munin server')
        EX.SshProcess('export DEBIAN_MASTER=noninteractive ; apt-get update && apt-get install -y -t unstable  --force-yes munin', 
               self.service_node ).run()
        f = open('munin-nodes', 'w')
        i_vm =0
        for host in self.hosts:
            get_ip = EX.Process('host '+host.address).run()
            ip =  get_ip.stdout.strip().split(' ')[3]
            f.write('['+host.address+']\n    address '+ip+'\n   use_node_name yes\n\n')
        f.close()
        EX.Put([self.service_node], ['munin-nodes'] , remote_location='/etc/munin').run()
        SshProcess('[ -f /etc/munin/munin.conf.bak ] && cp /etc/munin/munin.conf.bak /etc/munin/munin.conf'+\
           ' || cp /etc/munin/munin.conf /etc/munin/munin.conf.bak ;'+\
           ' cat /etc/munin/munin-nodes >> /etc/munin/munin.conf ; service munin restart', self.service_node).run()
        
        
        logger.info('Install munin-node on all hosts (VM + PM):\n'+','.join([host.address for host in self.hosts ]))
        self.fact.get_remote('export DEBIAN_MASTER=noninteractive ; apt-get update && apt-get install -y --force-yes munin-node', 
               self.hosts ).run()
        logger.info('Configuring munin-nodes')
        get_service_node_ip = EX.Process('host '+self.service_node.address).run()
        service_node_ip = get_service_node_ip.stdout.strip().split(' ')[3]
        logger.info('Authorizing connection from '+service_node_ip)
        self.fact.get_remote('[ -f /etc/munin/munin-node.conf.bak ] && cp /etc/munin/munin-node.conf.bak /etc/munin/munin-node.conf'+\
                   ' || cp /etc/munin/munin-node.conf /etc/munin/munin-node.conf.bak ;'+\
                   ' echo allow ^'+'\.'.join( [ i for i in service_node_ip.split('.') ])+'$ >> /etc/munin/munin-node.conf', self.hosts).run()
        logger.info('Configuring munin plugins')
        plugins = [ 'cpu', 'memory', 'iostat']
        cmd = 'rm /etc/munin/plugins/* ; '+' ; '.join( ['ln -s /usr/share/munin/plugins/'+plugin+' /etc/munin/plugins/' 
                                                      for plugin in plugins])+\
                '; ln -s /usr/share/munin/plugins/if_ /etc/munin/plugins/if_eth0; killall munin-node ; munin-node ;'
        self.fact.get_remote(cmd, self.hosts).run()


    def create_disk_image(self, disk_image = '/grid5000/images/KVM/squeeze-x64-base.qcow2', clean = True):
        """Create a base image in RAW format for using qemu-img than can be used as the vms backing file """
        
        if clean:
            logger.info('Removing existing disks')
            self.fact.get_remote('rm -f /tmp/*.img; rm -f /tmp/*.qcow2', self.hosts, 
                            connection_params = {'user': 'root'}).run()
        
        ls_image = EX.SshProcess('ls '+disk_image, self.hosts[0], connection_params = {'user': 'root'})
        ls_image.ignore_exit_code = True
        ls_image.log_exit_code = False
        ls_image.run()
                                 
        if ls_image.stdout.strip() == disk_image:
            logger.info("Image found in deployed hosts")
            copy_file = self.fact.get_remote('cp '+disk_image+' /tmp/', self.hosts,
                                    connection_params = {'user': 'root'}).run()
        else:
            logger.info("Copying backing file from frontends")
            copy_file = EX.ChainPut(self.hosts, [disk_image], remote_location='/tmp/',
                                    connection_params = {'user': 'root'}).run()

            if not copy_file.ok:
                logger.error('Unable to copy the backing file')
                raise ActionsFailed, [copy_file]
        
        logger.info("Creating disk image on /tmp/vm-base.img")
        cmd = 'qemu-img convert -O raw /tmp/'+disk_image.split('/')[-1]+' /tmp/vm-base.img'
        self.fact.get_remote(cmd, self.hosts, connection_params = {'user': 'root'}).run()  
        
    def ssh_keys_on_vmbase(self, ssh_key = None):
        """ Copy your public key into the .ssh/authorized_keys and copy the keys in .ssh
        in the backing file  for taktuk execution"""
        logger.info('Copying ssh key on vm-base ...')

        ssh_key = '~/.ssh/id_rsa' if ssh_key is None else ssh_key

        cmd = 'modprobe nbd max_part=1; '+ \
                'qemu-nbd --connect=/dev/nbd0 /tmp/vm-base.img ; sleep 3 ; '+ \
                'mount /dev/nbd0p1 /mnt; mkdir /mnt/root/.ssh ; '+ \
                'cp /root/.ssh/authorized_keys  /mnt/root/.ssh/authorized_keys ; '+ \
                'cp -r '+ssh_key+'* /mnt/root/.ssh/ ;'+ \
                'umount /mnt; qemu-nbd -d /dev/nbd0 '
        logger.debug(cmd)
        copy_on_vm_base = self.fact.get_remote(cmd, self.hosts, connection_params = {'user': 'root'}).run()
        logger.debug('%s', copy_on_vm_base.ok)
    
    def write_placement_file(self, vms = None):
        """ Generate an XML file with the VM deployment topology """
       
       
        if vms is None:
            vms = self.vms
        deployment = ETree.Element('vm5k')
        
        for vm in vms:
            host_info = vm['host'].address
            host_uid =   host_info.split('-')[0]+'-'+host_info.split('-')[1]
            cluster_uid = host_info.split('-')[0]
            site_uid = host_info.split('.')[1]
            if deployment.find("./site[@id='"+site_uid+"']") is None:
                site = ETree.SubElement(deployment, 'site', attrib = {'id': site_uid})
            else:
                site = deployment.find("./site[@id='"+site_uid+"']")
            if site.find("./cluster/[@id='"+cluster_uid+"']") is None:
                cluster = ETree.SubElement(site, 'cluster', attrib = {'id': cluster_uid})
            else:
                cluster = site.find("./cluster/[@id='"+cluster_uid+"']")
            if cluster.find("./host/[@id='"+host_uid+"']") is None:
                host = ETree.SubElement(cluster, 'host', attrib = {'id': host_uid})
            else:
                host = cluster.find("./host/[@id='"+host_uid+"']")
            el_vm = ETree.SubElement(host, 'vm', attrib = {'id': vm['vm_id'], 'ip': vm['ip'], 'mac': vm['mac'], 
                        'mem': str(vm['mem_size']), 'cpu': str(vm['vcpus']), 'hdd': str(vm['hdd_size'])})
        
        f = open(self.outdir+'/placement.xml', 'w')
        f.write(prettify(deployment))
        f.close()

        
    def distribute_vms(self, vms, mode = 'distributed', placement = None):    
        """ Add a host information for every VM """
        
        if placement is None:
            dist_hosts = self.hosts[:]
            iter_hosts = cycle(dist_hosts)
            host = iter_hosts.next()
            hosts_vm = {}
            max_mem = {}
            max_cpu = {}
            total_mem = {}
            total_cpu = {}
            if mode == 'distributed':
                for h in self.hosts:        
                    max_mem[h.address] = self.hosts_attr[h.address.split('-')[0]]['ram_size']/1048576 
                    max_cpu[h.address] = self.hosts_attr[h.address.split('-')[0]]['n_cpu']*2
                    total_mem[h.address] =  0 
                    total_cpu[h.address] =  0
                        
                for vm in vms:
                    if total_mem[host.address] + vm['mem_size'] > max_mem[host.address] \
                        or total_cpu[host.address] + vm['vcpus'] > max_cpu[host.address]:
                        dist_hosts.remove(host)
                        iter_hosts = cycle(dist_hosts)
                    vm['host'] = host
                    total_mem[host.address] += vm['mem_size']
                    total_cpu[host.address] += vm['vcpus']
                    if not hosts_vm.has_key(host.address):
                        hosts_vm[host.address] = []
                    hosts_vm[host.address].append(vm['vm_id'])
                    host = iter_hosts.next()
                    
            elif mode == 'concentrated':
                api_host = kavname_to_shortname(host)
                max_mem = self.hosts_attr[host.address.split('-')[0]]['ram_size']/1048576
                total_mem = 0
                for vm in vms:
                    total_mem += vm['mem_size']
                    if total_mem > max_mem:
                        host = iter_hosts.next()
                        max_mem = get_host_attributes(api_host)['main_memory']['ram_size']/10**6
                        total_mem = vm['mem_size']
                    if not hosts_vm.has_key(host.address):
                        hosts_vm[host.address] = []
                    vm['host'] = host
                    hosts_vm[host.address].append(vm['vm_id'])
            elif mode == 'n_by_hosts':
                i_vm = 0
                for host in self.hosts:
                    for i in range(len(vms)/len(self.hosts)):
                        vms[i_vm]['host'] = host
                        i_vm += 1
                
        else: 
            clusters = []
            sites = []
            log = ''
            logger.info('Distributing the virtual machines according to the topology file')
            for site in placement.findall('./site'):
                sites.append(site.get('id'))
                log += '\n'+site.get('id')+': '
                for cluster in site.findall('./cluster'):
                    clusters.append(cluster.get('id'))
                    log += cluster.get('id')+' ('+str(len(cluster.findall('.//host')))+' hosts - '+str(len(cluster.findall('.//vm')))+' vms) '
            logger.info(log)
            
            
            vms = []
            i_vm = 0
            for site in placement.findall('./site'):
                for host in site.findall('.//host'):
                    for vm in host.findall('./vm'):
                        vms.append({'vm_id': vm.get('vm_id') if vm.get('vm_id') is not None else 'vm-'+str(i_vm),
                                    'host': Host(host.get('id')+'-kavlan-'+str(self.config['kavlan_id'])+'.'+site.get('id')+'.grid5000.fr'), 
                                    'hdd_size': vm.get('hdd') if vm.get('hdd') is not None else 2,
                                    'mem_size': vm.get('mem') if vm.get('mem') is not None else 256, 
                                    'vcpus': vm.get('cpu') if vm.get('cpu') is not None else 1,
                                    'cpuset': vm.get('cpusets') if vm.get('cpusets') is not None else 'auto',
                                    'ip': self.ip_mac[i_vm][0], 
                                    'mac': self.ip_mac[i_vm][1] })
                        i_vm += 1
        logger.info( '\n%s', '\n'.join( [style.host(host)+': '+\
                                          ', '.join( [style.emph(vm)  for vm in host_vms]) for host, host_vms in hosts_vm.iteritems() ] ))
        
        return vms

    def get_max_vms(self, vm_template):
        """ A basic function that determine the maximum number of VM you can have depending on the vm template"""
        vm_ram_size = int(ETree.fromstring(vm_template).get('mem'))
        vm_vcpu = int(ETree.fromstring(vm_template).get('cpu'))
        self.get_hosts_attr()
        max_vms_ram = int(self.hosts_attr['total']['ram_size']/vm_ram_size)
        max_vms_cpu = int(2*self.hosts_attr['total']['n_cpu']/vm_vcpu) 
        max_vms = min(max_vms_ram, max_vms_cpu)
        
        logger.info('Maximum number of VM is %s', str(max_vms))
        return max_vms 
        

    def get_fastest_host(self):
        """ Use the G5K api to have the fastest node"""
        max_flops = 0
        for host in self.hosts:
            attr = self.hosts_attr[host.address.split('-')[0]]
            if attr['node_flops'] > max_flops:
                max_flops = attr['node_flops']
                fastest_host = host
        return fastest_host

    def get_hosts_attr(self):
        """ Get the node_flops, ram_size and smt_size from g5k API"""
        self.hosts_attr = {}
        self.hosts_attr['total'] = {'ram_size': 0, 'n_cpu': 0}
        for host in self.hosts:
            api_host = kavname_to_shortname(host).address if 'kavlan' in host.address else host.address
            if not self.hosts_attr.has_key(api_host.split('-')[0]):
                attr = get_host_attributes(api_host)
                self.hosts_attr[host.address.split('-')[0]] = {'node_flops': attr['performance']['node_flops'] if attr.has_key('performance') else 0, 
                                           'ram_size': attr['main_memory']['ram_size'],
                                           'n_cpu': attr['architecture']['smt_size'] }
            self.hosts_attr['total']['ram_size'] += self.hosts_attr[host.address.split('-')[0]]['ram_size']
            self.hosts_attr['total']['n_cpu'] += self.hosts_attr[host.address.split('-')[0]]['n_cpu']
                    
    

def kavname_to_shortname( host):
    """ """
    if 'kavlan' in host.address:
        return Host(host.address.split('kavlan')[0][0:-1])
    else:
        return host       
        
def prettify(elem):
    """Return a pretty-printed XML string for the Element.  """
    rough_string = ETree.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")

    
def get_clusters(sites = None, n_nodes = 1, node_flops = 10**1, virt = False, kavlan = False):
    """Function that returns the list of cluster with some filters"""
    if sites is None:
        sites = get_g5k_sites()
    
    big_clusters = []
    virt_clusters = []
    kavlan_clusters = []
    for site in sites:
        for cluster in get_site_clusters(site):
            if n_nodes > 1 and get_resource_attributes('sites/'+site+'/clusters/'+cluster+'/nodes')['total'] >= n_nodes:
                big_clusters.append(cluster)
            if virt and get_host_attributes(cluster+'-1.'+site+'.grid5000.fr')['supported_job_types']['virtual'] in [ 'ivt', 'amd-v']:
                virt_clusters.append(cluster)
            if kavlan and get_cluster_attributes(cluster)['kavlan']:
                kavlan_clusters.append(cluster)
                
    logger.debug('Clusters with more than '+str(n_nodes)+' nodes \n%s',
                 ', '.join([cluster for cluster in big_clusters]))
    logger.debug('Clusters with virtualization capacities \n%s', 
                 ', '.join([cluster for cluster in virt_clusters]))
    logger.debug('Clusters with a kavlan activated \n%s',
                 ', '.join([cluster for cluster in kavlan_clusters] ))
    
    

    if virt and kavlan:
        return list(set(virt_clusters) & set(big_clusters)  & set(kavlan_clusters))
    elif virt:
        return list(set(virt_clusters) & set(big_clusters)  )
    elif kavlan:
        return list(set(kavlan_clusters) & set(big_clusters)  )
    else:
        return list(set(big_clusters) )
    

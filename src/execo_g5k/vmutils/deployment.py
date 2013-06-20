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


from pprint import pprint, pformat
import time, xml.etree.ElementTree as ETree, re, os
from netaddr import IPNetwork
from itertools import cycle
import random
import execo as EX, execo_g5k as EX5
from execo import logger, Host, SshProcess
from execo.action import ActionFactory
from execo.log import set_style
from execo.config import TAKTUK
from execo_g5k import get_oar_job_subnets
from execo_g5k.config import g5k_configuration, default_frontend_connexion_params
from execo_g5k.utils import get_kavlan_host_name
from execo_g5k.api_utils import get_host_cluster, get_cluster_site, get_g5k_sites, get_site_clusters, get_cluster_attributes, get_host_attributes, get_resource_attributes, get_host_site
from xml.dom import minidom
   

class Virsh_Deployment(object):
    """ A object that allow you to deploy some wheezy-based hosts with an up-to-date version of libvirt with
    a pool of ip-MAC addresses for your virtual machines """
    def __init__(self, hosts = None, env_name = None, env_file = None, kavlan = None, oarjob_id = None, outdir = None ):
        """ Initialization of the object"""
        logger.info('Initializing Virsh_Deployment on %s hosts', len(hosts))
        self.max_vms = 10227        
        self.fact = ActionFactory(remote_tool = TAKTUK,
                                fileput_tool = TAKTUK,
                                fileget_tool = TAKTUK)
        self.hosts      = hosts
        self.clusters   = list(set([ get_host_cluster(host) for host in self.hosts ]))
        self.sites      = list(set([ get_cluster_site(cluster) for cluster in self.clusters ]))
        self.kavlan     = kavlan
        self.oarjob_id  = oarjob_id
        self.env_name   = env_name if env_name is not None else 'wheezy-x64-base'
        self.env_file   = env_file
        self.outdir     = '/tmp/deploy_'+ time.strftime("%Y%m%d_%H%M%S_%z") if outdir is None else outdir
        try:
            os.mkdir(self.outdir)
        except:
            pass 
        self.get_hosts_attr()
        
        logger.debug('hosts %s',    pformat (self.hosts))
        logger.debug('clusters %s', pformat (self.clusters))
        logger.debug('sites %s',    pformat (self.sites))
        logger.debug('sites %s',    pformat (self.hosts_attr))
        
    def deploy_hosts(self, out = False, max_tries = 2):
        """ Deploy the environment specified by env_name or env_file """
        if self.env_file is not None:
            logger.info('Deploying environment %s ...', set_style( self.env_file, 'emph') )
            deployment = EX5.Deployment( hosts = self.hosts, env_file = self.env_file,
                                        vlan = self.kavlan)
        elif self.env_name is not None:
            logger.info('Deploying environment %s ...',set_style( self.env_name, 'emph') )
            deployment = EX5.Deployment( hosts = self.hosts, env_name = self.env_name,
                                        vlan = self.kavlan)
        
        
          
        Hosts = EX5.deploy(deployment, out = out, num_tries = max_tries)

        deployed_hosts = list(Hosts[0])
        undeployed_hosts = list(Hosts[1])
        
        if len(undeployed_hosts) > 0:
            logger.warning('Hosts %s haven\'t been deployed', ', '.join( [node.address for node in undeployed_hosts] ))
        
        if self.kavlan is not None:
            self.hosts = [ Host(get_kavlan_host_name(host, self.kavlan)) for host in deployed_hosts ]
        else: 
            self.hosts = deployed_hosts
        logger.info('%s deployed', ' '.join([host.address for host in self.hosts]))
   
   
    def enable_taktuk(self, ssh_key = None):
        """Copying your ssh_keys on hosts for automatic connexion"""
        logger.info('Copying ssh key to prepare hosts for taktuk execution and file transfer ...')
        #taktuk_gateway = Host(self.get_fastest_host(), user = 'root')
        taktuk_gateway = Host(self.get_fastest_host())
        
        ssh_key = '~/.ssh/id_rsa' if ssh_key is None else ssh_key
        
        EX.Remote('export DEBIAN_MASTER=noninteractive ; apt-get install -y --force-yes taktuk', [taktuk_gateway], connexion_params = {'user': 'root'}).run()
        EX.Put([taktuk_gateway], [ssh_key, ssh_key+'.pub'], remote_location='.ssh/', connexion_params = {'user': 'root'} ).run()
        
        configure_taktuk = self.fact.remote('cat '+ssh_key+'.pub >> .ssh/authorized_keys; echo "Host *" >> /root/.ssh/config ; echo " StrictHostKeyChecking no" >> /root/.ssh/config; ',
                        self.hosts, connexion_params = { 
                'user': 'root',
                'host_rewrite_func': lambda host: re.sub("\.g5k$", ".grid5000.fr", host),        
                'taktuk_gateway': taktuk_gateway,
                'taktuk_options': ('-s', '-S', '/root/.ssh/id_rsa:/root/.ssh/id_rsa,/root/.ssh/id_rsa.pub:/root/.ssh/id_rsa.pub'),
                'taktuk_gateway_connexion_params': {'user': 'root'} }).run()
        
        self.taktuk_params = {     
                'user': 'root',
                'host_rewrite_func': lambda host: re.sub("\.g5k$", ".grid5000.fr", host),        
                'taktuk_gateway': taktuk_gateway,
                'taktuk_gateway_connexion_params': {'user': 'root'} }     
        if configure_taktuk.ok():
            logger.debug('Taktuk configuration finished %s', pformat(self.taktuk_params))
        else:
            logger.error('Unable to configure TakTuk ..')
            exit()
        
    def configure_apt(self):
        """ Add testing and unstable to /etc/apt/sources.list and set the priority wheezy > testing > unstable """
        logger.info('Configuring APT')
        f = open(self.outdir+'/sources.list', 'w')
        f.write('deb http://ftp.debian.org/debian stable main contrib non-free\n'+\
                'deb http://ftp.debian.org/debian testing main \n'+\
                'deb http://ftp.debian.org/debian unstable main \n')
        f.close()
        f = open(self.outdir+'/preferences', 'w')
        f.write('Package: * \nPin: release a=stable \nPin-Priority: 900\n\n'+\
                'Package: * \nPin: release a=testing \nPin-Priority: 850\n\n'+\
                'Package: * \nPin: release a=unstable \nPin-Priority: 800\n\n')
        f.close()
        
        EX.Put([self.taktuk_params['taktuk_gateway']], [self.outdir+'/sources.list', self.outdir+'/preferences' ], 
               connexion_params = { 'user': 'root' }).run()
        apt_conf = self.fact.fileput(self.hosts, [ 'sources.list', 'preferences' ], 
                remote_location = '/etc/apt/', connexion_params = self.taktuk_params).run()
        if apt_conf.ok():
            logger.debug('apt configured successfully')
        else:
            logger.error('Error in configuring apt')
            exit()
        

    def upgrade_hosts(self):
        """ Perform apt-get update && apt-get dist-upgrade in noninteractive mode """
        logger.info('Upgrading hosts')
        cmd = " echo 'debconf debconf/frontend select noninteractive' | debconf-set-selections; \
                echo 'debconf debconf/priority select critical' | debconf-set-selections ;      \
                apt-get update ; export DEBIAN_MASTER=noninteractive ; apt-get upgrade -y --force-yes;"
        upgrade = self.fact.remote( cmd, self.hosts, connexion_params = self.taktuk_params).run()
        if upgrade.ok():
            logger.debug('Upgrade finished')
        else:
            logger.error('Unable to perform dist-upgrade on the nodes ..')
            exit()


    def install_packages(self, packages_list = None):
        """ Installation of packages on the nodes """
    
        base_packages = 'uuid-runtime bash-completion nmap qemu-kvm taktuk'
        logger.info('Installing usefull packages %s', set_style(base_packages, 'emph'))
        cmd = 'export DEBIAN_MASTER=noninteractive ; apt-get update && apt-get install -y --force-yes '+ base_packages
        install_base = self.fact.remote(cmd, self.hosts, connexion_params = self.taktuk_params).run()        
        if install_base.ok():
            logger.debug('Packages installed')
        else:
            logger.error('Unable to install packages on the nodes ..')
            exit()
        
        libvirt_packages = 'libvirt-bin virtinst python2.7 python-pycurl python-libxml2'
        logger.info('Installing libvirt updated packages %s', set_style(libvirt_packages, 'emph'))
        cmd = 'export DEBIAN_MASTER=noninteractive ; apt-get update && apt-get install -y --force-yes '+\
            '-o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold"  -t unstable '+\
            libvirt_packages
        install_libvirt = self.fact.remote(cmd, self.hosts, connexion_params = self.taktuk_params).run()
            
        if install_libvirt.ok():
            logger.debug('Packages installed')
        else:
            logger.error('Unable to install packages on the nodes ..')
            exit()

    def configure_libvirt(self, network_xml = None, bridge = 'br0'):
        """Configure libvirt: make host unique, configure and restart the network """
        
        logger.info('Making libvirt host unique ...')
        cmd = 'uuid=`uuidgen` && sed -i "s/00000000-0000-0000-0000-000000000000/${uuid}/g" /etc/libvirt/libvirtd.conf '\
                +'&& sed -i "s/#host_uuid/host_uuid/g" /etc/libvirt/libvirtd.conf && service libvirt-bin restart'
        self.fact.remote(cmd, self.hosts, connexion_params = self.taktuk_params).run()
        
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
        
        self.fact.remote('virsh net-destroy default; virsh net-undefine default', self.hosts,
                    connexion_params = self.taktuk_params, log_exit_code = False).run()
        
        EX.Put([self.taktuk_params['taktuk_gateway']], 'default.xml' ,remote_location = '/etc/libvirt/qemu/networks/', 
               connexion_params = { 'user': 'root' } ).run()
        
        self.fact.fileput(self.hosts, '/etc/libvirt/qemu/networks/default.xml', remote_location = '/etc/libvirt/qemu/networks/',
                      connexion_params = self.taktuk_params).run()
              
        self.fact.remote('virsh net-define /etc/libvirt/qemu/networks/default.xml ; virsh net-start default; virsh net-autostart default; ', 
                        self.hosts, connexion_params = self.taktuk_params).run()
        
        self.setup_virsh_network()
        
        logger.info('Restarting libvirt ...')        
        self.fact.remote('service libvirt-bin restart', self.hosts, connexion_params = self.taktuk_params).run()
        
        


    def create_bridge(self, bridge_name = 'br0'):
        """ Creation of a bridge to be used for the virtual network """
        logger.info('Configuring the bridge')
        bridge_exists = self.fact.remote('brctl show |grep '+bridge_name, self.hosts,
                         connexion_params = self.taktuk_params, log_exit_code = False).run()
        nobr_hosts = []
        for p in bridge_exists.processes():
            if len(p.stdout()) == 0:
                nobr_hosts.append(p.host())
        
        cmd = 'export br_if=`ip route |grep default |cut -f 5 -d " "`;  echo " " >> /etc/network/interfaces ; echo "auto '+bridge_name+'" >> /etc/network/interfaces ; '+\
            'echo "iface '+bridge_name+' inet dhcp" >> /etc/network/interfaces ; echo "bridge_ports $br_if" >> /etc/network/interfaces ;'+\
            ' echo "bridge_stp off" >> /etc/network/interfaces ; echo "bridge_maxwait 0" >> /etc/network/interfaces ;'+\
            ' echo "bridge_fd 0" >> /etc/network/interfaces ; ifup '+bridge_name
        
        create_br = self.fact.remote(cmd, nobr_hosts, connexion_params = self.taktuk_params).run()
        
        if create_br.ok():
            logger.debug('Bridge has been created')
        else:
            logger.error('Impossible to setup the bridge')
            exit()

    def setup_virsh_network(self, n_vms = None):
        """ Use the KaVLAN IP range or the IP-MAC list from g5k_subnets and configure 
         dnsmasq to setup a DNS/DHCP server for the VM i"""
        logger.info('Retrieving the list of IP and MAC for the virtual machines')
        if self.kavlan is not None:
            logger.info('Using a KaVLAN global')
            vm_ip = []
            all_ip = IPNetwork('10.'+str(3+(self.kavlan-10)*4)+'.216.0/18')
            subnets = list(all_ip.subnet(21))
            for subnet in subnets:
                if subnet.ip.words[2] >= 216:
                    for ip in subnet.iter_hosts():
                        vm_ip.append(ip)
            network = str(min(vm_ip))+','+str(max(vm_ip[0:-1]))+','+str(all_ip.netmask)
            dhcp_range = 'dhcp-range='+network+',12h\n'
            dhcp_router = 'dhcp-option=option:router,'+str(max(vm_ip))+'\n'
            dhcp_hosts =''
            self.ip_mac = []
            for ip in vm_ip[0:n_vms]:
                mac = [ 0x00, 0x020, 0x4e,
                    random.randint(0x00, 0x7f),
                    random.randint(0x00, 0xff),
                    random.randint(0x00, 0xff) ]
                self.ip_mac.append( ( str(ip), ':'.join( map(lambda x: "%02x" % x, mac) ) ) )
                dhcp_hosts += 'dhcp-host='+':'.join( map(lambda x: "%02x" % x, mac))+','+str(ip)+'\n'
            self.configure_service_node(dhcp_range, dhcp_router, dhcp_hosts)
        elif self.oarjob_id is not None:
            logger.info('Using a g5k-subnet')
            self.ip_mac = get_oar_job_subnets( self.oarjob_id, self.sites[0] )[0]
        else:
            logger.error('No KaVLAN global and no g5k-subnet, the network cannot be configured')
            exit()
        logger.debug(pformat(self.ip_mac))


    def configure_service_node(self, dhcp_range, dhcp_router, dhcp_hosts):
        """ Generate the hosts lists, the vms list, the dnsmasq configuration and setup a DNS/DHCP server """      
        service_node = self.get_fastest_host()   
        
        f = open(self.outdir+'/hosts.list', 'w')
        for host in self.hosts:
            f.write(host.address+'\n')
        f.close()
        f = open(self.outdir+'/vms.list', 'w')
        f.write('\n')
        for idx, val in enumerate(self.ip_mac):
            f.write(val[0]+'     vm-'+str(idx)+'\n')
        f.close()
        get_ip = SshProcess('host '+service_node.address+' |cut -d \' \' -f 4', g5k_configuration['default_frontend'], 
                connexion_params = default_frontend_connexion_params).run()
        ip = get_ip.stdout().strip()
        f = open(self.outdir+'/resolv.conf', 'w')
        f.write('domain grid5000.fr\nsearch grid5000.fr '+' '.join( [site+'.grid5000.fr' for site in self.sites] )+' \nnameserver '+ip+ '\n')
        f.close()
        f = open(self.outdir+'/dnsmasq.conf', 'w')
        f.write(dhcp_range+dhcp_router+dhcp_hosts)
        f.close()
        
        
        logger.info('Configuring %s as a %s server', set_style(service_node.address.split('.')[0], 'host')
                    , set_style('DNS/DCHP', 'emph'))
        
        EX.Remote('export DEBIAN_MASTER=noninteractive ; apt-get install -t testing -y dnsmasq taktuk', [service_node]).run()
        EX.Put([service_node], self.outdir+'/dnsmasq.conf', remote_location='/etc/', connexion_params = { 'user': 'root' }).run()
        
        logger.info('Adding the VM in /etc/hosts ...')
        EX.Remote('[ -f /etc/hosts.bak ] && cp /etc/hosts.bak /etc/hosts || cp /etc/hosts /etc/hosts.bak', [service_node]).run()
        EX.Put([service_node], self.outdir+'/vms.list', remote_location= '/root/', connexion_params = { 'user': 'root' }).run()
        EX.Remote('cat /root/vms.list >> /etc/hosts', [service_node]).run()
        
        logger.info('Restarting service ...')
        EX.Remote('service dnsmasq restart', [service_node]).run()
        
        logger.info('Configuring resolv.conf on all hosts')
        clients = list(self.hosts)
        clients.remove(service_node)
        
        EX.Put([service_node], self.outdir+'/resolv.conf', remote_location= '/root/', connexion_params = { 'user': 'root' }).run()
        EX.TaktukPut(clients, '/root/resolv.conf', remote_location = '/etc/',
                     connexion_params = self.taktuk_params).run()

    def create_disk_image(self, disk_image = '/grid5000/images/KVM/squeeze-x64-base.qcow2', clean = True):
        """Create a base image in RAW format for using qemu-img than can be used as the vms backing file """
       
        ls_image = EX.SshProcess('ls '+disk_image, self.taktuk_params['taktuk_gateway']).run()
        if ls_image.stdout().strip() == disk_image:
            logger.info("Image found in deployed hosts")
            copy_file = EX.TaktukRemote('cp '+disk_image+' /tmp/', self.hosts,
                                    connexion_params = self.taktuk_params).run()
        else:
            logger.info("Copying backing file")
            frontends = [get_host_site(host)+'.grid5000.fr' for host in self.hosts]
            copy_file = EX.TaktukRemote('scp '+default_frontend_connexion_params['user']+\
                                    '@{{frontends}}:'+disk_image+' /tmp/', self.hosts,
                                    connexion_params = self.taktuk_params).run()
            if not copy_file.ok():
                logger.error('Unable to copy the backing file')
                exit()               
                    
        if clean:
            logger.info('Removing existing disks')
            self.fact.remote('rm -f /tmp/*.img; rm -f /tmp/*.qcow2', self.hosts, 
                            connexion_params = self.taktuk_params).run()
        
        logger.info("Creating disk image on /tmp/vm-base.img")
        cmd = 'qemu-img convert -O raw /tmp/'+disk_image.split('/')[-1]+' /tmp/vm-base.img'
        self.fact.remote(cmd, self.hosts, connexion_params = self.taktuk_params).run()  
        
    def ssh_keys_on_vmbase(self, ssh_key = None):
        """ Copy your public key into the .ssh/authorized_keys """
        logger.info('Copying ssh key on vm-base ...')
        ssh_key = '~/.ssh/id_rsa' if ssh_key is None else ssh_key

        cmd = 'modprobe nbd max_part=1; '+ \
                'qemu-nbd --connect=/dev/nbd0 /tmp/vm-base.img; sleep 3; '+ \
                'mount /dev/nbd0p1 /mnt; mkdir /mnt/root/.ssh; '+ \
                'cat '+ssh_key+'.pub >> /mnt/root/.ssh/authorized_keys; '+ \
                'umount /mnt; qemu-nbd -d /dev/nbd0'
        logger.debug(cmd)
        copy_on_vm_base = self.fact.remote(cmd, self.hosts, connexion_params = self.taktuk_params).run()
        logger.debug('%s', copy_on_vm_base.ok())
    
    def write_placement_file(self):
        """ Generate an XML file with the VM deployment topology """
        deployment = ETree.Element('virsh_deployment')  
        for vm in self.vms:
            host_info = vm['host'].address.split('.')[0:-2]
            host_uid =   host_info[0].split('-')[0]+'-'+host_info[0].split('-')[1]
            cluster_uid = host_info[0].split('-')[0]
            site_uid = host_info[1]
        #    print host_uid, cluster_uid, site_uid
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
            total_mem = {}
            
            if mode is 'distributed':
                for h in self.hosts:        
                    max_mem[h.address] = self.hosts_attr[h.address.split('-')[0]]['ram_size']/10**6 
                    total_mem[h.address] =  0 
                        
                for vm in vms:
                    if total_mem[host.address] + vm['mem_size'] > max_mem[host.address]:
                        dist_hosts.remove(host)
                        iter_hosts = cycle(dist_hosts)
                
                    vm['host'] = host
                    total_mem[host.address] += vm['mem_size']
                    if not hosts_vm.has_key(host.address):
                        hosts_vm[host.address] = []
                    hosts_vm[host.address].append(vm['vm_id'])
                    host = iter_hosts.next()
                    
            elif mode is 'concentrated':
                api_host = kavname_to_shortname(host)
                max_mem = get_host_attributes(api_host)['main_memory']['ram_size']/10**6
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
        logger.info( '\n%s', '\n'.join( [set_style(host, 'host')+': '+\
                                          ', '.join( [set_style(vm,'emph')  for vm in host_vms]) for host, host_vms in hosts_vm.iteritems() ] ))
        
        self.vms = vms

    def get_max_vms(self, vm_template):
        """ A basic function that determine the maximum number of VM you can have depending on the vm template"""
        vm_ram_size = int(ETree.fromstring(vm_template).get('mem'))
        self.get_hosts_attr()
        max_vms = int(self.hosts_attr['total']['ram_size']/vm_ram_size) 
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
                    
   
#  
#        
#    def reboot(self):
#        """ Reboot the nodes """
#        logger.info('Rebooting nodes ...')
#        reboot=self.fact.remote('reboot', self.hosts , connexion_params = self.taktuk_params).run()
#        if reboot.ok():
#            while True:
#                T.sleep(5)
#                ping=[]
#                for host in self.hosts:
#                    logger.info('Waiting for node %s',host.address)
#                    ping.append(EX.Remote('ping -c 4 '+host.address, [self.frontend],
#                                connexion_params={'user':'lpouilloux'}, log_exit_code=False))
#                all_ping = EX.ParallelActions(ping).run()
#
#                if all_ping.ok():
#                    break;
#                logger.info('Nodes down, sleeping for 5 seconds ...')
#            logger.info('All hosts have been rebooted !')
#        else:
#            logger.error('Not able to connect to the hosts ...')
#        self.state.append('rebooted')
#

                


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
            if get_resource_attributes('grid5000/sites/'+site+'/clusters/'+cluster+'/nodes')['total'] >= n_nodes:
                big_clusters.append(cluster)
            if get_host_attributes(cluster+'-1.'+site+'.grid5000.fr')['supported_job_types']['virtual'] in [ 'ivt', 'amd-v']:
                virt_clusters.append(cluster)
            if get_cluster_attributes(cluster)['kavlan']:
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
    
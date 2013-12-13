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
"""A set of functions to manipulate virtual machines on Grid'5000"""

from pprint import pformat, pprint
from execo import SshProcess, Remote, Put, logger, get_remote, Process, ParallelActions
from execo.log import style
from execo.time_utils import sleep
from execo_g5k import default_frontend_connection_params
from execo_g5k.api_utils import get_host_site
import tempfile
from copy import deepcopy
from execo.exception import ActionsFailed

def list_vm( host, all = False ):
    """ Return the list of VMs on host """
    if all :
        list_vm = get_remote('virsh list --all', [host], connection_params = {'user': 'root'} ).run()
    else:
        list_vm = get_remote('virsh list', [host], connection_params = {'user': 'root'} ).run()
    vms_id = []
    for p in list_vm.processes:
        lines = p.stdout.split('\n')
        for line in lines:
            if 'vm' in line:
                std = line.split()
                vms_id.append(std[1])
    logger.debug('List of VM on host %s\n%s', style.host(host.address),
                 ' '.join([style.emph(vm_id) for vm_id in vms_id]))
    return [ {'vm_id': vm_id} for vm_id in vms_id ]


def define_vms( n_vm, ip_mac = None, mem_size = 256, hdd_size = 6, n_cpu = 1, cpusets = None, vms = None, offset = 0 ):
    """ Create a dict of the VM parameters """
    if vms is None:
        vms = []
    if ip_mac is None:
        ip_mac = [ '0.0.0.0' for i in range(n_vm)]
    if cpusets is None:
        cpusets = {}
        for i in range(n_vm): cpusets['vm-'+str(i+offset)] = 'auto'
    logger.debug('cpusets: %s', pformat(cpusets))

    
    for i_vm in range( len(vms), n_vm + len(vms)):
        vms.append( {'vm_id': 'vm-'+str(i_vm), 'hdd_size': hdd_size,
                'mem_size': mem_size, 'vcpus': n_cpu, 'cpuset': cpusets['vm-'+str(i_vm)],
                'ip': ip_mac[i_vm+offset][0], 'mac': ip_mac[i_vm+offset][1], 'host': None})
    logger.debug('VM parameters have been defined:\n%s',
                 ' '.join([style.emph(param['vm_id']) for param in vms]))
    return vms


def create_disks(vms, backing_file = '/tmp/vm-base.img', backing_file_fmt = 'raw'):
    """ Return an action to create the disks for the VMs on the hosts"""
    hosts_cmds = {}
    for vm in vms:
        cmd = 'qemu-img create -f qcow2 -o backing_file='+backing_file+',backing_fmt='+backing_file_fmt+' /tmp/'+\
            vm['vm_id']+'.qcow2 '+str(vm['hdd'])+'G ; '
        hosts_cmds[vm['host']] = cmd if not hosts_cmds.has_key(vm['host']) else hosts_cmds[vm['host']]+cmd
    
    logger.debug(pformat(hosts_cmds.values()))
    
    return get_remote('{{hosts_cmds.values()}}', list(hosts_cmds.keys()), connection_params = {'user': 'root'})

def create_disks_on_hosts(vms, hosts, backing_file = '/tmp/vm-base.img', backing_file_fmt = 'raw'):
    """ Return a Parallel action to create the qcow2 disks on all hosts"""
    host_actions = []
    for host in hosts:
        tmp_vms = deepcopy(vms)
        for vm in tmp_vms:
            vm['host'] = host
        host_actions.append(create_disks(tmp_vms, backing_file, backing_file_fmt))
    
    return ParallelActions(host_actions)

def install_vms(vms):
    """ Return an action to install the VM on the hosts"""
    hosts_cmds = {}
    for vm in vms:
        cmd = 'virt-install -d --import --connect qemu:///system --nographics --noautoconsole --noreboot'+ \
        ' --name=' + vm['vm_id'] + ' --network network=default,mac='+vm['mac']+' --ram='+str(vm['mem'])+ \
        ' --disk path=/tmp/'+vm['vm_id']+'.qcow2,device=disk,format=qcow2,size='+str(vm['hdd'])+',cache=none '+\
        ' --vcpus='+ str(vm['n_cpu'])+' --cpuset='+vm['cpuset']+' ; '
        hosts_cmds[vm['host']] = cmd if not hosts_cmds.has_key(vm['host']) else hosts_cmds[vm['host']]+cmd 

    logger.debug(pformat(hosts_cmds))
    return get_remote('{{hosts_cmds.values()}}', list(hosts_cmds.keys()), connection_params = {'user': 'root'})
    
    
def start_vms(vms):
    """ Return an action to start the VMs on the hosts """
    hosts_cmds = {}
    for vm in vms:
        cmd = 'virsh --connect qemu:///system start '+vm['vm_id']+' ; '
        hosts_cmds[vm['host']] = cmd if not hosts_cmds.has_key(vm['host']) else hosts_cmds[vm['host']]+cmd 

    logger.debug(pformat(hosts_cmds))
    return get_remote('{{hosts_cmds.values()}}', list(hosts_cmds.keys()), connection_params = {'user': 'root'})
    


def wait_vms_have_started(vms, host = None):
    """ Try to make a ls on all vms and return True when all process are ok", need a taktuk gateway"""
    if host is None:
        host = get_host_site(vms[0]['host'])
        user = default_frontend_connection_params['user']
    else:
        user = 'root'
        
    vms = [vm['ip'] for vm in vms ] 
    tmpdir = tempfile.mkdtemp()
    tmpfile = tempfile.mkstemp(prefix='vmips')
    f = open(tmpfile[1], 'w')
    for ip in vms:
        f.write(ip+'\n')
    f.close()
    Put([host], [tmpfile[1]], connection_params = {'user': user}).run()
    Process("rm -rf " + tmpdir).run()
    nmap_tries = 0
    started_vms = '0'
    ssh_open = False
    while (not ssh_open) and nmap_tries < 40:
        sleep(20)
        logger.debug('nmap_tries %s', nmap_tries)
        nmap_tries += 1            
        nmap = SshProcess('nmap -i '+tmpfile[1].split('/')[-1]+' -p 22', host, connection_params = {'user': user}).run()
        logger.debug('%s', nmap.cmd)
        for line in nmap.stdout.split('\n'):
            if 'Nmap done' in line:
                logger.debug(line)
                ssh_open = line.split()[2] == line.split()[5].replace('(','')
                started_vms = line.split()[5].replace('(','')
        if not ssh_open:
            logger.info(  started_vms+'/'+str(len(vms)) )
    SshProcess('rm '+tmpfile[1].split('/')[-1], host, connection_params = {'user': user}).run()    
    if ssh_open:
        logger.info('All VM have been started')
        return True
    else:
        logger.error('All VM have not been started')
        return False
    
    return ssh_open


def migrate_vm(vm, host):
    """ Migrate a VM to an host """
    if vm['host'] is None:
        raise NameError
        return None
    else:
        src = vm['host']
        
    # Check that the disk is here
    test_disk = get_remote('ls /tmp/'+vm['vm_id']+'.qcow2', [host]).run()
    if not test_disk.ok:
        vm['host'] = host
        create_disk_on_dest = create_disks([vm]).run()
        if not create_disk_on_dest:
            raise ActionsFailed, [create_disk_on_dest]
    
    cmd = 'virsh --connect qemu:///system migrate '+vm['vm_id']+' --live --copy-storage-inc '+\
            'qemu+ssh://'+host.address+"/system'  "
    return get_remote(cmd, [src], connection_params = {'user': 'root'} ) 
    
    
    


def destroy_vms( hosts):
    """Destroy all the VM on the hosts"""
    
    cmds = []
    hosts_with_vms = []
    for host in hosts:
        vms = list_vm(host, all = True)
        if len(vms) > 0:
            cmds.append( '; '.join('virsh destroy '+vm['vm_id']+'; virsh undefine '+vm['vm_id'] for vm in vms))
            hosts_with_vms.append(host)
        
    if len(cmds) > 0:
        get_remote('{{cmds}}', hosts_with_vms, connection_params = {'user': 'root'}).run()
        
    



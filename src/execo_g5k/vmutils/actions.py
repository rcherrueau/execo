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



from pprint import pformat
from execo import SshProcess, Remote, Put, logger, TaktukRemote
from execo.log import set_style


def list_vm( host, all = False ):
    """ Return the list of VMs on host """
    if all :
        list_vm = Remote('virsh list --all', [host] ).run()
    else:
        list_vm = Remote('virsh list', [host] ).run()
    vms_id = []
    for p in list_vm.processes():
        lines = p.stdout().split('\n')
        for line in lines:
            if 'vm' in line:
                std = line.split()
                vms_id.append(std[1])
    logger.debug('List of VM on host %s\n%s', set_style(host.address, 'host'),
                 ' '.join([set_style(vm_id, 'emph') for vm_id in vms_id]))
    return [ {'vm_id': vm_id} for vm_id in vms_id ]


def define_vms( n_vm, ip_mac, mem_size = 256, hdd_size = 2, n_cpu = 1, cpusets = None, vms = None, offset = 0 ):
    """ Create a dict of the VM parameters """
    if vms is None:
        vms = []
    if cpusets is None:
        cpusets = {}
        for i in range(n_vm): cpusets['vm-'+str(i)] = 'auto'
    logger.debug('cpusets: %s', pformat(cpusets))

    for i_vm in range( len(vms), n_vm + len(vms)):
        vms.append( {'vm_id': 'vm-'+str(i_vm), 'hdd_size': hdd_size,
                'mem_size': mem_size, 'vcpus': n_cpu, 'cpuset': cpusets['vm-'+str(i_vm)],
                'ip': ip_mac[i_vm+offset][0], 'mac': ip_mac[i_vm+offset][1], 'host': None})
    logger.debug('VM parameters have been defined:\n%s',
                 ' '.join([set_style(param['vm_id'], 'emph') for param in vms]))
    return vms




def create_disks(vms, taktuk_params = None, backing_file = '/tmp/vm-base.img', backing_file_fmt = 'raw'):
    """ Return an action to create the disks for the VMs on the hosts"""
    hosts = []
    cmds = []
    for vm in vms:
        cmds.append('qemu-img create -f qcow2 -o backing_file='+backing_file+',backing_fmt='+backing_file_fmt+' /tmp/'+\
            vm['vm_id']+'.qcow2 '+str(vm['hdd_size'])+'G')
#        cmds.append('qemu-img create -f qcow2 -b '+backing_file+' /tmp/'+vm['vm_id']+'.qcow2 '+str(vm['hdd_size'])+'G')
        hosts.append(vm['host'])

    if taktuk_params is not None:
        return TaktukRemote('{{cmds}}', hosts, connexion_params = taktuk_params)
    else:
        return Remote('{{cmds}}', hosts)

def install_vms(vms, taktuk_params = None):
    """ Return an action to install the VM on the hosts"""
    hosts_cmds = {}
    for vm in vms:
        cmd = 'virt-install -d --import --connect qemu:///system --nographics --noautoconsole --noreboot'+ \
        ' --name=' + vm['vm_id'] + ' --network network=default,mac='+vm['mac']+' --ram='+str(vm['mem_size'])+ \
        ' --disk path=/tmp/'+vm['vm_id']+'.qcow2,device=disk,format=qcow2,size='+str(vm['hdd_size'])+',cache=none '+\
        ' --vcpus='+ str(vm['vcpus'])+' --cpuset='+vm['cpuset']+' ; '
        hosts_cmds[vm['host']] = cmd if not hosts_cmds.has_key(vm['host']) else hosts_cmds[vm['host']]+cmd 

    if taktuk_params is not None:
        return TaktukRemote('{{hosts_cmds.values()}}', list(hosts_cmds.keys()), connexion_params = taktuk_params)
    else:
        return Remote('{{hosts_cmds.values()}}', list(hosts_cmds.keys()))
    
def start_vms(vms, taktuk_params = None):
    """ Return an action to start the VMs on the hosts """
    hosts_cmds = {}
    for vm in vms:
        cmd = 'virsh --connect qemu:///system start '+vm['vm_id']+' ; '
        hosts_cmds[vm['host']] = cmd if not hosts_cmds.has_key(vm['host']) else hosts_cmds[vm['host']]+cmd 

    if taktuk_params is not None:
        return TaktukRemote('{{hosts_cmds.values()}}', list(hosts_cmds.keys()), connexion_params = taktuk_params)
    else:
        return Remote('{{hosts_cmds.values()}}', list(hosts_cmds.keys()))

def wait_vms_have_started(vms, taktuk_params = None):
    """ Try to make a ls on all vms and return True when all process are ok", need a taktuk gateway"""
    host = vms[0]['host']
    vms = [vm['ip'] for vm in vms ] 
    f = open('/tmp/vmips', 'w')
    for ip in vms:
        f.write(ip+'\n')
    f.close()
    Put([host], '/tmp/vmips').run()
    nmap_tries = 0
    ssh_open = False
    while (not ssh_open) and nmap_tries < 50:
        logger.debug('nmap_tries %s', nmap_tries)
        nmap_tries += 1            
        nmap = SshProcess('nmap -i vmips -p 22', host)
        nmap.run()
        logger.debug('%s', nmap.cmd())
        stdout = nmap.stdout().split('\n')
        for line in stdout:
            if 'Nmap done' in line:
                logger.debug(line)
                ssh_open = line.split()[2] == line.split()[5].replace('(','')
    if ssh_open:
        logger.info('All VM have been started')
    else:
        logger.error('All VM have not been started')
    
    
#    nmap_tries = 0   
#    test_vm = TaktukRemote('ls', vms, connexion_params = taktuk_params, log_exit_code = False, log_error = False)
#    while (not ssh_open) and ls_tries < 50:
#        print ls_tries
#        test_vm.run()
#        if test_vm.finished_ok():
#            ssh_open = True
#            return ssh_open
#        else:
#            test_vm.reset()
#    
#    while (not ssh_open) and ls_tries < 50:
#        if taktuk_params is not None:
#            test_vm = TaktukRemote('ls', vms, connexion_params = taktuk_params, log_exit_code = False, log_error = False).run()
#        else:
#            test_vm = Remote('ls', vms, log_exit_code = False, log_error = False).run()
#        ls_tries += 1
#        logger.debug(str(ls_tries))
#        if test_vm.finished_ok():
#            ssh_open = True
#            return ssh_open

    return ssh_open



def destroy_vms( hosts, taktuk_params = None):
    """Destroy all the VM on the hosts"""
    
    cmds = []
    hosts_with_vms = []
    for host in hosts:
        vms = list_vm(host, all = True)
        if len(vms) > 0:
            cmds.append( '; '.join('virsh destroy '+vm['vm_id']+'; virsh undefine '+vm['vm_id'] for vm in vms))
            hosts_with_vms.append(host)
        
    if len(cmds) > 0:
        if taktuk_params is not None:
            TaktukRemote('{{cmds}}', hosts_with_vms, connexion_params = taktuk_params).run()
        else:
            Remote('{{cmds}}', hosts_with_vms, connexion_params = taktuk_params).run()
    



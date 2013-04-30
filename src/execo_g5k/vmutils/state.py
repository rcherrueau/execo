from pprint import pformat, pprint
from execo import Host, SshProcess, Remote, SequentialActions, ParallelActions, logger
from execo.log import set_style
from execo_g5k.config import default_frontend_connexion_params
from execo_g5k.api_utils import get_cluster_site



def list_vm( host ):
    """List the vm on host"""
    list_vm = Remote('virsh list --all', [host] ).run()
    vms_id = []
    for p in list_vm.processes():
        lines = p.stdout().split('\n')
        for line in lines:
            if 'vm' in line:
                std = line.split()
                vms_id.append(std[1])
    logger.debug('List of VM on host %s\n%s', set_style(host.address, 'host'),
                 ' '.join([set_style(vm_id, 'object_repr') for vm_id in vms_id]))
    return  [ {'vm_id': vm_id} for vm_id in vms_id ] 
    
def define_vms_params( n_vm, ip_mac, mem_size = 256, hdd_size = 2, n_cpu = 1, cpusets = None, vms_params = []):
    """ Create a dict of the VM parameters """
    
    if cpusets is None:
        cpusets =  {'vm-'+str(i): 'auto' for i in range(n_vm)} 
        
    for i_vm in range( len(vms_params), n_vm+len(vms_params)):
        vms_params.append( {'vm_id': 'vm-'+str(i_vm), 'hdd_size': hdd_size, 
                'mem_size': mem_size, 'vcpus': n_cpu, 'cpuset': cpusets['vm-'+str(i_vm)],
                'ip': ip_mac[i_vm][0], 'mac': ip_mac[i_vm][1]} )
    logger.debug('VM parameters have been defined:\n%s', 
                 ' '.join([set_style(param['vm_id'], 'object_repr') for param in vms_params]))
    return vms_params


def create_disks( hosts, vms_params):
    """ Create the VM disks on the hosts and the dict of vm parameters"""
    logger.debug('%s', pformat(hosts))
    disk_actions = []
    for vm_params in vms_params:
        logger.info('Creating disk for %s (%s)', set_style(vm_params['vm_id'], 'object_repr'), vm_params['ip'] )
        cmd = 'qemu-img create -f qcow2 -o backing_file=/tmp/vm-base.img,backing_fmt=raw /tmp/'+\
            vm_params['vm_id']+'.qcow2 '+str(vm_params['hdd_size'])+'G';
        disk_actions.append( Remote(cmd, hosts))
    logger.debug('%s', pformat(disk_actions))
    disks_created = ParallelActions(disk_actions).run()
    
    if disks_created.ok():
        return True
    else:
        return False
    
def install( vms_params, host, autostart = True, packages = None):
    """Perform virt-install using the dict vm_params"""
    install_actions = []
    log_vm = ' '.join([set_style(param['vm_id'], 'object_repr') for param in vms_params])
    for param in vms_params:
        cmd = 'virt-install -d --import --connect qemu:///system --nographics --noautoconsole --noreboot'+ \
        ' --name=' + param['vm_id'] + ' --network network=default,mac='+param['mac']+' --ram='+str(param['mem_size'])+ \
        ' --disk path=/tmp/'+param['vm_id']+'.qcow2,device=disk,format=qcow2,size='+str(param['hdd_size'])+',cache=none '+\
        ' --vcpus='+ str(param['vcpus'])+' --cpuset='+param['cpuset']
        logger.debug('%s', cmd)
        install_actions.append(Remote(cmd, [host]))            
    logger.debug('%s', pformat(install_actions))
    logger.info('Installing %s on host %s', log_vm, set_style(host.address, 'host'))
    action = SequentialActions(install_actions).run()
    
    if not action.ok():
        return False
    
    ## FIX VIRT-INSTALL BUG WITH QCOW2 THAT DEFINE A WRONG DRIVER FOR THE DISK
    fix_actions = []
    for param in vms_params:
        cmd = 'sed "s/raw/qcow2/g" /etc/libvirt/qemu/'+param['vm_id']+'.xml >  /etc/libvirt/qemu/'+ \
        param['vm_id']+'.xml.cor ; mv /etc/libvirt/qemu/'+param['vm_id']+'.xml.cor /etc/libvirt/qemu/'+ \
        param['vm_id']+'.xml; virsh define /etc/libvirt/qemu/'+param['vm_id']+'.xml; '
        fix_actions.append(Remote(cmd, [host]))
    logger.debug('%s', pformat(fix_actions))
    ParallelActions(fix_actions).run()
    logger.info('%s are ready to be started', log_vm )
    
    if not action.ok():
        return False
    
    if autostart:
        result = start( vms_params, host )
        if not result:
            return False
        
    if packages is not None:
        logger.info('Installing additionnal packages %s', packages )
        cmd = 'apt-get update && apt-get install -y '+packages 
        action = Remote(cmd, [ Host(vm['ip']+'.grid5000.fr') for vm in vms_params ]).run()
        if not action.ok():
            return False
        
    return True
     
        
        
def start( vms_params, host, migspeed = 100 ):
    """Start vm on hosts """
    log_vm = ' '.join([set_style(param['vm_id'], 'object_repr') for param in vms_params])
    start_tries = 0
    vm_started = False
    while (not vm_started) and start_tries < 5:
        
        logger.debug('start_tries %s', start_tries)
        start_tries += 1
        start_actions = []
        for param in vms_params:
            cmd = 'virsh --connect qemu:///system destroy '+param['vm_id']+';  virsh --connect qemu:///system start '+param['vm_id']
            logger.debug('%s', cmd)
            start_actions.append(Remote(cmd, [host]))
        logger.debug('%s', pformat(start_actions))
        logger.info('Starting %s ...', log_vm)
        ParallelActions(start_actions).run()
        
        ip_range = vms_params[0]['ip'].rsplit('.', 1)[0]+'.'+','.join([vm_param['ip'].split('.')[3] for vm_param in vms_params])
        
        nmap_tries = 0
        ssh_open = False
        while (not ssh_open) and nmap_tries < 20:
            logger.debug('nmap_tries %s', nmap_tries)
            nmap_tries += 1
            nmap = SshProcess('nmap '+ip_range+' -p 22', host)
            nmap.run()
            logger.debug('%s', nmap.cmd())
            stdout = nmap.stdout().split('\n')
            for line in stdout:
                if 'Nmap done' in line:
                    logger.debug(line)
                    ssh_open = line.split()[2] == line.split()[5].replace('(','')
            
        if ssh_open: 
            vm_started = True
        else:
            logger.error('All VM have not been started')
        logger.debug('vm_started %s', vm_started)
        
    return vm_started
  
def destroy( vms_params, host, autoundefine = True ):
    """Destroy vm on hosts """
    if len(vms_params) > 0:
        logger.info('Destroying %s VM on hosts %s', ' '.join([set_style(param['vm_id'], 'object_repr') for param in vms_params]), 
                    set_style(host.address, 'host') )
        destroy_actions = []
        for param in vms_params:
            cmd = "virsh destroy "+param['vm_id']
            destroy_actions.append(Remote(cmd, [host], ignore_exit_code = True))
        action = ParallelActions(destroy_actions).run()
        
        if not action.ok():
            return False
    
        if autoundefine:
            logger.info('Undefining %s VM on hosts %s', ' '.join([set_style(param['vm_id'], 'object_repr') for param in vms_params]),
                        set_style(host.address, 'host') )
            result = undefine( vms_params, host )
            if not result:
                return False
            
    return True

def destroy_all( hosts):
    """Destroy all the VM on the hosts"""
    actions = []
    for host in hosts:
        vms = list_vm(host)
        if len(list_vm(host)) > 0:
            action = destroy( vms, host )
            actions.append(action.ok())
    
    logger.debug('%s', pprint(actions))
    
    if False in actions:
        return False
    else: 
        return True
        
      
def undefine(vms_params, host):
    undefine_actions = []
    for param in vms_params:
        cmd = "virsh undefine "+param['vm_id']
        undefine_actions.append(Remote(cmd, [host], ignore_exit_code = True))
    logger.info('Destroying %s VM on hosts %s', ' '.join([set_style(param['vm_id'], 'object_repr') for param in vms_params]), 
                    set_style(host.address, 'host') )

    action = ParallelActions(undefine_actions).run()
    
    return action.ok()
 
    

        
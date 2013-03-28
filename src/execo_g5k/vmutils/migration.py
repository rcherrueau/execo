from pprint import pprint, pformat
from execo import Remote, Host, SequentialActions, ParallelActions, configuration
from execo.log import set_style, logger
from collections import deque


def split_vm( vms_params, n = 2 ):
    split_vms = [0] * n
    for i_params in range(n):
        split_vms[i_params] = vms_params[i_params::n]
    return split_vms

def host_shortname( host, color_style = None ):
    ''' Return the short name of a G5K host, with a color_style '''
    return set_style(host.address.split('.')[0], color_style)
    
def set_migspeed( vms_params, hosts, speed = 125000000):
    """Change the speed migration of libvirt"""
    actions = []
    for hosts in hosts:
        actions.append('virsh migrate-setspeed '+str(speed))
    
    
def migration_measure( vm, host_src, host_dest, i_mes = 0, label = 'MIG', mig_speed = 32):
    ''' Return an Remote action to measure migration time of vm_id from
    host_src to host_dest '''
    cmd = "virsh --connect qemu:///system migrate-setspeed "+vm['vm_id']+" "+str(mig_speed)+"; timestamp=`date +%s`; "+ \
            "duration=`/usr/bin/time  -f \""+str(i_mes)+"\t%e\" sh -c '"+ \
            "virsh --connect qemu:///system migrate "+vm['vm_id']+" --live --copy-storage-inc "+\
            "qemu+ssh://"+host_dest.address+"/system'  2>&1 `;"+ \
            "echo $timestamp "+vm['vm_id']+" $duration >> "+\
            label+"_"+host_shortname(host_src)+"_"+host_shortname(host_dest)+".out"
    logger.info(set_style(vm['vm_id'], 'object_repr')+': '+host_shortname(host_src, color_style ='host')+" -> "+host_shortname(host_dest, color_style ='host'))
    logger.debug('%s %s %s', cmd, host_src, host_dest)    
    return Remote(cmd, [ host_src ])

def measurements_loop(n_measure, vms, hosts, mig_function, mode, label = None, mig_speed = 32):
    ''' '''
    n_nodes = len(hosts)
    permut = deque(''.join([`num` for num in range(n_nodes)]))
    for i_mes in range( n_measure ):
        logger.info( set_style('Measure '+str(i_mes)+'/'+str(n_measure), 'user3'))
        ii = [int(permut[i]) for i in range(n_nodes)]
        
        nodes = [ hosts[ii[i]] for i in range(n_nodes)]
        
        migractions = mig_function( vms, nodes, i_mes = i_mes, 
                    mode = mode, label = label, mig_speed = mig_speed)
        migractions.run()
        permut.rotate(+1)

def twonodes_migrations( vms, hosts, mode = 'sequential', i_mes = 0, label = 'SEQ', mig_speed = 32):
    ''' Return SequentialActions to perform sequential measurements '''
    migractions = []
    for vm in vms:
        migractions.append(migration_measure( vm, hosts[0], hosts[1], i_mes, label, mig_speed = mig_speed))
    if mode == 'sequential':
        return SequentialActions(migractions)
    else:
        return ParallelActions(migractions)
    
def crossed_migrations( vms, hosts, mode = 'parallel', i_mes = 0, label = 'CROSSED', mig_speed = 32):
    ''' Return ParallelActions to perform parallel measurements '''
    vms = split_vm(vms)
    migractions_01 = []; migractions_10 = []
    for vm in vms[0]:
        migractions_01.append(migration_measure( vm, hosts[0], hosts[1], i_mes, label, mig_speed = mig_speed))
    for vm in vms[1]:
        migractions_10.append(migration_measure( vm, hosts[1], hosts[0], i_mes, label, mig_speed = mig_speed))
    if mode == 'sequential':
        return ParallelActions( [ SequentialActions( migractions_01 ), SequentialActions( migractions_10 ) ] )
    else:
        return ParallelActions( migractions_01 + migractions_10 )

def circular_migrations( vms, hosts, mode = 'sequential', i_mes = 0, label = 'CIRC', mig_speed = 32):
    n_nodes = len(hosts)
    if n_nodes < 3:
        print 'Error, number of hosts must be >= 3'
    elif len(vms) % (n_nodes) !=0:
        print 'Error, number of VMs not divisible by number of hosts'
    else:
        vms = split_vm(vms, n_nodes )
        migractions = []
        for i_from in range(n_nodes):
            i_to = i_from+1 if i_from < n_nodes-1 else 0            
            if mode == 'sequential':
                label = 'CIRCSEQ'
            elif mode == 'parallel':
                label = 'CIRCPARA'
            migractions.append(twonodes_migrations(vms[i_to], hosts[i_from], hosts[i_to], mode = mode, i_mes = 0, label = label ))
        return ParallelActions(migractions)
                
def split_merge_migrations( vms, hosts, way = 'split', mode = 'parallel', i_mes = 0, label = 'SPLIT', mig_speed = 32):
    ''' Return ParallelActions to perform split migration '''
    if len(hosts) < 3:
        print 'Error, number of hosts must be >= 3'
    elif len(vms) % (len(hosts)-1) !=0:
        print 'Error, number of VMs not divisible by number of hosts'
    else:
        vms = split_vm(vms, len(hosts)-1 )
        migractions = []
        for idx in range(len(hosts)-1):
            migractions.append([])
            for vm in vms[idx]:
                if way == 'split':
                    migractions[idx].append(migration_measure( vm, hosts[0], hosts[idx+1], i_mes, label, mig_speed = mig_speed))
                elif way == 'merge':
                    migractions[idx].append(migration_measure( vm, hosts[idx+1], hosts[0], i_mes, label, mig_speed = mig_speed))
            if mode == 'sequential':
                migractions[idx] = SequentialActions( migractions[idx])
            else:
                migractions[idx] = ParallelActions( migractions[idx])
        return ParallelActions( migractions )

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

from config import default_connexion_params

def get_ssh_scp_auth_options(user = None, keyfile = None, port = None, connexion_params = None):
    """Return tuple with ssh / scp authentifications options.

    :param user: the user to connect with. If None, will try to get
      the user from the given connexion_params, or fallback to the
      default user in `default_connexion_params`, or no user option at
      all.

    :param keyfile: the keyfile to connect with. If None, will try to
      get the keyfile from the given connexion_params, or fallback to
      the default keyfile in `default_connexion_params`, or no keyfile
      option at all.

    :param port: the port to connect to. If None, will try to get the
      port from the given connexion_params, or fallback to the default
      port in `default_connexion_params`, or no port option at all.
        
    :param connexion_params: a dict similar to
      `default_connexion_params`, whose values will override those in
      `default_connexion_params`
    """
    ssh_scp_auth_options = ()
    
    if user != None:
        ssh_scp_auth_options += ("-o", "User=%s" % (user,))
    elif connexion_params != None and connexion_params.has_key('user'):
        if connexion_params['user'] != None:
            ssh_scp_auth_options += ("-o", "User=%s" % (connexion_params['user'],))
    elif default_connexion_params != None and default_connexion_params.has_key('user'):
        if default_connexion_params['user'] != None:
            ssh_scp_auth_options += ("-o", "User=%s" % (default_connexion_params['user'],))
            
    if keyfile != None:
        ssh_scp_auth_options += ("-i", str(keyfile))
    elif connexion_params != None and connexion_params.has_key('keyfile'):
        if connexion_params['keyfile'] != None:
            ssh_scp_auth_options += ("-i", str(connexion_params['keyfile']))
    elif default_connexion_params != None and default_connexion_params.has_key('keyfile'):
        if default_connexion_params['keyfile'] != None:
            ssh_scp_auth_options += ("-i", str(default_connexion_params['keyfile']))
            
    if port != None:
        ssh_scp_auth_options += ("-o", "Port=%i" % port)
    elif connexion_params != None and connexion_params.has_key('port'):
        if connexion_params['port'] != None:
            ssh_scp_auth_options += ("-o", "Port=%i" % connexion_params['port'])
    elif default_connexion_params != None and default_connexion_params.has_key('port'):
        if default_connexion_params['port'] != None:
            ssh_scp_auth_options += ("-o", "Port=%i" % default_connexion_params['port'])
            
    return ssh_scp_auth_options

def _get_connector_command(connector_params_entry,
                           connector_options_params_entry,
                           user = None,
                           keyfile = None,
                           port = None,
                           connexion_params = None):
    """build an ssh / scp / taktuk connector command line.

    Constructs the command line based on values of
    <connector_params_entry> and <connector_options_params_entry> in
    connexion_params, if any, or fallback to
    `default_connexion_params`, and add authentification options got
    from `get_ssh_scp_auth_options`

    :param connector_params_entry: name of field in connexion_params
      or default_connexion_params containing the connector executable
      name

    :param connector_options_params_entry: name of field in
      connexion_params or default_connexion_params containing the
      connector options

    :param user: see `get_ssh_scp_auth_options`

    :param keyfile: see `get_ssh_scp_auth_options`
    
    :param port: see `get_ssh_scp_auth_options`
    
    :param connexion_params: see `get_ssh_scp_auth_options`
    """
    command = ()
    
    if connexion_params != None and connexion_params.has_key(connector_params_entry):
        if connexion_params[connector_params_entry] != None:
            command += (connexion_params[connector_params_entry],)
        else:
            raise ValueError, "invalid connector command %s in connexion_params %s" % (connector_params_entry,
                                                                                       connexion_params,)
    elif default_connexion_params != None and default_connexion_params.has_key(connector_params_entry):
        if default_connexion_params[connector_params_entry] != None:
            command += (default_connexion_params[connector_params_entry],)
        else:
            raise ValueError, "invalid connector command %s in default_connexion_params %s" % (connector_params_entry,
                                                                                               default_connexion_params,)
    else:
        raise ValueError, "no connector command %s in default_connexion_params %s" % (connector_params_entry,
                                                                                      default_connexion_params,)
    
    if connexion_params != None and connexion_params.has_key(connector_options_params_entry):
        if connexion_params[connector_options_params_entry] != None:
            command += connexion_params[connector_options_params_entry]
    elif default_connexion_params != None and default_connexion_params.has_key(connector_options_params_entry):
        if default_connexion_params[connector_options_params_entry] != None:
            command += default_connexion_params[connector_options_params_entry]
            
    command += get_ssh_scp_auth_options(user, keyfile, port, connexion_params)
    return command

def get_ssh_command(user = None, keyfile = None, port = None, connexion_params = None):
    """Return tuple with complete ssh command line.

    Constructs the command line based on values of 'ssh' and
    'ssh_options' in connexion_params, if any, or fallback to
    `default_connexion_params`, and add authentification options got
    from `get_ssh_scp_auth_options`

    :param user: see `get_ssh_scp_auth_options`

    :param keyfile: see `get_ssh_scp_auth_options`
    
    :param port: see `get_ssh_scp_auth_options`
    
    :param connexion_params: see `get_ssh_scp_auth_options`
    """
    return _get_connector_command('ssh',
                                  'ssh_options',
                                  user,
                                  keyfile,
                                  port,
                                  connexion_params)

def get_scp_command(user = None, keyfile = None, port = None, connexion_params = None):
    """Return tuple with complete scp command line.

    Constructs the command line based on values of 'scp' and
    'scp_options' in connexion_params, if any, or fallback to
    `default_connexion_params`, and add authentification options got
    from `get_ssh_scp_auth_options`

    :param user: see `get_ssh_scp_auth_options`

    :param keyfile: see `get_ssh_scp_auth_options`

    :param port: see `get_ssh_scp_auth_options`

    :param connexion_params: see `get_ssh_scp_auth_options`
    """
    return _get_connector_command('scp',
                                  'scp_options',
                                  user,
                                  keyfile,
                                  port,
                                  connexion_params)

def get_taktuk_connector_command(user = None, keyfile = None, port = None, connexion_params = None):
    """Return tuple with complete taktuk connector command line.

    Constructs the command line based on values of 'taktuk_connector'
    and 'taktuk_connector_options' in connexion_params, if any, or
    fallback to `default_connexion_params`, and add authentification
    options got from `get_ssh_scp_auth_options`

    :param user: see `get_ssh_scp_auth_options`

    :param keyfile: see `get_ssh_scp_auth_options`

    :param port: see `get_ssh_scp_auth_options`

    :param connexion_params: see `get_ssh_scp_auth_options`
    """
    return _get_connector_command('taktuk_connector',
                                  'taktuk_connector_options',
                                  user,
                                  keyfile,
                                  port,
                                  connexion_params)

def get_ssh_scp_pty_option(connexion_params):
    """Based on given connexion_params or default_connexion_params, return a boolean suitable for pty option for Process creation."""
    if connexion_params != None and connexion_params.has_key('ssh_scp_pty'):
        return connexion_params['ssh_scp_pty']
    elif default_connexion_params != None and default_connexion_params.has_key('ssh_scp_pty'):
        return default_connexion_params['ssh_scp_pty']
    else:
        return False

def get_rewritten_host_address(host_addr, connexion_params):
    """Based on given connexion_params or default_connexion_params, return a rewritten host address."""
    if connexion_params != None and connexion_params.has_key('host_rewrite_func'):
        return connexion_params['host_rewrite_func'](host_addr)
    elif default_connexion_params != None and default_connexion_params.has_key('host_rewrite_func'):
        return default_connexion_params['host_rewrite_func'](host_addr)
    else:
        return host_addr


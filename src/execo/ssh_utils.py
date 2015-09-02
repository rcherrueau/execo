# Copyright 2009-2015 INRIA Rhone-Alpes, Service Experimentation et
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

from execo.config import make_connection_params

def get_ssh_scp_auth_options(user = None, keyfile = None, port = None, connection_params = None):
    """Return tuple with ssh / scp authentifications options.

    :param user: the user to connect with. If None, will try to get
      the user from the given connection_params, or fallback to the
      default user in `execo.config.default_connection_params`, or no
      user option at all.

    :param keyfile: the keyfile to connect with. If None, will try to
      get the keyfile from the given connection_params, or fallback to
      the default keyfile in `execo.config.default_connection_params`,
      or no keyfile option at all.

    :param port: the port to connect to. If None, will try to get the
      port from the given connection_params, or fallback to the default
      port in `execo.config.default_connection_params`, or no port
      option at all.

    :param connection_params: a dict similar to
      `execo.config.default_connection_params`, whose values will
      override those in `execo.config.default_connection_params`
    """
    ssh_scp_auth_options = ()
    actual_connection_params = make_connection_params(connection_params)

    if user != None:
        ssh_scp_auth_options += ("-o", "User=%s" % (user,))
    elif actual_connection_params.get('user'):
        ssh_scp_auth_options += ("-o", "User=%s" % (actual_connection_params['user'],))

    if keyfile != None:
        ssh_scp_auth_options += ("-i", str(keyfile))
    elif actual_connection_params.get('keyfile'):
        ssh_scp_auth_options += ("-i", str(actual_connection_params['keyfile']))

    if port != None:
        ssh_scp_auth_options += ("-o", "Port=%i" % port)
    elif actual_connection_params.get('port'):
        ssh_scp_auth_options += ("-o", "Port=%i" % actual_connection_params['port'])

    return ssh_scp_auth_options

def _get_connector_command(connector_params_entry,
                           connector_options_params_entry,
                           user = None,
                           keyfile = None,
                           port = None,
                           connection_params = None):
    """build an ssh / scp / taktuk connector command line.

    Constructs the command line based on values of
    <connector_params_entry> and <connector_options_params_entry> in
    connection_params, if any, or fallback to
    `execo.config.default_connection_params`, and add authentification
    options got from `execo.ssh_utils.get_ssh_scp_auth_options`

    :param connector_params_entry: name of field in connection_params
      or default_connection_params containing the connector executable
      name

    :param connector_options_params_entry: name of field in
      connection_params or default_connection_params containing the
      connector options

    :param user: see `execo.ssh_utils.get_ssh_scp_auth_options`

    :param keyfile: see `execo.ssh_utils.get_ssh_scp_auth_options`

    :param port: see `execo.ssh_utils.get_ssh_scp_auth_options`

    :param connection_params: see
      `execo.ssh_utils.get_ssh_scp_auth_options`
    """
    command = ()
    actual_connection_params = make_connection_params(connection_params)
    command += (actual_connection_params[connector_params_entry],)
    command += actual_connection_params[connector_options_params_entry]
    command += get_ssh_scp_auth_options(user, keyfile, port, connection_params)
    return command

def get_ssh_command(user = None, keyfile = None, port = None, connection_params = None):
    """Return tuple with complete ssh command line.

    Constructs the command line based on values of 'ssh' and
    'ssh_options' in connection_params, if any, or fallback to
    `execo.config.default_connection_params`, and add authentification
    options got from `execo.ssh_utils.get_ssh_scp_auth_options`

    :param user: see `execo.ssh_utils.get_ssh_scp_auth_options`

    :param keyfile: see `execo.ssh_utils.get_ssh_scp_auth_options`

    :param port: see `execo.ssh_utils.get_ssh_scp_auth_options`

    :param connection_params: see
      `execo.ssh_utils.get_ssh_scp_auth_options`
    """
    return _get_connector_command('ssh',
                                  'ssh_options',
                                  user,
                                  keyfile,
                                  port,
                                  connection_params)

def get_scp_command(user = None, keyfile = None, port = None, connection_params = None):
    """Return tuple with complete scp command line.

    Constructs the command line based on values of 'scp' and
    'scp_options' in connection_params, if any, or fallback to
    `execo.config.default_connection_params`, and add authentification
    options got from `execo.ssh_utils.get_ssh_scp_auth_options`

    :param user: see `execo.ssh_utils.get_ssh_scp_auth_options`

    :param keyfile: see `execo.ssh_utils.get_ssh_scp_auth_options`

    :param port: see `execo.ssh_utils.get_ssh_scp_auth_options`

    :param connection_params: see
      `execo.ssh_utils.get_ssh_scp_auth_options`
    """
    return _get_connector_command('scp',
                                  'scp_options',
                                  user,
                                  keyfile,
                                  port,
                                  connection_params)

def get_taktuk_connector_command(user = None, keyfile = None, port = None, connection_params = None):
    """Return tuple with complete taktuk connector command line.

    Constructs the command line based on values of 'taktuk_connector'
    and 'taktuk_connector_options' in connection_params, if any, or
    fallback to `execo.config.default_connection_params`, and add
    authentification options got from
    `execo.ssh_utils.get_ssh_scp_auth_options`

    :param user: see `execo.ssh_utils.get_ssh_scp_auth_options`

    :param keyfile: see `execo.ssh_utils.get_ssh_scp_auth_options`

    :param port: see `execo.ssh_utils.get_ssh_scp_auth_options`

    :param connection_params: see
      `execo.ssh_utils.get_ssh_scp_auth_options`
    """
    return _get_connector_command('taktuk_connector',
                                  'taktuk_connector_options',
                                  user,
                                  keyfile,
                                  port,
                                  connection_params)

def get_rewritten_host_address(host_addr, connection_params):
    """Based on given connection_params or default_connection_params, return a rewritten host address."""
    func = make_connection_params(connection_params).get('host_rewrite_func')
    if func: return func(host_addr)
    else: return host_addr

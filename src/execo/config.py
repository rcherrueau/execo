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

import logging
import os
import sys

SSH = 0
SCP = 1
TAKTUK = 2
CHAINPUT = 3

def checktty(f):
    try:
        if (get_ipython().__class__.__name__ == 'ZMQInteractiveShell' #@UndefinedVariable
            and f.__class__.__module__ == 'IPython.zmq.iostream'
            and f.__class__.__name__ == 'OutStream'):
            return True
    except NameError:
        pass
    if hasattr(f, 'isatty'):
        return f.isatty()
    if hasattr(f, 'fileno'):
        return os.isatty(f.fileno())
    return False

IODEBUG = 3
FDEBUG = 5
TRACE = 12
DETAIL = 15

# _STARTOF_ configuration
configuration = {
    'log_level': logging.INFO,
    'remote_tool': SSH,
    'fileput_tool': SCP,
    'fileget_tool': SCP,
    'compact_output_threshold': 4096,
    'kill_timeout': 5,
    'intr_period': 1,
    'port_range': (25500, 26700),
    'kill_childs_at_end': True,
    'color_mode': checktty(sys.stdout)
                  and checktty(sys.stderr),
    'color_styles': {
        'log_header': ('yellow',),
        'object_repr': ('blue',),
        'emph': ('cyan',),
        'report_warn': ('magenta',),
        'report_error': ('red', 'bold'),
        'command': ('blue', 'bold'),
        'host': ('magenta', 'bold'),
        'user1': ('green', 'bold'),
        'user2': ('yellow', 'bold'),
        'user3': ('cyan', 'bold'),
        IODEBUG: ('green', 'reverse'),
        FDEBUG: ('green', 'reverse'),
        logging.DEBUG: ('green',),
        TRACE: ('green', 'bold',),
        DETAIL: ('magenta', 'bold'),
        logging.INFO: ('magenta',),
        logging.WARNING: ('cyan',),
        logging.ERROR: ('red',),
        logging.CRITICAL: ('yellow', 'on_red')
        },
    }
# _ENDOF_ configuration
"""Global execo configuration parameters.

- ``log_level``: the log level (see module `logging`)

- ``remote_tool``: default tool to use when instanciating remote
  processes. Can be `execo.config.SSH` or `execo.config.TAKTUK`

- ``fileput_tool``: default tool to use to put files remotely. Can be
  `execo.config.SCP` or `execo.config.TAKTUK`

- ``fileget_tool``: default tool to use to get remote files. Can be
  `execo.config.SCP` or `execo.config.TAKTUK`

- ``compact_output_threshold``: only beginning and end of stdout /
  stderr are displayed by `execo.process.ProcessBase.dump` when their
  size is greater than this threshold. 0 for no threshold

- ``kill_timeout``: number of seconds to wait after a clean SIGTERM
  kill before assuming that the process is not responsive and killing
  it with SIGKILL

- ``intr_period``: number of seconds between periodic check interrupt,
  for correct handling of ctrl-c

- ``port_range``: a tuple (start port, end port) of ports to use for
  the function ``execo.utils.get_port``. As all python ranges, start
  is inclusive, end is exclusive.

- ``kill_childs_at_end``: Whether to try sending SIGTERM to all
  subprocesses started through execo when the script
  terminates. Warnings:

  - this config option must be set at execo import time, changing it
    later will be ignored

  - SIGTERM is only sent to all childs or subchilds which did not try
    to daemonize by changing their process group.

- ``color_mode``: whether to colorize output (with ansi escape
  sequences)

- ``color_styles``: mapping of identifiers to iterables of ansi
  attributes identifiers (see `execo.log._ansi_styles`)
"""

def make_default_connection_params():
# _STARTOF_ default_connection_params
    default_connection_params = {
        'user':        None,
        'keyfile':     None,
        'port':        None,
        'ssh':         'ssh',
        'scp':         'scp',
        'taktuk':      'taktuk',
        'ssh_options': ( '-tt',
                         '-o', 'BatchMode=yes',
                         '-o', 'PasswordAuthentication=no',
                         '-o', 'StrictHostKeyChecking=no',
                         '-o', 'UserKnownHostsFile=/dev/null',
                         '-o', 'ConnectTimeout=20' ),
        'scp_options': ( '-o', 'BatchMode=yes',
                         '-o', 'PasswordAuthentication=no',
                         '-o', 'StrictHostKeyChecking=no',
                         '-o', 'UserKnownHostsFile=/dev/null',
                         '-o', 'ConnectTimeout=20',
                         '-rp' ),
        'taktuk_options': ( '-s', ),
        'taktuk_connector': 'ssh',
        'taktuk_connector_options': ( '-o', 'BatchMode=yes',
                                      '-o', 'PasswordAuthentication=no',
                                      '-o', 'StrictHostKeyChecking=no',
                                      '-o', 'UserKnownHostsFile=/dev/null',
                                      '-o', 'ConnectTimeout=20'),
        'chainput_nc_client_timeout': 5,
        'chainput_nc_server_timeout': 30,
        'nc': '/bin/nc.traditional -v -v',
        'chainput_host_retry': 5,
        'chainput_chain_retry': 10,
        'chainput_try_delay': 1,
        'pty': False,
        'host_rewrite_func': None
        }
# _ENDOF_ default_connection_params
    return default_connection_params

default_connection_params = make_default_connection_params()
"""Default connection params for ``ssh``/``scp``/``taktuk`` connections.

- ``user``: the user to connect with.

- ``keyfile``: the keyfile to connect with.

- ``port``: the port to connect to.

- ``ssh``: the ssh or ssh-like command.

- ``scp``: the scp or scp-like command.

- ``taktuk``: the taktuk command.

- ``ssh_options``: tuple of options passed to ssh.

- ``scp_options``: tuple of options passed to scp.

- ``taktuk_options``: tuple of options passed to taktuk.

- ``taktuk_connector``: the ssh-like connector command for taktuk.

- ``taktuk_connector_options``: tuple of options passed to
  taktuk_connector.

- ``nc``: the netcat command to use

- ``chainput_nc_client_timeout``: timeout for client connection to
  next hop

- ``chainput_nc_server_timeout``: timeout for server to wait for
  incoming connection from previous hop

- ``chainput_host_retry``: number of times each hop in the transfer
  chain retries to connect to next hop

- ``chainput_chain_retry``: number of times each hop in the transfer
  chain tries a new next hop if all tries to current next hop fail. If
  given a float (between 0.0 and 1.0), this is expressed as a ratio of
  the total number of hosts in the chain.

- ``chainput_try_delay``: delay in seconds between TCP client to
  server connection attempts.

- ``pty``: boolean. Wether to allocate or not a pty for
  ssh/scp.

- ``host_rewrite_func``: function called to rewrite hosts
  addresses. Takes a host address, returns a host address.
"""

def make_connection_params(connection_params = None, default_params = None):
    return_params = make_default_connection_params()
    if default_params == None:
        default_params = default_connection_params
    return_params.update(default_params)
    if connection_params: return_params.update(connection_params)
    return return_params

def load_configuration(filename, dicts_confs):
    """Update dicts with those found in file.

    :param filename: file to load dicts from

    :param dicts_confs: an iterable of couples (dict, string)

    Used to read configuration dicts. For each couple (dict, string),
    if a dict named string is defined in <file>, update
    dict with the content of this dict. Does nothing if unable to open
    <file>.
    """
    if os.path.isfile(filename):
        jailed_globals = {}
        try:
            execfile(filename, jailed_globals)
        except Exception, exc: #IGNORE:W0703
            print "ERROR while reading config file %s:" % (filename,)
            print exc
        for (dictio, conf) in dicts_confs:
            if jailed_globals.has_key(conf):
                dictio.update(jailed_globals[conf])

def get_user_config_filename():
    _user_conf_file = None
    if os.environ.has_key('HOME'):
        _user_conf_file = os.environ['HOME'] + '/.execo.conf.py'
    return _user_conf_file

load_configuration(
  get_user_config_filename(),
  ((configuration, 'configuration'),
   (default_connection_params, 'default_connection_params')))

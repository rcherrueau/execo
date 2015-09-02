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

"""Handles launch and control of several OS level processes concurrently.

Handles remote executions and file copies with ssh or similar tools.
"""

from log import logger
from config import configuration, default_connection_params, \
  SSH, TAKTUK, SCP, CHAINPUT
from time_utils import sleep, Timer, format_date, format_duration, \
  get_seconds, get_unixts
from host import Host
from process import Process, SshProcess, get_process, \
     ProcessLifecycleHandler, ProcessOutputHandler, \
     PortForwarder, Serial, SerialSsh, STDOUT, STDERR, \
     ExpectOutputHandler
from action import Action, wait_any_actions, wait_all_actions, \
  Remote, Put, Get, TaktukRemote, TaktukPut, TaktukGet, Local, \
  ParallelActions, SequentialActions, default_action_factory, \
  get_remote, get_fileput, get_fileget, \
  ActionLifecycleHandler, ChainPut, filter_bad_hosts, \
  RemoteSerial
from report import Report
from exception import ProcessesFailed, ActionsFailed
try:
    from _version import __version__
except:
    pass

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

from config import g5k_configuration
from execo.action import Remote, ActionNotificationProcessLH, \
    Action, get_remote
from execo.config import make_connection_params
from execo.host import get_hosts_set, Host
from execo.log import style, logger
from execo.process import ProcessOutputHandler, get_process, \
    handle_process_output
from execo.time_utils import format_seconds
from execo.utils import comma_join, compact_output, singleton_to_collection
from execo_g5k.config import default_frontend_connection_params
from execo_g5k.utils import get_frontend_host, get_kavlan_host_name
from utils import get_default_frontend
import copy
import re
import time

class Deployment(object):
    """A kadeploy3 deployment, POD style class."""

    def __init__(self,
                 hosts = None,
                 env_file = None,
                 env_name = None,
                 user = None,
                 vlan = None,
                 other_options = None):
        self.hosts = hosts
        """hosts: iterable of `execo.host.Host` on which to deploy."""
        self.env_file = env_file
        """filename of an environment to deploy"""
        self.env_name = env_name
        """name of a kadeploy3 registered environment to deploy.

        there must be either one of env_name or env_file parameter
        given. If none given, will try to use the default environement
        from `g5k_configuration`."""
        self.user = user
        """kadeploy3 user"""
        self.vlan = vlan
        """if given, kadeploy3 will automatically switch the nodes to this vlan after deployment"""
        self.other_options = other_options
        """string of other options to pass to kadeploy3"""

    def _get_common_kadeploy_command_line(self):
        cmd_line = g5k_configuration.get('kadeploy3')
        cmd_line += " " + g5k_configuration.get('kadeploy3_options')
        if self.env_file and self.env_name:
            raise ValueError, "Deployment cannot have both env_file and env_name"
        if (not self.env_file) and (not self.env_name):
            if g5k_configuration.get('default_env_name') and g5k_configuration.get('default_env_file'):
                raise Exception, "g5k_configuration cannot have both default_env_name and default_env_file"
            if (not g5k_configuration.get('default_env_name')) and (not g5k_configuration.get('default_env_file')):
                raise Exception, "no environment name or file found"
            if g5k_configuration.get('default_env_name'):
                cmd_line += " -e %s" % (g5k_configuration['default_env_name'],)
            elif g5k_configuration.get('default_env_file'):
                cmd_line += " -a %s" % (g5k_configuration['default_env_file'],)
        elif self.env_name:
            cmd_line += " -e %s" % (self.env_name,)
        elif self.env_file:
            cmd_line += " -a %s" % (self.env_file,)
        if self.user != None:
            cmd_line += " -u %s" % (self.user,)
        if self.vlan != None:
            cmd_line += " --vlan %s" % (self.vlan,)
        if self.other_options:
            cmd_line += " %s" % (self.other_options,)
        return cmd_line

    def __repr__(self):
        s = ""
        if self.hosts != None: s = comma_join(s, "hosts=%r" % (self.hosts,))
        if self.env_file != None: s = comma_join(s, "env_file=%r" % (self.env_file,))
        if self.env_name != None: s = comma_join(s, "env_name=%r" % (self.env_name,))
        if self.user != None: s = comma_join(s, "user=%r" % (self.user,))
        if self.vlan != None: s = comma_join(s, "vlan=%r" % (self.vlan,))
        if self.other_options: s = comma_join(s, "other_options=%r" % (self.other_options,))
        return "Deployment(%s)" % (s,)

_ksoh_deployed_nodes_header_re1 = re.compile("^Nodes correctly deployed on cluster \w+\s*$")       # for kadeploy3 < 3.2
_ksoh_undeployed_nodes_header_re1 = re.compile("^Nodes not correctly deployed on cluster \w+\s*$") # for kadeploy3 < 3.2
_ksoh_deployed_nodes_header_re2 = re.compile("^The \w+ is successful on nodes\s*$")                # for kadeploy3 >= 3.2
_ksoh_undeployed_nodes_header_re2 = re.compile("^The \w+ failed on nodes\s*$")                     # for kadeploy3 >= 3.2
_ksoh_deployed_node_re = re.compile("^(\w+-\d+\.\w+\.grid5000\.fr)\s*$")
_ksoh_undeployed_node_re = re.compile("^(\w+-\d+\.\w+\.grid5000\.fr)(\s+\(.*)?\s*$")

class _KadeployStdoutHandler(ProcessOutputHandler):

    """Parse kadeploy3 stdout."""

    def __init__(self):
        super(_KadeployStdoutHandler, self).__init__()
        self._SECTION_NONE, self._SECTION_DEPLOYED_NODES, self._SECTION_UNDEPLOYED_NODES = range(3)
        self._current_section = self._SECTION_NONE

    def action_reset(self):
        self._current_section = self._SECTION_NONE

    def read_line(self, process, stream, string, eof, error):
        if (_ksoh_deployed_nodes_header_re1.search(string) != None
            or _ksoh_deployed_nodes_header_re2.search(string) != None):
            self._current_section = self._SECTION_DEPLOYED_NODES
            return
        if (_ksoh_undeployed_nodes_header_re1.search(string) != None
            or _ksoh_undeployed_nodes_header_re2.search(string) != None):
            self._current_section = self._SECTION_UNDEPLOYED_NODES
            return
        if self._current_section == self._SECTION_DEPLOYED_NODES:
            so = _ksoh_deployed_node_re.search(string)
            if so != None:
                host_address = so.group(1)
                process.kadeployer.deployed_hosts.add(host_address)
                process.deployed_hosts.add(host_address)
        elif self._current_section == self._SECTION_UNDEPLOYED_NODES:
            so = _ksoh_undeployed_node_re.search(string)
            if so != None:
                host_address = so.group(1)
                process.kadeployer.undeployed_hosts.add(host_address)
                process.undeployed_hosts.add(host_address)

_host_site_re1 = re.compile("^[^ \t\n\r\f\v\.]+\.([^ \t\n\r\f\v\.]+)\.grid5000.fr$")
_host_site_re2 = re.compile("^[^ \t\n\r\f\v\.]+\.([^ \t\n\r\f\v\.]+)$")
_host_site_re3 = re.compile("^[^ \t\n\r\f\v\.]+$")

def _get_host_frontend(host):
    # Get the frontend of a host.
    #
    # This function is specific to kadeploy because:
    #
    # - only handles execo.Host
    #
    # - we could use get_host_site but api_utils is not always
    #   available (if api is down or if httplib2 is not available)
    #   -> this is not true anymore (13/2/2014)
    #
    # - special bahavior: fallback to default frontend if unable to
    #   extract site from host name
    #
    # - raises exception if host name format invalid (not sure we
    #   would want this for a generic function)
    frontend = None
    mo1 = _host_site_re1.search(host.address)
    if mo1 != None:
        frontend = mo1.group(1)
    else:
        mo2 = _host_site_re2.search(host.address)
        if mo2 != None:
            frontend = mo1.group(1)
        else:
            mo3 = _host_site_re3.search(host.address)
            if mo3 != None:
                frontend = get_default_frontend()
            else:
                raise ValueError, "unknown frontend for host %s" % host.address
    return frontend

class FrontendPrefixWrapper(ProcessOutputHandler):

    def __init__(self, handler):
        super(FrontendPrefixWrapper, self).__init__()
        self.handler = handler

    def read_line(self, process, stream, string, eof, error):
        handle_process_output(process,
                              stream,
                              self.handler,
                              process.frontend + ": " + string,
                              eof, error)

class Kadeployer(Remote):

    """Deploy an environment with kadeploy3 on several hosts.

    Able to deploy in parallel to multiple frontends.
    """

    def __init__(self, deployment, frontend_connection_params = None,
                 stdout_handlers = None, stderr_handlers = None):
        """
        :param deployment: instance of Deployment class describing the
          intended kadeployment.

        :param frontend_connection_params: connection params for
          connecting to frontends if needed. Values override those in
          `execo_g5k.config.default_frontend_connection_params`.

        :param stdout_handlers: iterable of `ProcessOutputHandlers`
          which will be passed to the actual deploy processes.

        :param stderr_handlers: iterable of `ProcessOutputHandlers`
          which will be passed to the actual deploy processes.
        """
        super(Remote, self).__init__()
        self.deployed_hosts = set()
        """Iterable of `Host` containing the deployed hosts. This iterable won't be
        complete if the Kadeployer has not terminated."""
        self.undeployed_hosts = set()
        """Iterable of `Host` containing the hosts not deployed. This iterable
        won't be complete if the Kadeployer has not terminated."""
        self._stdout_handlers = stdout_handlers
        self._stderr_handlers = stderr_handlers
        """Iterable of `ProcessOutputHandlers` which will be passed to the
        actual deploy processes."""
        self.frontend_connection_params = frontend_connection_params
        """Connection params for connecting to frontends if needed. Values override
        those in `execo_g5k.config.default_frontend_connection_params`."""
        self.deployment = deployment
        """Instance of Deployment class describing the intended kadeployment."""
        self.timeout = None
        """Deployment timeout"""
        self._init_processes()
        self.name = "%s on %i hosts / %i frontends" % (self.__class__.__name__, len(self._unique_hosts), len(self.processes))

    def _init_processes(self):
        self.processes = []
        self._unique_hosts = get_hosts_set(self.deployment.hosts)
        frontends = dict()
        for host in self._unique_hosts:
            frontend = _get_host_frontend(host)
            if frontends.has_key(frontend):
                frontends[frontend].append(host)
            else:
                frontends[frontend] = [host]
        lifecycle_handler = ActionNotificationProcessLH(self, len(frontends))
        deploy_stdout_handler = _KadeployStdoutHandler()
        for frontend in frontends.keys():
            kadeploy_command = self.deployment._get_common_kadeploy_command_line()
            for host in frontends[frontend]:
                kadeploy_command += " -m %s" % (host.address,)
            p = get_process(kadeploy_command,
                            host = get_frontend_host(frontend),
                            connection_params = make_connection_params(self.frontend_connection_params,
                                                                     default_frontend_connection_params))
            p.pty = True
            p.timeout = self.timeout
            p.stdout_handlers.append(deploy_stdout_handler)
            p.stdout_handlers.extend([ FrontendPrefixWrapper(h)
                                       for h in singleton_to_collection(self._stdout_handlers) ])
            p.stderr_handlers.extend([ FrontendPrefixWrapper(h)
                                       for h in singleton_to_collection(self._stderr_handlers) ])
            p.lifecycle_handlers.append(lifecycle_handler)
            p.frontend = frontend
            p.kadeploy_hosts = [ host.address for host in frontends[frontend] ]
            p.deployed_hosts = set()
            p.undeployed_hosts = set()
            p.kadeployer = self
            self.processes.append(p)

    def _common_reset(self):
        super(Kadeployer, self)._common_reset()
        self.deployed_hosts = set()
        self.undeployed_hosts = set()

    def _check_ok(self, log):
        ok = True
        error_logs = []
        warn_logs = []
        for process in self.processes:
            if ( len(process.deployed_hosts.intersection(process.undeployed_hosts)) != 0
                 or len(process.deployed_hosts.union(process.undeployed_hosts).symmetric_difference(process.kadeploy_hosts)) != 0 ):
                error_logs.append("deploy on %s, total/deployed/undeployed = %i/%i/%i:\n%s\nstdout:\n%s\nstderr:\n%s" % (
                        process.frontend, len(process.kadeploy_hosts), len(process.deployed_hosts), len(process.undeployed_hosts),
                        process, compact_output(process.stdout), compact_output(process.stderr)))
                ok = False
            else:
                if len(process.deployed_hosts) == 0:
                    warn_logs.append("deploy on %s, total/deployed/undeployed = %i/%i/%i:\n%s\nstdout:\n%s\nstderr:\n%s" % (
                            process.frontend, len(process.kadeploy_hosts), len(process.deployed_hosts), len(process.undeployed_hosts),
                            process, compact_output(process.stdout), compact_output(process.stderr)))
                    ok = False
                elif len(process.undeployed_hosts) > 0:
                    warn_logs.append("deploy on %s, total/deployed/undeployed = %i/%i/%i" % (
                            process.frontend, len(process.kadeploy_hosts), len(process.deployed_hosts), len(process.undeployed_hosts)))
        if log:
            if len(warn_logs) > 0:
                logger.warn(str(self) + ":\n" + "\n".join(warn_logs))
            if len(error_logs) > 0:
                logger.error(str(self) + ":\n" + "\n".join(error_logs))
        return ok

    def _notify_terminated(self):
        self._check_ok(True)
        super(Kadeployer, self)._notify_terminated()

    def _args(self):
        return [ repr(self.deployment) ] + Action._args(self) + Kadeployer._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self.frontend_connection_params: kwargs.append("frontend_connection_params=%r" % (self.frontend_connection_params,))
        return kwargs

    def _infos(self):
        return super(Remote, self)._infos() + [ "total/deployed/undeployed = %i/%i/%i" % (
                len(self._unique_hosts), len(self.deployed_hosts), len(self.undeployed_hosts),) ]

    @property
    def ok(self):
        return (not self.ended or self._check_ok(False)) and super(Kadeployer, self).ok

def deploy(deployment,
           check_deployed_command = True,
           node_connection_params = {'user': 'root'},
           num_tries = 1,
           check_enough_func = None,
           frontend_connection_params = None,
           deploy_timeout = None,
           check_timeout = 30,
           stdout_handlers = None,
           stderr_handlers = None):
    """Deploy nodes, many times if needed, checking which of these nodes are already deployed with a user-supplied command. If no command given for checking if nodes deployed, rely on kadeploy to know which nodes are deployed.

    - loop `num_tries` times:

      - if ``check_deployed_command`` given, try to connect to these
        hosts using the supplied `node_connection_params` (or the
        default ones), and to execute ``check_deployed_command``. If
        connection succeeds and the command returns 0, the host is
        assumed to be deployed, else it is assumed to be undeployed.

      - optionnaly call user-supplied ``check_enough_func``, passing
        to it the list of deployed and undeployed hosts, to let user
        code decide if enough nodes deployed. Otherwise, try as long
        as there are undeployed nodes.

      - deploy the undeployed nodes

    returns a tuple with the list of deployed hosts and the list of
    undeployed hosts.

    When checking correctly deployed nodes with
    ``check_deployed_command``, and if the deployment is using the
    kavlan option, this function will try to contact the nodes using
    the appropriate DNS hostnames in the new vlan.

    :param deployment: instance of `execo.kadeploy.Deployment` class
      describing the intended kadeployment.

    :param check_deployed_command: command to perform remotely to
      check node deployement. May be a String, True, False or None. If
      String: the actual command to be used (This command should
      return 0 if the node is correctly deployed, or another value
      otherwise). If True, the default command value will be used
      (from `execo_g5k.config.g5k_configuration`). If None or False,
      no check is made and deployed/undeployed status will be taken
      from kadeploy's output.

    :param node_connection_params: a dict similar to
      `execo.config.default_connection_params` whose values will
      override those in `execo.config.default_connection_params` when
      connecting to check node deployment with
      ``check_deployed_command`` (see below).

    :param num_tries: number of deploy tries

    :param check_enough_func: a function taking as parameter a list of
      deployed hosts and a list of undeployed hosts, which will be
      called at each deployment iteration end, and that should return
      a boolean indicating if there is already enough nodes (in this
      case, no further deployement will be attempted).

    :param frontend_connection_params: connection params for connecting
      to frontends if needed. Values override those in
      `execo_g5k.config.default_frontend_connection_params`.

    :param deploy_timeout: timeout for deployement. Default is None,
      which means no timeout.

    :param check_timeout: timeout for node deployment checks. Default
      is 30 seconds.

    :param stdout_handlers: iterable of `ProcessOutputHandlers`
          which will be passed to the actual deploy processes.

    :param stderr_handlers: iterable of `ProcessOutputHandlers`
          which will be passed to the actual deploy processes.
    """

    if check_enough_func == None:
        check_enough_func = lambda deployed, undeployed: len(undeployed) == 0

    if check_deployed_command == True:
        check_deployed_command = g5k_configuration.get('check_deployed_command')

    def check_update_deployed(undeployed_hosts, check_deployed_command, node_connection_params, vlan): #IGNORE:W0613
        logger.debug(style.emph("check which hosts are already deployed among:") + " %s", undeployed_hosts)
        deployment_hostnames_mapping = dict()
        if vlan:
            for host in undeployed_hosts:
                deployment_hostnames_mapping[get_kavlan_host_name(host, vlan)] = host
        else:
            for host in undeployed_hosts:
                deployment_hostnames_mapping[host] = host
        deployed_check = get_remote(check_deployed_command,
                                    deployment_hostnames_mapping.keys(),
                                    connection_params = node_connection_params)
        for p in deployed_check.processes:
                p.nolog_exit_code = True
                p.nolog_timeout = True
                p.nolog_error = True
                p.timeout = check_timeout
        deployed_check.run()
        newly_deployed = list()
        for process in deployed_check.processes:
            logger.debug(style.emph("check on %s:" % (process.host,))
                         + " %s\n" % (process,)
                         + style.emph("stdout:") + "\n%s\n" % (process.stdout)
                         + style.emph("stderr:") + "\n%s\n" % (process.stderr))
            if (process.ok):
                newly_deployed.append(deployment_hostnames_mapping[process.host.address])
                logger.debug("OK %s", deployment_hostnames_mapping[process.host.address])
            else:
                logger.debug("KO %s", deployment_hostnames_mapping[process.host.address])
        return newly_deployed

    start_time = time.time()
    deployed_hosts = set()
    undeployed_hosts = set([ Host(host).address for host in deployment.hosts ])
    my_newly_deployed = []
    if check_deployed_command:
        my_newly_deployed = check_update_deployed(undeployed_hosts, check_deployed_command, node_connection_params, deployment.vlan)
        deployed_hosts.update(my_newly_deployed)
        undeployed_hosts.difference_update(my_newly_deployed)
    num_tries_done = 0
    elapsed = time.time() - start_time
    last_time = time.time()
    deploy_stats = list()   # contains tuples ( timestamp,
                            #                   num attempted deploys,
                            #                   len(kadeployer.deployed_hosts),
                            #                   len(my_newly_deployed),
                            #                   len(deployed_hosts),
                            #                   len(undeployed_hosts )
    deploy_stats.append((elapsed, None, None, len(my_newly_deployed), len(deployed_hosts), len(undeployed_hosts)))
    while (not check_enough_func(deployed_hosts, undeployed_hosts)
           and num_tries_done < num_tries):
        num_tries_done += 1
        logger.debug(style.emph("try %i, deploying on:" % (num_tries_done,)) + " %s", undeployed_hosts)
        tmp_deployment = copy.copy(deployment)
        tmp_deployment.hosts = undeployed_hosts
        kadeployer = Kadeployer(tmp_deployment,
                                frontend_connection_params = frontend_connection_params,
                                stdout_handlers = stdout_handlers,
                                stderr_handlers = stderr_handlers)
        kadeployer.timeout = deploy_timeout
        kadeployer.run()
        my_newly_deployed = []
        if check_deployed_command:
            my_newly_deployed = check_update_deployed(undeployed_hosts, check_deployed_command, node_connection_params, deployment.vlan)
            deployed_hosts.update(my_newly_deployed)
            undeployed_hosts.difference_update(my_newly_deployed)
        else:
            deployed_hosts.update(kadeployer.deployed_hosts)
            undeployed_hosts.difference_update(kadeployer.deployed_hosts)
        logger.debug(style.emph("kadeploy reported newly deployed hosts:") + "   %s", kadeployer.deployed_hosts)
        logger.debug(style.emph("check reported newly deployed hosts:") + "   %s", my_newly_deployed)
        logger.debug(style.emph("all deployed hosts:") + "     %s", deployed_hosts)
        logger.debug(style.emph("still undeployed hosts:") + " %s", undeployed_hosts)
        elapsed = time.time() - last_time
        last_time = time.time()
        deploy_stats.append((elapsed,
                             len(tmp_deployment.hosts),
                             len(kadeployer.deployed_hosts),
                             len(my_newly_deployed),
                             len(deployed_hosts),
                             len(undeployed_hosts)))

    logger.detail(style.emph("deploy finished") + " in %i tries, %s", num_tries_done, format_seconds(time.time() - start_time))
    logger.detail("deploy  duration  attempted  deployed     deployed     total     total")
    logger.detail("                  deploys    as reported  as reported  already   still")
    logger.detail("                             by kadeploy  by check     deployed  undeployed")
    logger.detail("---------------------------------------------------------------------------")
    for (deploy_index, deploy_stat) in enumerate(deploy_stats):
        logger.detail("#%-5.5s  %-8.8s  %-9.9s  %-11.11s  %-11.11s  %-8.8s  %-10.10s",
            deploy_index,
            format_seconds(deploy_stat[0]),
            deploy_stat[1],
            deploy_stat[2],
            deploy_stat[3],
            deploy_stat[4],
            deploy_stat[5])
    logger.debug(style.emph("deployed hosts:") + " %s", deployed_hosts)
    logger.debug(style.emph("undeployed hosts:") + " %s", undeployed_hosts)

    return (deployed_hosts, undeployed_hosts)

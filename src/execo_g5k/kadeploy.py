# Copyright 2009-2013 INRIA Rhone-Alpes, Service Experimentation et
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
from execo.process import ProcessOutputHandler, get_process
from execo.time_utils import format_seconds
from execo.utils import comma_join
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

_ksoh_good_nodes_header_re = re.compile("^Nodes correctly deployed on cluster \w+\s*$")
_ksoh_bad_nodes_header_re = re.compile("^Nodes not correctly deployed on cluster \w+\s*$")
_ksoh_good_node_re = re.compile("^(\S+)\s*$")
_ksoh_bad_node_re = re.compile("^(\S+)(\s+\(.*\))?\s*$")

class _KadeployStdoutHandler(ProcessOutputHandler):

    """Parse kadeploy3 stdout."""

    def __init__(self, kadeployer):
        """
        :param kadeployer: the `execo_g5k.kadeploy.Kadeployer` to
          which this `execo.process.ProcessOutputHandler` is attached.
        """
        super(_KadeployStdoutHandler, self).__init__()
        self.kadeployer = kadeployer
        self._SECTION_NONE, self._SECTION_GOODNODES, self._SECTION_BADNODES = range(3)
        self._current_section = self._SECTION_NONE

    def action_reset(self):
        self._current_section = self._SECTION_NONE

    def read_line(self, process, string, eof, error):
        if self.kadeployer.out:
            print string,
        if _ksoh_good_nodes_header_re.search(string) != None:
            self._current_section = self._SECTION_GOODNODES
            return
        if _ksoh_bad_nodes_header_re.search(string) != None:
            self._current_section = self._SECTION_BADNODES
            return
        if self._current_section == self._SECTION_GOODNODES:
            so = _ksoh_good_node_re.search(string)
            if so != None:
                host_address = so.group(1)
                self.kadeployer.good_hosts.add(host_address)
        elif self._current_section == self._SECTION_BADNODES:
            so = _ksoh_bad_node_re.search(string)
            if so != None:
                host_address = so.group(1)
                self.kadeployer.bad_hosts.add(host_address)

class _KadeployStderrHandler(ProcessOutputHandler):

    """Parse kadeploy3 stderr."""

    def __init__(self, kadeployer):
        """
        :param kadeployer: the `execo_g5k.kadeploy.Kadeployer` to
          which this `execo.process.ProcessOutputHandler` is attached.
        """
        super(_KadeployStderrHandler, self).__init__()
        self.kadeployer = kadeployer

    def read_line(self, process, string, eof, error):
        if self.kadeployer.out:
            print string,

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
    #
    # - special bahavior: fallback to default frontend if unable to
    #   extract site from host name
    #
    # - raises exception if host name format invalid (not sure we
    #   would want this for a generic function
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

class Kadeployer(Remote):

    """Deploy an environment with kadeploy3 on several hosts.

    Able to deploy in parallel to multiple frontends.
    """

    def __init__(self, deployment, frontend_connection_params = None):
        """
        :param deployment: instance of Deployment class describing the
          intended kadeployment.

        :param frontend_connection_params: connection params for
          connecting to frontends if needed. Values override those in
          `execo_g5k.config.default_frontend_connection_params`.
        """
        super(Remote, self).__init__()
        self.good_hosts = set()
        """Iterable of `Host` containing the deployed hosts. This iterable won't be
        complete if the Kadeployer has not terminated."""
        self.bad_hosts = set()
        """Iterable of `Host` containing the hosts not deployed. This iterable
        won't be complete if the Kadeployer has not terminated."""
        self.out = False
        """If True, output kadeploy stdout / stderr to stdout."""
        self.frontend_connection_params = frontend_connection_params
        """Connection params for connecting to frontends if needed. Values override
        those in `execo_g5k.config.default_frontend_connection_params`."""
        self.deployment = deployment
        """Instance of Deployment class describing the intended kadeployment."""
        self._init_processes()
        self.name = "%s on %i hosts / %i frontends" % (self.__class__.__name__, len(self._fhosts), len(self.processes))

    def _init_processes(self):
        self.processes = []
        self._fhosts = get_hosts_set(self.deployment.hosts)
        frontends = dict()
        for host in self._fhosts:
            frontend = _get_host_frontend(host)
            if frontends.has_key(frontend):
                frontends[frontend].append(host)
            else:
                frontends[frontend] = [host]
        lifecycle_handler = ActionNotificationProcessLH(self, len(frontends))
        for frontend in frontends.keys():
            kadeploy_command = self.deployment._get_common_kadeploy_command_line()
            for host in frontends[frontend]:
                kadeploy_command += " -m %s" % (host.address,)
            p = get_process(kadeploy_command,
                            host = get_frontend_host(frontend),
                            connection_params = make_connection_params(self.frontend_connection_params,
                                                                     default_frontend_connection_params))
            p.pty = True
            kdstdouthandler = _KadeployStdoutHandler(self)
            p.stdout_handlers.append(kdstdouthandler)
            kdstderrhandler = _KadeployStderrHandler(self)
            p.stderr_handlers.append(kdstderrhandler)
            p.lifecycle_handlers.append(lifecycle_handler)
            self.processes.append(p)

    def _common_reset(self):
        super(Kadeployer, self)._common_reset()
        self.good_hosts = set()
        self.bad_hosts = set()

    def _args(self):
        return [ repr(self.deployment) ] + Action._args(self) + Kadeployer._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self.frontend_connection_params: kwargs.append("frontend_connection_params=%r" % (self.frontend_connection_params,))
        return kwargs

    def _infos(self):
        return Remote._infos(self) + [ "cmds=%r" % ([ process.cmd for process in self.processes],),
                                       "deployed_hosts=%r" % (self.good_hosts,),
                                       "error_hosts=%r" % (self.bad_hosts,) ]

    def ok(self):
        ok = super(Kadeployer, self).ok
        if self.ended:
            if len(self.good_hosts.intersection(self.bad_hosts)) != 0:
                ok = False
            if len(self.good_hosts.union(self.bad_hosts).symmetric_difference(self._fhosts)) != 0:
                ok = False
        return ok

def kadeploy(deployment, frontend_connection_params = None, timeout = None, out = False):
    """Deploy hosts with kadeploy3.

    :param deployment: instance of Deployment class describing the
      intended kadeployment.

    :param frontend_connection_params: connection params for connecting
      to frontends if needed. Values override those in
      `execo_g5k.config.default_frontend_connection_params`.

    :param timeout: deployment timeout. None (which is the default
      value) means no timeout.

    :param out: if True, output kadeploy stdout / stderr to stdout.

    Returns a tuple (iterable of `execo.host.Host` containing the
    deployed host, iterable of `execo.host.Host` containing the nodes
    not deployed).
    """
    kadeployer = Kadeployer(deployment,
                            frontend_connection_params = frontend_connection_params)
    kadeployer.out = out
    kadeployer.run()
    if not kadeployer.ok:
        logoutput = style.emph("deployment error:") + " %s\n" % (kadeployer,) + style.emph("kadeploy processes:\n")
        for p in kadeployer.processes:
            logoutput += "%s\n" % (p,)
            logoutput += style.emph("stdout:") + "\n%s\n" % (p.stdout)
            logoutput += style.emph("stderr:") + "\n%s\n" % (p.stderr)
        logger.error(logoutput)
    return (kadeployer.good_hosts, kadeployer.bad_hosts)

def deploy(deployment,
           check_deployed_command = True,
           node_connection_params = {'user': 'root'},
           num_tries = 2,
           check_enough_func = None,
           frontend_connection_params = None,
           deploy_timeout = None,
           check_timeout = 30,
           timeout = False,
           out = False):
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

    :param timeout: timeout for g5k operations, except deployment.
      Default is False, which means use
      ``execo_g5k.config.g5k_configuration['default_timeout']``. None
      means no timeout.

    :param out: if True, output kadeploy stdout / stderr to stdout.
    """

    if isinstance(timeout, bool) and timeout == False:
        timeout = g5k_configuration.get('default_timeout')

    if check_enough_func == None:
        check_enough_func = lambda deployed, undeployed: len(undeployed) == 0

    if check_deployed_command == True:
        check_deployed_command = g5k_configuration.get('check_deployed_command')

    def check_update_deployed(deployed_hosts, undeployed_hosts, check_deployed_command, node_connection_params, vlan): #IGNORE:W0613
        logger.info(style.emph("check which hosts are already deployed among:") + " %s", undeployed_hosts)
        deployment_hostnames_mapping = dict()
        if vlan:
            for host in undeployed_hosts:
                deployment_hostnames_mapping[get_kavlan_host_name(host, vlan)] = host
        else:
            for host in undeployed_hosts:
                deployment_hostnames_mapping[host.address] = host
        deployed_check = get_remote(check_deployed_command,
                                    deployment_hostnames_mapping.keys(),
                                    connection_params = node_connection_params)
        for p in deployed_check.processes:
                p.nolog_exit_code = True
                p.nolog_timeout = True
                p.nolog_error = True
        deployed_check.run()
        newly_deployed = list()
        for process in deployed_check.processes:
            logger.debug(style.emph("check on %s:" % (process.host,))
                         + " %s\n" % (process,)
                         + style.emph("stdout:") + "\n%s\n" % (process.stdout)
                         + style.emph("stderr:") + "\n%s\n" % (process.stderr))
            if (process.ok):
                newly_deployed.append(deployment_hostnames_mapping[process.host.address])
                logger.info("OK %s", deployment_hostnames_mapping[process.host.address])
            else:
                logger.info("KO %s", deployment_hostnames_mapping[process.host.address])
        return newly_deployed

    start_time = time.time()
    deployed_hosts = set()
    undeployed_hosts = get_hosts_set(deployment.hosts)
    my_newly_deployed = []
    if check_deployed_command:
        my_newly_deployed = check_update_deployed(deployed_hosts, undeployed_hosts, check_deployed_command, node_connection_params, deployment.vlan)
        deployed_hosts.update(my_newly_deployed)
        undeployed_hosts.difference_update(my_newly_deployed)
    num_tries_done = 0
    elapsed = time.time() - start_time
    last_time = time.time()
    deploy_stats = list()   # contains tuples ( timestamp,
                            #                   num attempted deploys,
                            #                   len(kadeploy_newly_deployed),
                            #                   len(my_newly_deployed),
                            #                   len(deployed_hosts),
                            #                   len(undeployed_hosts )
    deploy_stats.append((elapsed, None, None, len(my_newly_deployed), len(deployed_hosts), len(undeployed_hosts)))
    while (not check_enough_func(deployed_hosts, undeployed_hosts)
           and num_tries_done < num_tries):
        num_tries_done += 1
        logger.info(style.emph("try %i, deploying on:" % (num_tries_done,)) + " %s", undeployed_hosts)
        tmp_deployment = copy.copy(deployment)
        tmp_deployment.hosts = undeployed_hosts
        kadeploy_newly_deployed, _ = kadeploy(tmp_deployment,
                                              frontend_connection_params = frontend_connection_params,
                                              out = out)
        my_newly_deployed = []
        if check_deployed_command:
            my_newly_deployed = check_update_deployed(deployed_hosts, undeployed_hosts, check_deployed_command, node_connection_params, deployment.vlan)
            deployed_hosts.update(my_newly_deployed)
            undeployed_hosts.difference_update(my_newly_deployed)
        else:
            deployed_hosts.update(kadeploy_newly_deployed)
            undeployed_hosts.difference_update(kadeploy_newly_deployed)
        logger.info(style.emph("kadeploy reported newly deployed hosts:") + "   %s", kadeploy_newly_deployed)
        logger.info(style.emph("check reported newly deployed hosts:") + "   %s", my_newly_deployed)
        logger.info(style.emph("all deployed hosts:") + "     %s", deployed_hosts)
        logger.info(style.emph("still undeployed hosts:") + " %s", undeployed_hosts)
        elapsed = time.time() - last_time
        last_time = time.time()
        deploy_stats.append((elapsed,
                             len(tmp_deployment.hosts),
                             len(kadeploy_newly_deployed),
                             len(my_newly_deployed),
                             len(deployed_hosts),
                             len(undeployed_hosts)))

    logger.info(style.emph("deploy finished") + " in %i tries, %s", num_tries_done, format_seconds(time.time() - start_time))
    logger.info("deploy  duration  attempted  deployed     deployed     total     total")
    logger.info("                  deploys    as reported  as reported  already   still")
    logger.info("                             by kadeploy  by check     deployed  undeployed")
    logger.info("---------------------------------------------------------------------------")
    for (deploy_index, deploy_stat) in enumerate(deploy_stats):
        logger.info("#%-5.5s  %-8.8s  %-9.9s  %-11.11s  %-11.11s  %-8.8s  %-10.10s",
            deploy_index,
            format_seconds(deploy_stat[0]),
            deploy_stat[1],
            deploy_stat[2],
            deploy_stat[3],
            deploy_stat[4],
            deploy_stat[5])
    logger.info(style.emph("deployed hosts:") + " %s", deployed_hosts)
    logger.info(style.emph("undeployed hosts:") + " %s", undeployed_hosts)

    return (deployed_hosts, undeployed_hosts)

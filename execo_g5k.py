# -*- coding: utf-8 -*-

r"""Tools and extensions to execo suitable for use in Grid5000

Detailed description
--------------------

important exported classes:

- `Kadeployer`

important exported functions:

- `get_current_oar_jobs`

- `get_current_oargrid_jobs`

- `get_oar_job_start_time`

- `wait_oar_job_start`

- `get_oargrid_job_start_time`

- `wait_oargrid_job_start`

- `get_oar_job_nodes`

- `get_oargrid_job_nodes`

- `kadeploy`

- `prepare_xp`

Configuration
-------------

This module may be configured at import time by defining two dicts
``g5k_configuration`` and ``default_frontend_connexion_params`` in the
file ``~/.execo_conf.py``

The ``g5k_configuration`` dict contains global g5k configuration
parameters. It's default values are::

  g5k_configuration = {
      'default_environment_name': None,
      'default_environment_file': None,
      'default_timeout': 300,
      }

The ``default_frontend_connexion_params`` dict contains default
parameters for remote connexions to grid5000 frontends. It's default
values are::

  default_frontend_connexion_params = {
      'user':        None,
      'keyfile':     None,
      'port':        None,
      'ssh':         ('ssh',),
      'scp':         ('scp',),
      'ssh_options': ('-o', 'BatchMode=yes',
                      '-o', 'PasswordAuthentication=no',
                      '-o', 'StrictHostKeyChecking=no',
                      '-o', 'UserKnownHostsFile=/dev/null',
                      '-o', 'ConnectTimeout=20'),
      'scp_options': ('-o', 'BatchMode=yes',
                      '-o', 'PasswordAuthentication=no',
                      '-o', 'StrictHostKeyChecking=no',
                      '-o', 'UserKnownHostsFile=/dev/null',
                      '-o', 'ConnectTimeout=20', '-rp'),
      }

Author
------

matthieu.imbert@inria.fr
SED INRIA Rhone-Alpes
December 2009
"""

from execo import *
import operator

g5k_configuration = {
    'default_environment_name': None,
    'default_environment_file': None,
    'default_timeout': 300,
    }
"""Global Grid5000 configuration parameters.

- ``default_environment_name``: a default environment name to use for
  deployments (as registered to kadeploy3).

- ``default_environment_file``: a default environment file to use for
  deployments (for kadeploy3).

- ``default_timeout``: default timeout for all calls to g5k services.
"""

default_oarsh_oarcp_params = {
    'user':        None,
    'keyfile':     None,
    'port':        None,
    'ssh':         ('oarsh',),
    'scp':         ('oarcp',),
    'ssh_options': ('-o', 'BatchMode=yes', '-o', 'PasswordAuthentication=no', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', '-o', 'ConnectTimeout=20'),
    'scp_options': ('-o', 'BatchMode=yes', '-o', 'PasswordAuthentication=no', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', '-o', 'ConnectTimeout=20', '-rp'),
}
"""A convenient, predefined connexion paramaters dict with oarsh / oarcp configuration.

See `execo.default_connexion_params`
"""

default_frontend_connexion_params = default_ssh_scp_params.copy()
"""Default connexion params when connecting to a Grid5000 frontend."""

read_user_configuration_dicts(((g5k_configuration, 'g5k_configuration'), (default_frontend_connexion_params, 'default_frontend_connexion_params')))

def _get_local_site():
    """Return the name of the local site."""
    try:
        local_site = re.search("^[^ \t\n\r\f\v\.]+\.([^ \t\n\r\f\v\.]+)\.grid5000.fr$", socket.getfqdn()).group(1)
    except:
        raise EnvironmentError, "unable to get local site name"
    return local_site

class _KadeployOutputHandler(ProcessOutputHandler):

    """Parse kadeploy3 output."""
    
    def __init__(self, kadeployer):
        """
        :Parameters:
          kadeployer
            the `Kadeployer` to which this `ProcessOutputHandler` is attached.
        """
        self._kadeployer = kadeployer
        self._good_nodes_header_re = re.compile("^Nodes correctly deployed on cluster \w+$")
        self._bad_nodes_header_re = re.compile("^Nodes not correctly deployed on cluster \w+$")
        self._node_re = re.compile("^\s*(\S+)\s*$")
        self._SECTION_NONE, self._SECTION_GOODNODES, self._SECTION_BADNODES = range(3)
        self._current_section = self._SECTION_NONE

    @line_buffered
    def read(self, process, string, eof = False, error = False):
        if self._good_nodes_header_re.search(string) != None:
            self._current_section = self._SECTION_GOODNODES
            return
        if self._bad_nodes_header_re.search(string) != None:
            self._current_section = self._SECTION_BADNODES
            return
        if self._current_section == self._SECTION_GOODNODES or self._current_section == self._SECTION_BADNODES:
            so = self._node_re.search(string)
            if so != None:
                host_address = so.group(1)
                if self._current_section == self._SECTION_GOODNODES:
                    self._kadeployer._add_good_host_address(host_address)
                if self._current_section == self._SECTION_BADNODES:
                    self._kadeployer._add_bad_host_address(host_address)

    def __repr__(self):
        return "<_KadeployOutputHandler(...)>"

class Kadeployer(Remote):

    """Deploy an environment with kadeploy3 on several nodes.

    Able to deploy in parallel to multiple Grid5000 sites.
    """

    def __init__(self, hosts = None, environment_name = None, environment_file = None, connexion_params = None, **kwargs):
        """
        :Parameters:
          hosts
            an iterable of `Host` to deploy
          environment_name
            name of the environment as registered to kadeploy3
          environment_file
            path of an environment description for kadeploy3
          connexion_params
            a dict similar to `default_frontend_connexion_params`
            whose values will override those in
            `default_frontend_connexion_params` for connexion.

        there must be either one of environment_name or
        environment_file parameter given. If none given, will try to
        use the default environement from `g5k_configuration`.
        """
        if (environment_name == None and environment_file == None):
            environment_name = g5k_configuration['default_environment_name']
            environment_file = g5k_configuration['default_environment_file']
        if (environment_name != None and environment_file != None):
            raise ValueError, "must provide either an environment_name or an environment_file"
        if not kwargs.has_key('name') or kwargs['name'] == None:
            kwargs['name'] = "%s %s on %s" % (self.__class__.__name__, environment_name or environment_file, hosts)
        super(Remote, self).__init__(**kwargs)
        self._connexion_params = connexion_params
        self._fhosts = get_frozen_hosts_set(hosts)
        self._good_hosts = set()
        self._bad_hosts = set()
        searchre1 = re.compile("^[^ \t\n\r\f\v\.]+\.([^ \t\n\r\f\v\.]+)\.grid5000.fr$")
        searchre2 = re.compile("^[^ \t\n\r\f\v\.]+\.([^ \t\n\r\f\v\.]+)$")
        searchre3 = re.compile("^[^ \t\n\r\f\v\.]+$")
        self._environment_name = environment_name
        self._environment_file = environment_file
        sites = dict()
        for host in self._fhosts:
            site = None
            mo1 = searchre1.search(host.address)
            if mo1 != None:
                site = mo1.group(1)
            else:
                mo2 = searchre2.search(host.address)
                if mo2 != None:
                    site = mo1.group(1)
                else:
                    mo3 = searchre3.search(host.address)
                    if mo3 != None:
                        site = _get_local_site()
                    else:
                        raise ValueError, "unknown grid5000 site for host %s" % host.address
            if sites.has_key(site):
                sites[site].append(host)
            else:
                sites[site] = [host]
        self._processes = dict()
        if connexion_params == None: connexion_params = default_frontend_connexion_params
        for site in sites.keys():
            kadeploy_command = "kadeploy3 -d"
            if self._environment_name != None:
                kadeploy_command += " -e %s" % self._environment_name
            elif self._environment_file != None:
                kadeploy_command += " -a %s" % self._environment_file
            for host in sites[site]:
                kadeploy_command += " -m %s" % host.address
            if site == _get_local_site():
                self._processes[site] = Process(kadeploy_command, stdout_handler = _KadeployOutputHandler(self), timeout = self._timeout, ignore_exit_code = self._ignore_exit_code, ignore_timeout = self._ignore_timeout)
            else:
                real_command = get_ssh_command(connexion_params = connexion_params) + (site,) + (kadeploy_command,)
                self._processes[site] = Process(real_command, stdout_handler = _KadeployOutputHandler(self), timeout = self._timeout, shell = False, ignore_exit_code = self._ignore_exit_code)

    def __repr__(self):
        r = style("Kadeployer", 'object_repr') + "(name=%r, timeout=%r" % (self._name, self._timeout)
        if self._environment_name: r += ", environment_name=%r" % (self._environment_name,)
        if self._environment_file: r += ", environment_file=%r" % (self._environment_file,)
        r += ", connexion_params=%r, ignore_exit_code=%r, ignore_timeout=%r)" % (self._connexion_params, self._ignore_exit_code, self._ignore_timeout)
        return r

    def __str__(self):
        r = "<" + style("Kadeployer", 'object_repr') + "(name=%r, timeout=%r" % (self._name, self._timeout)
        if self._environment_name: r += ", environment_name=%r" % (self._environment_name,)
        if self._environment_file: r += ", environment_file=%r" % (self._environment_file,)
        r += ", connexion_params=%r, ignore_exit_code=%r, ignore_timeout=%r, cmds=%r, deployed_hosts=%r error_hosts=%r)>" % (self._connexion_params, self._ignore_exit_code, self._ignore_timeout, [ process.cmd() for process in self._processes.values()], self._good_hosts, self._bad_hosts)
        return r

    def _add_good_host_address(self, host_address):
        """Add a host to the deployed hosts list. Intended to be called from the `ProcessOutputHandler`."""
        self._good_hosts.add(FrozenHost(host_address))

    def _add_bad_host_address(self, host_address):
        """Add a host to the hosts not deployed list. Intended to be called from the `ProcessOutputHandler`."""
        self._bad_hosts.add(FrozenHost(host_address))

    def get_deploy_hosts(self):
        """Return an iterable of `FrozenHost` containing the hosts that have to be deployed."""
        return list(self._fhosts)

    def get_deployed_hosts(self):
        """Return an iterable of `FrozenHost` containing the deployed hosts.

        this iterable won't be complete if `Kadeployer` has not
        terminated.
        """
        return list(self._good_hosts)

    def get_error_hosts(self):
        """Return an iterable of `FrozenHost` containing the hosts not deployed.

        this iterable won't be complete if `Kadeployer` has not
        terminated.
        """
        return list(self._fhosts.difference(self._good_hosts))

    def error(self):
        error = super(Kadeployer, self).error()
        if self.ended():
            if len(self._good_hosts.intersection(self._bad_hosts)) != 0:
                error = True
            if len(self._good_hosts.union(self._bad_hosts).symmetric_difference(self._fhosts)) != 0:
                error = True
        return error

def get_current_oar_jobs(sites = None, local = True, connexion_params = None, timeout = g5k_configuration['default_timeout']):
    """Return a list of current active oar job ids. The list contains tuples (oarjob id, site), with site == None for local site.

    :Parameters:
      sites
        an iterable of sites to connect to.
      local
        boolean indicating if we retrieve from local site
      connexion_params
        connexion params to connect to other site's frontend if needed
      timeout
        timeout for retrieving. default:
        ``g5k_configuration['default_timeout']``
    """
    if connexion_params == None:
        connexion_params = default_frontend_connexion_params
    processes = []
    if local:
        cmd = "oarstat -u"
        process = Process(cmd, timeout = timeout)
        process.site = None
        processes.append(process)
    if sites:
        for site in sites:
            remote_cmd = "oarstat -u"
            cmd = get_ssh_command(connexion_params = connexion_params) + (site,) + (remote_cmd,)
            process = Process(cmd, timeout = timeout, shell = False)
            process.site = site
            processes.append(process)
    oar_job_ids = []
    if len(processes) == 0:
        return oar_job_ids
    map(Process.start, processes)
    map(Process.wait, processes)
    if reduce(operator.and_, map(Process.ok, processes)):
        for process in processes:
            jobs = re.findall("^(\d+)\s", process.stdout(), re.MULTILINE)
            oar_job_ids.extend([ (int(jobid), process.site) for jobid in jobs ])
        return oar_job_ids
    raise Exception, "error list of current oar jobs: %s" % (processes,)

def get_current_oargrid_jobs(timeout = g5k_configuration['default_timeout']):
    """Return a list of current active oargrid job ids.

    :Parameters:
      timeout
        timeout for retrieving. default:
        ``g5k_configuration['default_timeout']``
    """
    cmd = "oargridstat"
    process = Process(cmd, timeout = timeout).run()
    if process.ok():
        jobs = re.findall("^Reservation # (\d+):$", process.stdout(), re.MULTILINE)
        oargrid_job_ids = map(int, jobs)
        return oargrid_job_ids
    raise Exception, "error list of current oargrid jobs: %s" % (process,)

def get_oar_job_start_time(oar_job_id = None, site = None, connexion_params = None, timeout = g5k_configuration['default_timeout']):
    """Return a unix timestamp of an oar job start time.

    :Parameters:
      oar_job_id
        the oar job id. If None given, will try to get it from
        ``OAR_JOB_ID`` environment variable.
      site
        the Grid5000 site of the oar job. If None given, assume local
        oar job (only works if run on the local frontend).
      connexion_params
        connexion params to connect to other site's frontend in case
        the oar job is on a remote site (default:
        `default_frontend_connexion_params`)
      timeout
        timeout for retrieving. default:
        ``g5k_configuration['default_timeout']``
    """
    if oar_job_id == None:
        if os.environ.has_key('OAR_JOB_ID'):
            oar_job_id = os.environ['OAR_JOB_ID']
        else:
            raise ValueError, "no oar job id given and no OAR_JOB_ID environment variable found"
    if site != None:
        remote_cmd = "oarstat -fj %i" % oar_job_id
        if connexion_params == None:
            connexion_params = default_frontend_connexion_params
        cmd = get_ssh_command(connexion_params = connexion_params) + (site,) + (remote_cmd,)
        process = Process(cmd, timeout = timeout, shell = False)
    else:
        cmd = "oarstat -fj %i" % oar_job_id
        process = Process(cmd, timeout = timeout)
    process.run()
    if process.ok():
        result = re.search("^\s*startTime = (\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d)$", process.stdout(), re.MULTILINE)
        if result:
            start_time = time.mktime(time.strptime(result.group(1), "%Y-%m-%d %H:%M:%S"))
            return start_time
    raise Exception, "error retrieving info for oar job %i on site %s: %s" % (oar_job_id, site, process)

def wait_oar_job_start(oar_job_id = None, site = None, connexion_params = None, timeout = g5k_configuration['default_timeout']):
    """Sleep until an oar job's start time.

    :Parameters:
      oar_job_id
        the oar job id. If None given, will try to get it from
        ``OAR_JOB_ID`` environment variable.
      site
        the Grid5000 site of the oar job. If None given, assume local
        oar job (only works if run on the local frontend).
      connexion_params
        connexion params to connect to other site's frontend in case
        the oar job is on a remote site (default:
        `default_frontend_connexion_params`)
      timeout
        timeout for retrieving. default:
        ``g5k_configuration['default_timeout']``
    """
    sleep_until(get_oar_job_start_time(oar_job_id, site, connexion_params, timeout))
    
def get_oargrid_job_start_time(oargrid_job_id = None, timeout = g5k_configuration['default_timeout']):
    """Return a unix timestamp of an oargrid job start time.

    :Parameters:
      oargrid_job_id
        the oargrid job id.
      timeout
        timeout for retrieving. default:
        ``g5k_configuration['default_timeout']``
    """
    cmd = "oargridstat %i" % oargrid_job_id
    process = Process(cmd, timeout = timeout)
    process.run()
    if process.ok():
        result = re.search("^start date : (\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d)$", process.stdout(), re.MULTILINE)
        if result:
            start_time = time.mktime(time.strptime(result.group(1), "%Y-%m-%d %H:%M:%S"))
            return start_time
    raise Exception, "error retrieving info for oargrid job %i: %s" % (oargrid_job_id, process)

def wait_oargrid_job_start(oargrid_job_id = None, timeout = g5k_configuration['default_timeout']):
    """Sleep until an oargrid job's start time.

    :Parameters:
      oargrid_job_id
        the oargrid job id.
      timeout
        timeout for retrieving. default:
        ``g5k_configuration['default_timeout']``
    """
    sleep_until(get_oargrid_job_start_time(oargrid_job_id, timeout))

def get_oar_job_nodes(oar_job_id = None, site = None, connexion_params = None, timeout = g5k_configuration['default_timeout']):
    """Return an iterable of `FrozenHost` containing the hosts of an oar job.

    :Parameters:
      oar_job_id
        the oar job id. If None given, will try to get it from
        ``OAR_JOB_ID`` environment variable.
      site
        the Grid5000 site of the oar job. If None given, assume local
        oar job (only works if run on the local frontend).
      connexion_params
        connexion params to connect to other site's frontend in case
        the oar job is on a remote site (default:
        `default_frontend_connexion_params`)
      timeout
        timeout for retrieving. default:
        ``g5k_configuration['default_timeout']``
    """
    if oar_job_id == None:
        if os.environ.has_key('OAR_JOB_ID'):
            oar_job_id = os.environ['OAR_JOB_ID']
        else:
            raise ValueError, "no oar job id given and no OAR_JOB_ID environment variable found"
    if site != None:
        remote_cmd = "while (oarstat -sj %(oar_job_id)i | grep Waiting) > /dev/null 2>&1 ; do sleep 5 ; done ; if (oarstat -sj %(oar_job_id)i | grep Running) > /dev/null 2>&1 ; then oarstat -pj %(oar_job_id)i | oarprint host -f - ; else false ; fi" % {'oar_job_id': oar_job_id}
        if connexion_params == None:
            connexion_params = default_frontend_connexion_params
        cmd = get_ssh_command(connexion_params = connexion_params) + (site,) + (remote_cmd,)
        process = Process(cmd, timeout = timeout, shell = False)
    else:
        cmd = "while (oarstat -sj %(oar_job_id)i | grep Waiting) > /dev/null 2>&1 ; do sleep 5 ; done ; if (oarstat -sj %(oar_job_id)i | grep Running) > /dev/null 2>&1 ; then oarstat -pj %(oar_job_id)i | oarprint host -f - ; else false ; fi" % {'oar_job_id': oar_job_id}
        process = Process(cmd, timeout = timeout)
    process.run()
    if process.ok():
        host_addresses = re.findall("^\s*(\S+)\s*$", process.stdout(), re.MULTILINE)
        hosts = set()
        for host_address in host_addresses:
            hosts.add(FrozenHost(host_address))
        return hosts
    raise Exception, "error retrieving nodes list for oar job %i on site %s: %s" % (oar_job_id, site, process)

def get_oargrid_job_nodes(oargrid_job_id, timeout = g5k_configuration['default_timeout']):
    """Return an iterable of `FrozenHost` containing the hosts of an oargrid job.

    :Parameters:
      oargrid_job_id
        the oargrid job id.
      timeout
        timeout for retrieving. default:
        ``g5k_configuration['default_timeout']``
    """
    cmd = "oargridstat -wl %i" % oargrid_job_id
    process = Process(cmd, timeout = timeout, pty = True)
    process.run()
    if process.ok():
        host_addresses = re.findall("^\s*(\S+)\s*$", process.stdout(), re.MULTILINE)
        hosts = set()
        for host_address in host_addresses:
            hosts.add(FrozenHost(host_address))
        return hosts
    raise Exception, "error retrieving nodes list for oargrid job %i: %s" % (oargrid_job_id, process)

def kadeploy(hosts = None, environment_name = None, environment_file = None):
    """Deploy hosts with kadeploy3.

    :Parameters:
      hosts
        iterable of `Host` to deploy.
      environment_name
        name of an environment registered to kadeploy3.
      environment_file
        name of an environment file for kadeploy3.

    Returns a tuple (iterable of `FrozenHost` containing the deployed
    host, iterable of `FrozenHost` containing the nodes not deployed).
    """
    kadeployer = Kadeployer(hosts = hosts, environment_name = environment_name, environment_file = environment_file).run()
    if kadeployer.error():
        raise Exception, "error deploying nodes: %s" % (kadeployer,)
    return (kadeployer.get_deployed_hosts(), kadeployer.get_error_hosts())

def prepare_xp(oar_job_id_tuples = None, oargrid_job_ids = None, hosts = None, environment_name = None, environment_file = None, connexion_params = None, check_deployed_command = 'true', num_deploy_retries = 2):

    """Wait for jobs start date, get hosts list, deploy them if needed.

    - Given a list of oar/oargrid job ids, will wait until all these
      jobs have started, then retrieves the list of Hosts of all these
      jobs.

    - also takes explicit nodes list

    - loop `num_deploy_retries` times:

      - try to connect to these hosts using the supplied
        `connexion_params` (or the default ones), and to execute the
        given `check_deployed_command`. If connexion succeeds and the
        command returns 0, the host is assumed to be deployed, else it
        is assumed to be undeployed.

      - deploy the undeployed nodes

    :Parameters:
      oar_job_id_tuples
        iterable of tuple (oar job id, g5k site). Put None as g5k site
        for local site.
      oargrid_job_ids
        iterable of oargrid job id.
      hosts
        iterable of `Host`.
      environment_name
        name of an environment registered to kadeploy3.
      environment_file
        name of an environment file for kadeploy3.
      connexion_params
        a dict similar to `execo.default_connexion_params` whose
        values will override those in `execo.default_connexion_params`
        when connecting to check node deployment.
      check_deployed_command
        command to perform remotely to check node deployement. This
        command should return 0 if the node is correctly deployed, or
        another value otherwise.
      num_deploy_retries
        number of deploy retries
    """

    # get hosts list
    all_hosts = set()
    if hosts:
        all_hosts.add(hosts)
    if oar_job_id_tuples != None:
        for (job_id, site) in oar_job_id_tuples:
            wait_oar_job_start(job_id, site)
            all_hosts.update(get_oar_job_nodes(job_id, site))
    if oargrid_job_ids != None:
        for job_id in oargrid_job_ids:
            wait_oargrid_job_start(job_id)
            all_hosts.update(get_oargrid_job_nodes(job_id))
    logger.info(style("hosts:", 'emph') + " %s" % (all_hosts,))

    # check deployed/undeployed hosts, and deploy those needed
    deployed_hosts = set()
    undeployed_hosts = set(all_hosts)
    while len(undeployed_hosts) != 0 and num_deploy_retries > 0:
        # check which of those hosts are deployed
        deployed_check = Remote(undeployed_hosts, check_deployed_command, connexion_params = connexion_params, ignore_exit_code = True)
        deployed_check.run()
        for (host, process) in deployed_check.get_hosts_processes().iteritems():
            if process.exit_code() == 0:
                undeployed_hosts.remove(host)
                deployed_hosts.add(host)
        logger.info(style("deployed hosts:", 'emph') + " %s" % (deployed_hosts,))
        logger.info(style("undeployed hosts:", 'emph') + " %s" % (undeployed_hosts,))
        if len(undeployed_hosts) != 0:
            # deploy undeployed hosts
            (newly_deployed_hosts, error_hosts) = kadeploy(undeployed_hosts, environment_name = environment_name, environment_file = environment_file)
        num_deploy_retries -= 1

    return (deployed_hosts, undeployed_hosts, all_hosts)

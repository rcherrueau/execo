# -*- coding: utf-8 -*-

r"""Tools and extensions to execo suitable for use in Grid5000."""

from execo import *
import operator, copy

# _STARTOF_ g5k_configuration
g5k_configuration = {
    'kadeploy3': 'kadeploy3',
    'kadeploy3_options': '-d',
    'default_env_name': None,
    'default_env_file': None,
    'default_timeout': 900,
    'check_deployed_command': "! (mount | grep -E '^/dev/[[:alpha:]]+2 on / ' || ps -u oar -o args | grep sshd)",
    }
# _ENDOF_ g5k_configuration
"""Global Grid5000 configuration parameters.

- ``kadeploy3``: kadeploy3 command.

- ``kadeploy3_options``: common kadeploy3 command line options.

- ``default_env_name``: a default environment name to use for
  deployments (as registered to kadeploy3).

- ``default_env_file``: a default environment file to use for
  deployments (for kadeploy3).

- ``default_timeout``: default timeout for all calls to g5k services
  (except deployments).

- ``check_deployed_command``: default shell command used by `deploy`
  to check that the nodes are correctly deployed. This command should
  return 0 if the node is correctly deployed, or another value
  otherwise. This default value checks that there is no ssh daemon
  running under user oar, and that the root is not on the second
  partition of the disk.
"""

# _STARTOF_ default_oarsh_oarcp_params
default_oarsh_oarcp_params = {
    'user':        None,
    'keyfile':     None,
    'port':        None,
    'ssh':         'oarsh',
    'scp':         'oarcp',
    'taktuk':      'taktuk',
    'ssh_options': ( '-o', 'BatchMode=yes',
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
    'ssh_scp_pty': True,
    }
# _ENDOF_ default_oarsh_oarcp_params
"""A convenient, predefined connexion paramaters dict with oarsh / oarcp configuration.

See `execo.default_connexion_params`
"""

default_frontend_connexion_params = default_connexion_params.copy()
"""Default connexion params when connecting to a Grid5000 frontend."""

read_user_configuration_dicts(((g5k_configuration, 'g5k_configuration'), (default_frontend_connexion_params, 'default_frontend_connexion_params'), (default_oarsh_oarcp_params, 'default_oarsh_oarcp_params')))

def _get_local_site():
    """Return the name of the local site."""
    try:
        local_site = re.search("^[^ \t\n\r\f\v\.]+\.([^ \t\n\r\f\v\.]+)\.grid5000.fr$", socket.gethostname()).group(1)
    except:
        raise EnvironmentError, "unable to get local site name"
    return local_site

class Deployment(object):
    """A kadeploy3 deployment.

    POD style class.

    members are:

    - hosts: iterable of hosts on which to deploy.

    - env_file:

    - env_name:

    - user:

    - other_options:

    there must be either one of env_name or env_file parameter
    given. If none given, will try to use the default environement
    from `g5k_configuration`.
    """

    def __init__(self,
                 hosts = None,
                 env_file = None,
                 env_name = None,
                 user = None,
                 other_options = None):
        self.hosts = hosts
        self.env_file = env_file
        self.env_name = env_name
        self.user = user
        self.other_options = other_options

    def _get_common_kadeploy_command_line(self):
        cmd_line = g5k_configuration['kadeploy3']
        cmd_line += " " + g5k_configuration['kadeploy3_options']
        if self.env_file and self.env_name:
            raise ValueError, "Deployment cannot have both env_file and env_name"
        if (not self.env_file) and (not self.env_name):
            if g5k_configuration.has_key('default_environment_name') and g5k_configuration.has_key('default_environment_file'):
                raise Exception, "g5k_configuration cannot have both default_environment_name and default_environment_file"
            if (not g5k_configuration.has_key('default_environment_name')) and (not g5k_configuration.has_key('default_environment_file')):
                raise Exception, "no environment name or file found"
            if g5k_configuration.has_key('default_environment_name'):
                cmd_line += " -e %s" % (g5k_configuration['default_environment_name'],)
            elif g5k_configuration.has_key('default_environment_file'):
                cmd_line += " -a %s" % (g5k_configuration['default_environment_file'],)
        elif self.env_name:
            cmd_line += " -e %s" % (self.env_name,)
        elif self.env_file:
            cmd_line += " -a %s" % (self.env_file,)
        if self.user != None:
            cmd_line += " -u %s" % (self.user,)
        if self.other_options:
            cmd_line += " %s" % (self.other_options,)
        return cmd_line

    def __repr__(self):
        s = "Deployment(hosts=%r" % (self.hosts,)
        if self.env_file != None: s += ", env_file=%r" % (self.env_file,)
        if self.env_name != None: s += ", env_name=%r" % (self.env_name,)
        if self.user != None: s += ", user=%r" % (self.user,)
        if self.other_options: s += ", other_options=%r" % (self.other_options,)
        s += ")"
        return s

class _KadeployOutputHandler(ProcessOutputHandler):

    """Parse kadeploy3 output."""
    
    def __init__(self, kadeployer):
        """
        :param kadeployer: the `Kadeployer` to which this
          `ProcessOutputHandler` is attached.
        """
        super(_KadeployOutputHandler, self).__init__()
        self._kadeployer = kadeployer
        self._good_nodes_header_re = re.compile("^Nodes correctly deployed on cluster \w+$")
        self._bad_nodes_header_re = re.compile("^Nodes not correctly deployed on cluster \w+$")
        self._node_re = re.compile("(\S+)(\s+\(.*\))?")
        self._SECTION_NONE, self._SECTION_GOODNODES, self._SECTION_BADNODES = range(3)
        self._current_section = self._SECTION_NONE

    def read_line(self, process, string, eof = False, error = False):
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

    def __init__(self, deployment, connexion_params = None, **kwargs):
        """
        :param deployment: instance of Deployment class describing the
          intended kadeployment.

        :param connexion_params: a dict similar to
          `default_frontend_connexion_params` whose values will
          override those in `default_frontend_connexion_params` for
          connexion.
        """
        if not kwargs.has_key('name') or kwargs['name'] == None:
            kwargs['name'] = "%s %s on %s" % (self.__class__.__name__, deployment.env_name or deployment.env_file, deployment.hosts)
        super(Remote, self).__init__(**kwargs)
        self._connexion_params = connexion_params
        self._deployment = deployment
        self._fhosts = get_frozen_hosts_set(deployment.hosts)
        self._good_hosts = set()
        self._bad_hosts = set()
        searchre1 = re.compile("^[^ \t\n\r\f\v\.]+\.([^ \t\n\r\f\v\.]+)\.grid5000.fr$")
        searchre2 = re.compile("^[^ \t\n\r\f\v\.]+\.([^ \t\n\r\f\v\.]+)$")
        searchre3 = re.compile("^[^ \t\n\r\f\v\.]+$")
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
        if connexion_params == None:
            connexion_params = default_frontend_connexion_params
        lifecycle_handler = ActionNotificationProcessLifecycleHandler(self, len(sites))
        for site in sites.keys():
            kadeploy_command = self._deployment._get_common_kadeploy_command_line()
            for host in sites[site]:
                kadeploy_command += " -m %s" % host.address
            if site == _get_local_site():
                self._processes[site] = Process(kadeploy_command,
                                                stdout_handler = _KadeployOutputHandler(self),
                                                timeout = self._timeout,
                                                ignore_exit_code = self._ignore_exit_code,
                                                ignore_timeout = self._ignore_timeout,
                                                process_lifecycle_handler = lifecycle_handler,
                                                pty = True)
            else:
                self._processes[site] = SshProcess(Host(site),
                                                   kadeploy_command,
                                                   connexion_params = connexion_params,
                                                   stdout_handler = _KadeployOutputHandler(self),
                                                   timeout = self._timeout,
                                                   ignore_exit_code = self._ignore_exit_code,
                                                   process_lifecycle_handler = lifecycle_handler,
                                                   pty = True)

    def __repr__(self):
        r = style("Kadeployer", 'object_repr') + "(name=%r, deployment=%r, timeout=%r" % (self._name,
                                                                                          self._deployment,
                                                                                          self._timeout)
        r += ", connexion_params=%r, ignore_exit_code=%r, ignore_timeout=%r)" % (self._connexion_params,
                                                                                 self._ignore_exit_code,
                                                                                 self._ignore_timeout)
        return r

    def __str__(self):
        r = "<" + style("Kadeployer", 'object_repr') + "(name=%r, deployment=%r, timeout=%r" % (self._name,
                                                                                                self._deployment,
                                                                                                self._timeout)
        r += ", connexion_params=%r, ignore_exit_code=%r, ignore_timeout=%r" % (self._connexion_params,
                                                                                self._ignore_exit_code,
                                                                                self._ignore_timeout)
        r += ", cmds=%r, deployed_hosts=%r error_hosts=%r)>" % ([ process.cmd() for process in self._processes.values()],
                                                                self._good_hosts, self._bad_hosts)
        return r

    def _add_good_host_address(self, host_address):
        """Add a host to the deployed hosts list. Intended to be called from the `ProcessOutputHandler`."""
        self._good_hosts.add(FrozenHost(host_address))

    def _add_bad_host_address(self, host_address):
        """Add a host to the hosts not deployed list. Intended to be called from the `ProcessOutputHandler`."""
        self._bad_hosts.add(FrozenHost(host_address))

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

def _convert_endpoint(endpoint):
    """Convert endpoint from `datetime.datetime` or `datetime.timedelta`, or deltat in seconds, to unix timestamp."""
    if endpoint != None:
        if isinstance(endpoint, datetime.datetime):
            endpoint = datetime_to_unixts(endpoint)
        elif isinstance(endpoint, datetime.timedelta):
            endpoint = time.time() + timedelta_to_seconds(endpoint)
        elif endpoint < 315532800: # timestamp before Jan 1 1980, assume it is a deltat (less than 10 years)
            endpoint = time.time() + endpoint
    return endpoint

def _date_in_range(date, range):
    """Check that a date is inside a range. If range is None, return True."""
    if range == None: return True
    if range[0] and date < range[0]:
        return False
    if range[1] and date > range[1]:
        return False
    return True

def format_oar_time(secs):
    """Return a string with the formatted time (year, month, day, hour, min, sec, ms) formatted for oar.

    timezone is discarded since oar doesn't know about them.

    :param secs: a unix timestamp (integer or float)
    """
    if secs == None:
        return None
    t = time.localtime(secs)
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", t)
    return formatted_time

def format_oar_duration(secs):
    """Return a string with a formatted duration (hours, mins, secs).

    :param secs: a duration in seconds (integer or float)
    """
    if secs == None:
        return None
    s = secs
    h = (s - (s % 3600)) / 3600
    s -= h * 3600
    m = (s - (s % 60)) / 60
    s -= m * 60
    formatted_duration = ""
    if secs >= 3600:
        formatted_duration += "%i:" % h
    else:
        formatted_duration += "0:"
    if secs >= 60:
        formatted_duration += "%i:" % m
    else:
        formatted_duration += "0:"
    formatted_duration += "%i" % s
    return formatted_duration

class OarSubmission(object):
    """An oar submission.

    POD style class.

    members are:

    - resources: Set the requested resources for the job. Oar option
      -l, without the walltime.

    - walltime: Job walltime. Walltime part of oar -l option.

    - job_type: Job type, oar option -t: deploy, besteffort, cosystem,
      checkpoint, timesharing.

    - sql_properties: constraints to properties for the job, oar
      option -p.

    - queue: the queue to submit the job to. Oar option -q.

    - reservation_date: Request that the job starts at a specified
      time. Oar option -r.

    - directory: Specify the directory where to launch the command
      (default is current directory). Oar option -d.

    - project: pecify a name of a project the job belongs to. Oar
      option --project.

    - name: Specify an arbitrary name for the job. Oar option -n.
    """

    def __init__(self,
                 resources = "nodes=1",
                 walltime = None,
                 job_type = None,
                 sql_properties = None,
                 queue = None,
                 reservation_date = None,
                 directory = None,
                 project = None,
                 name = None):
        if walltime != None:
            if isinstance(walltime, datetime.timedelta):
                walltime = timedelta_to_seconds(walltime)
            walltime = int(walltime)
        if reservation_date != None:
            if isinstance(reservation_date, datetime.datetime):
                reservation_date = datetime_to_unixts(reservation_date)
            reservation_date = int(reservation_date)
        self.resources = resources
        self.walltime = walltime
        self.job_type = job_type
        self.sql_properties = sql_properties
        self.queue = queue
        self.reservation_date = reservation_date
        self.directory = directory
        self.project = project
        self.name = name

    def __repr__(self):
        s = "OarSubmission(resources=%r" % (self.resources,)
        if self.walltime != None: s += ", walltime=%r" % (self.walltime,)
        if self.job_type != None: s += ", job_type=%r" % (self.job_type,)
        if self.sql_properties != None: s += ", sql_properties=%r" % (self.sql_properties,)
        if self.queue != None: s += ", queue=%r" % (self.queue,)
        if self.reservation_date != None: s += ", reservation_date=%r" % (self.reservation_date,)
        if self.directory != None: s += ", directory=%r" % (self.directory,)
        if self.project != None: s += ", project=%r" % (self.project,)
        if self.name != None: s += ", name=%r" % (self.name,)
        s += ")"
        return s

def oarsub(job_specs, connexion_params = None, timeout = False):
    """Submit jobs.

    :param job_specs: iterable of tuples (OarSubmission, site) with None
      for local site

    :param connexion_params: connexion params to connect to other
      site's frontend if needed
    
    :param timeout: timeout for retrieving. Default is False, which
      means use ``g5k_configuration['default_timeout']``. None means no
      timeout.

    Returns a list of tuples (oarjob id, site), with site == None for
    local site. If submission error, oarjob id == None. The returned
    list matches, in the same order, the job_specs parameter.
    """
    if timeout == False:
        timeout = g5k_configuration['default_timeout']
    if connexion_params == None:
        connexion_params = default_frontend_connexion_params
    processes = []
    for (spec, site) in job_specs:
        oarsub_cmdline = "oarsub -l %s" % (spec.resources,)
        if spec.walltime != None:
            oarsub_cmdline += ",walltime=%s" % (format_oar_duration(spec.walltime),)
        if os.environ.has_key('OAR_JOB_KEY_FILE'):
            oarsub_cmdline += " -k -i %s" % (os.environ['OAR_JOB_KEY_FILE'],)
        if spec.job_type != None:
            oarsub_cmdline += " -t '%s'" % (spec.job_type,)
        if spec.sql_properties != None:
            oarsub_cmdline += " -p \"%s\"" % (spec.sql_properties,)
        if spec.queue != None:
            oarsub_cmdline += " -q '%s'" % (spec.queue,)
        if spec.reservation_date != None:
            oarsub_cmdline += " -r '%s'" % (format_oar_time(spec.reservation_date),)
        if spec.directory != None:
            oarsub_cmdline += " -d '%s'" % (spec.directory,)
        if spec.project != None:
            oarsub_cmdline += " --project '%s'" % (spec.project,)
        if spec.name != None:
            oarsub_cmdline += " -n '%s'" % (spec.name,)
        oarsub_cmdline += " 'sleep 31536000'"
        if site == None:
            processes.append(Process(oarsub_cmdline,
                                     timeout = timeout,
                                     pty = True))
        else:
            processes.append(SshProcess(Host(site),
                                        oarsub_cmdline,
                                        connexion_params = connexion_params,
                                        timeout = timeout,
                                        pty = True))
    oar_job_ids = []
    if len(processes) == 0:
        return oar_job_ids
    map(Process.start, processes)
    map(Process.wait, processes)
    for process in processes:
        if isinstance(process, SshProcess):
            host = process.host().address
        else:
            host = None
        job_id = None
        if process.ok():
            mo = re.search("^OAR_JOB_ID=(\d+)$", process.stdout(), re.MULTILINE)
            if mo != None:
                job_id = int(mo.group(1))
        oar_job_ids.append((job_id, host))
    return oar_job_ids

def oardel(job_specs, connexion_params = None, timeout = False):
    """Delete oar jobs.

    Ignores any error, so you can delete inexistant jobs, already
    deleted jobs, or jobs that you don't own. Those deletions will be
    ignored.

    :param job_specs: iterable of tuples (job_id, site) with None for
      local site

    :param connexion_params: connexion params to connect to other
      site's frontend if needed
    
    :param timeout: timeout for retrieving. Default is False, which
      means use ``g5k_configuration['default_timeout']``. None means no
      timeout.
    """
    if timeout == False:
        timeout = g5k_configuration['default_timeout']
    if connexion_params == None:
        connexion_params = default_frontend_connexion_params
    processes = []
    for (job_id, site) in job_specs:
        oardel_cmdline = "oardel %i" % (job_id,)
        if site == None:
            processes.append(Process(oardel_cmdline,
                                     timeout = timeout,
                                     ignore_exit_code = True,
                                     pty = True))
        else:
            processes.append(SshProcess(Host(site),
                                        oardel_cmdline,
                                        connexion_params = connexion_params,
                                        timeout = timeout,
                                        ignore_exit_code = True,
                                        pty = True))
    map(Process.start, processes)
    map(Process.wait, processes)

def oargridsub(job_specs, reservation_date = None, walltime = None, job_type = None, queue = None, directory = None, timeout = False):
    """Submit oargrid jobs.

    :param job_specs: iterable of tuples (OarSubmission,
      clusteralias). Reservation date, walltime, queue, directory,
      project of the OarSubmission are ignored.

    :param reservation_date: grid job reservation date. Default: now.

    :param walltime: grid job walltime.

    :param job_type: type of job for all clusters: deploy, besteffort,
      cosystem, checkpoint, timesharing.

    :param queue: oar queue to use.

    :param directory: directory where the reservation will be
      launched.

    :param timeout: timeout for retrieving. Default is False, which
      means use ``g5k_configuration['default_timeout']``. None means no
      timeout.

    Returns a tuple (oargrid_job_id, ssh_key), or (None, None) if
    error.
    """
    if timeout == False:
        timeout = g5k_configuration['default_timeout']
    if reservation_date != None:
        if isinstance(reservation_date, datetime.datetime):
            reservation_date = datetime_to_unixts(reservation_date)
    else:
        reservation_date = datetime_to_unixts(datetime.datetime.utcnow())
    if walltime != None:
        if isinstance(walltime, datetime.timedelta):
            walltime = timedelta_to_seconds(walltime)
        walltime = int(walltime)
    oargridsub_cmdline = "oargridsub -v -s '%s' " % (format_oar_time(reservation_date),)
    if os.environ.has_key('OAR_JOB_KEY_FILE'):
        oargridsub_cmdline += " -i %s" % (os.environ['OAR_JOB_KEY_FILE'],)
    if queue != None:
        oargridsub_cmdline += "-q '%s' " % (queue,)
    if job_type != None:
        oargridsub_cmdline += "-t '%s' " % (job_type,)
    if walltime != None:
        oargridsub_cmdline += "-w '%s' " % (format_oar_duration(walltime),)
    if directory != None:
        oargridsub_cmdline += "-d '%s' " % (directory,)
    firstclusteralias = True
    for (spec, clusteralias) in job_specs:
        if firstclusteralias:
            firstclusteralias = False
        else:
            oargridsub_cmdline += ','
        oargridsub_cmdline += "%s:rdef='%s'" % (clusteralias, spec.resources)
        if spec.job_type != None:
            oargridsub_cmdline += ":type='%s'" % (spec.job_type,)
        if spec.sql_properties != None:
            oargridsub_cmdline += ":prop=\"%s\"" % (spec.sql_properties,)
        if spec.name != None:
            oargridsub_cmdline += ":name='%s'" % (spec.name,)
    process = Process(oargridsub_cmdline,
                      timeout = timeout,
                      pty = True)
    process.run()
    job_id = None
    ssh_key = None
    if process.ok():
        mo = re.search("^\[OAR_GRIDSUB\] Grid reservation id = (\d+)$", process.stdout(), re.MULTILINE)
        if mo != None:
            job_id = int(mo.group(1))
        mo = re.search("^\[OAR_GRIDSUB\] SSH KEY : (.*)$", process.stdout(), re.MULTILINE)
        if mo != None:
            ssh_key = mo.group(1)
    if job_id != None:
        return (job_id, ssh_key)
    else:
        return (None, None)

def oargriddel(job_ids, timeout = False):
    """Delete oargrid jobs.

    Ignores any error, so you can delete inexistant jobs, already
    deleted jobs, or jobs that you don't own. Those deletions will be
    ignored.

    :param job_ids: iterable of oar grid job ids.

    :param timeout: timeout for retrieving. Default is False, which
      means use ``g5k_configuration['default_timeout']``. None means no
      timeout.
    """
    if timeout == False:
        timeout = g5k_configuration['default_timeout']
    processes = []
    for job_id in job_ids:
        oargriddel_cmdline = "oargriddel %i" % (job_id,)
        processes.append(Process(oargriddel_cmdline,
                                 timeout = timeout,
                                 ignore_exit_code = True,
                                 pty = True))
    map(Process.start, processes)
    map(Process.wait, processes)

def get_current_oar_jobs(sites = None, local = True, start_between = None, end_between = None, connexion_params = None, timeout = False, abort_on_error = False):
    """Return a list of current active oar job ids.

    The list contains tuples (oarjob id, site), with site == None for
    local site.

    :param sites: an iterable of sites to connect to.

    :param local: boolean indicating if we retrieve from local site

    :param start_between: a tuple (low, high) of endpoints. Filters
      and returns only jobs whose start date is in between these
      endpoints. Each endpoint may be given as a `datetime.datetime`
      (absolute date), as a `datetime.timedelta` (delta from now), as
      an absolute unix timestamp, or as a delta from now in seconds
      (if unix timestamp before 315532800 (Jan 1 1980), then assume it
      is a deltat (less than 10 years)).
        
    :param end_between: a tuple (low, high) of endpoints. Filters and
      returns only jobs whose end date is in between these
      endpoints. Each endpoint may be given as a `datetime.datetime`
      (absolute date), as a `datetime.timedelta` (delta from now), as
      an absolute unix timestamp, or as a delta from now in seconds
      (if unix timestamp before 315532800 (Jan 1 1980), then assume it
      is a deltat, (less than 10 years)).
        
    :param connexion_params: connexion params to connect to other
      site's frontend if needed.
    
    :param timeout: timeout for retrieving. Default is False, which
      means use ``g5k_configuration['default_timeout']``. None means no
      timeout.

    :param abort_on_error: default False. If True, raises an exception
      on any error. If False, will returned the list of job got, even
      if incomplete (some sites may have failed to answer).
    """
    if timeout == False:
        timeout = g5k_configuration['default_timeout']
    if start_between: start_between = map(_convert_endpoint, start_between)
    if end_between: end_between = map(_convert_endpoint, end_between)
    if connexion_params == None:
        connexion_params = default_frontend_connexion_params
    processes = []
    if local:
        cmd = "oarstat -u"
        process = Process(cmd,
                          timeout = timeout,
                          pty = True)
        process.site = None
        processes.append(process)
    if sites:
        for site in sites:
            process = SshProcess(Host(site),
                                 "oarstat -u",
                                 connexion_params = connexion_params,
                                 timeout = timeout,
                                 pty = True)
            processes.append(process)
    oar_job_ids = []
    if len(processes) == 0:
        return oar_job_ids
    map(Process.start, processes)
    map(Process.wait, processes)
    if reduce(operator.and_, map(Process.ok, processes)) or not abort_on_error:
        for process in processes:
            if process.ok():
                jobs = re.findall("^(\d+)\s", process.stdout(), re.MULTILINE)
                if isinstance(process, SshProcess):
                    host = process.host().address
                else:
                    host = None
                oar_job_ids.extend([ (int(jobid), host) for jobid in jobs ])
        if start_between or end_between:
            filtered_job_ids = []
            for jobsite in oar_job_ids:
                info = get_oar_job_info(jobsite[0], jobsite[1], connexion_params, timeout)
                if (_date_in_range(info['start_date'], start_between)
                    and _date_in_range(info['start_date'] + info['duration'], end_between)):
                    filtered_job_ids.append(jobsite)
            oar_job_ids = filtered_job_ids
        return oar_job_ids
    raise Exception, "error, list of current oar jobs: %s" % (processes,)

def get_current_oargrid_jobs(start_between = None, end_between = None, timeout = False):
    """Return a list of current active oargrid job ids.

    :param start_between: a tuple (low, high) of endpoints. Filters
      and returns only jobs whose start date is in between these
      endpoints. Each endpoint may be given as a `datetime.datetime`
      (absolute date), as a `datetime.timedelta` (delta from now), as
      an absolute unix timestamp, or as a delta from now in seconds
      (if unix timestamp before 315532800 (Jan 1 1980), then assume it
      is a deltat (less than 10 years)).
        
    :param end_between: a tuple (low, high) of endpoints. Filters and
      returns only jobs whose end date is in between these
      endpoints. Each endpoint may be given as a `datetime.datetime`
      (absolute date), as a `datetime.timedelta` (delta from now), as
      an absolute unix timestamp, or as a delta from now in seconds
      (if unix timestamp before 315532800 (Jan 1 1980), then assume it
      is a deltat, (less than 10 years)).
        
    :param timeout: timeout for retrieving. Default is False, which
      means use ``g5k_configuration['default_timeout']``. None means no
      timeout.
    """
    if timeout == False:
        timeout = g5k_configuration['default_timeout']
    if start_between: start_between = map(_convert_endpoint, start_between)
    if end_between: end_between = map(_convert_endpoint, end_between)
    cmd = "oargridstat"
    process = Process(cmd, timeout = timeout, pty = True).run()
    if process.ok():
        jobs = re.findall("Reservation # (\d+):", process.stdout(), re.MULTILINE)
        oargrid_job_ids = map(int, jobs)
        if start_between or end_between:
            filtered_job_ids = []
            for job in oargrid_job_ids:
                info = get_oargrid_job_info(job, timeout)
                if (_date_in_range(info['start_date'], start_between)
                    and _date_in_range(info['start_date'] + info['duration'], end_between)):
                    filtered_job_ids.append(job)
            oargrid_job_ids = filtered_job_ids
        return oargrid_job_ids
    raise Exception, "error, list of current oargrid jobs: %s" % (process,)

def get_oar_job_info(oar_job_id = None, site = None, connexion_params = None, timeout = False):
    """Return a dict with informations about an oar job.

    :param oar_job_id: the oar job id. If None given, will try to get
      it from ``OAR_JOB_ID`` environment variable.
      
    :param site: the Grid5000 site of the oar job. If None given,
      assume local oar job (only works if run on the local frontend).
        
    :param connexion_params: connexion params to connect to other
      site's frontend in case the oar job is on a remote site
      (default: `default_frontend_connexion_params`)
        
    :param timeout: timeout for retrieving. Default is False, which
      means use ``g5k_configuration['default_timeout']``. None means no
      timeout.
    
    Hash returned may contain these keys:

    - ``start_date``: unix timestamp of job's start date

    - ``duration``: unix timestamp of job's duration

    But no info may be available as long as the job is not scheduled.
    """
    if timeout == False:
        timeout = g5k_configuration['default_timeout']
    if oar_job_id == None:
        if os.environ.has_key('OAR_JOB_ID'):
            oar_job_id = os.environ['OAR_JOB_ID']
        else:
            raise ValueError, "no oar job id given and no OAR_JOB_ID environment variable found"
    cmd = "oarstat -fj %i" % (oar_job_id,)
    if site != None:
        if connexion_params == None:
            connexion_params = default_frontend_connexion_params
        process = SshProcess(Host(site),
                             cmd,
                             connexion_params = connexion_params,
                             timeout = timeout,
                             pty = True)
    else:
        process = Process(cmd, timeout = timeout, pty = True)
    process.run()
    if process.ok():
        job_info = dict()
        start_date_result = re.search("^\s*startTime = (\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d)$", process.stdout(), re.MULTILINE)
        if start_date_result:
            start_date = time.mktime(time.strptime(start_date_result.group(1), "%Y-%m-%d %H:%M:%S"))
            job_info['start_date'] = start_date
        duration_result = re.search("^\s*walltime = (\d+):(\d\d):(\d\d)$", process.stdout(), re.MULTILINE)
        if duration_result:
            duration = int(duration_result.group(1)) * 3600 + int(duration_result.group(2)) * 60 + int(duration_result.group(3))
            job_info['duration'] = duration
        return job_info
    raise Exception, "error retrieving info for oar job %i on site %s: %s" % (oar_job_id, site, process)

def wait_oar_job_start(oar_job_id = None, site = None, connexion_params = None, timeout = False):
    """Sleep until an oar job's start time.

    As long as the job isn't scheduled, wait_oar_job_start will sleep
    / poll every 30 seconds until it is scheduled. Then, knowing its
    start date, it will sleep the amount of time necessary to wait for
    the job start.

    :param oar_job_id: the oar job id. If None given, will try to get
      it from ``OAR_JOB_ID`` environment variable.

    :param site: the Grid5000 site of the oar job. If None given,
      assume local oar job (only works if run on the local frontend).

    :param connexion_params: connexion params to connect to other
      site's frontend in case the oar job is on a remote site
      (default: `default_frontend_connexion_params`)
    
    :param timeout: timeout for retrieving. Default is False, which
      means use ``g5k_configuration['default_timeout']``. None means no
      timeout.
    """
    if timeout == False:
        timeout = g5k_configuration['default_timeout']
    while True:
        infos = get_oar_job_info(oar_job_id, site, connexion_params, timeout)
        if infos.has_key('start_date'):
            break
        sleep(30)
    sleep(until = infos['start_date'])
    
def get_oargrid_job_info(oargrid_job_id = None, timeout = False):
    """Return a dict with informations about an oargrid job.

    :param oargrid_job_id: the oargrid job id.

    :param timeout: timeout for retrieving. Default is False, which
      means use ``g5k_configuration['default_timeout']``. None means no
      timeout.

    Hash returned contains these keys:

    - ``start_date``: unix timestamp of job's start date

    - ``duration``: unix timestamp of job's duration
    """
    if timeout == False:
        timeout = g5k_configuration['default_timeout']
    cmd = "oargridstat %i" % oargrid_job_id
    process = Process(cmd, timeout = timeout, pty = True)
    process.run()
    if process.ok():
        job_info = dict()
        start_date_result = re.search("start date : (\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d)", process.stdout(), re.MULTILINE)
        if start_date_result:
            start_date = time.mktime(time.strptime(start_date_result.group(1), "%Y-%m-%d %H:%M:%S"))
            job_info['start_date'] = start_date
        duration_result = re.search("walltime : (\d+):(\d\d):(\d\d)", process.stdout(), re.MULTILINE)
        if duration_result:
            duration = int(duration_result.group(1)) * 3600 + int(duration_result.group(2)) * 60 + int(duration_result.group(3))
            job_info['duration'] = duration
        return job_info
    raise Exception, "error retrieving info for oargrid job %i: %s" % (oargrid_job_id, process)

def wait_oargrid_job_start(oargrid_job_id = None, timeout = False):
    """Sleep until an oargrid job's start time.

    :param oargrid_job_id: the oargrid job id.

    :param timeout: timeout for retrieving. Default is False, which
      means use ``g5k_configuration['default_timeout']``. None means no
      timeout.
    """
    sleep(until = get_oargrid_job_info(oargrid_job_id, timeout)['start_date'])

def get_oar_job_nodes(oar_job_id = None, site = None, connexion_params = None, timeout = False):
    """Return an iterable of `FrozenHost` containing the hosts of an oar job.

    :param oar_job_id: the oar job id. If None given, will try to get
      it from ``OAR_JOB_ID`` environment variable.

    :param site: the Grid5000 site of the oar job. If None given,
      assume local oar job (only works if run on the local frontend).

    :param connexion_params: connexion params to connect to other
      site's frontend in case the oar job is on a remote site
      (default: `default_frontend_connexion_params`)

    :param timeout: timeout for retrieving. Default is False, which
      means use ``g5k_configuration['default_timeout']``. None means no
      timeout.
    """
    if timeout == False:
        timeout = g5k_configuration['default_timeout']
    if oar_job_id == None:
        if os.environ.has_key('OAR_JOB_ID'):
            oar_job_id = os.environ['OAR_JOB_ID']
        else:
            raise ValueError, "no oar job id given and no OAR_JOB_ID environment variable found"
    cmd = "while (oarstat -sj %(oar_job_id)i | grep 'Waiting\|Launching') > /dev/null 2>&1 ; do sleep 5 ; done ; if (oarstat -sj %(oar_job_id)i | grep Running) > /dev/null 2>&1 ; then oarstat -pj %(oar_job_id)i | oarprint host -f - ; else false ; fi" % {'oar_job_id': oar_job_id}
    if site != None:
        if connexion_params == None:
            connexion_params = default_frontend_connexion_params
        process = SshProcess(Host(site),
                             cmd,
                             connexion_params = connexion_params,
                             timeout = timeout,
                             pty = True)
    else:
        process = Process(cmd, timeout = timeout, pty = True)
    process.run()
    if process.ok():
        host_addresses = re.findall("(\S+)", process.stdout(), re.MULTILINE)
        hosts = set()
        for host_address in host_addresses:
            hosts.add(FrozenHost(host_address))
        return hosts
    raise Exception, "error retrieving nodes list for oar job %i on site %s: %s" % (oar_job_id, site, process)

def get_oargrid_job_nodes(oargrid_job_id, timeout = False):
    """Return an iterable of `FrozenHost` containing the hosts of an oargrid job.

    :param oargrid_job_id: the oargrid job id.

    :param timeout: timeout for retrieving. Default is False, which
      means use ``g5k_configuration['default_timeout']``. None means no
      timeout.
    """
    if timeout == False:
        timeout = g5k_configuration['default_timeout']
    cmd = "oargridstat -wl %i" % oargrid_job_id
    process = Process(cmd, timeout = timeout, pty = True)
    process.run()
    if process.ok():
        host_addresses = re.findall("(\S+)", process.stdout(), re.MULTILINE)
        hosts = set()
        for host_address in host_addresses:
            hosts.add(FrozenHost(host_address))
        return hosts
    raise Exception, "error retrieving nodes list for oargrid job %i: %s" % (oargrid_job_id, process)

def kadeploy(deployment, connexion_params = None, timeout = None):
    """Deploy hosts with kadeploy3.

    :param deployment: instance of Deployment class describing the
      intended kadeployment.

    :param timeout: deployment timeout. None (which is the default
      value) means no timeout.

    Returns a tuple (iterable of `FrozenHost` containing the deployed
    host, iterable of `FrozenHost` containing the nodes not deployed).
    """
    kadeployer = Kadeployer(deployment,
                            timeout = timeout).run()
    if kadeployer.error():
        logoutput = style("deployment failed:", 'emph') + " %s\n" % (kadeployer,) + style("kadeploy processes:\n", 'emph')
        for p in kadeployer.processes():
            logoutput += "%s\n" % (p,)
            logoutput += style("stdout:", 'emph') + "\n%s\n" % (p.stdout())
            logoutput += style("stderr:", 'emph') + "\n%s\n" % (p.stderr())
        logger.error(logoutput)
    return (kadeployer.get_deployed_hosts(), kadeployer.get_error_hosts())

def deploy(deployment, connexion_params = None,
           check_deployed_command = True,
           num_deploy_retries = 2, check_enough_func = None,
           timeout = False, deploy_timeout = None, check_timeout = 30):

    """Deploy nodes, many times if needed, checking which of these nodes are already deployed with a user-supplied command. If no command given for checking if nodes deployed, rely on kadeploy to know which nodes are deployed.

    - loop `num_deploy_retries` times:

      - if `check_deployed_command` given, try to connect to these
        hosts using the supplied `connexion_params` (or the default
        ones), and to execute `check_deployed_command`. If connexion
        succeeds and the command returns 0, the host is assumed to be
        deployed, else it is assumed to be undeployed.

      - optionnaly call user-supplied ``check_enough_func``, passing
        to it the list of deployed and undeployed hosts, to let user
        code decide if enough nodes deployed. Otherwise, try as long
        as there are undeployed nodes.

      - deploy the undeployed nodes

    returns a tuple with the list of deployed hosts and the list of
    undeployed hosts.

    :param deployment: instance of Deployment class describing the
      intended kadeployment.

    :param connexion_params: a dict similar to
      `execo.default_connexion_params` whose values will override
      those in `execo.default_connexion_params` when connecting to
      check node deployment with ``check_deployed_command`` (see
      below).

    :param check_deployed_command: command to perform remotely to
      check node deployement. May be a String, True, False or None. If
      String: the actual command to be used (This command should
      return 0 if the node is correctly deployed, or another value
      otherwise). If True, the default command value will be used
      (from `g5k_configuration`). If None or False, no check is made
      and deployed/undeployed status will be taken from kadeploy's
      output.

    :param num_deploy_retries: number of deploy retries

    :param check_enough_func: a function taking as parameter a list of
      deployed hosts and a list of undeployed hosts, which will be
      called at each deployment iteration end, and that should return
      a boolean indicating if there is already enough nodes (in this
      case, no further deployement will be attempted).

    :param timeout: timeout for g5k operations, except deployment.
      Default is False, which means use
      ``g5k_configuration['default_timeout']``. None means no timeout.

    :param deploy_timeout: timeout for deployement. Default is None,
      which means no timeout.

    :param check_timeout: timeout for node deployment checks. Default
      is 30 seconds.
    """

    if timeout == False:
        timeout = g5k_configuration['default_timeout']

    if check_enough_func == None:
        check_enough_func = lambda deployed, undeployed: len(undeployed) == 0

    if check_deployed_command == True:
        check_deployed_command = g5k_configuration['check_deployed_command']

    def check_update_deployed(deployed_hosts, undeployed_hosts, check_deployed_command, connexion_params):
        logger.info(style("check which hosts are already deployed among:", 'emph') + " %s" % (undeployed_hosts,))
        deployed_check = Remote(undeployed_hosts,
                                check_deployed_command,
                                connexion_params = connexion_params,
                                ignore_exit_code = True,
                                ignore_timeout = True,
                                ignore_error = True,
                                timeout = check_timeout)
        deployed_check.run()
        newly_deployed = list()
        for (host, process) in deployed_check.get_hosts_processes().iteritems():
            logger.debug(style("check on %s:" % (host,), 'emph')
                         + " %s\n" % (process,)
                         + style("stdout:", 'emph') + "\n%s\n" % (process.stdout())
                         + style("stderr:", 'emph') + "\n%s\n" % (process.stderr()))
            if (process.exit_code() == 0
                and process.error() == False
                and process.timeouted() == False):
                undeployed_hosts.remove(host)
                deployed_hosts.add(host)
                newly_deployed.append(host)
                logger.info("OK %s" % host)
            else:
                logger.info("KO %s" % host)
        logger.info(style("newly deployed hosts:", 'emph') + " %s" % (newly_deployed,))
        logger.info(style("still undeployed hosts:", 'emph') + " %s" % (undeployed_hosts,))
        
    deployed_hosts = set()
    undeployed_hosts = get_frozen_hosts_set(deployment.hosts)
    if check_deployed_command:
        check_update_deployed(deployed_hosts, undeployed_hosts, check_deployed_command, connexion_params)
    num_tries = 0
    while not check_enough_func(deployed_hosts, undeployed_hosts):
        num_tries += 1
        if num_tries > num_deploy_retries:
            break
        logger.info(style("try %i, deploying on:" % (num_tries,), 'emph') + " %s" % (undeployed_hosts,))
        tmp_deployment = copy.copy(deployment)
        tmp_deployment.hosts = undeployed_hosts
        (newly_deployed_hosts, error_hosts) = kadeploy(tmp_deployment,
                                                       timeout = deploy_timeout)
        if check_deployed_command:
            check_update_deployed(deployed_hosts, undeployed_hosts, check_deployed_command, connexion_params)
        else:
            deployed_hosts.update(newly_deployed_hosts)
            undeployed_hosts.difference_update(newly_deployed_hosts)
            logger.info(style("newly deployed hosts:", 'emph') + " %s" % (newly_deployed_hosts,))
            logger.info(style("still undeployed hosts:", 'emph') + " %s" % (undeployed_hosts,))

    logger.info(style("deploy finished", 'emph'))
    logger.info(style("deployed hosts:", 'emph') + " %s" % (deployed_hosts,))
    logger.info(style("undeployed hosts:", 'emph') + " %s" % (undeployed_hosts,))
    return (deployed_hosts, undeployed_hosts)

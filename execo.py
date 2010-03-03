# -*- coding: utf-8 -*-

r"""Handles launching of several operating system level processes in parallel and controlling them asynchronously.

Overview
========

This module offers a high level API for parallel local or remote
processes execution with the `Action` class hierarchy, and a
lower level API with the `Process` class, for handling
individual subprocesses.

Action
------

An `Action` is an abstraction of a set of parallel processes. It is an
abstract class. Child classes are: `Remote`, `Get`,
`Put`, `Local`. A `Remote` is a remote process
execution on a group of hosts. The remote connexion is performed by
ssh or a similar tool. `Put` and `Get` are actions for
copying to or from a group of hosts. The copy is performed with scp or
a similar tool. A `Local` is a local process (it is a very
lightweight `Action` on top of a single `Process`
instance).

`Remote`, `Get`, `Put` require a list of remote
hosts to perform their tasks. These hosts are passed as an iterable of
instances of `Host`. The `Host` class instances have an
address, and may have a ssh port, a ssh keyfile, a ssh user, if
needed.

As an example of the usage of the `Remote` class, let's launch
some commands on a few remote hosts::

  a = Remote(hosts=(Host('nancy'), Host('Rennes')),
             cmd='whoami ; uname -a').start()
  b = Remote(hosts=(Host('lille'), Host('sophia')),
             cmd='cd ~/project ; make clean ; make').start()
  a.wait()
  # do something else....
  b.wait()

Process
-------

A `Process` is an operating system process, similar to the
`subprocess.Popen` class in the python standard library (actually
execo `Process` use them internally). The main differences is that a
background thread (which is started when execo module is imported)
takes care of reading asynchronously stdout and stderr, and of
handling the lifecycle of the process. This allows writing easily code
in a style appropriate for conducting several processes in parallel.

important exported classes
--------------------------

- `Host`, `FrozenHost`: abstraction of a remote host.

- `Action`, `MultiAction`, `Remote`, `Put`, `Get`, `Local`: all action
  classes.

- `Report`: for gathering and summarizing the results of many `Action`

- `Process`: operating-system level process abstraction, on top of
  which `Action` are implemented.

- `Timer`: convenience timer class.

important exported functions
----------------------------

- `sleep`: easy to use sleep function.

- `set_log_level`: setting execo log level.

General information
===================

ssh configuration for Remote, Get, Put
--------------------------------------

For `Remote`, `Get`, `Put` to work correctly, ssh/scp connexions need
to be fully automatic: No password has to be asked. The default
configuration in execo is to force a passwordless, public key based
authentification. As this tool is growing in a cluster/grid
environment where servers are frequently redeployed, default
configuration also disables strict key checking, and the recording of
hosts keys to ``~/.ssh/know_hosts``. This may be a security hole in a
different context.

substitutions for Remote, Get, Put
----------------------------------

In the command line given for a `Remote`, as well as in pathes given
to `Get` and `Put`, some patterns are automatically substituted:

- all occurences of the literal string ``{{{host}}}`` are substituted
  by the address of the `Host` to which execo connects to.

- all occurences of ``{{<expression>}}`` are substituted in the
  following way: ``<expression>`` must be a python expression, which
  will be evaluated in the context (globals and locals) where the
  `Remote`, `Put`, `Get` is instanciated, and which must return a
  sequence. ``{{<expression>}}`` will be replaced by
  ``<expression>[index % len(<expression>)]``.

For example, in the following code::

  execo.Remote(hosts1, 'iperf -c {{[host.address for host in hosts2]}}')

When execo runs this command on all hosts of ``hosts1``, it will
produce a different command line for each host, each command line
connecting a host from ``hosts1`` to a host from ``hosts2``

if ``hosts1`` contains six hosts ``a``, ``b``, ``c``, ``d``, ``e``,
``f`` and hosts2 contains 3 hosts ``1``, ``2``, ``3`` the mapping
between hosts1 and hosts2 could be:

- ``a`` -> ``1``

- ``b`` -> ``2``

- ``c`` -> ``3``

- ``d`` -> ``1``

- ``e`` -> ``2``

- ``f`` -> ``3``

the real mapping will actually depend on the ordering of the hosts
lists, which are stored as dicts, so it is implementation dependent.

Timestamps
----------

Internally all dates are unix timestamps, ie. number of seconds
elapsed since the unix epoch (00:00:00 on Jan 1 1970), possibly with
or without subsecond precision (float or integer).  In most cases (see
detailed API documentation), timestamps can be passed as unix
timestamps (integers or floats) or as `datetime.datetime`, and
time lengths can be passed as seconds (integers or floats) or as
`datetime.timedelta`

Configuration
-------------

This module may be configured at import time by defining two dicts
`configuration` and `default_connexion_params` in the file
``~/.execo_conf.py``

The `configuration` dict contains global configuration parameters. Its
default values are::

  configuration = {
      'log_level': logging.WARNING,
      'kill_timeout': 5,
      'color_mode': os.isatty(sys.stdout.fileno())
                    and os.isatty(sys.stderr.fileno()),
      'style_log_header': ('yellow',),
      'style_log_level' : ('magenta',),
      'style_object_repr': ('blue', 'bold'),
      'style_emph': ('magenta', 'bold'),
      'style_report_warn': ('magenta',),
      'style_report_error': ('red', 'bold'),
      }

The `default_connexion_params` dict contains default parameters for
remote connexions. Its default values are::

  default_connexion_params = {
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

These default connexion parameters are the ones used when no other
specific connexion parameters are given to the `Remote`, `Get` or
`Put` actions, or given to the `Host`. Actually, when connecting to a
remote host, the connexion parameters are first taken from the `Host`
instance to which the connexion is made, then from the
``connexion_params`` given to the `Remote`/`Get`/`Put`, if there are
some, then from the `default_connexion_params`.

after import time, the configuration may be changed dynamically from
code in the following ways:

- the log level may be adjusted from code with `set_log_level`

- `default_connexion_params` may be modified directly.

Logging
-------

execo uses the standard `logging` module for logging. By default, logs
are sent to stderr, logging level is `logging.WARNING`, so no logs
should be output when everything works correctly. Some logs will
appear if some processes don't exit with a zero return code (unless
they were created with flag ignore_non_zer_exit_code), or if some
processes timeout (unless they were created with flag ignore_timeout),
or if process instanciation resulted in a operating system error
(unless they were created with flag ignore_error).

As told before, `set_log_level` may be used to change execo log level.

Exceptions at shutdown
----------------------

Sometimes, some exceptions are triggered during python shutdown, at
the end of a script using execo. These exceptions are reported by
python to be ``most likely raised during interpreter shutdown``. It
seems (i think) to be related to a bug in shutdown code's handling of
threads termination, and thus it should be ignored. See
http://bugs.python.org/issue1856

Detailed description
====================
"""

from __future__ import with_statement
import datetime, logging, os, select, time, thread, threading, subprocess
import signal, errno, fcntl, sys, traceback, Queue, re, socket, pty
import termios, functools, inspect

configuration = {
    'log_level': logging.WARNING,
    'kill_timeout': 5,
    'color_mode': os.isatty(sys.stdout.fileno())
                  and os.isatty(sys.stderr.fileno()),
    'style_log_header': ('yellow',),
    'style_log_level' : ('magenta',),
    'style_object_repr': ('blue', 'bold'),
    'style_emph': ('magenta', 'bold'),
    'style_report_warn': ('magenta',),
    'style_report_error': ('red', 'bold'),
    }
"""Global execo configuration parameters.

- ``log_level``: the log level (see module `logging`)

- ``kill_timeout``: number of seconds to wait after a clean SIGTERM
  kill before assuming that the process is not responsive and killing
  it with SIGKILL

- ``color_mode``: whether to colorize output (with ansi escape
  sequences).

- ``style_log_header``, ``style_log_level``, ``style_object_repr``,
  ``style_emph``, ``style_report_warn``, ``style_report_error``:
  iterables of ansi attributes identifiers (those found in `_styles`)

"""

default_ssh_scp_params = {
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
"""Default connexion params for ``ssh``/``scp`` connexions.

- ``user``: the user to connect with.

- ``keyfile``: the keyfile to connect with.

- ``port``: the port to connect to.

- ``ssh``: the ssh command.

- ``scp``: the scp command.

- ``ssh_options``: options passed to ssh.

- ``scp_options``: options passed to scp.
"""

default_connexion_params = default_ssh_scp_params.copy()
"""The global default connexion params.

If needed, after modifying default_connexion_params, the ssh/scp
defaults are still available in default_ssh_scp_params.
"""

_styles = {
    'default'    : '\033[m',
    # styles
    'bold'       : '\033[1m',
    'underline'  : '\033[4m',
    'blink'      : '\033[5m',
    'reverse'    : '\033[7m',
    'concealed'  : '\033[8m',
    # font colors
    'black'      : '\033[30m',
    'red'        : '\033[31m',
    'green'      : '\033[32m',
    'yellow'     : '\033[33m',
    'blue'       : '\033[34m',
    'magenta'    : '\033[35m',
    'cyan'       : '\033[36m',
    'white'      : '\033[37m',
    # background colors
    'on_black'   : '\033[40m', 
    'on_red'     : '\033[41m',
    'on_green'   : '\033[42m',
    'on_yellow'  : '\033[43m',
    'on_blue'    : '\033[44m',
    'on_magenta' : '\033[45m',
    'on_cyan'    : '\033[46m',
    'on_white'   : '\033[47m',
    }
"""Definition of ansi escape sequences for colorized output."""

# max number of bytes read when reading asynchronously from a pipe
# (from _POSIX_SSIZE_MAX)
_MAXREAD = 32767

def read_user_configuration_dicts(dicts_confs):
    """Update dicts with those found in ``~/.execo_conf.py``.

    :param dicts_confs: an iterable of couples (dict, string)

    Used to read configuration dicts. For each couple (dict, string),
    if a dict named string is defined in ``~/.execo_conf.py``, update
    dict with the content of this dict. Does nothing if unable to open
    ``~/.execo_conf.py``.
    """
    if not os.environ.has_key('HOME'):
        return
    user_conf_file = os.environ['HOME'] + '/.execo_conf.py'
    if not os.path.exists(user_conf_file):
        return
    conf_dict = {}
    execfile(user_conf_file, conf_dict)
    for (dict, conf) in dicts_confs:
        if conf_dict.has_key(conf):
            dict.update(conf_dict[conf])

# update configuration and default_connexion_params dicts from config file
read_user_configuration_dicts(((configuration, 'configuration'), (default_connexion_params, 'default_connexion_params')))

# logger is the execo logging object
logger = logging.getLogger("execo")
logger_handler = logging.StreamHandler(sys.stderr)
logger.addHandler(logger_handler)

def style(string, style):
    """Enclose a string with ansi color escape codes if `configuration['color_mode']` is True.

    :param string: the string to enclose
    
    :param style: an iterable of ansi attributes identifiers (those
      found in `_styles`)
    """
    style = 'style_' + style
    if (configuration.has_key('color_mode')
        and configuration['color_mode']
        and configuration.has_key(style)):
        style_seq = ""
        for attr in configuration[style]:
            style_seq += _styles[attr]
        return "%s%s%s" % (style_seq, string, _styles['default'])
    else:
        return string

def set_log_level(level, formatter = None):
    """Set execo log level.

    :param level: the level (see module `logging`)

    :param formatter: an optional custom formatter

    By default, the formatter is changed based on the log level. In
    level `logging.DEBUG`, a more detailed formatter is used.
    """
    if formatter != None:
        logger_handler.setFormatter(formatter)
    else:
        if level < 20:
            logger_handler.setFormatter(logging.Formatter(style("%(asctime)s | ", 'log_header') + style("%(levelname)-5.5s ", 'log_level') + style("| %(threadName)-10.10s |", 'log_header') + " %(message)s"))
        else:
            logger_handler.setFormatter(logging.Formatter(style("%(asctime)s", 'log_header') + style(" %(levelname)s", 'log_level') + " %(message)s"))
    logger.setLevel(level)

if configuration.has_key('log_level'):
    set_log_level(configuration['log_level'])
else:
    set_log_level(logging.WARNING)

def _get_milliseconds_suffix(secs):
    """Return a formatted millisecond suffix, either empty if ms = 0, or dot with 3 digits otherwise.

    :param secs: a unix timestamp (integer or float)
    """
    ms_suffix = ""
    msecs = int (round(secs - int(secs), 3) * 1000)
    if msecs != 0:
        ms_suffix = ".%03i" % msecs
    return ms_suffix

def format_time(secs):
    """Return a string with the formatted time (year, month, day, hour, min, sec, ms).

    :param secs: a unix timestamp (integer or float)
    """
    if secs == None:
        return None
    t = time.localtime(secs)
    formatted_time = time.strftime("%Y-%m-%d_%H:%M:%S", t)
    formatted_time += _get_milliseconds_suffix(secs)
    timezone = time.strftime("%Z", t)
    if timezone != "": formatted_time += "_" + timezone
    return formatted_time

def format_duration(secs):
    """Return a string with a formatted duration (days, hours, mins, secs, ms).

    :param secs: a duration in seconds (integer or float)
    """
    if secs == None:
        return None
    s = secs
    d = (s - (s % 86400)) / 86400
    s -= d * 86400
    h = (s - (s % 3600)) / 3600
    s -= h * 3600
    m = (s - (s % 60)) / 60
    s -= m * 60
    formatted_duration = ""
    if secs >= 86400: formatted_duration += "%id" % d
    if secs >= 3600: formatted_duration += "%ih" % h
    if secs >= 60: formatted_duration += "%im" % m
    formatted_duration += "%i%ss" % (s, _get_milliseconds_suffix(s))
    return formatted_duration

def _safe_sleep(secs):
    """Safe sleeping: restarted if interrupted by signals.

    :param secs: time to sleep in seconds (int or float)
    """
    end = time.time() + secs
    while time.time() < end:
        time.sleep(end - time.time())

def timedelta_to_seconds(td):
    """Convert a `datetime.timedelta` to a number of seconds (float)."""
    return td.days * 86400 + td.seconds + td.microseconds / 1e6

_epoch = datetime.datetime (1970, 1, 1, 0, 0, 0, 0)

def datetime_to_unixts(dt):
    """Convert a `datetime.datetime` to a unix timestamp (float)."""
    elapsed = dt - _epoch
    return timedelta_to_seconds(elapsed)

def sleep(delay = None, until = None):
    """Sleep until a given delay has elapsed or until a given date.

    If both present, will sleep at least for the delay and at least
    until the date.

    :param delay: the delay to sleep as a `datetime.timedelta` or in
      seconds (int or float).

    :param until: the date until which to sleep as a
      `datetime.datetime` or as a unix timestamp (int or float).
    """
    if delay != None and isinstance(delay, datetime.timedelta):
        delay = timedelta_to_seconds(delay)
    if until != None and isinstance(until, datetime.datetime):
        until = datetime_to_unixts(until)
    sleeptime = 0
    if delay != None:
        sleeptime = delay
    if until != None:
        dt = until - time.time()
        if (sleeptime > 0 and dt > sleeptime) or (sleeptime <= 0 and dt > 0):
            sleeptime = dt
    if sleeptime > 0:
        logger.info("sleeping %s" % format_duration(sleeptime))
        _safe_sleep(sleeptime)
        logger.info("end sleeping")
        return sleeptime

class Timer(object):
    
    """Keep track of elapsed time."""
    
    def start(self):
        """Start the Timer."""
        self._start = time.time()
        return self

    def wait_elapsed(self, elapsed):
        """Sleep until the given amount of time has elapsed since the Timer's start."""
        really_elapsed = time.time() - self._start
        if really_elapsed < elapsed:
            sleep(elapsed - really_elapsed)
        return self

    def get_start(self):
        """Return this Timer's instance start time."""
        return self._start

    def get_elapsed(self):
        """Return this Timer's instance elapsed time since start."""
        return time.time() - self._start

#---------------------------------------------------------------------
#
# start of Process related stuff (low level API)

def _event_desc(event):
    """For debugging: user friendly representation of the event bitmask returned by poll()."""
    desc = ""
    first = True
    for t in ('POLLIN', 'POLLPRI', 'POLLOUT', 'POLLERR', 'POLLHUP', 'POLLNVAL'):
        if event & select.__getattribute__(t):
            if not first: desc += '|'
            desc += t
            first = False
    return desc

def _checked_waitpid(pid, options):
    """Wrapper for waitpid.
    
    - restarts if interrupted by signal
    
    - returns (0, 0) if no child processes
    
    - returns the waitpid tuple containing process id and exit status
      indication otherwise.
    """
    while True:
        try:
            return os.waitpid(pid, options)
        except OSError, e:
            if e.errno == errno.ECHILD:
                return 0, 0
            elif e.errno == errno.EINTR:
                continue
            else:
                raise

class ProcessOutputHandler(object):
    
    """Abstract handler for `Process` output."""

    def read(self, process, string, eof = False, error = False):
        """Handle string read from a `Process`'s stream.

        :param process: the process which outputs the string

        :param string: the string read

        :param eof:(boolean) true if the stream is now at eof.
        
        :param error: (boolean) true if there was an error on the
          stream
        """
        pass

def line_buffered(func):
    """Decorator to use (only) on the read method of a `ProcessOutputHandler`, for the decorated method to receive data line by line."""
    # not very clean design but currently it works
    @functools.wraps(func)
    def wrapper(self, process, string, eof = False, error = False):
        wrapper.buffer += string
        while True:
            (line, sep, remaining) = wrapper.buffer.partition('\n')
            if remaining != '':
                func(self, process, line + sep)
                wrapper.buffer = remaining
            else:
                break
        if eof or error:
            func(self, process, wrapper.buffer, eof, error)
            wrapper.buffer = ""
    wrapper.buffer = ""
    return wrapper

class Process(object):

    r"""Handle an operating system process.

    In coordination with an I/O and process lifecycle management
    thread which is started when execo is imported, this class allows
    creation, start, interruption (kill), and waiting (for the
    termination) of an operating system process. The subprocess output
    streams (stdout, stderr), as well as various informations about
    the subprocess and its state can be accessed asynchronously.

    It is possible to register custom output handlers to the
    `Process`, in order to provide specific stdout/stderr parsing when
    needed.

    Example usage of the `Process` class: run an iperf server, and
    connect to it with an iperf client:

    >>> server = Process('iperf -s', ignore_exit_code = True).start()
    >>> client = Process('iperf -c localhost -t 2').start()
    >>> client.started()
    True
    >>> client.ended()
    False
    >>> client.wait()
    Process(cmd='iperf -c localhost -t 2', timeout=None, stdout_handler=None, stderr_handler=None, close_stdin=True, shell=True, ignore_exit_code=False, ignore_timeout=False, pty=False)
    >>> client.ended()
    True
    >>> server.ended()
    False
    >>> server.kill()
    >>> server.wait()
    Process(cmd='iperf -s', timeout=None, stdout_handler=None, stderr_handler=None, close_stdin=True, shell=True, ignore_exit_code=True, ignore_timeout=False, pty=False)
    >>> server.ended()
    True
    """

    def __synchronized(func):
        # decorator (similar to java synchronized) to ensure mutual
        # exclusion between some methods that may be called by
        # different threads (the main thread and the _Conductor
        # thread), to ensure that the Process instance always has a
        # consistent state.
        @functools.wraps(func)
        def wrapper(*args, **kw):
            with args[0].__lock:
                return func(*args, **kw)
        return wrapper

    def __init__(self, cmd, timeout = None, stdout_handler = None, stderr_handler = None, close_stdin = None, shell = True, ignore_exit_code = False, ignore_timeout = False, ignore_error = False, pty = False):
        """
        :param cmd: string or tuple containing the command and args to
          run.

        :param timeout: timeout (in seconds, or None for no timeout)
          after which the subprocess will automatically be sent a
          SIGTERM

        :param stdout_handler: instance of `ProcessOutputHandler` for
          handling activity on subprocess stdout

        :param stderr_handler: instance of `ProcessOutputHandler` for
          handling activity on subprocess stderr

        :param close_stdin: boolean. whether or not to close
          subprocess's stdin. If None (default value), automatically
          choose based on pty.

        :param shell: whether or not to use a shell to run the
          cmd. See `subprocess.Popen`

        :param ignore_exit_code: if True, a subprocess with a return
          code != 0 won't generate a warning

        :param ignore_timeout: if True, a subprocess which reaches its
          timeout will be sent a SIGTERM, but it won't generate a
          warning

        :param ignore_error: if True, a subprocess raising an OS level
          error won't generate a warning

        :param pty: open a pseudo tty and connect process's stdin and
          stdout to it (stderr is still connected as a pipe). Make
          process a session leader. If lacking permissions to signals
          to the process, try to simulate sending control characters
          to its pty.
        """
        self.__lock = threading.RLock()
        self.__cmd = cmd
        self.__process = None
        self.__started = False
        self.__start_date = None
        self.__ended = False
        self.__end_date = None
        self.__pid = None
        self.__error = False
        self.__error_reason = None
        self.__exit_code = None
        self.__timeout = timeout
        self.__timeout_date = None
        self.__timeouted = False
        self.__already_got_sigterm = False
        self.__forced_kill = False
        self.__stdout = ""
        self.__stderr = ""
        self.__stdout_ioerror = False
        self.__stderr_ioerror = False
        self.__shell = shell
        self.__ignore_exit_code = ignore_exit_code
        self.__ignore_timeout = ignore_timeout
        self.__ignore_error = ignore_error
        self.__stdout_handler = stdout_handler
        self.__stderr_handler = stderr_handler
        self.__pty = pty
        self.__ptymaster = None
        self.__ptyslave = None
        if close_stdin == None:
            if self.__pty:
                self.__close_stdin = False
            else:
                self.__close_stdin = True
        else:
            self.__close_stdin = close_stdin

    @__synchronized
    def __repr__(self):
        with self.__lock:
            return style("Process", 'object_repr') + "(cmd=%r, timeout=%r, stdout_handler=%r, stderr_handler=%r, close_stdin=%r, shell=%r, ignore_exit_code=%r, ignore_timeout=%r, pty=%r)" % (self.__cmd, self.__timeout, self.__stdout_handler, self.__stderr_handler, self.__close_stdin, self.__shell, self.__ignore_exit_code, self.__ignore_timeout, self.__pty)

    @__synchronized
    def __str__(self):
        with self.__lock:
            return "<" + style("Process", 'object_repr') + "(cmd=%r, timeout=%s, shell=%s, pty=%s, ignore_exit_code=%s, ignore_timeout=%s, ignore_error=%s, started=%s, start_date=%s, ended=%s end_date=%s, pid=%s, error=%s, error_reason=%s, timeouted=%s, forced_kill=%s, exit_code=%s, ok=%s)>" % (self.__cmd, self.__timeout, self.__shell, self.__pty, self.__ignore_exit_code, self.__ignore_timeout, self.__ignore_error, self.__started, format_time(self.__start_date), self.__ended, format_time(self.__end_date), self.__pid, self.__error, self.__error_reason, self.__timeouted, self.__forced_kill, self.__exit_code, self.ok())

    @__synchronized
    def cmd(self):
        """Return the subprocess command line."""
        return self.__cmd
    
    @__synchronized
    def started(self):
        """Return a boolean indicating if the subprocess was started or not."""
        return self.__started
    
    @__synchronized
    def start_date(self):
        """Return the subprocess start date or None if not yet started."""
        return self.__start_date
    
    @__synchronized
    def ended(self):
        """Return a boolean indicating if the subprocess ended or not."""
        return self.__ended
    
    @__synchronized
    def end_date(self):
        """Return the subprocess end date or None if not yet ended."""
        return self.__end_date
    
    @__synchronized
    def pid(self):
        """Return the subprocess's pid, if available (subprocess started) or None."""
        return self.__pid
    
    @__synchronized
    def error(self):
        """Return a boolean indicating if there was an error starting the subprocess.

        This is *not* the subprocess's return code.
        """
        return self.__error
    
    @__synchronized
    def error_reason(self):
        """Return the operating system level errno, if there was an error starting the subprocess, or None."""
        return self.__error_reason
    
    @__synchronized
    def exit_code(self):
        """Return the subprocess exit code.

        If available (if the subprocess ended correctly from the OS
        point of view), or None.
        """
        return self.__exit_code
    
    @__synchronized
    def timeout(self):
        """Return the timeout in seconds after which the subprocess would be killed."""
        return self.__timeout
    
    @__synchronized
    def timeout_date(self):
        """Return the date at which the subprocess will reach its timeout.

        Or none if not available.
        """
        return self.__timeout_date

    @__synchronized
    def timeouted(self):
        """Return a boolean indicating if the subprocess has reached its timeout.

        Or None if we don't know yet (subprocess still running,
        timeout not reached).
        """
        return self.__timeouted
    
    @__synchronized
    def forced_kill(self):
        """Return a boolean indicating if the subprocess was killed forcibly.

        When a subprocess is killed with SIGTERM (either manually or
        automatically, due to reaching a timeout), execo will wait
        some time (constant set in execo source) and if after this
        timeout the subprocess is still running, it will be killed
        forcibly with a SIGKILL.
        """
        return self.__forced_kill
    
    @__synchronized
    def stdout(self):
        """Return a string containing the subprocess stdout."""
        return self.__stdout

    @__synchronized
    def stderr(self):
        """Return a string containing the subprocess stderr."""
        return self.__stderr

    @__synchronized
    def stdout_fd(self):
        """Return the subprocess stdout filehandle.

        Or None if not available.
        """
        if self.__process != None:
            if self.__pty:
                return self.__ptymaster
            else:
                return self.__process.stdout.fileno()
        else:
            return None

    @__synchronized
    def stderr_fd(self):
        """Return the subprocess stderr filehandle.

        Or None if not available.
        """
        if self.__process != None:
            return self.__process.stderr.fileno()
        else:
            return None

    @__synchronized
    def stdin_fd(self):
        """Return the subprocess stdin filehandle.

        Or None if not available.
        """
        if self.__process != None and not self.__close_stdin:
            if self.__pty:
                return self.__ptymaster
            else:
                return self.__process.stdin.fileno()
        else:
            return None

    @__synchronized
    def stdout_handler(self):
        """Return this `Process` stdout `ProcessOutputHandler`."""
        return self.__stdout_handler
    
    @__synchronized
    def stderr_handler(self):
        """Return this `Process` stderr `ProcessOutputHandler`."""
        return self.__stderr_handler

    def _handle_stdout(self, string, eof = False, error = False):
        """Handle stdout activity.

        :param string: available stream output in string

        :param eof: True if end of file on stream

        :param error: True if error on stream
        """
        self.__stdout += string
        if error == True:
            self.__stdout_ioerror = True
        if self.__stdout_handler != None:
            self.__stdout_handler.read(self, string, eof, error)
        
    def _handle_stderr(self, string, eof = False, error = False):
        """Handle stderr activity.

        :param string: available stream output in string

        :param eof: True if end of file on stream

        :param error: True if error on stream
        """
        self.__stderr += string
        if error == True:
            self.__stderr_ioerror = True
        if self.__stderr_handler != None:
            self.__stderr_handler.read(self, string, eof, error)

    @__synchronized
    def start(self):
        """Start the subprocess."""
        if self.__started:
            raise StandardError, "unable to start an already started process"
        logger.debug(style("start:", 'emph') + " %s" % self)
        self.__started = True
        self.__start_date = time.time()
        if self.__timeout != None:
            self.__timeout_date = self.__start_date + self.__timeout
        with _the_conductor.get_lock():
        # this lock is needed to ensure that
        # Conductor.__update_terminated_processes() won't be called
        # before the process has been registered to the conductor
            try:
                if self.__pty:
                    (self.__ptymaster, self.__ptyslave) = pty.openpty()
                    self.__process = subprocess.Popen(self.__cmd,
                                                      stdin = self.__ptyslave,
                                                      stdout = self.__ptyslave,
                                                      stderr = subprocess.PIPE,
                                                      close_fds = True,
                                                      shell = self.__shell,
                                                      preexec_fn = os.setsid)
                else:
                    self.__process = subprocess.Popen(self.__cmd,
                                                      stdin = subprocess.PIPE,
                                                      stdout = subprocess.PIPE,
                                                      stderr = subprocess.PIPE,
                                                      close_fds = True,
                                                      shell = self.__shell)
            except OSError, e:
                self.__error = True
                self.__error_reason = e
                self.__ended = True
                self.__end_date = self.__start_date
                if self.__ignore_error:
                    logger.info(style("error:", 'emph') + " %s on %s" % (e, self,))
                else:
                    logger.warning(style("error:", 'emph') + " %s on %s" % (e, self,))
                return self
            self.__pid = self.__process.pid
            _the_conductor.add_process(self)
        if self.__close_stdin:
            self.__process.stdin.close()
        return self

    @__synchronized
    def kill(self, sig = signal.SIGTERM, auto_sigterm_timeout = True):
        """Send a signal (default: SIGTERM) to the subprocess.

        :param sig: the signal to send

        :param auto_sigterm_timeout: whether or not execo will check
          that the subprocess has terminated after a preset timeout,
          when it has received a SIGTERM, and automatically send
          SIGKILL if the subprocess is not yet terminated
        """
        if self.__pid != None and not self.__ended:
            logger.debug(style("kill with signal %s:" % sig, 'emph') + " %s" % self)
            if sig == signal.SIGTERM:
                self.__already_got_sigterm = True
                if auto_sigterm_timeout == True:
                    self.__timeout_date = time.time() + configuration['kill_timeout']
                    _the_conductor.update_process(self)
            if sig == signal.SIGKILL:
                self.__forced_kill = True
            try:
                os.kill(self.__pid, sig)
            except OSError, e:
                if e.errno == errno.EPERM:
                    char = None
                    if self.__pty:
                        # unable to send signal to process due to lack
                        # of permissions. If __pty == True, then there
                        # is a pty through which we can try to
                        # simulate sending control characters
                        if (sig == signal.SIGTERM
                            or sig == signal.SIGHUP
                            or sig == signal.SIGINT
                            or sig == signal.SIGKILL
                            or sig == signal.SIGPIPE):
                            if hasattr(termios, 'VINTR'):
                                char = termios.tcgetattr(self.__ptymaster)[6][termios.VINTR]
                            else:
                                char = chr(3)
                        elif sig == signal.SIGQUIT:
                            if hasattr(termios, 'VQUIT'):
                                char = termios.tcgetattr(self.__ptymaster)[6][termios.VQUIT]
                            else:
                                char = chr(28)
                    if char != None:
                        logger.debug("sending %r to pty of %s" % (char, self))
                        os.write(self.stdin_fd(), char)
                    else:
                        logger.debug(style("unable to send signal", 'emph') + " to %s" % self)
                elif e.errno == errno.ESRCH:
                    # process terminated so recently that self.__ended
                    # has not been updated yet
                    pass
                else:
                    raise e

    @__synchronized
    def _timeout_kill(self):
        """Send SIGTERM to the subprocess, due to the reaching of its timeout.

        This method is intended to be used by the `_Conductor` thread.
        
        If the subprocess already got a SIGTERM and is still there, it
        is directly killed with SIGKILL.
        """
        if self.__pid != None:
            self.__timeouted = True
            if self.__already_got_sigterm and self.__timeout_date >= time.time():
                self.kill(signal.SIGKILL)
            else:
                self.kill()

    @__synchronized
    def _set_terminated(self, exit_code):
        """Update `Process` state: set it to terminated.

        This method is intended to be used by the `_Conductor` thread.

        Update its exit_code, end_date, ended flag, and log its
        termination (INFO or WARNING depending on how it ended).
        """
        logger.debug("set terminated %s" % self)
        self.__exit_code = exit_code
        self.__end_date = time.time()
        self.__ended = True
        if self.__ptymaster != None:
            os.close(self.__ptymaster)
        if self.__ptyslave != None:
            os.close(self.__ptyslave)
        if self.__process.stdin:
            self.__process.stdin.close()
        if self.__process.stdout:
            self.__process.stdout.close()
        if self.__process.stderr:
            self.__process.stderr.close()
        if (self.__started
            and self.__ended
            and (not self.__error)
            and (not self.__timeouted or self.__ignore_timeout)
            and (self.__exit_code == 0 or self.__ignore_exit_code)):
            logger.info(style("terminated:", 'emph') + " %s\n" % (self,)+ style("stdout:", 'emph') + "\n%s\n" % (self.__stdout,) + style("stderr:", 'emph') + "\n%s" % (self.__stderr,))
        else:
            logger.warning(style("terminated:", 'emph') + " %s\n" % (self,)+ style("stdout:", 'emph') + "\n%s\n" % (self.__stdout,) + style("stderr:", 'emph') + "\n%s" % (self.__stderr,))

    def wait(self):
        """Wait for the subprocess end."""
        with _the_conductor.get_lock():
            if self.__error:
                #raise StandardError, "Trying to wait a process which is in error"
                return self
            if not self.__started or self.__pid == None:
                raise StandardError, "Trying to wait a process which has not been started"
            logger.debug(style("wait:", 'emph') + " %s" % self)
            while self.__ended != True:
                _the_conductor.get_condition().wait()
            logger.debug(style("wait finished:", 'emph') + " %s" % self)
        return self

    def run(self):
        """Start subprocess then wait for its end."""
        return self.start().wait()

    @__synchronized
    def ok(self):
        """Check subprocess has correctly finished.

        A `Process` is ok, if its has:

        - started and ended

        - has no error (or was instructed to ignore them)

        - did not timeout (or was instructed to ignore it)

        - returned 0 (or was instructed to ignore a non zero exit
          code)
        """
        return (self.__started and self.__ended
                and (not self.__error or self.__ignore_error)
                and (not self.__timeouted or self.__ignore_timeout)
                and (self.__exit_code == 0 or self.__ignore_exit_code))

class _Conductor(object):

    """Manager of the subprocesses outputs and lifecycle.

    There must be **one and only one** instance of this class

    Instance of _Conductor will start two threads, one for handling
    asynchronously subprocesses outputs and part of the process
    lifecycle management (the main 'conductor' thread), and a second
    thread (the 'reaper' thread) for handling asynchronously
    subprocess terminations.
    """

    def __init__(self):
        self.__conductor_thread = threading.Thread(target = self.__main_run_func, name = "Conductor")
        self.__conductor_thread.setDaemon(True)
        # thread will terminate automatically when the main thread
        # exits.  once in a while, this can trigger an exception, but
        # this seems to be safe and to be related to this issue:
        # http://bugs.python.org/issue1856
        self.__lock = threading.RLock()
        self.__condition = threading.Condition(self.__lock)
        # this lock and conditions are used for:
        #
        # - mutual exclusion and synchronization beetween sections of
        # code in _Conductor.__io_loop (conductor thread), and in
        # Process.start() and Process.wait() (main thread)
        #
        # - mutual exclusion beetween sections of code in
        #   _Conductor.__io_loop() (conductor thread) and in
        #   _Conductor.__reaper_run_func() (reaper thread)
        self.__rpipe, self.__wpipe = os.pipe() # pipe used to wakeup
                                               # the conductor thread
                                               # from the main thread
                                               # when needed
        self.__poller = select.poll() # asynchronous I/O with all
                                      # subprocesses filehandles
        self.__poller.register(self.__rpipe,
                               select.POLLIN
                               | select.POLLERR
                               | select.POLLHUP)
        self.__processes = set() # the set of `Process` handled by
                                 # this `_Conductor`
        self.__fds = dict() # keys: the file descriptors currently polled by
                            # this `_Conductor`
                            #
                            # values: tuples (`Process`, `Process`'s
                            # function to handle activity for this
                            # descriptor)
        self.__pids = dict() # keys: the pids of the subprocesses
                             # launched by this `_Conductor`
                             #
                             # values: their `Process`
        self.__timeline = [] # list of `Process` with a timeout date
        self.__process_actions = Queue.Queue()
                             # thread-safe FIFO used to send requests
                             # from main thread and conductor thread:
                             # we enqueue tuples (function to call,
                             # tuple of parameters to pass to this
                             # function))

    def __str__(self):
        return "<" + style("Conductor", 'obj_repr') + "(num processes=%i, num fds=%i, num pids=%i, timeline length=%i)>" % (len(self.__processes), len(self.__fds), len(self.__pids), len(self.__timeline))

    def __wakeup(self):
        # wakeup the conductor thread
        os.write(self.__wpipe, ".")

    def get_lock(self):
        # TODO: document why and how the locking is done
        return self.__lock

    def get_condition(self):
        # TODO: document why and how the locking is done
        return self.__condition

    def start(self):
        """Start the conductor thread."""
        self.__conductor_thread.start()
        return self

    def terminate(self):
        """Close the conductor thread."""
        # the closing of the pipe will wake the conductor which will
        # detect this closing and self stop
        logger.debug("terminating I/O thread of %s" % self)
        os.close(self._wpipe)

    def add_process(self, process):
        """Register a new `Process` to the conductor.

        Intended to be called from main thread.
        """
        self.__process_actions.put_nowait((self.__handle_add_process, (process,)))
        self.__wakeup()

    def update_process(self, process):
        """Update `Process` to the conductor.

        Intended to be called from main thread.

        Currently: only update the timeout. This is related to the way
        the system for forcing SIGKILL on processes not killing
        cleanly is implemented.
        """
        self.__process_actions.put_nowait((self.__handle_update_process, (process,)))
        self.__wakeup()

    def remove_process(self, process, exit_code = None):
        """Remove a `Process` from the conductor.

        Intended to be called from main thread.
        """
        self.__process_actions.put_nowait((self.__handle_remove_process, (process, exit_code)))
        self.__wakeup()

    def notify_process_terminated(self, pid, exit_code):
        """Tell the conductor thread that a `Process` has terminated.

        Intended to be called from the reaper thread.
        """
        self.__process_actions.put_nowait((self.__handle_notify_process_terminated, (pid, exit_code)))
        self.__wakeup()

    def __handle_add_process(self, process):
        # intended to be called from conductor thread
        # register a process to conductor
        logger.debug("add %s to %s" % (process, self))
        if process not in self.__processes:
            if not process.ended():
                fileno_stdout = process.stdout_fd()
                fileno_stderr = process.stderr_fd()
                self.__processes.add(process)
                self.__fds[fileno_stdout] = (process, process.__getattribute__('_handle_stdout'))
                self.__fds[fileno_stderr] = (process, process.__getattribute__('_handle_stderr'))
                self.__poller.register(fileno_stdout,
                                       select.POLLIN
                                       | select.POLLERR
                                       | select.POLLHUP)
                self.__poller.register(fileno_stderr,
                                       select.POLLIN
                                       | select.POLLERR
                                       | select.POLLHUP)
                self.__pids[process.pid()] = process
                if process.timeout_date() != None:
                    self.__timeline.append((process.timeout_date(), process))
                if len(self.__processes) == 1:
                    # the reaper thread is only running when there is
                    # at least one process to wait for
                    reaper_thread = threading.Thread(target = self.__reaper_run_func, name = "Reaper")
                    reaper_thread.setDaemon(True)
                    reaper_thread.start()
            else:
                raise ValueError, "trying to add an already finished process to conductor"

    def __handle_update_process(self, process):
        # intended to be called from conductor thread
        # Currently: only update the timeout. This is related to the
        # way the system for forcing SIGKILL on processes not killing
        # cleanly is implemented.
        logger.debug("update timeouts of %s in %s" % (process, self))
        if process not in self.__processes:
            return # this will frequently occur if the process kills
                   # quickly because the process will already be
                   # killed and reaped before __handle_update_process
                   # is called
        if process.timeout_date() != None:
            self.__timeline.append((process.timeout_date(), process))

    @staticmethod
    def __non_blocking_read(fileno, maxread):
        """Non blocking read.

        Temporary set the filehandle in non blocking reading mode,
        then try to read, then restore the filehandle in its previous
        mode, then return string read."""
        file_status_flags = fcntl.fcntl(fileno, fcntl.F_GETFL, 0)
        fcntl.fcntl(fileno, fcntl.F_SETFL, file_status_flags | os.O_NONBLOCK)
        try:
            string = os.read(fileno, maxread)
        except OSError, err:
            if err.errno == errno.EAGAIN:
                string = ""
            else:
                raise
        finally:
            fcntl.fcntl(fileno, fcntl.F_SETFL, file_status_flags)
        return string

    def __handle_remove_process(self, process, exit_code = None):
        # intended to be called from conductor thread
        # unregister a Process from conductor
        logger.debug("removing %s from %s" % (process, self))
        if process not in self.__processes:
            raise ValueError, "trying to remove a process which was not yet added to conductor"
        self.__timeline = [ x for x in self.__timeline if x[1] != process ]
        del self.__pids[process.pid()]
        fileno_stdout = process.stdout_fd()
        fileno_stderr = process.stderr_fd()
        if self.__fds.has_key(fileno_stdout):
            del self.__fds[fileno_stdout]
            self.__poller.unregister(fileno_stdout)
            # read the last data that may be available on stdout of
            # this process
            last_bytes = self.__non_blocking_read(fileno_stdout, _MAXREAD)
            process._handle_stdout(last_bytes, eof = True)
        if self.__fds.has_key(fileno_stderr):
            del self.__fds[fileno_stderr]
            self.__poller.unregister(fileno_stderr)
            # read the last data that may be available on stderr of
            # this process
            last_bytes = self.__non_blocking_read(fileno_stderr, _MAXREAD)
            process._handle_stderr(last_bytes, eof = True)
        self.__processes.remove(process)
        if exit_code != None:
            process._set_terminated(exit_code = exit_code)

    def __get_next_timeout(self):
        """Return the remaining time until the smallest timeout date of all registered `Process`."""
        next_timeout = None
        if len(self.__timeline) != 0:
            self.__timeline.sort(key = lambda x: x[0])
            next_timeout = (self.__timeline[0][0] - time.time())
        return next_timeout

    def __check_timeouts(self):
        """Iterate all registered `Process` whose timeout is reached, kill them gently.

        And remove them from the timeline.
        """
        now = time.time()
        remove_in_timeline = []
        for i in xrange(0, len(self.__timeline)):
            process = self.__timeline[i][1]
            if now >= process.timeout_date():
                logger.debug("timeout on %s" % process)
                process._timeout_kill()
                remove_in_timeline.append(i)
            else:
                break
        for j in reversed(remove_in_timeline):
            del(self.__timeline[j])

    def __update_terminated_processes(self):
        """Ask operating system for all processes that have terminated, self remove them.

        Intended to be called from conductor thread.
        """
        exit_pid, exit_code = _checked_waitpid(-1, os.WNOHANG)
        while exit_pid != 0:
            process = self.__pids[exit_pid]
            logger.debug("process pid %s terminated: %s" % (exit_pid, process))
            self.__handle_remove_process(process, exit_code)
            exit_pid, exit_code = _checked_waitpid(-1, os.WNOHANG)

    def __handle_notify_process_terminated(self, pid, exit_code):
        # intended to be called from conductor thread
        # remove a process based on its pid (as reported by waitpid)
        process = self.__pids[pid]
        self.__handle_remove_process(process, exit_code)

    def __remove_handle(self, fd):
        # remove a file descriptor both from our member(s) and from
        # the Poll object
        del self.__fds[fd]
        self.__poller.unregister(fd)
        
    def __main_run_func(self):
        # wrapper around the conductor thread actual func for
        # exception handling
        try:
            self.__io_loop()
        except SystemExit:
            pass
        except Exception, e:
            print "exception in conductor thread"
            traceback.print_exc()
            os.kill(os.getpid(), signal.SIGTERM)
            # killing myself works, whereas sys.exit(1) or
            # thread.interrupt_main() don't work if main thread is
            # waiting for an os level blocking call.

    def __io_loop(self):
        # conductor thread infinite loop
        finished = False
        while not finished:
            descriptors_events = []
            delay = self.__get_next_timeout() # poll timeout will be
                                              # the delay until the
                                              # first of our
                                              # registered processes
                                              # reaches its timeout
            logger.debug("polling %i descriptors (+ rpipe) with timeout %s" % (len(self.__fds), "%.3fs" % delay if delay != None else "None"))
            if delay == None or delay > 0: # don't even poll if poll timeout is <= 0
                if delay != None: delay *= 1000 # poll needs delay in millisecond
                descriptors_events = self.__poller.poll(delay)
            logger.debug("len(descriptors_events) = %i" % len(descriptors_events))
            event_on_rpipe = None # we want to handle any event on
                                  # rpipe after all other file
                                  # descriptors, hence this flag
            for descriptor_event in descriptors_events:
                fd = descriptor_event[0]
                event = descriptor_event[1]
                if fd == self.__rpipe:
                    event_on_rpipe = event
                else:
                    if self.__fds.has_key(fd):
                        process, stream_handler_func = self.__fds[fd]
                        logger.debug("event %s on fd %s, process %s" % (_event_desc(event), fd, process))
                        if event & select.POLLIN:
                            string = os.read(fd, _MAXREAD)
                            if string == '':
                                # see os.read() doc for semantics of
                                # reading empty string
                                stream_handler_func('', eof = True)
                                self.__remove_handle(fd)
                            else:
                                stream_handler_func(string)
                        if event & select.POLLHUP:
                            stream_handler_func('', eof = True)
                            self.__remove_handle(fd)
                        if event & select.POLLERR:
                            stream_handler_func('', error = True)
                            self.__remove_handle(fd)
            self.__check_timeouts()
            if event_on_rpipe != None:
                logger.debug("event %s on inter-thread pipe" % _event_desc(event_on_rpipe))
                if event_on_rpipe & select.POLLIN:
                    string = os.read(self.__rpipe, _MAXREAD)
                    if string == '':
                        # pipe closed -> auto stop the thread
                        finished = True
                if event_on_rpipe & select.POLLHUP:
                    finished = True
                if event_on_rpipe & select.POLLERR:
                    finished = True
                    raise IOError, "Error on inter-thread communication pipe"
            with self.__lock:
                while True:
                    try:
                        # call (in the right order!) all functions
                        # enqueued from other threads
                        func, args = self.__process_actions.get_nowait()
                    except Queue.Empty:
                        break
                    func(*args)
                self.__update_terminated_processes()
                self.__condition.notifyAll()
        self.__poller.unregister(self.__rpipe)
        os.close(self.__rpipe)
        os.close(self.__wpipe)

    def __reaper_run_func(self):
        # run func for the reaper thread, whose role is to wait to be
        # notified by the operating system of terminated processes
        while True:
            exit_pid, exit_code = _checked_waitpid(-1, 0)
            if (exit_pid, exit_code) == (0, 0):
                # no more child processes, we stop this thread
                # (another instance will be restarted as soon as
                # another process is started)
                break
            logger.debug("process with pid=%s terminated, exit_code=%s" % (exit_pid, exit_code))
            with self.__lock:
                # this lock is needed to ensure that
                # Conductor.__update_terminated_processes() won't be
                # called before the process has been registered to the
                # conductor
                self.notify_process_terminated(exit_pid, exit_code)
        
_the_conductor = _Conductor().start()
"""The **one and only** `_Conductor` instance."""


# end of Process related stuff (low level API)
#
#---------------------------------------------------------------------
#
# start of Action related stuff (high level API)


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
        ssh_scp_auth_options += ("-o", "IdentityFile=%s" % (keyfile,))
    elif connexion_params != None and connexion_params.has_key('keyfile'):
        if connexion_params['keyfile'] != None:
            ssh_scp_auth_options += ("-o", "IdentityFile=%s" % connexion_params['keyfile'])
    elif default_connexion_params != None and default_connexion_params.has_key('keyfile'):
        if default_connexion_params['keyfile'] != None:
            ssh_scp_auth_options += ("-o", "IdentityFile=%s" % default_connexion_params['keyfile'])
            
    if port != None:
        ssh_scp_auth_options += ("-o", "Port=%i" % port)
    elif connexion_params != None and connexion_params.has_key('port'):
        if connexion_params['port'] != None:
            ssh_scp_auth_options += ("-o", "Port=%i" % connexion_params['port'])
    elif default_connexion_params != None and default_connexion_params.has_key('port'):
        if default_connexion_params['port'] != None:
            ssh_scp_auth_options += ("-o", "Port=%i" % default_connexion_params['port'])
            
    return ssh_scp_auth_options

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
    ssh_command = ()
    
    if connexion_params != None and connexion_params.has_key('ssh'):
        if connexion_params['ssh'] != None:
            ssh_command += connexion_params['ssh']
        else:
            raise ValueError, "invalid ssh command in connexion_params %s" % (connexion_params,)
    elif default_connexion_params != None and default_connexion_params.has_key('ssh'):
        if default_connexion_params['ssh'] != None:
            ssh_command += default_connexion_params['ssh']
        else:
            raise ValueError, "invalid ssh command in default_connexion_params %s" % (default_connexion_params,)
    else:
        raise ValueError, "no ssh command in default_connexion_params %s" % (default_connexion_params,)
    
    if connexion_params != None and connexion_params.has_key('ssh_options'):
        if connexion_params['ssh_options'] != None:
            ssh_command += connexion_params['ssh_options']
    elif default_connexion_params != None and default_connexion_params.has_key('ssh_options'):
        if default_connexion_params['ssh_options'] != None:
            ssh_command += default_connexion_params['ssh_options']

    ssh_command += get_ssh_scp_auth_options(user, keyfile, port, connexion_params)
    return ssh_command

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
    scp_command = ()
    
    if connexion_params != None and connexion_params.has_key('scp'):
        if connexion_params['scp'] != None:
            scp_command += connexion_params['scp']
        else:
            raise ValueError, "invalid scp command in connexion_params %s" % (connexion_params,)
    elif default_connexion_params != None and default_connexion_params.has_key('scp'):
        if default_connexion_params['scp'] != None:
            scp_command += default_connexion_params['scp']
        else:
            raise ValueError, "invalid scp command in default_connexion_params %s" % (default_connexion_params,)
    else:
        raise ValueError, "no scp command in default_connexion_params %s" % (default_connexion_params,)
    
    if connexion_params != None and connexion_params.has_key('scp_options'):
        if connexion_params['scp_options'] != None:
            scp_command += connexion_params['scp_options']
    elif default_connexion_params != None and default_connexion_params.has_key('scp_options'):
        if default_connexion_params['scp_options'] != None:
            scp_command += default_connexion_params['scp_options']

    scp_command += get_ssh_scp_auth_options(user, keyfile, port, connexion_params)
    return scp_command

class Host(object):

    """A host to connect to.

    - Has an address (mandatory)

    - Can optionaly have a user, a keyfile, a port, which are used for
      remote connexion and authentification (with a ssh like remote
      connexion tool).

    >>> h1 = Host('localhost')
    >>> h1.user = 'root'
    >>> h1
    Host('localhost', user='root')
    >>> h2 = Host('localhost', user = 'root')
    >>> h1 == h2
    True
    """

    def __init__(self, host, user = False, keyfile = False, port = False):
        """
        :param host: (string or `Host`) the host address or another
          `Host` instance which will be copied into this new instance

        :param user: (string) optional user whith which to connect. If
          False (default value), means use the default user. If None,
          means don't use any user.

        :param keyfile: (string) optional keyfile whith which to
          connect. If False (default value), means use the default
          keyfile. If None, means don't use any keyfile.

        :param port: (integer) optional port to which to connect. If
          False (default value), means use the default port. If None,
          means don't use any port.
        """
        if isinstance(host, Host):
            self.address = host.address
            self.user = host.user
            self.keyfile = host.keyfile
            self.port = host.port
        else:
            self.address = host
            self.user = None
            self.keyfile = None
            self.port = None
        if user != False: self.user = user
        if keyfile != False: self.keyfile = keyfile
        if port != False: self.port = port

    def __eq__(self, other):
        return (self.address == other.address and
                self.user == other.user and
                self.keyfile == other.keyfile and
                self.port == other.port)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        r = "Host(%r" % self.address
        if self.user: r += ", user=%r" % self.user
        if self.keyfile: r += ", keyfile=%r" % self.keyfile
        if self.port: r += ", port=%r" % self.port
        r += ")"
        return r

class FrozenHost(Host):

    """Readonly subclass of `Host` with more usefull (and intuitive) hashing behavior. 

    As the `Host` class is not readonly, it has the default python
    hashing behavior based on its id(). This means that two `Host`
    with the same members (address, user, keyfile, port) won't have
    the same hash, and will thus be seen as different keys in a set or
    dict. This subclass is readonly (by redefining __setattr__), and
    redefines __hash__ in order to have a more usefull hashing
    behavior: two `FrozenHost` with the same members (address, user,
    keyfile, port) will hash equally and will be seen as identical
    keys in a set or dict.

    >>> h1 = FrozenHost('localhost', user = 'root')
    >>> h2 = FrozenHost('localhost', user = 'root')
    >>> d = dict()
    >>> d[h1] = True
    >>> d[h2]
    True
    """

    def __setattr__(self, *args):
        """Redefined to force instances of this class to be readonly."""
        raise TypeError("can't modify immutable instance")

    __delattr__ = __setattr__

    def __init__(self, host, user = False, keyfile = False, port = False):
        """See `Host.__init__`."""
        if isinstance(host, Host):
            super(FrozenHost, self).__setattr__('address', host.address)
            super(FrozenHost, self).__setattr__('user', host.user)
            super(FrozenHost, self).__setattr__('keyfile', host.keyfile)
            super(FrozenHost, self).__setattr__('port', host.port)
        else:
            super(FrozenHost, self).__setattr__('address', host)
            super(FrozenHost, self).__setattr__('user', None)
            super(FrozenHost, self).__setattr__('keyfile', None)
            super(FrozenHost, self).__setattr__('port', None)
        if user != False: super(FrozenHost, self).__setattr__('user', user)
        if keyfile != False: super(FrozenHost, self).__setattr__('keyfile', keyfile)
        if port != False: super(FrozenHost, self).__setattr__('port', port)

    def __hash__(self):
        return (self.address.__hash__()
                ^ self.user.__hash__()
                ^ self.keyfile.__hash__()
                ^ self.port.__hash__())

    def __repr__(self):
        r = "FrozenHost(%r" % self.address
        if self.user: r += ", user=%r" % self.user
        if self.keyfile: r += ", keyfile=%r" % self.keyfile
        if self.port: r += ", port=%r" % self.port
        r += ")"
        return r

    def __str__(self):
        return super(FrozenHost, self).__repr__()

def get_frozen_hosts_set(hosts):
    """Deep copy an iterable of `Host` to a set of `FrozenHost` (thus removing duplicates)."""
    copy = set()    
    for host in hosts:
        fhost = FrozenHost(host)
        copy.add(fhost)
    return copy

def get_hosts_sequence(hosts):
    """Deep copy an iterable of `Host` (possibly `FrozenHost`) to a sequence of `Host`."""
    copy = []
    for host in hosts:
        host_copy = Host(host)
        copy.append(host_copy)
    return copy

def _sort_reports(reports):
    def key_func(report):
        if report.stats()['start_date'] != None:
            return report.stats()['start_date']
        else:
            return sys.maxint
    reports.sort(key = key_func)
    
class Report(object):

    """Human-readable summary of one or more `Action`.

    A Report gathers the results of `Action` or (recursively) of other
    `Report`. `Report.output` returns a formatted string with a
    human-readable summary of all `Action` results.

    To be able to gather transparently results both from `Action` and
    sub-`Report`, both `Action` and `Report` implement the
    `Report.stats`, `Report.reports`, `Report.name` methods.

    >>> r = Report()
    >>> r
    Report(reports=set([]), name='Report')
    >>> r.stats()
    {'num_errors': 0, 'num_ok': 0, 'end_date': None, 'num_ended': 0, 'num_started': 0, 'num_processes': 0, 'num_non_zero_exit_codes': 0, 'num_forced_kills': 0, 'start_date': None, 'num_timeouts': 0}
    """

    def __init__(self, name = None, reports = None):
        """
        :param name a name given to this report. If None, a default
          name will be given.

        :param reports: a `Report`, an `Action`, or an iterable of
          these, which will be added to this `Report`.
        """
        if name == None:
            self._name = "%s" % (self.__class__.__name__,)
        else:
            self._name = name
        self._reports = set()
        if reports != None:
            self.add(reports)

    def add(self, reports):
        """Add some sub-`Report` or `Action` to this `Report`.
        
        :param reports: an iterable of `Report` or `Action`, which
          will be added to this `Report`.
        """
        self._reports.update(reports)

    def reports(self):
        """Return a sorted (by start date) copy of the list of `Report` or `Action` registered to this `Report`."""
        reports = list(self._reports)
        _sort_reports(reports)
        return reports

    def name(self):
        """Return the `Report` name."""
        return self._name

    @staticmethod
    def empty_stats():
        """Return a stats dict all initialized to zero."""
        return {
            'start_date': None,
            'end_date': None,
            'num_processes': 0,
            'num_started': 0,
            'num_ended': 0,
            'num_errors': 0,
            'num_timeouts': 0,
            'num_forced_kills': 0,
            'num_non_zero_exit_codes': 0,
            'num_ok': 0,
            }

    def stats(self):
        """Return a dict summarizing the statistics of all `Action` and sub-`Report` registered to this `Report`.

        This summary dict contains the following metrics:
        
        - ``start_date``: earliest start date (unix timestamp) of all
          `Action` or None if none have started yet.
        
        - ``end_date``: latest end date (unix timestamp) of all
          `Action` or None if not available (not all started, not all
          ended).
        
        - ``num_processes``: number of subprocesses in all `Action`.
        
        - ``num_started``: number of subprocesses that have started.
        
        - ``num_ended``: number of subprocesses that have ended.
        
        - ``num_errors``: number of subprocesses that went in error
          when started.
        
        - ``num_timeouts``: number of subprocesses that had to be
          killed (SIGTERM) after reaching their timeout.
        
        - ``num_forced_kills``: number of subprocesses that had to be
          forcibly killed (SIGKILL) after not responding for some
          time.
        
        - ``num_non_zero_exit_codes``: number of subprocesses that ran
          correctly but whose return code was != 0.
        
        - ``num_ok``: number of subprocesses which did not went in
          error (or where launched with flag ignore_error) , did not
          timeout (or where launched with flag ignore_timeout), and
          had an exit code == 0 (or where launched with flag
          ignore_exit_code).
        """
        stats = Report.empty_stats()
        no_start_date = False
        no_end_date = False
        for report in self._reports:
            stats2 = report.stats()
            for k in stats2.keys():
                if k == 'start_date':
                    if stats2[k] == None:
                        no_start_date = True
                    elif stats[k] == None or stats2[k] < stats[k]:
                        stats[k] = stats2[k]
                elif k == 'end_date':
                    if stats2[k] == None:
                        no_end_date = True
                    elif stats[k] == None or stats2[k] > stats[k]:
                        stats[k] = stats2[k]
                else:
                    stats[k] += stats2[k]
        if no_start_date:
            stats['start_date'] = None
        if no_end_date:
            stats['end_date'] = None
        if stats['num_ended'] != stats['num_processes']:
            stats['end_date'] = None
        return stats

    def __repr__(self):
        return "Report(reports=%r, name=%r)" % (self._reports, self._name)

    def output(self, wide = False):
        """Returns a formatted string with a human-readable summary of all `Action` results.

        :param wide: if False (default), report format is designed for
          80 columns display. If True, output a wide report.
        """
        stats = self.stats()
        output = ""
        if wide:
            output += "Name                                    start               end                 length         started   ended     errors    timeouts  kills     badretval ok        total     \n"
            output += "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
        else:
            output += "Name                                    start               end                \n"
            output += "  length       started ended   error   timeout killed  ret!=0  ok      total   \n"
            output += "-------------------------------------------------------------------------------\n"
        def format_line(name, stats, indent):
            result = ""
            indented_name = " " * indent + name
            length = ""
            if stats['start_date'] != None and stats['end_date'] != None:
                length = format_duration(stats['end_date'] - stats['start_date'])
            else:
                length = ""
            if wide:
                tmpline = "%-39.39s %-19.19s %-19.19s %-15.15s%-10.10s%-10.10s%-10.10s%-10.10s%-10.10s%-10.10s%-10.10s%-10.10s\n" % (
                    indented_name,
                    format_time(stats['start_date']),
                    format_time(stats['end_date']),
                    length,
                    stats['num_started'],
                    stats['num_ended'],
                    stats['num_errors'],
                    stats['num_timeouts'],
                    stats['num_forced_kills'],
                    stats['num_non_zero_exit_codes'],
                    stats['num_ok'],
                    stats['num_processes'])
            else:
                tmpline = "%-39.39s %-19.19s %-19.19s\n" % (
                    indented_name,
                    format_time(stats['start_date']),
                    format_time(stats['end_date']),)
                tmpline += "  %-13.13s%-8.8s%-8.8s%-8.8s%-8.8s%-8.8s%-8.8s%-8.8s%-8.8s\n" % (
                    length,
                    stats['num_started'],
                    stats['num_ended'],
                    stats['num_errors'],
                    stats['num_timeouts'],
                    stats['num_forced_kills'],
                    stats['num_non_zero_exit_codes'],
                    stats['num_ok'],
                    stats['num_processes'],)
            if stats['num_ok'] < stats['num_processes']:
                if stats['num_ok'] == stats['num_ended']:
                    tmpline = style(tmpline, 'report_warn')
                else:
                    tmpline = style(tmpline, 'report_error')
            result += tmpline
            return result

        def recurse_report(report, indent):
            result = ""
            result += format_line(report.name(), report.stats(), indent)
            subreports = report.reports()
            if len(subreports) != 0:
                for subreport in subreports:
                    result += recurse_report(subreport, indent+2)
            return result

        subreports = self.reports()
        if len(subreports) != 0:
            for report in subreports:
                output += recurse_report(report, 0)
            if wide:
                output += "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
            else:
                output += "-------------------------------------------------------------------------------\n"

        output += format_line(self.name(), stats, 0)
        return output

class Action(object):

    """Abstract base class. A set of parallel processes.

    An `Action` can be started (`Action.start`), stopped
    (`Action.stop`). One can wait (`Action.wait`) for an `Action`, it
    means waiting for all processes in the `Action` to finish. An
    `Action` can be run (`Action.wait`), it means start it then wait
    for it to complete.
    """
    
    def __init__(self, name = None, timeout = None, ignore_exit_code = False, ignore_timeout = False, ignore_error = False):
        """
        :param name: `Action` name, one will be generated if None
          given

        :param timeout: timeout for all subprocesses of this
          `Action`. None means no timeout.

        :param ignore_exit_code: if True, subprocesses with return
          value != 0 won't generate a warning and will still be
          counted as ok.

        :param ignore_timeout: if True, subprocesses which timeout
          won't generate a warning and will still be counted as ok.

        :param ignore_error: if True, subprocesses which have an error
          won't generate a warning and will still be counted as ok.
        """
        if name == None:
            self._name = "%s 0x%08.8x" % (self.__class__.__name__, id(self))
        else:
            self._name = name
        self._timeout = timeout
        self._started = False
        self._ignore_exit_code = ignore_exit_code
        self._ignore_timeout = ignore_timeout
        self._ignore_error = ignore_error

    def name(self):
        """Return the `Report` name."""
        return self._name

    def start(self):
        """Start all subprocesses."""
        if self._started:
            raise ValueError, "Actions may be started only once"
        self._started = True
        logger.info(style("start:", 'emph') + " %s" % (self,))
        return self

    def stop(self):
        """Stop all subprocesses."""
        logger.info(style("stop:", 'emph') + " %s" % (self,))
        return self
    
    def wait(self):
        """Wait for all subprocesses to complete."""
        logger.info(style("wait:", 'emph') + " %s" % (self,))
        return self

    def run(self):
        """Start all subprocesses then wait for them to complete."""
        logger.info(style("run:", 'emph') + " %s" % (self,))
        self.start()
        self.wait()
        return self

    def processes(self):
        """Return an iterable of all `Process`."""
        return ()

    def started(self):
        """Return whether this `Action` was started (boolean)."""
        return self._started

    def ended(self):
        """Return whether all subprocesses of this `Action` have ended (boolean)."""
        ended = True
        for process in self.processes():
            if not process.ended():
                ended = False
        return ended

    def error(self):
        """Return a boolean indicating if one or more subprocess failed.

        A subprocess failed if it went in error (unless ignore_error
        flag was given), if it timeouted (unless ignore_timeout flag
        was given), if its exit_code is != 0 (unless ignore_exit_code
        flag was given).
        """
        error = False
        for process in self.processes():
            if ((process.error() and not self._ignore_error)
                or (process.timeouted() and not self._ignore_timeout)
                or (not self._ignore_exit_code
                    and process.exit_code() != None
                    and process.exit_code() != 0)):
                error = True
        return error

    def ok(self):
        return not self.error()

    def stats(self):
        """Return a dict summarizing the statistics of all subprocesses of this `Action`.

        see `Report.stats`.
        """
        stats = Report.empty_stats()
        for process in self.processes():
            if (stats['start_date'] == None
                or (process.start_date() != None
                    and process.start_date() < stats['start_date'])):
                stats['start_date'] = process.start_date()
            if (stats['end_date'] == None
                or (process.end_date() != None
                    and process.end_date() > stats['end_date'])):
                stats['end_date'] = process.end_date()
            stats['num_processes'] += 1
            if process.started(): stats['num_started'] += 1
            if process.ended(): stats['num_ended'] += 1
            if process.error(): stats['num_errors'] += 1
            if process.timeouted(): stats['num_timeouts'] += 1
            if process.forced_kill(): stats['num_forced_kills'] += 1
            if (process.started()
                and process.ended()
                and process.exit_code() != 0):
                stats['num_non_zero_exit_codes'] += 1
            if process.ok():
                stats['num_ok'] += 1
        return stats

    def reports(self):
        """See `Report.reports`."""
        return ()

class MultiAction(Action):

    """An `Action` gathering several `Action`.

    Will start, stop, wait, run every `Action` in parallel.
    """

    def __init__(self, actions = None, **kwargs):
        if kwargs.has_key('timeout'):
            raise AttributeError, "MultiAction don't support timeouts. The timeouts are those of each contained Actions"
        if kwargs.has_key('ignore_exit_code'):
            raise AttributeError, "MultiAction don't support ignore_exit_code. The ignore_exit_code flags are those of each contained Actions"
        if kwargs.has_key('ignore_timeout'):
            raise AttributeError, "MultiAction don't support ignore_timeout. The ignore_timeout flags are those of each contained Actions"
        if not kwargs.has_key('name') or kwargs['name'] == None:
            kwargs['name'] = "%s" % (self.__class__.__name__,)
        super(MultiAction, self).__init__(**kwargs)
        self._actions = actions

    def __repr__(self):
        return style("MultiAction", 'object_repr') + "(name=%r, actions=%r)" % (self._name, self._actions)

    def actions(self):
        """Return an iterable of `Action` that this `MultiAction` gathers."""
        return self._actions

    def start(self):
        retval = super(MultiAction, self).start()
        for action in self._actions:
            action.start()
        return retval

    def stop(self):
        retval = super(MultiAction, self).stop()
        for action in self._actions:
            action.stop()
        return retval

    def wait(self):
        retval = super(MultiAction, self).wait()
        for action in self._actions:
            action.wait()
        return retval

    def processes(self):
        p = []
        for action in self._actions:
            p.extend(action.processes())
        return p

    def reports(self):
        reports = list(self.actions())
        _sort_reports(reports)
        return reports

    def stats(self):
        return Report(self.actions()).stats()

def remote_substitute(string, all_hosts, index, frame_context):
    """Perform some tag substitutions in a specific context.

    :param string: the string onto which to perfom the substitution.

    :param all_hosts: an iterable of `Host` which is the context into
      which the substitution will be made. all_hosts[index] is the
      `Host` to which this string applies.

    :param index: the index in all_hosts of the `Host` to which this
      string applies.

    :param frame_context: a tuple of mappings (globals, locals) in the
      context of which the expressions (if any) will be evaluated.

    - Replaces all occurences of the literal string ``{{{host}}}`` by
      the `Host` address itself.

    - Replaces all occurences of ``{{<expression>}}`` in the following
      way: ``<expression>`` must be a python expression, which will be
      evaluated in the context of frame_context (globals and locals),
      and which must return a sequence. ``{{<expression>}}`` will be
      replaced by ``<expression>[index % len(<expression>)]``.
    """

    def _subst_iterable(matchobj):
        sequence = eval(matchobj.group(1), frame_context[0], frame_context[1])
        if not hasattr(sequence, '__len__') or not hasattr(sequence, '__getitem__'):
            raise ValueError, "substitution of %s: %s must evaluate to a sequence" % (matchobj.group(), sequence)
        return str(sequence[index % len(sequence)])

    string = re.sub("\{\{\{host\}\}\}", all_hosts[index].address, string)
    string = re.sub("\{\{((?:(?!\}\}).)+)\}\}", _subst_iterable, string)
    return string

def get_caller_context():
    """Return a tuple with (globals, locals) of the calling context."""
    return (inspect.stack()[2][0].f_globals, inspect.stack()[2][0].f_locals)

class Remote(Action):

    """Launch a command remotely on several `Host`, with ``ssh`` or a similar remote connexion tool.

    Currently, one ssh process is launched for each connexion, so this
    may not be scalable to a huge number of hosts. In the future we
    could try using taktuk for remote executions, and also for `Get`
    and `Put`.
    """

    def __init__(self, hosts = None, cmd = None, connexion_params = None, **kwargs):
        """
        :param hosts: iterable of `Host` to which to connect and run
          the command.

        :param cmd: the command to run remotely. substitions described
          in `remote_substitute` will be performed.

        :param connexion_params: a dict similar to
          `default_connexion_params` whose values will override those
          in `default_connexion_params` for connexion.
        """
        if not kwargs.has_key('name') or kwargs['name'] == None:
            kwargs['name'] = "%s %s on %s" % (self.__class__.__name__, cmd, hosts)
        super(Remote, self).__init__(**kwargs)
        self._cmd = cmd
        self._connexion_params = connexion_params
        self._caller_context = get_caller_context()
        self._processes = dict()
        fhosts = list(get_frozen_hosts_set(hosts))
        for (index, host) in enumerate(fhosts):
            real_command = get_ssh_command(host.user, host.keyfile, host.port, self._connexion_params) + (host.address,) + (remote_substitute(self._cmd, fhosts, index, self._caller_context),)
            self._processes[host] = Process(real_command, timeout = self._timeout, shell = False, ignore_exit_code = self._ignore_exit_code, ignore_timeout = self._ignore_timeout, ignore_error = self._ignore_error)

    def __repr__(self):
        return style("Remote", 'object_repr') + "(name=%r, timeout=%r, ignore_exit_code=%r, ignore_timeout=%r, ignore_error=%r, hosts=%r, connexion_params=%r, cmd=%r)" % (self._name, self._timeout, self._ignore_exit_code, self._ignore_timeout, self._ignore_error, self._processes.keys(), self._connexion_params, self._cmd)

    def processes(self):
        return self._processes.values()

    def get_hosts_processes(self):
        """Return a dict whose keys are `Host` and values are `Process` run on those hosts."""
        return self._processes.copy()

    def start(self):
        retval = super(Remote, self).start()
        for process in self._processes.values():
            process.start()
        return retval

    def stop(self):
        retval = super(Remote, self).stop()
        for process in self._processes.values():
            process.kill()
        return retval

    def wait(self):
        retval = super(Remote, self).wait()
        for process in self._processes.values():
            process.wait()
        return retval

class Put(Remote):

    """Copy local files to several remote `Host`, with ``scp`` or a similar connexion tool ."""

    def __init__(self, hosts = None, local_files = None, remote_location = ".", create_dirs = False, connexion_params = None, **kwargs):
        """
        :param hosts: iterable of `Host` onto which to copy the files.

        :param local_files: an iterable of string of file
          paths. substitions described in `remote_substitute` will be
          performed.
        
        :param remote_location: the directory on the remote hosts were
          the files will be copied. substitions described in
          `remote_substitute` will be performed.

        :param create_dirs: boolean indicating if remote_location is a
          directory to be created

        :param connexion_params: a dict similar to
          `default_connexion_params` whose values will override those
          in `default_connexion_params` for connexion.
        """
        if local_files != None and (not hasattr(local_files, '__iter__')):
            local_files = (local_files,)
        if not kwargs.has_key('name') or kwargs['name'] == None:
            kwargs['name'] = "%s on %s" % (self.__class__.__name__, hosts)
        super(Remote, self).__init__(**kwargs)
        self._caller_context = get_caller_context()
        self._processes = dict()
        self._local_files = local_files
        self._remote_location = remote_location
        self._create_dirs = create_dirs
        self._connexion_params = connexion_params
        fhosts = list(get_frozen_hosts_set(hosts))
        for (index, host) in enumerate(fhosts):
            prepend_dir_creation = ()
            if self._create_dirs:
                prepend_dir_creation = get_ssh_command(host.user, host.keyfile, host.port, self._connexion_params) + (host.address,) + ('mkdir -p ' + remote_substitute(self._remote_location, fhosts, index, self._caller_context), '&&')
            real_command = list(prepend_dir_creation) + list(get_scp_command(host.user, host.keyfile, host.port, self._connexion_params)) + [ remote_substitute(local_file, fhosts, index, self._caller_context) for local_file in self._local_files ] + ["%s:%s" % (host.address, remote_substitute(self._remote_location, fhosts, index, self._caller_context)),]
            real_command = ' '.join(real_command)
            self._processes[host] = Process(real_command, timeout = self._timeout, shell = True, ignore_exit_code = self._ignore_exit_code, ignore_timeout = self._ignore_timeout, ignore_error = self._ignore_error)

    def __repr__(self):
        return style("Put", 'object_repr') + "(name=%r, timeout=%r, ignore_exit_code=%r, ignore_timeout=%r, ignore_error=%r, hosts=%r, local_files=%r, remote_location=%r, create_dirs=%r, connexion_params=%r)" % (self._name, self._timeout, self._ignore_exit_code, self._ignore_timeout, self._ignore_error, self._processes.keys(), self._local_files, self._remote_location, self._create_dirs, self._connexion_params)

class Get(Remote):

    """Copy remote files from several remote `Host` to a local directory, with ``scp`` or a similar connexion tool ."""

    def __init__(self, hosts = None, remote_files = None, local_location = ".", create_dirs = False, connexion_params = None, **kwargs):
        """
        :param hosts: iterable of `Host` from which to get the files.

        :param remote_files: an iterable of string of file
          paths. substitions described in `remote_substitute` will be
          performed.

        :param local_location: the local directory were the files will
          be copied. substitions described in `remote_substitute` will
          be performed.

        :param create_dirs: boolean indicating if local_location is a
          directory to be created

        :param connexion_params: a dict similar to
          `default_connexion_params` whose values will override those
          in `default_connexion_params` for connexion.
        """
        if remote_files != None and (not hasattr(remote_files, '__iter__')):
            remote_files = (remote_files,)
        if not kwargs.has_key('name') or kwargs['name'] == None:
            kwargs['name'] = "%s on %s" % (self.__class__.__name__, hosts)
        super(Remote, self).__init__(**kwargs)
        self._caller_context = get_caller_context()
        self._processes = dict()
        self._remote_files = remote_files
        self._local_location = local_location
        self._create_dirs = create_dirs
        self._connexion_params = connexion_params
        fhosts = list(get_frozen_hosts_set(hosts))
        for (index, host) in enumerate(fhosts):
            prepend_dir_creation = ()
            if self._create_dirs:
                prepend_dir_creation = ('mkdir', '-p', remote_substitute(local_location, fhosts, index, self._caller_context), '&&')
            remote_specs = ()
            for path in self._remote_files:
                remote_specs += ("%s:%s" % (host.address, remote_substitute(path, fhosts, index, self._caller_context)),)
            real_command = prepend_dir_creation + get_scp_command(host.user, host.keyfile, host.port, self._connexion_params) + remote_specs + (remote_substitute(self._local_location, fhosts, index, self._caller_context),)
            real_command = ' '.join(real_command)
            self._processes[host] = Process(real_command, timeout = self._timeout, shell = True, ignore_exit_code = self._ignore_exit_code, ignore_timeout = self._ignore_timeout, ignore_error = self._ignore_error)

    def __repr__(self):
        return style("Get", 'object_repr') + "(name=%r, timeout=%r, ignore_exit_code=%r, ignore_timeout=%r, ignore_error=%r, hosts=%r, remote_files=%r, local_location=%r, create_dirs=%r, connexion_params=%r)" % (self._name, self._timeout, self._ignore_exit_code, self._ignore_timeout, self._ignore_error, self._processes.keys(), self._remote_files, self._local_location, self._create_dirs, self._connexion_params)

class Local(Action):

    """Launch a command localy."""

    def __init__(self, cmd = None, **kwargs):
        """
        :param cmd: the command to run.
        """
        if not kwargs.has_key('name') or kwargs['name'] == None:
            kwargs['name'] = "%s %s" % (self.__class__.__name__, cmd)
        super(Local, self).__init__(**kwargs)
        self._cmd = cmd
        self._process = Process(self._cmd, timeout = self._timeout, shell = True, ignore_exit_code = self._ignore_exit_code, ignore_timeout = self._ignore_timeout, ignore_error = self._ignore_error)

    def __repr__(self):
        return style("Local", 'object_repr') + "(name=%r, timeout=%r, ignore_exit_code=%r, ignore_timeout=%r, ignore_error=%r, cmd=%r)" % (self._name, self._timeout, self._ignore_exit_code, self._ignore_error, self._ignore_timeout, self._cmd)

    def processes(self):
        return [ self._process ]

    def start(self):
        retval = super(Local, self).start()
        self._process.start()
        return retval

    def stop(self):
        retval = super(Local, self).stop()
        self._process.kill()
        return retval

    def wait(self):
        retval = super(Local, self).wait()
        self._process.wait()
        return retval

if __name__ == "__main__":
    import doctest
    configuration['color_mode'] = False
    doctest.testmod()

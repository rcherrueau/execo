# -*- coding: utf-8 -*-

r"""Handles launching of several operating system level processes in parallel and controlling them asynchronously. Handles remote executions and file copies with  ssh (or similar tools) and taktuk.
"""

from __future__ import with_statement
import datetime, logging, os, select, time, thread, threading, subprocess
import signal, errno, fcntl, sys, traceback, Queue, re, socket, pty
import termios, functools, inspect

# _STARTOF_ configuration
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
# _ENDOF_ configuration
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

# _STARTOF_ default_connexion_params
default_connexion_params = {
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
    'ssh_scp_pty': False,
    }
# _ENDOF_ default_connexion_params
"""Default connexion params for ``ssh``/``scp``/``taktuk`` connexions.

- ``user``: the user to connect with.

- ``keyfile``: the keyfile to connect with.

- ``port``: the port to connect to.

- ``ssh``: the ssh or ssh-like command.

- ``scp``: the scp or scp-like command.

- ``taktuk``: the taktuk command.

- ``ssh_options``: options passed to ssh.

- ``scp_options``: options passed to scp.

- ``taktuk_options``: options passed to taktuk.

- ``taktuk_connector``: the ssh-like connector command for taktuk.

- ``taktuk_connector_options``: options passed to taktuk_connector.

- ``ssh_scp_pty``: allocate a pty for ssh/scp.
"""

default_default_connexion_params = default_connexion_params.copy()
"""An initial backup copy of `default_default_connexion_params`

If needed, after modifying default_connexion_params, the ssh/scp
defaults are still available in default_default_connexion_params.
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
    """Update dicts with those found in ``~/.execo.conf.py``.

    :param dicts_confs: an iterable of couples (dict, string)

    Used to read configuration dicts. For each couple (dict, string),
    if a dict named string is defined in ``~/.execo.conf.py``, update
    dict with the content of this dict. Does nothing if unable to open
    ``~/.execo.conf.py``.
    """
    if not os.environ.has_key('HOME'):
        return
    user_conf_file = os.environ['HOME'] + '/.execo.conf.py'
    if not os.path.exists(user_conf_file):
        return
    conf_dict = {}
    execfile(user_conf_file, conf_dict)
    for (dict, conf) in dicts_confs:
        if conf_dict.has_key(conf):
            dict.update(conf_dict[conf])

# update configuration and default_connexion_params dicts from config file
read_user_configuration_dicts(((configuration, 'configuration'), (default_connexion_params, 'default_connexion_params')))

def style(string, style):
    """Enclose a string with ansi color escape codes if ``configuration['color_mode']`` is True.

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

# logger is the execo logging object
logger = logging.getLogger("execo")
"""The execo logger."""
logger_handler = logging.StreamHandler(sys.stderr)
logger_handler.setFormatter(logging.Formatter(style("%(asctime)s", 'log_header') + style(" %(name)s/%(levelname)s", 'log_level') + " %(message)s"))
logger.addHandler(logger_handler)
if configuration.has_key('log_level'):
    logger.setLevel(configuration['log_level'])
else:
    logger.setLevel(logging.WARNING)

def _set_internal_debug_formatter():
    logger_handler.setFormatter(logging.Formatter(style("%(asctime)s | ", 'log_header') + style("%(levelname)-5.5s ", 'log_level') + style("| %(threadName)-10.10s |", 'log_header') + " %(message)s"))

def _cjoin(*args):
    return ", ".join([ arg for arg in args if len(arg) > 0 ])

def _timedelta_to_seconds(td):
    """Convert a `datetime.timedelta` to a number of seconds (float)."""
    return td.days * 86400 + td.seconds + td.microseconds / 1e6

_epoch = datetime.datetime (1970, 1, 1, 0, 0, 0, 0)

def _datetime_to_unixts(dt):
    """Convert a `datetime.datetime` to a unix timestamp (float)."""
    elapsed = dt - _epoch
    return _timedelta_to_seconds(elapsed)

def _strdate_to_unixts(date):
    """Convert a date in format 'YYYY-MM-DD HH:MM:SS' to a unix timestamp (float)."""
    return time.mktime(time.strptime(date, "%Y-%m-%d %H:%M:%S"))

def _strduration_to_seconds(duration):
    """Convert a duration in format 'HH:MM:SS' to a number of seconds (float)."""
    parsed_duration = re.search("^(\d+):(\d?\d):(\d?\d)$", duration)
    duration = (int(parsed_duration.group(1)) * 3600
                + int(parsed_duration.group(2)) * 60
                + int(parsed_duration.group(3)))
    return duration

def get_seconds(duration):
    """Convert a duration to a number of seconds.

    :param duration: a duration in one of the supported types. if
      duration == None, returns None. Supported types

      - `datetime.timedelta`

      - string in format 'HH:MM:SS'

      - number of seconds, int or float
    """
    if duration == None:
        return None
    elif isinstance(duration, datetime.timedelta):
        duration = _timedelta_to_seconds(duration)
    elif isinstance(duration, str):
        duration = _strduration_to_seconds(duration)
    elif not isinstance(duration, int) and not isinstance(duration, float):
        raise ValueError, "unknown duration format: %s" % (date,)
    return duration

def get_unixts(date):
    """Convert a date to a unix timestamp.

    :param date: a date in one of the supported types. if date ==
      None, returns None. Supported types

      - `datetime.datetime`

      - string in format 'YYYY-MM-DD HH:MM:SS'

      - unix timestamp in seconds, int or float if >= 315532800
        (timestamp after Jan 1 1980)

      - offset from now otherwise, if type is supported by
        `get_seconds`
    """
    if date == None:
        return None
    try:
        seconds = get_seconds(date)
        if seconds < 315532800:
            return time.time() + seconds
    except:
        pass
    if isinstance(date, datetime.datetime):
        return _datetime_to_unixts(date)
    elif isinstance(date, str):
        return _strdate_to_unixts(date)
    elif isinstance(date, int) or isinstance(date, float):
        return date
    raise ValueError, "unknown date format: %s" % (date,)

def _get_milliseconds_suffix(secs):
    """Return a formatted millisecond suffix, either empty if ms = 0, or dot with 3 digits otherwise.

    :param secs: a unix timestamp (integer or float)
    """
    ms_suffix = ""
    msecs = int (round(secs - int(secs), 3) * 1000)
    if msecs != 0:
        ms_suffix = ".%03i" % msecs
    return ms_suffix

def format_unixts(secs, showms = False):
    """Return a string with the formatted date (year, month, day, hour, min, sec, ms) for pretty printing.

    :param secs: a unix timestamp (integer or float) (or None).

    :param showms: whether to show ms or not. Default False.
    """
    if secs == None:
        return None
    t = time.localtime(secs)
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", t)
    if showms:
        formatted_time += _get_milliseconds_suffix(secs)
    timezone = time.strftime("%Z", t)
    if timezone != "": formatted_time += " " + timezone
    return formatted_time

def format_seconds(secs, showms = False):
    """Return a string with a formatted duration (days, hours, mins, secs, ms) for pretty printing.

    :param secs: a duration in seconds (integer or float) (or None).

    :param showms: whether to show ms or not. Default False.
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
    if showms:
        formatted_duration += "%i%ss" % (s, _get_milliseconds_suffix(s))
    else:
        formatted_duration += "%is" % (s,)
    return formatted_duration

def format_date(date, showms = False):
    """Return a string with the formatted date (year, month, day, hour, min, sec, ms) for pretty printing.

    :param date: a date in one of the formats handled (or None) (see
      `get_unixts`).

    :param showms: whether to show ms or not. Default False.
    """
    return format_unixts(get_unixts(date), showms)

def format_duration(duration, showms = False):
    """Return a string with a formatted duration (days, hours, mins, secs, ms) for pretty printing.

    :param duration: a duration in one of the formats handled (or
      None) (see `get_seconds`).

    :param showms: whether to show ms or not. Default False.
    """
    return format_seconds(get_seconds(duration), showms)

def _safe_sleep(secs):
    """Safe sleeping: restarted if interrupted by signals.

    :param secs: time to sleep in seconds (int or float)
    """
    end = time.time() + secs
    sleep_time = secs
    while sleep_time > 0:
        time.sleep(sleep_time)
        sleep_time = end - time.time()

def sleep(delay = None, until = None):
    """Sleep until a given delay has elapsed or until a given date.

    If both present, will sleep at least for the delay and at least
    until the date.

    :param delay: the delay to sleep in one of the formats handled (or
      None) (see `get_seconds`).

    :param until: the date until which to sleep in one of the formats
      handled (or None) (see `get_unixts`).
    """
    delay = get_seconds(delay)
    until = get_unixts(until)
    sleeptime = 0
    if delay != None:
        sleeptime = delay
    if until != None:
        dt = until - time.time()
        if (sleeptime > 0 and dt > sleeptime) or (sleeptime <= 0 and dt > 0):
            sleeptime = dt
    if sleeptime > 0:
        logger.info("sleeping %s" % format_seconds(sleeptime))
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
        """Sleep until the given amount of time has elapsed since the Timer's start.

        :param elapsed: the delay to sleep in one of the formats
          handled (or None) (see `get_seconds`).
        """
        elapsed = get_seconds(elapsed)
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

def _read_asmuch(fileno):
    """Read as much as possible from a file descriptor withour blocking.

    Relies on the file descriptor to have been set non blocking.

    Returns a tuple (string, eof). string is the data read, eof is
    a boolean flag.
    """
    eof = False
    string = ""
    while True:
        try:
            tmpstring = os.read(fileno, _MAXREAD)
        except OSError, err:
            if err.errno == errno.EAGAIN:
                break
            else:
                raise
        if tmpstring == "":
            eof == True
            break
        else:
            string += tmpstring
    return (string, eof)

def _set_fd_nonblocking(fileno):
    """Sets a file descriptor in non blocking mode.

    Returns the previous state flags.
    """
    status_flags = fcntl.fcntl(fileno, fcntl.F_GETFL, 0)
    fcntl.fcntl(fileno, fcntl.F_SETFL, status_flags | os.O_NONBLOCK)
    return status_flags


class ProcessLifecycleHandler(object):

    """Abstract handler for `ProcessBase` lifecycle."""

    def start(self, process):
        """Handle `ProcessBase`'s start.

        :param process: The `ProcessBase` which starts.
        """
        pass

    def end(self, process):
        """Handle `ProcessBase`'s end.

        :param process: The `ProcessBase` which ends.
        """
        pass

    def reset(self, process):
        """Handle `ProcessBase`'s reset.

        :param process: The `ProcessBase` which is reset.
        """
        pass

class ProcessOutputHandler(object):
    
    """Abstract handler for `ProcessBase` output."""

    def __init__(self):
        """ProcessOutputHandler constructor. Call it in inherited classes."""
        self.__buffer = ""

    def read(self, process, string, eof = False, error = False):
        """Handle string read from a `ProcessBase`'s stream.

        :param process: the `ProcessBase` which outputs the string

        :param string: the string read

        :param eof:(boolean) true if the stream is now at eof.
        
        :param error: (boolean) true if there was an error on the
          stream
        """
        self.__buffer += string
        while True:
            (line, sep, remaining) = self.__buffer.partition('\n')
            if remaining != '':
                self.read_line(process, line + sep)
                self.__buffer = remaining
            else:
                break
        if eof or error:
            self.read_line(process, self.__buffer, eof, error)
            self.__buffer = ""

    def read_line(self, process, string, eof = False, error = False):
        """Handle string read line by line from a `ProcessBase`'s stream.

        :param process: the `ProcessBase` which outputs the line

        :param string: the line read

        :param eof:(boolean) true if the stream is now at eof.
        
        :param error: (boolean) true if there was an error on the
          stream
        """
        pass

def _synchronized(func):
    # decorator (similar to java synchronized) to ensure mutual
    # exclusion between some methods that may be called by different
    # threads (the main thread and the _Conductor thread), to ensure
    # that the Process instances always have a consistent state.
    # TO BE USED ONLY BY PROCESSBASE OR SUBCLASSES OF
    @functools.wraps(func)
    def wrapper(*args, **kw):
        with args[0]._lock:
            return func(*args, **kw)
    return wrapper

class ProcessBase(object):

    """An almost abstract base class for all kinds of processes.

    There are no abstract methods, but a `ProcessBase` by itself is
    almost useless, it only provides accessors to data shared with all
    subclasses, but no way to start it or stop it. These methods have
    to be implemented in concrete subclasses.

    It is possible to register custom lifecycle and output handlers to
    the `Process`, in order to provide specific actions or
    stdout/stderr parsing when needed. See `ProcessLifecycleHandler`
    and `ProcessOutputHandler`.
    """

    def __init__(self, cmd, timeout = None, stdout_handler = None, stderr_handler = None, ignore_exit_code = False, ignore_timeout = False, ignore_error = False, default_stdout_handler = True, default_stderr_handler = True, process_lifecycle_handler = None):
        """
        :param cmd: string or tuple containing the command and args to
          run.

        :param timeout: timeout (in seconds, or None for no timeout)
          after which the process will automatically be sent a SIGTERM

        :param stdout_handler: instance of `ProcessOutputHandler` for
          handling activity on process stdout

        :param stderr_handler: instance of `ProcessOutputHandler` for
          handling activity on process stderr

        :param ignore_exit_code: if True, a process with a return code
          != 0 won't generate a warning

        :param ignore_timeout: if True, a process which reaches its
          timeout will be sent a SIGTERM, but it won't generate a
          warning

        :param ignore_error: if True, a process raising an OS level
          error won't generate a warning

        :param default_stdout_handler: if True, a default handler
          sends stdout stream output to the member string accessed
          with self.stdout(). Default: True.

        :param default_stderr_handler: if True, a default handler
          sends stderr stream output to the member string accessed
          with self.stderr(). Default: True.

        :param process_lifecycle_handler: instance of
          `ProcessLifecycleHandler` for being notified of process
          lifecycle events.
        """
        self._common_reset()
        self._lock = threading.RLock()
        self._cmd = cmd
        self._timeout = timeout
        self._ignore_exit_code = ignore_exit_code
        self._ignore_timeout = ignore_timeout
        self._ignore_error = ignore_error
        self._stdout_handler = stdout_handler
        self._stderr_handler = stderr_handler
        self._default_stdout_handler = default_stdout_handler
        self._default_stderr_handler = default_stderr_handler
        self._process_lifecycle_handler = process_lifecycle_handler

    def _common_reset(self):
        # all methods _common_reset() of this class hierarchy contain
        # the code common to the constructor and reset() to
        # reinitialize an object. If redefined in a child class,
        # _common_reset() must then explicitely call _common_reset()
        # of its parent class.
        self._started = False
        self._start_date = None
        self._ended = False
        self._end_date = None
        self._error = False
        self._error_reason = None
        self._exit_code = None
        self._timeout_date = None
        self._timeouted = False
        self._forced_kill = False
        self._stdout = ""
        self._stderr = ""
        self._stdout_ioerror = False
        self._stderr_ioerror = False

    def _args(self):
        # to be implemented in all subclasses. Must return a list with
        # all arguments to the constructor, beginning by the
        # positionnal arguments, finishing by keyword arguments. This
        # list will be directly used in __repr__ methods.
        return [ repr(self._cmd) ] + ProcessBase._kwargs(self)

    def _kwargs(self):
        # to be implemented in all subclasses. Must return a list with
        # all keyword arguments to the constructor. This list will be
        # used to build the list returned by _args() of this class or
        # child classes.
        kwargs = []
        if self._timeout: kwargs.append("timeout=%r" % (self._timeout,))
        if self._stdout_handler: kwargs.append("stdout_handler=%r" % (self._stdout_handler,))
        if self._stderr_handler: kwargs.append("stderr_handler=%r" % (self._stderr_handler,))
        if self._ignore_exit_code != False: kwargs.append("ignore_exit_code=%r" % (self._ignore_exit_code,))
        if self._ignore_timeout != False: kwargs.append("ignore_timeout=%r" % (self._ignore_timeout,))
        if self._ignore_error != False: kwargs.append("ignore_error=%r" % (self._ignore_error,))
        if self._default_stdout_handler != True: kwargs.append("default_stdout_handler=%r" % (self._default_stdout_handler,))
        if self._default_stderr_handler != True: kwargs.append("default_stderr_handler=%r" % (self._default_stderr_handler,))
        if self._process_lifecycle_handler: kwargs.append("process_lifecycle_handler=%r" % (self._process_lifecycle_handler,))
        return kwargs

    def _infos(self):
        # to be implemented in all subclasses. Must return a list with
        # all relevant infos other than those returned by _args(), for
        # use in __str__ methods.
        return [ "started=%s" % (self._started,),
                 "start_date=%s" % (format_unixts(self._start_date),),
                 "ended=%s" % (self._ended,),
                 "end_date=%s" % (format_unixts(self._end_date),),
                 "error=%s" % (self._error,),
                 "error_reason=%s" % (self._error_reason,),
                 "timeouted=%s" % (self._timeouted,),
                 "exit_code=%s" % (self._exit_code,),
                 "ok=%s" % (self.ok(),) ]

    @_synchronized
    def __repr__(self):
        # implemented once for all subclasses
        return "%s(%s)" % (self.__class__.__name__, ", ".join(self._args()))

    @_synchronized
    def __str__(self):
        # implemented once for all subclasses
        return "<" + style(self.__class__.__name__, 'object_repr') + "(%s)>" % (", ".join(self._args() + self._infos()),)

    def cmd(self):
        """Return the process command line."""
        return self._cmd
    
    def started(self):
        """Return a boolean indicating if the process was started or not."""
        return self._started
    
    def start_date(self):
        """Return the process start date or None if not yet started."""
        return self._start_date
    
    def ended(self):
        """Return a boolean indicating if the process ended or not."""
        return self._ended
    
    def end_date(self):
        """Return the process end date or None if not yet ended."""
        return self._end_date
    
    def error(self):
        """Return a boolean indicating if there was an error starting the process.

        This is *not* the process's return code.
        """
        return self._error
    
    def error_reason(self):
        """Return the operating system level errno, if there was an error starting the process, or None."""
        return self._error_reason
    
    def exit_code(self):
        """Return the process exit code.

        If available (if the process ended correctly from the OS point
        of view), or None.
        """
        return self._exit_code
    
    def timeout(self):
        """Return the timeout in seconds after which the process would be killed."""
        return self._timeout
    
    def timeout_date(self):
        """Return the date at which the process will reach its timeout.

        Or none if not available.
        """
        return self._timeout_date

    def timeouted(self):
        """Return a boolean indicating if the process has reached its timeout.

        Or None if we don't know yet (process still running, timeout
        not reached).
        """
        return self._timeouted
    
    def forced_kill(self):
        """Return a boolean indicating if the process was killed forcibly.

        When a process is killed with SIGTERM (either manually or
        automatically, due to reaching a timeout), execo will wait
        some time (constant set in execo source) and if after this
        timeout the process is still running, it will be killed
        forcibly with a SIGKILL.
        """
        return self._forced_kill
    
    def stdout(self):
        """Return a string containing the process stdout."""
        return self._stdout

    def stderr(self):
        """Return a string containing the process stderr."""
        return self._stderr

    def stdout_handler(self):
        """Return this `ProcessBase` stdout `ProcessOutputHandler`."""
        return self._stdout_handler
    
    def stderr_handler(self):
        """Return this `ProcessBase` stderr `ProcessOutputHandler`."""
        return self._stderr_handler

    def _handle_stdout(self, string, eof = False, error = False):
        """Handle stdout activity.

        :param string: available stream output in string

        :param eof: True if end of file on stream

        :param error: True if error on stream
        """
        if self._default_stdout_handler:
            self._stdout += string
        if error == True:
            self._stdout_ioerror = True
        if self._stdout_handler != None:
            self._stdout_handler.read(self, string, eof, error)
        
    def _handle_stderr(self, string, eof = False, error = False):
        """Handle stderr activity.

        :param string: available stream output in string

        :param eof: True if end of file on stream

        :param error: True if error on stream
        """
        if self._default_stderr_handler:
            self._stderr += string
        if error == True:
            self._stderr_ioerror = True
        if self._stderr_handler != None:
            self._stderr_handler.read(self, string, eof, error)

    @_synchronized
    def ok(self):
        """Check process is ok.

        A `ProcessBase` is ok, if:

        - it is not yet started or not yet ended

        - it started and ended and:

          - has no error (or was instructed to ignore them)

          - did not timeout (or was instructed to ignore it)

          - returned 0 (or was instructed to ignore a non zero exit
            code)
        """
        if not self._started: return True
        if self._started and not self._ended: return True
        return ((not self._error or self._ignore_error)
                and (not self._timeouted or self._ignore_timeout)
                and (self._exit_code == 0 or self._ignore_exit_code))

    @_synchronized
    def _log_terminated(self):
        """To be called (in subclasses) when a process terminates.

        This method will log process termination as needed.
        """
        if (self._started
            and self._ended
            and (not self._error or self._ignore_error)
            and (not self._timeouted or self._ignore_timeout)
            and (self._exit_code == 0 or self._ignore_exit_code)):
            logger.debug(style("terminated:", 'emph') + " %s\n" % (self,)+ style("stdout:", 'emph') + "\n%s\n" % (self._stdout,) + style("stderr:", 'emph') + "\n%s" % (self._stderr,))
        else:
            logger.warning(style("terminated:", 'emph') + " %s\n" % (self,)+ style("stdout:", 'emph') + "\n%s\n" % (self._stdout,) + style("stderr:", 'emph') + "\n%s" % (self._stderr,))

    def reset(self):
        """Reinitialize a ProcessBase so that it can later be restarted.

        If it is running, this method will first kill it then wait for
        its termination before reseting;
        """
        logger.debug(style("reset:", 'emph') + " %s" % self)
        if self._started and not self._ended:
            self.kill()
            self.wait()
        if self._process_lifecycle_handler != None:
            self._process_lifecycle_handler.reset(self)
        self._common_reset()
        return self

class Process(ProcessBase):

    r"""Handle an operating system process.

    In coordination with the internal _Conductor I/O and process
    lifecycle management thread which is started when execo is
    imported, this class allows creation, start, interruption (kill),
    and waiting (for the termination) of an operating system
    process. The subprocess output streams (stdout, stderr), as well
    as various informations about the subprocess and its state can be
    accessed asynchronously.

    Example usage of the `Process` class: run an iperf server, and
    connect to it with an iperf client:

    >>> server = Process('iperf -s', ignore_exit_code = True).start()
    >>> client = Process('iperf -c localhost -t 2').start()
    >>> client.started()
    True
    >>> client.ended()
    False
    >>> client.wait()
    Process(cmd='iperf -c localhost -t 2', timeout=None, stdout_handler=None, stderr_handler=None, ignore_exit_code=False, ignore_timeout=False, close_stdin=True, shell=True, pty=False)
    >>> client.ended()
    True
    >>> server.ended()
    False
    >>> server.kill()
    >>> server.wait()
    Process(cmd='iperf -s', timeout=None, stdout_handler=None, stderr_handler=None, ignore_exit_code=True, ignore_timeout=False, close_stdin=True, shell=True, pty=False)
    >>> server.ended()
    True
    """

    def __init__(self, cmd, close_stdin = None, shell = True, pty = False, **kwargs):
        """
        :param cmd: string or tuple containing the command and args to
          run.

        :param close_stdin: boolean. whether or not to close
          subprocess's stdin. If None (default value), automatically
          choose based on pty.

        :param shell: whether or not to use a shell to run the
          cmd. See `subprocess.Popen`

        :param pty: open a pseudo tty and connect process's stdin and
          stdout to it (stderr is still connected as a pipe). Make
          process a session leader. If lacking permissions to send
          signals to the process, try to simulate sending control
          characters to its pty.
        """
        super(Process, self).__init__(cmd, **kwargs)
        self._shell = shell
        self._pty = pty
        if close_stdin == None:
            if self._pty:
                self._close_stdin = False
            else:
                self._close_stdin = True
        else:
            self._close_stdin = close_stdin

    def _common_reset(self):
        super(Process, self)._common_reset()
        self._process = None
        self._pid = None
        self._already_got_sigterm = False
        self._ptymaster = None
        self._ptyslave = None

    def _args(self):
        return ProcessBase._args(self) + Process._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self._close_stdin: kwargs.append("close_stdin=%r" % (self._close_stdin,))
        if self._shell != True: kwargs.append("shell=%r" % (self._shell,))
        if self._pty != False: kwargs.append("pty=%r" % (self._pty,))
        return kwargs

    def _infos(self):
        return ProcessBase._infos(self) + [ "pid=%s" % (self._pid,),
                                            "forced_kill=%s" % (self._forced_kill,) ]
    
    def pid(self):
        """Return the subprocess's pid, if available (subprocess started) or None."""
        return self._pid
    
    @_synchronized
    def stdout_fd(self):
        """Return the subprocess stdout filehandle.

        Or None if not available.
        """
        if self._process != None:
            if self._pty:
                return self._ptymaster
            else:
                return self._process.stdout.fileno()
        else:
            return None

    @_synchronized
    def stderr_fd(self):
        """Return the subprocess stderr filehandle.

        Or None if not available.
        """
        if self._process != None:
            return self._process.stderr.fileno()
        else:
            return None

    @_synchronized
    def stdin_fd(self):
        """Return the subprocess stdin filehandle.

        Or None if not available.
        """
        if self._process != None and not self._close_stdin:
            if self._pty:
                return self._ptymaster
            else:
                return self._process.stdin.fileno()
        else:
            return None

    @_synchronized
    def start(self):
        """Start the subprocess."""
        if self._started:
            raise ValueError, "unable to start an already started process"
        logger.debug(style("start:", 'emph') + " %s" % self)
        self._started = True
        self._start_date = time.time()
        if self._timeout != None:
            self._timeout_date = self._start_date + self._timeout
        if self._process_lifecycle_handler != None:
            self._process_lifecycle_handler.start(self)
        with _the_conductor.get_lock():
        # this lock is needed to ensure that
        # Conductor.__update_terminated_processes() won't be called
        # before the process has been registered to the conductor
            try:
                if self._pty:
                    (self._ptymaster, self._ptyslave) = pty.openpty()
                    self._process = subprocess.Popen(self._cmd,
                                                     stdin = self._ptyslave,
                                                     stdout = self._ptyslave,
                                                     stderr = subprocess.PIPE,
                                                     close_fds = True,
                                                     shell = self._shell,
                                                     preexec_fn = os.setsid)
                else:
                    self._process = subprocess.Popen(self._cmd,
                                                     stdin = subprocess.PIPE,
                                                     stdout = subprocess.PIPE,
                                                     stderr = subprocess.PIPE,
                                                     close_fds = True,
                                                     shell = self._shell)
            except OSError, e:
                self._error = True
                self._error_reason = e
                self._ended = True
                self._end_date = self._start_date
                if self._ignore_error:
                    logger.info(style("error:", 'emph') + " %s on %s" % (e, self,))
                else:
                    logger.warning(style("error:", 'emph') + " %s on %s" % (e, self,))
                if self._process_lifecycle_handler != None:
                    self._process_lifecycle_handler.end(self)
                return self
            self._pid = self._process.pid
            _the_conductor.add_process(self)
        if self._close_stdin:
            self._process.stdin.close()
        return self

    @_synchronized
    def kill(self, sig = signal.SIGTERM, auto_sigterm_timeout = True):
        """Send a signal (default: SIGTERM) to the subprocess.

        :param sig: the signal to send

        :param auto_sigterm_timeout: whether or not execo will check
          that the subprocess has terminated after a preset timeout,
          when it has received a SIGTERM, and automatically send
          SIGKILL if the subprocess is not yet terminated
        """
        if self._pid != None and not self._ended:
            logger.debug(style("kill with signal %s:" % sig, 'emph') + " %s" % self)
            if sig == signal.SIGTERM:
                self._already_got_sigterm = True
                if auto_sigterm_timeout == True:
                    self._timeout_date = time.time() + configuration['kill_timeout']
                    _the_conductor.update_process(self)
            if sig == signal.SIGKILL:
                self._forced_kill = True
            try:
                os.kill(self._pid, sig)
            except OSError, e:
                if e.errno == errno.EPERM:
                    if (self._pty
                        and (sig == signal.SIGTERM
                             or sig == signal.SIGHUP
                             or sig == signal.SIGINT
                             or sig == signal.SIGKILL
                             or sig == signal.SIGPIPE
                             or sig == signal.SIGQUIT)):
                        # unable to send signal to process due to lack
                        # of permissions. If _pty == True, then there
                        # is a pty, we can close its master side, it should
                        # trigger a signal (SIGPIPE?) on the other side
                        os.close(self._ptymaster)
                        logger.debug("EPERM for signal %s -> closing pty master side of %s" % (sig, self))
                    else:
                        logger.debug(style("EPERM: unable to send signal", 'emph') + " to %s" % self)
                elif e.errno == errno.ESRCH:
                    # process terminated so recently that self._ended
                    # has not been updated yet
                    pass
                else:
                    raise e
        return self

    @_synchronized
    def _timeout_kill(self):
        """Send SIGTERM to the subprocess, due to the reaching of its timeout.

        This method is intended to be used by the `_Conductor` thread.
        
        If the subprocess already got a SIGTERM and is still there, it
        is directly killed with SIGKILL.
        """
        if self._pid != None:
            self._timeouted = True
            if self._already_got_sigterm and self._timeout_date >= time.time():
                self.kill(signal.SIGKILL)
            else:
                self.kill()

    @_synchronized
    def _set_terminated(self, exit_code):
        """Update `Process` state: set it to terminated.

        This method is intended to be used by the `_Conductor` thread.

        Update its exit_code, end_date, ended flag, and log its
        termination (INFO or WARNING depending on how it ended).
        """
        logger.debug("set terminated %s, exit_code=%s" % (self, exit_code))
        self._exit_code = exit_code
        self._end_date = time.time()
        self._ended = True
        if self._ptymaster != None:
            try:
                os.close(self._ptymaster)
            except OSError, e:
                if e.errno == errno.EBADF: pass
                else: raise e
        if self._ptyslave != None:
            try:
                os.close(self._ptyslave)
            except OSError, e:
                if e.errno == errno.EBADF: pass
                else: raise e
        if self._process.stdin:
            self._process.stdin.close()
        if self._process.stdout:
            self._process.stdout.close()
        if self._process.stderr:
            self._process.stderr.close()
        self._log_terminated()
        if self._process_lifecycle_handler != None:
            self._process_lifecycle_handler.end(self)

    def wait(self):
        """Wait for the subprocess end."""
        with _the_conductor.get_lock():
            if self._error:
                return self
            if not self._started or self._pid == None:
                raise ValueError, "Trying to wait a process which has not been started"
            logger.debug(style("wait:", 'emph') + " %s" % self)
            while self._ended != True:
                _the_conductor.get_condition().wait()
            logger.debug(style("wait finished:", 'emph') + " %s" % self)
        return self

    def run(self):
        """Start subprocess then wait for its end."""
        return self.start().wait()

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
        _set_fd_nonblocking(self.__rpipe) # the reading function
                                          # _read_asmuch() relies on
                                          # file descriptors to be non
                                          # blocking
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
        signal.set_wakeup_fd(self.__wpipe)

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
        #logger.debug("add %s to %s" % (process, self))
        if process not in self.__processes:
            if not process.ended():
                fileno_stdout = process.stdout_fd()
                fileno_stderr = process.stderr_fd()
                self.__processes.add(process)
                _set_fd_nonblocking(fileno_stdout)
                _set_fd_nonblocking(fileno_stderr)
                self.__fds[fileno_stdout] = (process, process._handle_stdout)
                self.__fds[fileno_stderr] = (process, process._handle_stderr)
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
        #logger.debug("update timeouts of %s in %s" % (process, self))
        if process not in self.__processes:
            return # this will frequently occur if the process kills
                   # quickly because the process will already be
                   # killed and reaped before __handle_update_process
                   # is called
        if process.timeout_date() != None:
            self.__timeline.append((process.timeout_date(), process))

    def __handle_remove_process(self, process, exit_code = None):
        # intended to be called from conductor thread
        # unregister a Process from conductor
        #logger.debug("removing %s from %s" % (process, self))
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
            try:
                (last_bytes, eof) = _read_asmuch(fileno_stdout)
            except OSError, e:
                if e.errno == errno.EBADF: last_bytes = ""
                else: raise e
            process._handle_stdout(last_bytes, eof = True)
        if self.__fds.has_key(fileno_stderr):
            del self.__fds[fileno_stderr]
            self.__poller.unregister(fileno_stderr)
            # read the last data that may be available on stderr of
            # this process
            try:
                (last_bytes, eof) = _read_asmuch(fileno_stderr)
            except OSError, e:
                if e.errno == errno.EBADF: last_bytes = ""
                else: raise e
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
            #logger.debug("polling %i descriptors (+ rpipe) with timeout %s" % (len(self.__fds), "%.3fs" % delay if delay != None else "None"))
            if delay == None or delay > 0: # don't even poll if poll timeout is <= 0
                if delay != None: delay *= 1000 # poll needs delay in millisecond
                descriptors_events = self.__poller.poll(delay)
            #logger.debug("len(descriptors_events) = %i" % len(descriptors_events))
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
                        #logger.debug("event %s on fd %s, process %s" % (_event_desc(event), fd, process))
                        if event & select.POLLIN:
                            (string, eof) = _read_asmuch(fd)
                            stream_handler_func(string, eof = eof)
                            if eof:
                                self.__remove_handle(fd)
                        if event & select.POLLHUP:
                            stream_handler_func('', eof = True)
                            self.__remove_handle(fd)
                        if event & select.POLLERR:
                            stream_handler_func('', error = True)
                            self.__remove_handle(fd)
            self.__check_timeouts()
            if event_on_rpipe != None:
                #logger.debug("event %s on inter-thread pipe" % _event_desc(event_on_rpipe))
                if event_on_rpipe & select.POLLIN:
                    (string, eof) = _read_asmuch(self.__rpipe)
                    if eof:
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

    def __init__(self, address, user = False, keyfile = False, port = False):
        """
        :param address: (string or `Host`) the host address or another
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
        if isinstance(address, Host):
            self.address = address.address
            self.user = address.user
            self.keyfile = address.keyfile
            self.port = address.port
        else:
            self.address = address
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

    def _args(self):
        args = "%r" % (self.address,)
        if self.user: args = _cjoin(args, "user=%r" % (self.user,))
        if self.keyfile: args = _cjoin(args, "keyfile=%r" % (self.keyfile,))
        if self.port: args = _cjoin(args, "port=%r" % (self.port,))
        return args

    def __repr__(self):
        return "Host(%s)" % (self._args())

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

    def __init__(self, address, user = False, keyfile = False, port = False):
        """See `Host.__init__`."""
        if isinstance(address, Host):
            super(FrozenHost, self).__setattr__('address', address.address)
            super(FrozenHost, self).__setattr__('user', address.user)
            super(FrozenHost, self).__setattr__('keyfile', address.keyfile)
            super(FrozenHost, self).__setattr__('port', address.port)
        else:
            super(FrozenHost, self).__setattr__('address', address)
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
        return "FrozenHost(%s)" % (self._args())

def get_frozen_hosts_set(hosts):
    """Deep copy an iterable of `Host` (possibly `FrozenHost`) to a set of `FrozenHost` (thus removing duplicates)."""
    copy = set()    
    for host in hosts: copy.add(FrozenHost(host))
    return copy

def get_frozen_hosts_list(hosts):
    """Deep copy an iterable of `Host` (possibly `FrozenHost`) to a list of `FrozenHost`."""
    return [ FrozenHost(host) for host in hosts ]

def get_hosts_list(hosts):
    """Deep copy an iterable of `Host` (possibly `FrozenHost`) to a list of `Host`."""
    return [ Host(host) for host in hosts ]

class SshProcess(Process):

    r"""Handle a remote command execution through ssh or similar remote execution tool.

    Note: the closing of the remote process upon killing of the
    SshProcess depends on the ssh (or ssh-like) command behavior. With
    openssh, this can be obtained by passing options -tt (force tty
    creation), thus these are the default options in
    ``default_connexion_params``.
    """

    def __init__(self, host, remote_cmd, connexion_params = None, **kwargs):
        self._host = host
        self._remote_cmd = remote_cmd
        self._connexion_params = connexion_params
        real_cmd = (get_ssh_command(host.user,
                                    host.keyfile,
                                    host.port,
                                    connexion_params)
                    + (host.address,)
                    + (remote_cmd,))
        super(SshProcess, self).__init__(real_cmd, shell = False, **kwargs)

    def _args(self):
        return [ repr(self._host),
                 repr(self._remote_cmd) ] + Process._kwargs(self) + SshProcess._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self._connexion_params: kwargs.append("connexion_params=%r" % (self._connexion_params,))
        return kwargs

    def _infos(self):
        return [ "real cmd=%r" % (self._cmd,) ] + Process._infos(self)

    def remote_cmd(self):
        """Return the command line executed remotely through ssh."""
        return self._remote_cmd

    def host(self):
        """Return the remote host."""
        return self._host

    def connexion_params(self):
        """Return ssh connexion parameters."""
        return self._connexion_params

class TaktukProcess(ProcessBase):

    r"""Dummy process similar to `SshProcess`."""

    def __init__(self, host, remote_cmd, **kwargs):
        self._host = host
        self._remote_cmd = remote_cmd
        super(TaktukProcess, self).__init__(remote_cmd, **kwargs)

    def _args(self):
        return [ repr(self._host),
                 repr(self._remote_cmd) ] + ProcessBase._kwargs(self)

    def host(self):
        """Return the remote host."""
        return self._host

    @_synchronized
    def start(self):
        """Notify `TaktukProcess` of actual remote process start.

        This method is intended to be used by `TaktukRemote`.
        """
        if self._started:
            raise ValueError, "unable to start an already started process"
        logger.debug(style("start:", 'emph') + " %s" % self)
        self._started = True
        self._start_date = time.time()
        if self._timeout != None:
            self._timeout_date = self._start_date + self._timeout
        if self._process_lifecycle_handler != None:
            self._process_lifecycle_handler.start(self)
        return self

    @_synchronized
    def _set_terminated(self, exit_code = None, error = False, error_reason = None, timeouted = None, forced_kill = None):
        """Update `TaktukProcess` state: set it to terminated.

        This method is intended to be used by `TaktukRemote`.

        Update its exit_code, end_date, ended flag, and log its
        termination (INFO or WARNING depending on how it ended).
        """
        if not self._started:
            self.start()
        logger.debug("set terminated %s, exit_code=%s, error=%s" % (self, exit_code, error))
        if error != None:
            self._error = error
        if error_reason != None:
            self._error_reason = error_reason
        if exit_code != None:
            self._exit_code = exit_code
        if timeouted == True:
            self._timeouted = True
        if forced_kill == True:
            self._forced_kill = True
        self._end_date = time.time()
        self._ended = True
        self._log_terminated()
        if self._process_lifecycle_handler != None:
            self._process_lifecycle_handler.end(self)

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
    >>> sorted(r.stats().items())
    [('end_date', None), ('num_ended', 0), ('num_errors', 0), ('num_forced_kills', 0), ('num_non_zero_exit_codes', 0), ('num_ok', 0), ('num_processes', 0), ('num_started', 0), ('num_timeouts', 0), ('start_date', None)]
    """

    def __init__(self, reports = None, name = None):
        """
        :param reports: a `Report`, an `Action`, or an iterable of
          these, which will be added to this `Report`.

        :param name a name given to this report. If None, a default
          name will be given.
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
        return "<Report(<%i entries>, name=%r)>" % (len(self._reports), self._name)

    def __str__(self):
        stats = self.stats()
        return "<Report(<%i entries>, name=%r, start_date=%r, end_date=%r, num_processes=%r, num_started=%r, num_ended=%r, num_timeouts=%r, num_errors=%r, num_forced_kills=%r, num_non_zero_exit_codes=%r, num_ok=%r)>" % (len(self._reports), self._name, format_unixts(stats['start_date']), format_unixts(stats['end_date']), stats['num_processes'], stats['num_started'], stats['num_ended'], stats['num_timeouts'], stats['num_errors'], stats['num_forced_kills'], stats['num_non_zero_exit_codes'], stats['num_ok'])

    def output(self, wide = False, brief = False):
        """Returns a formatted string with a human-readable summary of all `Action` results.

        :param wide: if False (default), report format is designed for
          80 columns display. If True, output a (175 characters) wide
          report.

        :param brief: when True, only the Total summary is output, not
          each `Action` or `Report` summary. Default is False.
        """
        stats = self.stats()
        output = ""
        if wide:
            output += "Name                                    start               end                 length         started   ended     errors    timeouts  f.killed  badretval ok        total     \n"
            output += "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
        else:
            output += "Name                                    start               end                \n"
            output += "  length       started ended   error   timeout fkilled ret!=0  ok      total   \n"
            output += "-------------------------------------------------------------------------------\n"
        def format_line(name, stats, indent):
            result = ""
            indented_name = " " * indent + name
            length = ""
            if stats['start_date'] != None and stats['end_date'] != None:
                length = format_seconds(stats['end_date'] - stats['start_date'])
            else:
                length = ""
            if wide:
                tmpline = "%-39.39s %-19.19s %-19.19s %-15.15s%-10.10s%-10.10s%-10.10s%-10.10s%-10.10s%-10.10s%-10.10s%-10.10s\n" % (
                    indented_name,
                    format_unixts(stats['start_date']),
                    format_unixts(stats['end_date']),
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
                    format_unixts(stats['start_date']),
                    format_unixts(stats['end_date']),)
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
        if not brief and len(subreports) != 0:
            for report in subreports:
                output += recurse_report(report, 0)
            if wide:
                output += "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
            else:
                output += "-------------------------------------------------------------------------------\n"

        output += format_line(self.name(), stats, 0)
        return output

class ActionLifecycleHandler(object):

    """Abstract handler for `Action` lifecycle."""

    def start(self, action):
        """Handle `Action`'s start.

        :param action: The `Action` which starts.
        """
        pass

    def end(self, action):
        """Handle `Action`'s end.

        :param action: The `Action` which ends.
        """
        pass

    def reset(self, action):
        """Handle `Action`'s reset.

        :param action: the `Action` which is reset.
        """
        pass

class Action(object):

    """Abstract base class. A set of parallel processes.

    An `Action` can be started (`Action.start`), killed
    (`Action.kill`). One can wait (`Action.wait`) for an `Action`, it
    means waiting for all processes in the `Action` to finish. An
    `Action` can be run (`Action.wait`), it means start it then wait
    for it to complete.
    """
    
    def __init__(self, name = None, timeout = None, ignore_exit_code = False,
                 ignore_timeout = False, ignore_error = False):
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
        self._common_reset()
        self._name = name
        self._timeout = timeout
        self._ignore_exit_code = ignore_exit_code
        self._ignore_timeout = ignore_timeout
        self._ignore_error = ignore_error
        self._lifecycle_handler = list()
        self._end_event = threading.Event()

    def _common_reset(self):
        # all methods _common_reset() of this class hierarchy contain
        # the code common to the constructor and reset() to
        # reinitialize an object. _common_reset() of child classes
        # must explicitely call _common_reset() of their parent class.
        self._started = False
        self._ended = False

    def _args(self):
        # to be implemented in all subclasses. Must return a list with
        # all arguments to the constructor, beginning by the
        # positionnal arguments, finishing by keyword arguments. This
        # list will be directly used in __repr__ methods.
        return Action._kwargs(self)

    def _kwargs(self):
        # to be implemented in all subclasses. Must return a list with
        # all keyword arguments to the constructor. This list will be
        # used to build the list returned by _args() of this class or
        # child classes.
        kwargs = []
        if self._name: kwargs.append("name=%r" % (self._name,))
        if self._timeout: kwargs.append("timeout=%r" % (self._timeout,))
        if self._ignore_exit_code != False: kwargs.append("ignore_exit_code=%r" % (self._ignore_exit_code,))
        if self._ignore_timeout != False: kwargs.append("ignore_timeout=%r" % (self._ignore_timeout,))
        if self._ignore_error != False: kwargs.append("ignore_error=%r" % (self._ignore_error,))
        return kwargs

    def _infos(self):
        # to be implemented in all subclasses. Must return a list with
        # all relevant infos other than those returned by _args(), for
        # use in __str__ methods.
        stats = self.stats()
        return [ "started=%r" % (self._started,),
                 "start_date=%r" % (format_unixts(stats['start_date']),),
                 "ended=%r" % (self._ended,),
                 "end_date=%r" % (format_unixts(stats['end_date']),),
                 "num_processes=%r" % (stats['num_processes'],),
                 "num_started=%r" % (stats['num_started'],),
                 "num_ended=%r" % (stats['num_ended'],),
                 "num_timeouts=%r" % (stats['num_timeouts'],),
                 "num_errors=%r" % (stats['num_errors'],),
                 "num_forced_kills=%r" % (stats['num_forced_kills'],),
                 "num_non_zero_exit_codes=%r" % (stats['num_non_zero_exit_codes'],),
                 "num_ok=%r" % (stats['num_ok'],),
                 "ok=%r" % (self.ok(),) ]

    def __repr__(self):
        # implemented once for all subclasses
        return "%s(%s)" % (self.__class__.__name__, ", ".join(self._args()))

    def __str__(self):
        # implemented once for all subclasses
        return "<" + style(self.__class__.__name__, 'object_repr') + "(%s)>" % (", ".join(self._args() + self._infos()),)

    def name(self):
        """Return the `Report` name."""
        if self._name == None:
            return "%s 0x%08.8x" % (self.__class__.__name__, id(self))
        else:
            return self._name

    def add_lifecycle_handler(self, handler):
        """Add a lifecycle handler.

        An instance of `ActionLifecycleHandler` for being notified of
        action lifecycle events.
        """
        self._lifecycle_handler.append(handler)

    def _notify_terminated(self):
        logger.debug(style("got termination notification for:", 'emph') + " %s" % (self,))
        for handler in self._lifecycle_handler:
            handler.end(self)
        self._ended = True
        self._end_event.set()

    def start(self):
        """Start all subprocesses.

        return self"""
        if self._started:
            raise ValueError, "Actions may be started only once"
        self._started = True
        logger.debug(style("start:", 'emph') + " %s" % (self,))
        for handler in self._lifecycle_handler:
            handler.start(self)
        return self

    def kill(self):
        """Kill all subprocesses.

        return self"""
        logger.debug(style("kill:", 'emph') + " %s" % (self,))
        return self
    
    def wait(self):
        """Wait for all subprocesses to complete.

        return self"""
        logger.debug(style("start waiting:", 'emph') + " %s" % (self,))
        self._end_event.wait()
        logger.debug(style("end waiting:", 'emph') + " %s" % (self,))
        return self

    def run(self):
        """Start all subprocesses then wait for them to complete.

        return self"""
        logger.debug(style("run:", 'emph') + " %s" % (self,))
        self.start()
        self.wait()
        return self

    def reset(self):
        """Reinitialize an Action so that it can later be restarted.

        If it is running, this method will first kill it then wait for
        its termination before reseting;
        """
        logger.debug(style("reset:", 'emph') + " %s" % (self,))
        if self._started and not self._ended:
            self.kill()
            self.wait()
        for handler in self._lifecycle_handler:
            handler.reset(self)
        self._common_reset()
        return self

    def processes(self):
        """Return an iterable of all `Process`."""
        return ()

    def started(self):
        """Return whether this `Action` was started (boolean)."""
        return self._started

    def ended(self):
        """Return whether all subprocesses of this `Action` have ended (boolean)."""
        return self._ended

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

class ActionNotificationProcessLifecycleHandler(ProcessLifecycleHandler):

    def __init__(self, action, total_processes):
        self._action = action
        self._total_processes = total_processes
        self._terminated_processes = 0

    def end(self, process):
        self._terminated_processes += 1
        logger.debug("%i/%i processes terminated in %s" % (self._terminated_processes,
                                                           self._total_processes,
                                                           self._action))
        if self._terminated_processes == self._total_processes:
            self._action._notify_terminated()

    def action_reset(self):
        self._terminated_processes = 0

class Remote(Action):

    """Launch a command remotely on several `Host`, with ``ssh`` or a similar remote connexion tool.

    One ssh process is launched for each connexion.
    """

    def __init__(self, hosts, remote_cmd, connexion_params = None, **kwargs):
        """
        :param hosts: iterable of `Host` to which to connect and run
          the command.

        :param remote_cmd: the command to run remotely. substitions
          described in `remote_substitute` will be performed.

        :param connexion_params: a dict similar to
          `default_connexion_params` whose values will override those
          in `default_connexion_params` for connexion.
        """
        super(Remote, self).__init__(**kwargs)
        self._remote_cmd = remote_cmd
        self._connexion_params = connexion_params
        self._caller_context = get_caller_context()
        self._hosts = get_hosts_list(hosts)
        self._processes = list()
        self._process_lifecycle_handler = ActionNotificationProcessLifecycleHandler(self, len(self._hosts))
        for (index, host) in enumerate(self._hosts):
            self._processes.append(SshProcess(host,
                                              remote_substitute(remote_cmd, self._hosts, index, self._caller_context),
                                              connexion_params = connexion_params,
                                              timeout = self._timeout,
                                              ignore_exit_code = self._ignore_exit_code,
                                              ignore_timeout = self._ignore_timeout,
                                              ignore_error = self._ignore_error,
                                              process_lifecycle_handler = self._process_lifecycle_handler,
                                              pty = get_ssh_scp_pty_option(connexion_params)))

    def _args(self):
        return [ repr(self._hosts),
                 repr(self._remote_cmd) ] + Action._args(self) + Remote._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self._connexion_params: kwargs.append("connexion_params=%r" % (self._connexion_params,))
        return kwargs

    def _infos(self):
        return Action._infos(self)

    def name(self):
        if self._name == None:
            return "%s %s on %i hosts" % (self.__class__.__name__, self._remote_cmd, len(self._hosts))
        else:
            return self._name

    def processes(self):
        return list(self._processes)

    def start(self):
        retval = super(Remote, self).start()
        if len(self._processes) == 0:
            logger.debug("%s contains 0 processes -> immediately terminated" % (self,))
            self._notify_terminated()
        else:
            for process in self._processes:
                process.start()
        return retval

    def kill(self):
        retval = super(Remote, self).kill()
        for process in self._processes:
            process.kill()
        return retval

    def reset(self):
        retval = super(Remote, self).reset()
        for process in self._processes:
            process.reset()
        self._process_lifecycle_handler.action_reset()
        return retval

class _TaktukRemoteOutputHandler(ProcessOutputHandler):

    """Parse taktuk output."""
    
    def __init__(self, taktukaction):
        super(_TaktukRemoteOutputHandler, self).__init__()
        self._taktukaction = taktukaction

    def _log_unexpected_output(self, string):
        logger.critical("%s: Taktuk unexpected output parsing. Please report this message. Line received:" % (self.__class__.__name__,))
        logger.critical(self._describe_taktuk_output(string))

    def _describe_taktuk_output(self, string):
        s = "%s: " % (self.__class__.__name__,)
        try:
            if len(string) > 0:
                header = ord(string[0])
                (position, sep, line) = string[2:].partition(" # ")
                position = int(position)
                if position == 0:
                    host_address = "localhost"
                else:
                    host_address = self._taktukaction._processes[self._taktukaction._taktuk_hosts_order[position-1]].host().address
                if header in (65, 66, 67, 70, 71, 72):
                    if header == 65: t = "stdout"
                    elif header == 66: t = "stderr"
                    elif header == 67: t = "status"
                    elif header == 70: t = "info"
                    elif header == 71: t = "taktuk"
                    elif header == 72: t = "message"
                    s += "%s - host = %s - line = %s" % (t, host_address, line[:-1])
                elif header in (68, 69):
                    (peer_position, sep, line) = line.partition(" # ")
                    peer_host_address = None
                    try:
                        peer_position = int(peer_position)
                        if peer_position == 0:
                            peer_host_address = "localhost"
                        else:
                            peer_host_address = self._taktukaction._processes[self._taktukaction._taktuk_hosts_order[peer_position-1]].host().address
                    except:
                        pass
                    if header == 68:
                        s += "connector - host = %s - peer = %s - line = %s" % (host_address, peer_host_address, line[:-1])
                    elif header == 69:
                        (state_code, sep, state_msg) = line.partition(" # ")
                        s += "state - host = %s - peer = %s - state = %s" % (host_address, peer_host_address, state_msg[:-1])
                elif header == 73:
                    (taktuktype, sep, line) = line.partition(" # ")
                    s += "message type = %s - host = %s - line = %s" % (taktuktype, host_address, line[:-1])
                else:
                    s += "unexpected string = %s" % (string[:-1])
            else:
                s += "empty string"
            return s
        except Exception, e:
            logger.critical("%s: Unexpected exception %s while parsing taktuk output. Please report this message." % (self.__class__.__name__, e))
            logger.critical("line received = %s" % string.rstrip('\n'))
            return s

    def read_line(self, process, string, eof = False, error = False):
        # my taktuk output protocol:
        #  stream    format                                                    header normal?
        #  output    "A $position # $line"                                     65     YES
        #  error     "B $position # $line"                                     66     YES
        #  status    "C $position # $line"                                     67     YES
        #  connector "D $position # $peer_position # $line"                    68     YES
        #  state     "E $position # $peer_position # $line # event_msg($line)" 69     YES
        #  info      "F $position # $line"                                     70     NO
        #  taktuk    "G $position # $line"                                     71     NO
        #  message   "H $position # $line"                                     72     NO
        #  default   "I $position # $type # $line"                             73     NO
        try:
            if len(string) > 0:
                #logger.debug("taktuk: %s" % self._describe_taktuk_output(string))
                header = ord(string[0])
                (position, sep, line) = string[2:].partition(" # ")
                position = int(position)
                if header >= 65 and header <= 67: # stdout, stderr, status
                    if position == 0:
                        self._log_unexpected_output(string)
                        return
                    else:
                        process = self._taktukaction._processes[self._taktukaction._taktuk_hosts_order[position-1]]
                        if header == 65: # stdout
                            process._handle_stdout(line, eof = eof, error = error)
                        elif header == 66: # stderr
                            process._handle_stderr(line, eof = eof, error = error)
                        else: # 67: status
                            process._set_terminated(exit_code = int(line))
                elif header in (68, 69): # connector, state
                    (peer_position, sep, line) = line.partition(" # ")
                    if header == 68: # connector
                        peer_position = int(peer_position)
                        process = self._taktukaction._processes[self._taktukaction._taktuk_hosts_order[peer_position-1]]
                        process._handle_stderr(line)
                    else: # state
                        (state_code, sep, state_msg) = line.partition(" # ")
                        state_code = int(state_code)
                        if state_code == 6 or state_code == 7: # command started or remote command exec failed
                            process = self._taktukaction._processes[self._taktukaction._taktuk_hosts_order[position-1]]
                            if state_code == 6: # command started
                                process.start()
                            else: # 7: remote command exec failed
                                process._set_terminated(error = True, error_reason = "taktuk remote command execution failed")
                        elif state_code == 3 or state_code == 5: # connexion failed or lost
                            peer_position = int(peer_position)
                            process = self._taktukaction._processes[self._taktukaction._taktuk_hosts_order[peer_position-1]]
                            if state_code == 3: # connexion failed
                                process._set_terminated(error = True, error_reason = "taktuk connexion failed")
                            else: # 5: connexion lost
                                process._set_terminated(error = True, error_reason = "taktuk connexion lost")
                        elif state_code in (0, 1, 2, 4, 8):
                            pass
                        else:
                            self._log_unexpected_output(string)
                else:
                    self._log_unexpected_output(string)
        except Exception, e:
            logger.critical("%s: Unexpected exception %s while parsing taktuk output. Please report this message." % (self.__class__.__name__, e))
            logger.critical("line received = %s" % string.rstrip('\n'))

class _TaktukLifecycleHandler(ProcessLifecycleHandler):

    """Notify `TaktukProcess` of their real taktuk `Process` lifecyle."""
    
    def __init__(self, taktukremote):
        self._taktukremote = taktukremote

    def end(self, process):
        error = None
        error_reason = None
        timeouted = None
        forced_kill = None
        if process.error() == True:
            error = True
        if process.error_reason() != None:
            error_reason = "error of taktuk process: %s" % process.error_reason()
        if process.timeouted() == True:
            timeouted = True
        if process.forced_kill() == True:
            forced_kill = True
        for taktukprocess in self._taktukremote.processes():
            if not taktukprocess.ended():
                taktukprocess._set_terminated(error = error,
                                              error_reason = error_reason,
                                              timeouted = timeouted,
                                              forced_kill = forced_kill)

class TaktukRemote(Action):

    """Launch a command remotely on several `Host`, with ``taktuk``.

    One taktuk instance is ran, which itself connects to hosts through
    an ``ssh`` tree.

    Behavior should be identical to `Remote`. Current limitation are:

    - we can provide per-host user with taktuk, but we cannot provide
      per-host port or keyfile, so a check is made that all hosts and
      connexion_params have the same port / keyfile (or None). If not,
      an exception is raised during initialization.

    - remote processes are not killed when killing the
      TaktukRemote. See 'hanged commands' in
      http://taktuk.gforge.inria.fr/taktuk.html#bugs. With ssh the
      workaround is to pass options -tt but passing these options to
      taktuk connector causes immediate closing of the connector upon
      connexion, thus this option is not in default taktuk connector
      options in ``default_connexion_params``.
    """

    def __init__(self, hosts, remote_cmd, connexion_params = None, **kwargs):
        """
        :param hosts: iterable of `Host` to which to connect and run
          the command.

        :param remote_cmd: the command to run remotely. substitions
          described in `remote_substitute` will be performed.

        :param connexion_params: a dict similar to
          `default_connexion_params` whose values will override those
          in `default_connexion_params` for connexion.
        """
        super(TaktukRemote, self).__init__(**kwargs)
        self._remote_cmd = remote_cmd
        self._connexion_params = connexion_params
        self._caller_context = get_caller_context()
        self._hosts = get_hosts_list(hosts)
        self._processes = list()
        self._taktuk_stdout_output_handler = _TaktukRemoteOutputHandler(self)
        self._taktuk_stderr_output_handler = self._taktuk_stdout_output_handler
        self._taktuk_common_init()

    def _args(self):
        return [ repr(self._hosts),
                 repr(self._remote_cmd) ] + Action._args(self) + TaktukRemote._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self._connexion_params: kwargs.append("connexion_params=%r" % (self._connexion_params,))
        return kwargs

    def _infos(self):
        return Action._infos(self)

    def _gen_taktukprocesses(self):
        lifecycle_handler = ActionNotificationProcessLifecycleHandler(self, len(self._hosts))
        for (index, host) in enumerate(self._hosts):
            self._processes.append(TaktukProcess(host,
                                                 remote_substitute(self._remote_cmd, self._hosts, index, self._caller_context),
                                                 timeout = self._timeout,
                                                 ignore_exit_code = self._ignore_exit_code,
                                                 ignore_timeout = self._ignore_timeout,
                                                 ignore_error = self._ignore_error,
                                                 process_lifecycle_handler = lifecycle_handler))

    def _gen_taktuk_commands(self, hosts_with_explicit_user):
        self._taktuk_hosts_order = []
        for (index, host) in [ (idx, h) for (idx, h) in enumerate(self._hosts) if FrozenHost(h) not in hosts_with_explicit_user ]:
            self._taktuk_cmdline += ("-m", host.address, "-[", "exec", "[", self._processes[index].cmd(), "]", "-]",)
            self._taktuk_hosts_order.append(index)
        for (index, host) in [ (idx, h) for (idx, h) in enumerate(self._hosts) if FrozenHost(h) in hosts_with_explicit_user ]:
            self._taktuk_cmdline += ("-l", host.user, "-m", host.address, "-[", "exec", "[", self._processes[index].cmd(), "]", "-]",)
            self._taktuk_hosts_order.append(index)

    def _taktuk_common_init(self):
        # taktuk code common to TaktukRemote and subclasses TaktukGet
        # TaktukPut

        # we can provide per-host user with taktuk, but we cannot
        # provide per-host port or keyfile, so check that all hosts
        # and connexion_params have the same port / keyfile (or None)
        check_default_keyfile = None
        check_default_port = None
        if self._connexion_params != None and self._connexion_params.has_key('keyfile'):
            check_default_keyfile = self._connexion_params['keyfile']
        elif default_connexion_params != None and default_connexion_params.has_key('keyfile'):
            check_default_keyfile = default_connexion_params['keyfile']
        if self._connexion_params != None and self._connexion_params.has_key('port'):
            check_default_port = self._connexion_params['port']
        elif default_connexion_params != None and default_connexion_params.has_key('port'):
            check_default_port = default_connexion_params['port']
        check_keyfiles = set()
        check_ports = set()
        hosts_with_explicit_user = set()
        for host in self._hosts:
            if host.user != None:
                hosts_with_explicit_user.add(FrozenHost(host))
            if host.keyfile != None:
                check_keyfiles.add(host.keyfile)
            else:
                check_keyfiles.add(check_default_keyfile)
            if host.port != None:
                check_ports.add(host.port)
            else:
                check_ports.add(check_default_port)
        if len(check_keyfiles) > 1 or len(check_ports) > 1:
            raise ValueError, "unable to provide more than one keyfile / port for taktuk remote connexion"
        global_keyfile = None
        global_port = None
        if len(check_keyfiles) == 1:
            global_keyfile = list(check_keyfiles)[0]
        if len(check_ports) == 1:
            global_port = list(check_ports)[0]
        self._gen_taktukprocesses()
        self._taktuk_cmdline = ()
        if self._connexion_params != None and self._connexion_params.has_key('taktuk'):
            if self._connexion_params['taktuk'] != None:
                self._taktuk_cmdline += (self._connexion_params['taktuk'],)
            else:
                raise ValueError, "invalid taktuk command in connexion_params %s" % (self._connexion_params,)
        elif default_connexion_params != None and default_connexion_params.has_key('taktuk'):
            if default_connexion_params['taktuk'] != None:
                self._taktuk_cmdline += (default_connexion_params['taktuk'],)
            else:
                raise ValueError, "invalid taktuk command in default_connexion_params %s" % (default_connexion_params,)
        else:
            raise ValueError, "no taktuk command in default_connexion_params %s" % (default_connexion_params,)
        if self._connexion_params != None and self._connexion_params.has_key('taktuk_options'):
            if self._connexion_params['taktuk_options'] != None:
                self._taktuk_cmdline += self._connexion_params['taktuk_options']
        elif default_connexion_params != None and default_connexion_params.has_key('taktuk_options'):
            if default_connexion_params['taktuk_options'] != None:
                self._taktuk_cmdline += default_connexion_params['taktuk_options']
        self._taktuk_cmdline += ("-o", 'output="A $position # $line\\n"',
                                 "-o", 'error="B $position # $line\\n"',
                                 "-o", 'status="C $position # $line\\n"',
                                 "-o", 'connector="D $position # $peer_position # $line\\n"',
                                 "-o", 'state="E $position # $peer_position # $line # ".event_msg($line)."\\n"',
                                 "-o", 'info="F $position # $line\\n"',
                                 "-o", 'taktuk="G $position # $line\\n"',
                                 "-o", 'message="H $position # $line\\n"',
                                 "-o", 'default="I $position # $type > $line\\n"')
        self._taktuk_cmdline += ("-c", " ".join(
            get_taktuk_connector_command(keyfile = global_keyfile,
                                         port = global_port,
                                         connexion_params = self._connexion_params)))
        self._gen_taktuk_commands(hosts_with_explicit_user)
        self._taktuk_cmdline += ("quit",)
        handler = _TaktukRemoteOutputHandler(self)
        self._taktuk = Process(self._taktuk_cmdline,
                               timeout = self._timeout,
                               shell = False,
                               stdout_handler = self._taktuk_stdout_output_handler,
                               stderr_handler = self._taktuk_stderr_output_handler,
                               #default_stdout_handler = False,
                               #default_stderr_handler = False,
                               process_lifecycle_handler = _TaktukLifecycleHandler(self))

    def processes(self):
        return list(self._processes)

    def start(self):
        retval = super(TaktukRemote, self).start()
        if len(self._processes) == 0:
            logger.debug("%s contains 0 processes -> immediately terminated" % (self,))
            self._notify_terminated()
        else:
            self._taktuk.start()
        return retval

    def kill(self):
        retval = super(TaktukRemote, self).kill()
        self._taktuk.kill()
        return retval

    def reset(self):
        retval = super(TaktukRemote, self).reset()
        self._taktuk.reset()
        for process in self._processes:
            process.reset()
        return retval

class Put(Remote):

    """Copy local files to several remote `Host`, with ``scp`` or a similar connexion tool."""

    def __init__(self, hosts, local_files, remote_location = ".", create_dirs = False, connexion_params = None, **kwargs):
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
        super(Remote, self).__init__(**kwargs)
        self._caller_context = get_caller_context()
        self._hosts = get_hosts_list(hosts)
        self._processes = list()
        self._local_files = local_files
        self._remote_location = remote_location
        self._create_dirs = create_dirs
        self._connexion_params = connexion_params
        lifecycle_handler = ActionNotificationProcessLifecycleHandler(self, len(self._hosts))
        for (index, host) in enumerate(self._hosts):
            prepend_dir_creation = ()
            if self._create_dirs:
                created_dir = remote_substitute(self._remote_location, self._hosts, index, self._caller_context)
                prepend_dir_creation = get_ssh_command(host.user, host.keyfile, host.port, self._connexion_params) + (host.address,) + ('\'mkdir -p "%(dir)s" || test -d "%(dir)s"\'' % {'dir': created_dir}, '&&')
            real_command = list(prepend_dir_creation) + list(get_scp_command(host.user, host.keyfile, host.port, self._connexion_params)) + [ remote_substitute(local_file, self._hosts, index, self._caller_context) for local_file in self._local_files ] + ["%s:%s" % (host.address, remote_substitute(self._remote_location, self._hosts, index, self._caller_context)),]
            real_command = ' '.join(real_command)
            self._processes.append(Process(real_command,
                                           timeout = self._timeout,
                                           shell = True,
                                           ignore_exit_code = self._ignore_exit_code,
                                           ignore_timeout = self._ignore_timeout,
                                           ignore_error = self._ignore_error,
                                           process_lifecycle_handler = lifecycle_handler,
                                           pty = get_ssh_scp_pty_option(connexion_params)))

    def _args(self):
        return [ repr(self._hosts),
                 repr(self._local_files) ] + Action._args(self) + Put._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self._remote_location: kwargs.append("remote_location=%r" % (self._remote_location,))
        if self._create_dirs: kwargs.append("create_dirs=%r" % (self._create_dirs,))
        if self._connexion_params: kwargs.append("connexion_params=%r" % (self._connexion_params,))
        return kwargs

    def _infos(self):
        return Action._infos(self)

    def name(self):
        if self._name == None:
            return "%s to %i hosts" % (self.__class__.__name__, len(self._hosts))
        else:
            return self._name

class Get(Remote):

    """Copy remote files from several remote `Host` to a local directory, with ``scp`` or a similar connexion tool."""

    def __init__(self, hosts, remote_files, local_location = ".", create_dirs = False, connexion_params = None, **kwargs):
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
        super(Remote, self).__init__(**kwargs)
        self._caller_context = get_caller_context()
        self._hosts = get_hosts_list(hosts)
        self._processes = list()
        self._remote_files = remote_files
        self._local_location = local_location
        self._create_dirs = create_dirs
        self._connexion_params = connexion_params
        lifecycle_handler = ActionNotificationProcessLifecycleHandler(self, len(self._hosts))
        for (index, host) in enumerate(self._hosts):
            prepend_dir_creation = ()
            if self._create_dirs:
                created_dir = remote_substitute(local_location, self._hosts, index, self._caller_context)
                prepend_dir_creation = ('mkdir', '-p', created_dir, '||', 'test', '-d', created_dir, '&&')
            remote_specs = ()
            for path in self._remote_files:
                remote_specs += ("%s:%s" % (host.address, remote_substitute(path, self._hosts, index, self._caller_context)),)
            real_command = prepend_dir_creation + get_scp_command(host.user, host.keyfile, host.port, self._connexion_params) + remote_specs + (remote_substitute(self._local_location, self._hosts, index, self._caller_context),)
            real_command = ' '.join(real_command)
            self._processes.append(Process(real_command,
                                           timeout = self._timeout,
                                           shell = True,
                                           ignore_exit_code = self._ignore_exit_code,
                                           ignore_timeout = self._ignore_timeout,
                                           ignore_error = self._ignore_error,
                                           process_lifecycle_handler = lifecycle_handler,
                                           pty = get_ssh_scp_pty_option(connexion_params)))

    def _args(self):
        return [ repr(self._hosts),
                 repr(self._remote_files) ] + Action._args(self) + Get._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self._local_location: kwargs.append("local_location=%r" % (self._local_location,))
        if self._create_dirs: kwargs.append("create_dirs=%r" % (self._create_dirs,))
        if self._connexion_params: kwargs.append("connexion_params=%r" % (self._connexion_params,))
        return kwargs

    def _infos(self):
        return Action._infos(self)

    def name(self):
        if self._name == None:
            return "%s from %i hosts" % (self.__class__.__name__, len(self._hosts))
        else:
            return self._name

class _TaktukPutOutputHandler(_TaktukRemoteOutputHandler):

    """Parse taktuk output."""

    def _update_taktukprocess_end_state(self, process):
        if process._num_transfers_started > 0 and not process.started():
            process.start()
        if process._num_transfers_failed + process._num_transfers_terminated >= len(self._taktukaction._local_files):
            if process._num_transfers_failed > 0:
                process._set_terminated(error = True, error_reason = "taktuk file reception failed")
            else:
                process._set_terminated(exit_code = 0)
    
    def read_line(self, process, string, eof = False, error = False):
        try:
            if len(string) > 0:
                header = ord(string[0])
                (position, sep, line) = string[2:].partition(" # ")
                position = int(position)
                if header in (68, 69): # connector, state
                    (peer_position, sep, line) = line.partition(" # ")
                    if header == 68: # connector
                        peer_position = int(peer_position)
                        process = self._taktukaction._processes[self._taktukaction._taktuk_hosts_order[peer_position-1]]
                        process._handle_stderr(line)
                    else: # state
                        (state_code, sep, state_msg) = line.partition(" # ")
                        state_code = int(state_code)
                        if state_code in (13, 14, 15): # file reception started, failed, terminated
                            process = self._taktukaction._processes[self._taktukaction._taktuk_hosts_order[position-1]]
                            if state_code == 13: # file reception started
                                process._num_transfers_started += 1
                            elif state_code == 14: # file reception failed
                                process._num_transfers_failed += 1
                            else: # 15: file reception terminated
                                process._num_transfers_terminated += 1
                            self._update_taktukprocess_end_state(process)
                        elif state_code == 3 or state_code == 5: # connexion failed or lost
                            peer_position = int(peer_position)
                            process = self._taktukaction._processes[self._taktukaction._taktuk_hosts_order[peer_position-1]]
                            if state_code == 3: # connexion failed
                                process._set_terminated(error = True, error_reason = "taktuk connexion failed")
                            else: # 5: connexion lost
                                process._set_terminated(error = True, error_reason = "taktuk connexion lost")
                        elif state_code in (0, 1, 2, 4):
                            pass
                        else:
                            self._log_unexpected_output(string)
                else:
                    self._log_unexpected_output(string)
        except Exception, e:
            logger.critical("%s: Unexpected exception %s while parsing taktuk output. Please report this message." % (self.__class__.__name__, e))
            logger.critical("line received = %s" % string.rstrip('\n'))

class TaktukPut(TaktukRemote):

    """Copy local files to several remote `Host`, with ``taktuk``."""

    def __init__(self, hosts, local_files, remote_location = ".", connexion_params = None, **kwargs):
        """
        :param hosts: iterable of `Host` onto which to copy the files.

        :param local_files: an iterable of string of file
          paths. substitions described in `remote_substitute` will not
          be performed, but taktuk substitutions can be used (see
          http://taktuk.gforge.inria.fr/taktuk.html#item_put__2a_src__2a__2a_dest__2a)
        
        :param remote_location: the directory on the remote hosts were
          the files will be copied. substitions described in
          `remote_substitute` will not be performed, but taktuk
          substitutions can be used (see
          http://taktuk.gforge.inria.fr/taktuk.html#item_put__2a_src__2a__2a_dest__2a)

        :param connexion_params: a dict similar to
          `default_connexion_params` whose values will override those
          in `default_connexion_params` for connexion.
        """
        if local_files != None and (not hasattr(local_files, '__iter__')):
            local_files = (local_files,)
        super(TaktukRemote, self).__init__(**kwargs)
        self._caller_context = get_caller_context()
        self._hosts = get_hosts_list(hosts)
        self._processes = list()
        self._local_files = local_files
        self._remote_location = remote_location
        self._connexion_params = connexion_params
        self._taktuk_stdout_output_handler = _TaktukPutOutputHandler(self)
        self._taktuk_stderr_output_handler = self._taktuk_stdout_output_handler
        self._taktuk_common_init()

    def _args(self):
        return [ repr(self._hosts),
                 repr(self._local_files) ] + Action._args(self) + TaktukPut._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self._remote_location: kwargs.append("remote_location=%r" % (self._remote_location,))
        if self._connexion_params: kwargs.append("connexion_params=%r" % (self._connexion_params,))
        return kwargs

    def _infos(self):
        return TaktukRemote._infos(self)

    def name(self):
        if self._name == None:
            return "%s from %i hosts" % (self.__class__.__name__, len(self._hosts))
        else:
            return self._name

    def _gen_taktukprocesses(self):
        lifecycle_handler = ActionNotificationProcessLifecycleHandler(self, len(self._hosts))
        for (index, host) in enumerate(self._hosts):
            process = TaktukProcess(host,
                                    "",
                                    timeout = self._timeout,
                                    ignore_exit_code = self._ignore_exit_code,
                                    ignore_timeout = self._ignore_timeout,
                                    ignore_error = self._ignore_error,
                                    process_lifecycle_handler = lifecycle_handler)
            process._num_transfers_started = 0
            process._num_transfers_terminated = 0
            process._num_transfers_failed = 0
            self._processes.append(process)

    def _gen_taktuk_commands(self, hosts_with_explicit_user):
        self._taktuk_hosts_order = []
        for (index, host) in [ (idx, h) for (idx, h) in enumerate(self._hosts) if FrozenHost(h) not in hosts_with_explicit_user ]:
            self._taktuk_cmdline += ("-m", host.address)
            self._taktuk_hosts_order.append(index)
        for (index, host) in [ (idx, h) for (idx, h) in enumerate(self._hosts) if FrozenHost(h) in hosts_with_explicit_user ]:
            self._taktuk_cmdline += ("-l", host.user, "-m", host.address)
            self._taktuk_hosts_order.append(index)
        for src in self._local_files:
            self._taktuk_cmdline += ("broadcast", "put", "[", src, "]", "[", self._remote_location, "]", ";")

    def reset(self):
        retval = super(TaktukPut, self).reset()
        for process in self._processes:
            process._num_transfers_started = 0
            process._num_transfers_terminated = 0
            process._num_transfers_failed = 0
        return retval

class _TaktukGetOutputHandler(_TaktukRemoteOutputHandler):

    """Parse taktuk output."""

    def _update_taktukprocess_end_state(self, process):
        if process._num_transfers_started > 0 and not process.started():
            process.start()
        if process._num_transfers_failed + process._num_transfers_terminated >= len(self._taktukaction._remote_files):
            if process._num_transfers_failed > 0:
                process._set_terminated(error = True, error_reason = "taktuk file reception failed")
            else:
                process._set_terminated(exit_code = 0)
    
    def read_line(self, process, string, eof = False, error = False):
        try:
            if len(string) > 0:
                header = ord(string[0])
                (position, sep, line) = string[2:].partition(" # ")
                position = int(position)
                if header in (68, 69): # connector, state
                    (peer_position, sep, line) = line.partition(" # ")
                    if header == 68: # connector
                        peer_position = int(peer_position)
                        process = self._taktukaction._processes[self._taktukaction._taktuk_hosts_order[peer_position-1]]
                        process._handle_stderr(line)
                    else: # state
                        (state_code, sep, state_msg) = line.partition(" # ")
                        state_code = int(state_code)
                        if state_code in (13, 14, 15): # file reception started, failed, terminated
                            peer_position = int(peer_position)
                            process = self._taktukaction._processes[self._taktukaction._taktuk_hosts_order[peer_position-1]]
                            if state_code == 13: # file reception started
                                process._num_transfers_started += 1
                            elif state_code == 14: # file reception failed
                                process._num_transfers_failed += 1
                            else: # 15: file reception terminated
                                process._num_transfers_terminated += 1
                            self._update_taktukprocess_end_state(process)
                        elif state_code == 3 or state_code == 5: # connexion failed or lost
                            peer_position = int(peer_position)
                            process = self._taktukaction._processes[self._taktukaction._taktuk_hosts_order[peer_position-1]]
                            if state_code == 3: # connexion failed
                                process._set_terminated(error = True, error_reason = "taktuk connexion failed")
                            else: # 5: connexion lost
                                process._set_terminated(error = True, error_reason = "taktuk connexion lost")
                        elif state_code in (0, 1, 2, 4, 19):
                            pass
                        else:
                            self._log_unexpected_output(string)
                else:
                    self._log_unexpected_output(string)
        except Exception, e:
            logger.critical("%s: Unexpected exception %s while parsing taktuk output. Please report this message." % (self.__class__.__name__, e))
            logger.critical("line received = %s" % string.rstrip('\n'))

class TaktukGet(TaktukRemote):

    """Copy remote files from several remote `Host` to a local directory, with ``taktuk``."""

    def __init__(self, hosts, remote_files, local_location = ".", connexion_params = None, **kwargs):
        """
        :param hosts: iterable of `Host` from which to get the files.

        :param remote_files: an iterable of string of file
          paths. Substitions described in `remote_substitute` will not
          be performed, but taktuk substitutions can be used (see
          http://taktuk.gforge.inria.fr/taktuk.html#item_get__2a_src__2a__2a_dest__2a)

        :param local_location: the local directory were the files will
          be copied. Substitions described in `remote_substitute` will
          not be performed, but taktuk substitutions can be used (see
          http://taktuk.gforge.inria.fr/taktuk.html#item_get__2a_src__2a__2a_dest__2a)

        :param connexion_params: a dict similar to
          `default_connexion_params` whose values will override those
          in `default_connexion_params` for connexion.
        """
        if remote_files != None and (not hasattr(remote_files, '__iter__')):
            remote_files = (remote_files,)
        super(TaktukRemote, self).__init__(**kwargs)
        self._caller_context = get_caller_context()
        self._hosts = get_hosts_list(hosts)
        self._processes = list()
        self._remote_files = remote_files
        self._local_location = local_location
        self._connexion_params = connexion_params
        self._taktuk_stdout_output_handler = _TaktukGetOutputHandler(self)
        self._taktuk_stderr_output_handler = self._taktuk_stdout_output_handler
        self._taktuk_common_init()

    def _args(self):
        return [ repr(self._hosts),
                 repr(self._remote_files) ] + Action._args(self) + TaktukGet._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self._local_location: kwargs.append("local_location=%r" % (self._local_location,))
        if self._connexion_params: kwargs.append("connexion_params=%r" % (self._connexion_params,))
        return kwargs

    def _infos(self):
        return TaktukRemote._infos(self)

    def name(self):
        if self._name == None:
            return "%s from %i hosts" % (self.__class__.__name__, len(self._hosts))
        else:
            return self._name

    def _gen_taktukprocesses(self):
        lifecycle_handler = ActionNotificationProcessLifecycleHandler(self, len(self._hosts))
        for (index, host) in enumerate(self._hosts):
            process = TaktukProcess(host,
                                    "",
                                    timeout = self._timeout,
                                    ignore_exit_code = self._ignore_exit_code,
                                    ignore_timeout = self._ignore_timeout,
                                    ignore_error = self._ignore_error,
                                    process_lifecycle_handler = lifecycle_handler)
            process._num_transfers_started = 0
            process._num_transfers_terminated = 0
            process._num_transfers_failed = 0
            self._processes.append(process)

    def _gen_taktuk_commands(self, hosts_with_explicit_user):
        self._taktuk_hosts_order = []
        for (index, host) in [ (idx, h) for (idx, h) in enumerate(self._hosts) if FrozenHost(h) not in hosts_with_explicit_user ]:
            self._taktuk_cmdline += ("-m", host.address)
            self._taktuk_hosts_order.append(index)
        for (index, host) in [ (idx, h) for (idx, h) in enumerate(self._hosts) if FrozenHost(h) in hosts_with_explicit_user ]:
            self._taktuk_cmdline += ("-l", host.user, "-m", host.address)
            self._taktuk_hosts_order.append(index)
        for src in self._remote_files:
            self._taktuk_cmdline += ("broadcast", "get", "[", src, "]", "[", self._local_location, "]", ";")

    def reset(self):
        retval = super(TaktukGet, self).reset()
        for process in self._processes:
            process._num_transfers_started = 0
            process._num_transfers_terminated = 0
            process._num_transfers_failed = 0
        return retval

class Local(Action):

    """Launch a command localy."""

    def __init__(self, cmd, **kwargs):
        """
        :param cmd: the command to run.
        """
        super(Local, self).__init__(**kwargs)
        self._cmd = cmd
        self._process = Process(self._cmd,
                                timeout = self._timeout,
                                shell = True,
                                ignore_exit_code = self._ignore_exit_code,
                                ignore_timeout = self._ignore_timeout,
                                ignore_error = self._ignore_error,
                                process_lifecycle_handler = ActionNotificationProcessLifecycleHandler(self, 1))

    def _args(self):
        return [ repr(self._cmd) ] + Action._args(self) + Local._kwargs(self)

    def _kwargs(self):
        return []

    def _infos(self):
        return Action._infos(self)

    def name(self):
        if self._name == None:
            return "%s %s" % (self.__class__.__name__, self._cmd)
        else:
            return self._name

    def processes(self):
        return [ self._process ]

    def start(self):
        retval = super(Local, self).start()
        self._process.start()
        return retval

    def kill(self):
        retval = super(Local, self).kill()
        self._process.kill()
        return retval

    def reset(self):
        retval = super(Local, self).reset()
        self._process.reset()
        return retval

class ParallelSubActionLifecycleHandler(ActionLifecycleHandler):

    def __init__(self, parallelaction, total_parallel_subactions):
        self._parallelaction = parallelaction
        self._total_parallel_subactions = total_parallel_subactions
        self._terminated_subactions = 0

    def reset(self, action):
        self._terminated_subactions = 0

    def end(self, action):
        self._terminated_subactions += 1
        logger.debug("%i/%i subactions terminated in %s" % (self._terminated_subactions,
                                                            self._total_parallel_subactions,
                                                            self._parallelaction))
        if self._terminated_subactions == self._total_parallel_subactions:
            self._parallelaction._notify_terminated()

class ParallelActions(Action):

    """An `Action` running several sub-`Action` in parallel.

    Will start, kill, wait, run every `Action` in parallel.
    """

    def __init__(self, actions, **kwargs):
        if kwargs.has_key('timeout'):
            raise AttributeError, "ParallelActions doesn't support timeouts. The timeouts are those of each contained Actions"
        if kwargs.has_key('ignore_exit_code'):
            raise AttributeError, "ParallelActions doesn't support ignore_exit_code. The ignore_exit_code flags are those of each contained Actions"
        if kwargs.has_key('ignore_timeout'):
            raise AttributeError, "ParallelActions doesn't support ignore_timeout. The ignore_timeout flags are those of each contained Actions"
        super(ParallelActions, self).__init__(**kwargs)
        self._actions = list(actions)
        self._subactions_lifecycle_handler = ParallelSubActionLifecycleHandler(self, len(self._actions))
        for action in self._actions:
            action.add_lifecycle_handler(self._subactions_lifecycle_handler)

    def _args(self):
        return [ repr(self._actions) ] + Action._args(self) + ParallelActions._kwargs(self)

    def _kwargs(self):
        return []

    def _infos(self):
        return Action._infos(self)

    def name(self):
        if self._name == None:
            return "%s %i actions" % (self.__class__.__name__, len(self._actions))
        else:
            return self._name

    def actions(self):
        """Return an iterable of `Action` that this `ParallelActions` gathers."""
        return self._actions

    def start(self):
        retval = super(ParallelActions, self).start()
        for action in self._actions:
            action.start()
        return retval

    def kill(self):
        retval = super(ParallelActions, self).kill()
        for action in self._actions:
            action.kill()
        return retval

    def reset(self):
        retval = super(ParallelActions, self).reset()
        for action in self._actions:
            action.reset()
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

class SequentialSubActionLifecycleHandler(ActionLifecycleHandler):

    def __init__(self, sequentialaction, index, total, next_subaction):
        self._sequentialaction = sequentialaction
        self._index = index
        self._total = total
        self._next_subaction = next_subaction

    def end(self, action):
        logger.debug("%i/%i subactions terminated in %s" % (self._index,
                                                            self._total,
                                                            self._sequentialaction))
        if self._next_subaction:
            self._next_subaction.start()
        else:
            self._sequentialaction._notify_terminated()

class SequentialActions(Action):

    """An `Action` running several sub-`Action` sequentially.

    Will start, kill, wait, run every `Action` sequentially.
    """

    def __init__(self, actions, **kwargs):
        if kwargs.has_key('timeout'):
            raise AttributeError, "SequentialActions doesn't support timeouts. The timeouts are those of each contained Actions"
        if kwargs.has_key('ignore_exit_code'):
            raise AttributeError, "SequentialActions doesn't support ignore_exit_code. The ignore_exit_code flags are those of each contained Actions"
        if kwargs.has_key('ignore_timeout'):
            raise AttributeError, "SequentialActions doesn't support ignore_timeout. The ignore_timeout flags are those of each contained Actions"
        super(SequentialActions, self).__init__(**kwargs)
        self._actions = list(actions)
        for (index, action) in enumerate(self._actions):
            if index + 1 < len(self._actions):
                next_action = self._actions[index + 1]
            else:
                next_action = None
            action.add_lifecycle_handler(SequentialSubActionLifecycleHandler(self,
                                                                             index,
                                                                             len(self._actions),
                                                                             next_action))

    def _args(self):
        return [ repr(self._actions) ] + Action._args(self) + SequentialActions._kwargs(self)

    def _kwargs(self):
        return []

    def _infos(self):
        return Action._infos(self)

    def name(self):
        if self._name == None:
            return "%s %i actions" % (self.__class__.__name__, len(self._actions))
        else:
            return self._name

    def actions(self):
        """Return an iterable of `Action` that this `SequentialActions` gathers."""
        return self._actions

    def start(self):
        retval = super(SequentialActions, self).start()
        self._actions[0].start()
        return retval

    def kill(self):
        retval = super(SequentialActions, self).kill()
        for action in self._actions:
            action.kill()
        return retval

    def reset(self):
        retval = super(SequentialActions, self).reset()
        for action in self._actions:
            action.reset()
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

if __name__ == "__main__":
    import doctest
    configuration['color_mode'] = False
    doctest.testmod()


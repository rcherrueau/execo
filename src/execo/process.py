# Copyright 2009-2014 INRIA Rhone-Alpes, Service Experimentation et
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

from conductor import the_conductor
from config import configuration
from execo.config import make_connection_params
from execo.host import Host
from log import style, logger
from pty import openpty
from ssh_utils import get_ssh_command, get_rewritten_host_address
from time_utils import format_unixts, get_seconds
from utils import compact_output, name_from_cmdline, intr_cond_wait, get_port
from traceback import format_exc
from report import Report
import errno, os, re, shlex, signal, subprocess
import threading, time, pipes

class ProcessLifecycleHandler(object):

    """Abstract handler for `execo.process.ProcessBase` lifecycle."""

    def start(self, process):
        """Handle `execo.process.ProcessBase`'s start.

        :param process: The ProcessBase which starts.
        """
        pass

    def end(self, process):
        """Handle  `execo.process.ProcessBase`'s end.

        :param process: The ProcessBase which ends.
        """
        pass

    def reset(self, process):
        """Handle `execo.process.ProcessBase`'s reset.

        :param process: The ProcessBase which is reset.
        """
        pass

class ProcessOutputHandler(object):

    """Abstract handler for `execo.process.ProcessBase` output."""

    def __init__(self):
        """ProcessOutputHandler constructor. Call it in inherited classes."""
        self._buffer = {}

    def read(self, process, string, eof, error):
        """Handle string read from a `execo.process.ProcessBase`'s stream.

        :param process: the ProcessBase which outputs the string

        :param string: the string read

        :param eof:(boolean) true if the stream is now at eof.

        :param error: (boolean) true if there was an error on the
          stream
        """
        if not process in self._buffer:
            self._buffer[process] = ""
        self._buffer[process] += string
        while True:
            (line, sep, remaining) = self._buffer[process].partition('\n')
            if sep != '':
                self.read_line(process, line + sep, False, False)
                self._buffer[process] = remaining
            else:
                break
        if eof or error:
            self.read_line(process, self._buffer[process], eof, error)
            self._buffer[process] = ""

    def read_line(self, process, string, eof, error):
        """Handle string read line by line from a `execo.process.ProcessBase`'s stream.

        :param process: the ProcessBase which outputs the line

        :param string: the line read

        :param eof:(boolean) true if the stream is now at eof.

        :param error: (boolean) true if there was an error on the
          stream
        """
        pass

class _debugio_output_handler(ProcessOutputHandler):

    def __init__(self, prefix):
        super(_debugio_output_handler, self).__init__()
        self.prefix = prefix

    def read_line(self, process, string, eof, error):
        logger.iodebug(style.emph(("pid %i " % (process.pid,) if hasattr(process, "pid") else "")
                                  + self.prefix + ": "
                                  + ("(EOF) " if eof else "")
                                  + ("(ERROR) " if error else ""))
                       + repr(string))

_debugio_stdout_handler = _debugio_output_handler("stdout")
_debugio_stderr_handler = _debugio_output_handler("stderr")

class ProcessBase(object):

    """An almost abstract base class for all kinds of processes.

    There are no abstract methods, but a `execo.process.ProcessBase`
    by itself is almost useless, it only provides accessors to data
    shared with all subclasses, but no way to start it or stop
    it. These methods have to be implemented in concrete subclasses.

    It is possible to register custom lifecycle and output handlers to
    the process, in order to provide specific actions or stdout/stderr
    parsing when needed. See `execo.process.ProcessLifecycleHandler`
    and `execo.process.ProcessOutputHandler`.
    """

    def __init__(self, cmd):
        """
        :param cmd: string or tuple containing the command and args to run.
        """
        self.cmd = cmd
        """Process command line: string or tuple containing the command and args to
        run."""
        self.host = None
        """Remote host, if relevant"""
        self.started = False
        """Whether the process was started or not"""
        self.start_date = None
        """Process start date or None if not yet started"""
        self.ended = False
        """Whether the process has ended or not"""
        self.end_date = None
        """Process end date or None if not yet ended"""
        self.error = False
        """Whether there was an error starting the process. This is not the process
        return code."""
        self.error_reason = None
        """Operating system level errno, if there was an error starting the
        process, or None"""
        self.exit_code = None
        """Process exit code if available (if the process ended correctly from the
        operating system point of view), or None"""
        self.timeout = None
        """timeout (in seconds, or None for no timeout) after which the process
        will automatically be sent a SIGTERM"""
        self.timeout_date = None
        """The date at wich this process will timeout or none if not available."""
        self.timeouted = False
        """Whether the process has reached its timeout, or None if we don't know yet
        (process still running, timeout not reached)."""
        self.forced_kill = False
        """Whether the process was killed forcibly. When a process is killed with
        SIGTERM (either manually or automatically, due to reaching a
        timeout), execo will wait some time (constant set in execo
        source) and if after this timeout the process is still running,
        it will be killed forcibly with a SIGKILL."""
        self.stdout = ""
        """Process stdout"""
        self.stderr = ""
        """Process stderr"""
        self.ignore_exit_code = False
        """Boolean. If True, a process with a return code != 0 will still be
        considered ok"""
        self.ignore_timeout = False
        """Boolean. If True, a process which reaches its timeout will be sent a
        SIGTERM, but will still be considered ok (but it will most
        probably have an exit code != 0)"""
        self.ignore_error = False
        """Boolean. If True, a process raising an OS level error will still be
        considered ok"""
        self.nolog_exit_code = False
        """Boolean. If False, termination of a process with a return code != 0 will
        cause a warning in logs"""
        self.nolog_timeout = False
        """Boolean. If False, a process which reaches its timeout and is sent a
        SIGTERM will cause a warning in logs"""
        self.nolog_error = False
        """Boolean. If False, a process raising an OS level error will cause a
        warning in logs"""
        self.default_stdout_handler = True
        """if True, a default handler sends stdout stream output to the member string self.stdout"""
        self.default_stderr_handler = True
        """if True, a default handler sends stderr stream output to the member string self.stderr"""
        self.lifecycle_handlers = list()
        """List of instances of `execo.process.ProcessLifecycleHandler` for being
        notified of process lifecycle events."""
        self.stdout_handlers = list()
        """List which can contain instances of `execo.process.ProcessOutputHandler`
        for handling activity on process stdout, or existing file
        descriptors (positive integer), or existing file objects, or
        filenames, to which stdout will be sent. If a filename is given,
        it will be opened in write mode, and closed on eof"""
        self.stderr_handlers = list()
        """List which can contain instances of `execo.process.ProcessOutputHandler`
        for handling activity on process stderr, or existing file
        descriptors (positive integer), or existing file objects, or
        filenames, to which stderr will be sent. If a filename is given,
        it will be opened in write mode, and closed on eof"""
        self.name = name_from_cmdline(self.cmd)
        """User-friendly name. A default is generated and can be changed."""
        self.stdout_ioerror = False
        self.stderr_ioerror = False
        self._lock = threading.RLock()
        self._stdout_files = dict()
        self._stderr_files = dict()

    def _common_reset(self):
        # all methods _common_reset() of this class hierarchy contain
        # the code common to the constructor and reset() to
        # reinitialize an object. If redefined in a child class,
        # _common_reset() must then explicitely call _common_reset()
        # of its parent class.

        # no lock taken. This is the job of calling code to
        # synchronize.
        self.started = False
        self.start_date = None
        self.ended = False
        self.end_date = None
        self.error = False
        self.error_reason = None
        self.exit_code = None
        self.timeout_date = None
        self.timeouted = False
        self.forced_kill = False
        self.stdout = ""
        self.stderr = ""
        self.stdout_ioerror = False
        self.stderr_ioerror = False

    def _args(self):
        # to be implemented in all subclasses. Must return a list with
        # all arguments to the constructor, beginning by the
        # positionnal arguments, finishing by keyword arguments. This
        # list will be directly used in __repr__ methods.
        return [ style.command(repr(self.cmd)) ] + ProcessBase._kwargs(self)

    def _kwargs(self):
        # to be implemented in all subclasses. Must return a list with
        # all keyword arguments to the constructor. This list will be
        # used to build the list returned by _args() of this class or
        # child classes.
        return []

    def _infos(self):
        # to be implemented in all subclasses. Must return a list with
        # all relevant infos other than those returned by _args(), for
        # use in __str__ methods.
        infos = []
        if self.timeout: infos.append("timeout=%r" % (self.timeout,))
        if self.ignore_exit_code != False: infos.append("ignore_exit_code=%r" % (self.ignore_exit_code,))
        if self.ignore_timeout != False: infos.append("ignore_timeout=%r" % (self.ignore_timeout,))
        if self.ignore_error != False: infos.append("ignore_error=%r" % (self.ignore_error,))
        if self.nolog_exit_code != False: infos.append("nolog_exit_code=%r" % (self.nolog_exit_code,))
        if self.nolog_timeout != False: infos.append("nolog_timeout=%r" % (self.nolog_timeout,))
        if self.nolog_error != False: infos.append("nolog_error=%r" % (self.nolog_error,))
        if self.default_stdout_handler != True: infos.append("default_stdout_handler=%r" % (self.default_stdout_handler,))
        if self.default_stderr_handler != True: infos.append("default_stderr_handler=%r" % (self.default_stderr_handler,))
        if self.forced_kill: infos.append("forced_kill=%s" % (self.forced_kill,))
        infos.extend([
            "name=%s" % (self.name,),
            "started=%s" % (self.started,),
            "start_date=%s" % (format_unixts(self.start_date),),
            "ended=%s" % (self.ended,),
            "end_date=%s" % (format_unixts(self.end_date),),
            "error=%s" % (self.error,),
            "error_reason=%s" % (self.error_reason,),
            "timeouted=%s" % (self.timeouted,),
            "exit_code=%s" % (self.exit_code,),
            "ok=%s" % (self.ok,) ])
        return infos

    def __repr__(self):
        # implemented once for all subclasses
        with self._lock:
            return "%s(%s)" % (self.__class__.__name__, ", ".join(self._args()))

    def __str__(self):
        # implemented once for all subclasses
        with self._lock:
            return "<" + style.object_repr(self.__class__.__name__) + "(%s)>" % (", ".join(self._args() + self._infos()),)

    def dump(self):
        with self._lock:
            return " %s\n" % (str(self),)+ style.emph("stdout:") + "\n%s\n" % (compact_output(self.stdout),) + style.emph("stderr:") + "\n%s" % (compact_output(self.stderr),)

    @property
    def running(self):
        """If the process is currently running."""
        with self._lock:
            return self.started and not self.ended

    def _handle_stdout(self, string, eof, error):
        """Handle stdout activity.

        :param string: available stream output in string

        :param eof: True if end of file on stream

        :param error: True if error on stream
        """
        _debugio_stdout_handler.read(self, string, eof, error)
        if self.default_stdout_handler:
            self.stdout += string
        if error == True:
            self.stdout_ioerror = True
        for handler in self.stdout_handlers:
            try:
                if isinstance(handler, int):
                    os.write(handler, string)
                elif isinstance(handler, ProcessOutputHandler):
                    handler.read(self, string, eof, error)
                elif hasattr(handler, "write"):
                    handler.write(string)
                elif isinstance(handler, basestring):
                    if not self._stdout_files.get(handler):
                        self._stdout_files[handler] = open(handler, "w")
                    self._stdout_files[handler].write(string)
                    if eof:
                        self._stdout_files[handler].close()
                        del self._stdout_files[handler]
            except Exception, e:
                logger.error("process stdout handler %s raised exception for process %s:\n%s" % (
                        handler, self, format_exc()))

    def _handle_stderr(self, string, eof, error):
        """Handle stderr activity.

        :param string: available stream output in string

        :param eof: True if end of file on stream

        :param error: True if error on stream
        """
        _debugio_stderr_handler.read(self, string, eof, error)
        if self.default_stderr_handler:
            self.stderr += string
        if error == True:
            self.stderr_ioerror = True
        for handler in self.stderr_handlers:
            try:
                if isinstance(handler, int):
                    os.write(handler, string)
                elif isinstance(handler, ProcessOutputHandler):
                    handler.read(self, string, eof, error)
                elif hasattr(handler, "write"):
                    handler.write(string)
                elif isinstance(handler, basestring):
                    if not self._stderr_files.get(handler):
                        self._stderr_files[handler] = open(handler, "w")
                    self._stderr_files[handler].write(string)
                    if eof:
                        self._stderr_files[handler].close()
                        del self._stderr_files[handler]
            except Exception, e:
                logger.error("process stderr handler %s raised exception for process %s:\n%s" % (
                        handler, self, format_exc()))

    @property
    def ok(self):
        """Check process is ok.

        A process is ok, if:

        - it is not yet started or not yet ended

        - it started and ended and:

          - has no error (or was instructed to ignore them)

          - did not timeout (or was instructed to ignore it)

          - returned 0 (or was instructed to ignore a non zero exit
            code)
        """
        with self._lock:
            if not self.started: return True
            if self.started and not self.ended: return True
            return ((not self.error or self.ignore_error)
                    and (not self.timeouted or self.ignore_timeout)
                    and (self.exit_code == 0 or self.ignore_exit_code))

    @property
    def finished_ok(self):
        """Check process has ran and is ok.

        A process is finished_ok if it has started and ended and
        it is ok.
        """
        with self._lock:
            return self.started and self.ended and self.ok

    def _log_terminated(self):
        """To be called (in subclasses) when a process terminates.

        This method will log process termination as needed.
        """
        with self._lock:
            s = style.emph("terminated:") + self.dump()
            warn = ((self.error and not self.nolog_error)
                    or (self.timeouted and not self.nolog_timeout)
                    or (self.exit_code != 0 and not self.nolog_exit_code))
        # actual logging outside the lock to avoid deadlock between process lock and logging lock
        if warn:
            logger.warning(s)
        else:
            logger.debug(s)

    def kill(self, sig = signal.SIGTERM, auto_sigterm_timeout = True):
        raise NotImplementedError

    def wait(self, timeout = None):
        raise NotImplementedError

    def reset(self):
        """Reinitialize a process so that it can later be restarted.

        If it is running, this method will first kill it then wait for
        its termination before reseting;
        """
        logger.debug(style.emph("reset:") + " %s" % (str(self),))
        if self.started and not self.ended:
            self.kill()
            self.wait()
        for handler in self.lifecycle_handlers:
            try:
                handler.reset(self)
            except Exception, e:
                logger.error("process lifecycle handler %s reset raised exception for process %s:\n%s" % (
                        handler, self, format_exc()))
        with self._lock:
            self._common_reset()
        return self

    def stats(self):
        """Return a dict summarizing the statistics of this process.

        see `execo.report.Report.stats`.
        """
        with self._lock:
            stats = Report.empty_stats()
            stats['name'] = self.name
            stats['start_date'] = self.start_date
            stats['end_date'] = self.end_date
            stats['num_processes'] = 1
            if self.started: stats['num_started'] += 1
            if self.ended: stats['num_ended'] += 1
            if self.error: stats['num_errors'] += 1
            if self.timeouted: stats['num_timeouts'] += 1
            if self.forced_kill: stats['num_forced_kills'] += 1
            if (self.started
                and self.ended
                and self.exit_code != 0):
                stats['num_non_zero_exit_codes'] += 1
            if self.ok:
                stats['num_ok'] += 1
            if self.finished_ok:
                stats['num_finished_ok'] +=1
            return stats

def _get_childs(pid):
    childs = []
    try:
        s = subprocess.Popen(("ps", "--ppid", str(pid)), stdout=subprocess.PIPE).communicate()[0]
        tmp_childs = [ int(c) for c in re.findall("^\s*(\d+)\s+", s, re.MULTILINE) ]
        childs.extend(tmp_childs)
        for child in tmp_childs:
            childs.extend(_get_childs(child))
    except OSError:
        pass
    return childs

class Process(ProcessBase):

    r"""Handle an operating system process.

    In coordination with the internal _Conductor I/O and process
    lifecycle management thread which is started when execo is
    imported, this class allows creation, start, interruption (kill),
    and waiting (for the termination) of an operating system
    process. The subprocess output streams (stdout, stderr), as well
    as various informations about the subprocess and its state can be
    accessed asynchronously.

    Example usage of the process class: run an iperf server, and
    connect to it with an iperf client:

    >>> server = Process('iperf -s')
    >>> server.ignore_exit_code = server.nolog_exit_code = True
    >>> server.start()
    Process('iperf -s')
    >>> client = Process('iperf -c localhost -t 2').start()
    >>> client.started
    True
    >>> client.ended
    False
    >>> client.wait()
    Process('iperf -c localhost -t 2')
    >>> client.ended
    True
    >>> server.ended
    False
    >>> server.kill()
    Process('iperf -s')
    >>> server.wait()
    Process('iperf -s')
    >>> server.ended
    True
    """

    def __init__(self, cmd):
        """
        :param cmd: string or tuple containing the command and args to run.
        """
        super(Process, self).__init__(cmd)
        self.close_stdin = None
        """Whether or not to close subprocess's stdin. If None (default value),
        automatically choose based on pty."""
        self.shell = False
        """Whether or not to use a shell to run the cmd. See ``subprocess.Popen``"""
        self.pty = False
        """If True, open a pseudo tty and connect process's stdin and stdout to it
        (stderr is still connected as a pipe). Make process a session
        leader. If lacking permissions to send signals to the process,
        try to simulate sending control characters to its pty."""
        self.kill_subprocesses = None
        """If True, signals are also sent to subprocesses. If None, automatically
        decide based on shell = True/False."""
        self.process = None
        self.pid = None
        """Subprocess's pid, if available (subprocess started) or None"""
        self._already_got_sigterm = False
        self._ptymaster = None
        self._ptyslave = None
        self.__start_pending = False

    def _actual_close_stdin(self):
        # return actual close_stdin behavior
        if self.close_stdin == None:
            if self.pty: return False
            else: return True
        else:
            return self.close_stdin

    def _actual_kill_subprocesses(self):
        # return actual kill_subprocesses behavior
        if self.kill_subprocesses == None:
            return self.shell
        else:
            return self.kill_subprocesses

    def _actual_cmd(self):
        # return actual cmd
        if self.shell == False and (isinstance(self.cmd, str) or isinstance(self.cmd, unicode)):
            return shlex.split(self.cmd)
        elif self.shell == True and hasattr(self.cmd, '__iter__'):
            return str(" ".join([ pipes.quote(arg) for arg in self.cmd ]))
        else:
            if isinstance(self.cmd, unicode):
                return str(self.cmd)
            else:
                return self.cmd

    def _common_reset(self):
        super(Process, self)._common_reset()
        self.process = None
        self.pid = None
        self._already_got_sigterm = False
        self._ptymaster = None
        self._ptyslave = None

    def _args(self):
        return ProcessBase._args(self) + Process._kwargs(self)

    def _kwargs(self):
        return []

    def _infos(self):
        infos = []
        if self.close_stdin != None: infos.append("close_stdin=%r" % (self.close_stdin,))
        if self.shell != False: infos.append("shell=%r" % (self.shell,))
        if self.pty != False: infos.append("pty=%r" % (self.pty,))
        if self.kill_subprocesses != None: infos.append("kill_subprocesses=%r" % (self.kill_subprocesses,))
        infos.append("pid=%s" % (self.pid,))
        return ProcessBase._infos(self) + infos

    @property
    def stdout_fd(self):
        """Return the subprocess stdout filehandle.

        Or None if not available.
        """
        with self._lock:
            if self.process != None:
                if self.pty:
                    return self._ptymaster
                else:
                    return self.process.stdout.fileno()
            else:
                return None

    @property
    def stderr_fd(self):
        """Return the subprocess stderr filehandle.

        Or None if not available.
        """
        with self._lock:
            if self.process != None:
                return self.process.stderr.fileno()
            else:
                return None

    @property
    def stdin_fd(self):
        """Return the subprocess stdin filehandle.

        Or None if not available.
        """
        with self._lock:
            if self.process != None and not self._actual_close_stdin():
                if self.pty:
                    return self._ptymaster
                else:
                    return self.process.stdin.fileno()
            else:
                return None

    def start(self):
        """Start the subprocess."""
        with self._lock:
            if self.started or self.ended or self.__start_pending:
                return self
            self.__start_pending = True
        logger.debug("enqueue start of " + str(self))
        the_conductor.start_process(self)
        return self

    def _actual_start(self):
        # intended to be called from the conductor thread, in a
        # specific section of the conductor loop.  careful placement
        # of locked section to avoid deadlock with logging lock, and
        # to allow calling lifecycle handlers outside the lock
        with self._lock:
            self.started = True
            self.__start_pending = False
            self.start_date = time.time()
            if self.timeout != None:
                self.timeout_date = self.start_date + self.timeout
        logger.debug(style.emph("start: ") + str(self))
        for handler in self.lifecycle_handlers:
            try:
                handler.start(self)
            except Exception, e:
                logger.error("process lifecycle handler %s start raised exception for process %s:\n%s" % (
                        handler, self, format_exc()))
        try:
            if self.pty:
                (self._ptymaster, self._ptyslave) = openpty()
                self.process = subprocess.Popen(self._actual_cmd(),
                                                stdin = self._ptyslave,
                                                stdout = self._ptyslave,
                                                stderr = subprocess.PIPE,
                                                close_fds = True,
                                                shell = self.shell,
                                                preexec_fn = lambda: os.setpgid(0, the_conductor.pgrp))
            else:
                self.process = subprocess.Popen(self._actual_cmd(),
                                                stdin = subprocess.PIPE,
                                                stdout = subprocess.PIPE,
                                                stderr = subprocess.PIPE,
                                                close_fds = True,
                                                shell = self.shell,
                                                preexec_fn = lambda: os.setpgid(0, the_conductor.pgrp))
            self.pid = self.process.pid
            if self._actual_close_stdin():
                self.process.stdin.close()
        except OSError, e:
            with self._lock:
                self.error = True
                self.error_reason = e
                self.ended = True
                self.end_date = time.time()
                self._log_terminated()
            for handler in self.lifecycle_handlers:
                try:
                    handler.end(self)
                except Exception, e:
                    logger.error("process lifecycle handler %s end raised exception for process %s:\n%s" % (
                        handler, self, format_exc()))

    def kill(self, sig = signal.SIGTERM, auto_sigterm_timeout = True):
        """Send a signal (default: SIGTERM) to the subprocess.

        :param sig: the signal to send

        :param auto_sigterm_timeout: whether or not execo will check
          that the subprocess has terminated after a preset timeout,
          when it has received a SIGTERM, and automatically send
          SIGKILL if the subprocess is not yet terminated
        """
        logger.debug(style.emph("kill with signal %s:" % sig) + " %s" % (str(self),))
        if self.__start_pending:
            while self.started != True:
                logger.debug("waiting for process to be actually started: " + str(self))
                with the_conductor.lock:
                    intr_cond_wait(the_conductor.condition)
        other_debug_logs=[]
        additionnal_processes_to_kill = []
        with self._lock:
            if self.pid != None and not self.ended:
                if sig == signal.SIGTERM:
                    self._already_got_sigterm = True
                    if auto_sigterm_timeout == True:
                        self.timeout_date = time.time() + configuration.get('kill_timeout')
                        the_conductor.update_process(self)
                if sig == signal.SIGKILL:
                    self.forced_kill = True
                if self._actual_kill_subprocesses():
                    additionnal_processes_to_kill = _get_childs(self.pid)
                try:
                    os.kill(self.pid, sig)
                except OSError, e:
                    if e.errno == errno.EPERM:
                        if (self.pty
                            and (sig == signal.SIGTERM
                                 or sig == signal.SIGHUP
                                 or sig == signal.SIGINT
                                 or sig == signal.SIGKILL
                                 or sig == signal.SIGPIPE
                                 or sig == signal.SIGQUIT)):
                            # unable to send signal to process due to lack
                            # of permissions. If pty == True, then there
                            # is a pty, we can close its master side, it should
                            # trigger a signal (SIGPIPE?) on the other side
                            try:
                                other_debug_logs.append("EPERM for signal %s -> closing pty master side of %s" % (sig, str(self)))
                                os.close(self._ptymaster)
                            except OSError, e:
                                pass
                        else:
                            other_debug_logs.append(style.emph("EPERM: unable to send signal") + " to %s" % (str(self),))
                    elif e.errno == errno.ESRCH:
                        # process terminated so recently that self._ended
                        # has not been updated yet
                        pass
                    else:
                        raise e
        for l in other_debug_logs: logger.debug(l)
        for p in additionnal_processes_to_kill:
            try:
                os.kill(p, sig)
            except OSError, e:
                if e.errno == errno.EPERM or e.errno == errno.ESRCH:
                    pass
                else:
                    raise e
        return self

    def _timeout_kill(self):
        """Send SIGTERM to the subprocess, due to the reaching of its timeout.

        This method is intended to be used by the
        `execo.conductor._Conductor` thread.

        If the subprocess already got a SIGTERM and is still there, it
        is directly killed with SIGKILL.
        """
        with self._lock:
            if self.pid != None:
                self.timeouted = True
                if self._already_got_sigterm and self.timeout_date >= time.time():
                    self.kill(signal.SIGKILL)
                else:
                    self.kill()

    def _set_terminated(self, exit_code):
        """Update process state: set it to terminated.

        This method is intended to be used by the
        `execo.conductor._Conductor` thread.

        Update its exit_code, end_date, ended flag, and log its
        termination (INFO or WARNING depending on how it ended).
        """
        logger.debug("set terminated %s, exit_code=%s" % (str(self), exit_code))
        with self._lock:
            # careful placement of locked sections to avoid deadlock
            # between process lock and logging lock, and to allow
            # calling lifecycle handlers outside the lock
            self.exit_code = exit_code
            self.end_date = time.time()
            self.ended = True
            if self._ptymaster != None:
                try:
                    os.close(self._ptymaster)
                except OSError, e:
                    if e.errno == errno.EBADF: pass
                    else: raise e
                self._ptymaster = None
            if self._ptyslave != None:
                try:
                    os.close(self._ptyslave)
                except OSError, e:
                    if e.errno == errno.EBADF: pass
                    else: raise e
                self._ptyslave = None
            if self.process.stdin:
                self.process.stdin.close()
            if self.process.stdout:
                self.process.stdout.close()
            if self.process.stderr:
                self.process.stderr.close()
            for h in self._stdout_files:
                self._stdout_files[h].close()
                del self._stdout_files[h]
            for h in self._stderr_files:
                self._stderr_files[h].close()
                del self._stderr_files[h]
        self._log_terminated()
        for handler in self.lifecycle_handlers:
            try:
                handler.end(self)
            except Exception, e:
                logger.error("process lifecycle handler %s end raised exception for process %s:\n%s" % (
                        handler, self, format_exc()))

    def wait(self, timeout = None):
        """Wait for the subprocess end."""
        logger.debug(style.emph("wait: ") + " %s" % (str(self),))
        if self.__start_pending:
            while self.started != True:
                logger.debug("waiting for process to be actually started: " + str(self))
                with the_conductor.lock:
                    intr_cond_wait(the_conductor.condition)
        if not self.started:
            raise ValueError, "Trying to wait a process which has not been started"
        timeout = get_seconds(timeout)
        if timeout != None:
            end = time.time() + timeout
        while self.ended != True and (timeout == None or timeout > 0):
            with the_conductor.lock:
                intr_cond_wait(the_conductor.condition, timeout)
            if timeout != None:
                timeout = end - time.time()
        logger.debug(style.emph("wait finished:") + " %s" % (str(self),))
        return self

    def run(self, timeout = None):
        """Start subprocess then wait for its end."""
        return self.start().wait(timeout)

class SshProcess(Process):

    r"""Handle a remote command execution through ssh or similar remote execution tool.

    Note: the closing of the remote process upon killing of the
    SshProcess depends on the ssh (or ssh-like) command behavior. With
    openssh, this can be obtained by passing options -tt (force tty
    creation), thus these are the default options in
    ``execo.config.default_connection_params``.
    """

    def __init__(self, cmd, host, connection_params = None):
        """:param host: `execo.host.Host` to connect to

        :param connection_params: connection parameters
        """
        host = Host(host)
        self.remote_cmd = cmd
        """The command executed remotely"""
        self.connection_params = connection_params
        """Remote connection params"""
        real_cmd = (get_ssh_command(host.user,
                                    host.keyfile,
                                    host.port,
                                    connection_params)
                    + (get_rewritten_host_address(host.address, connection_params),))
        if hasattr(cmd, '__iter__'):
            real_cmd += cmd
        else:
            real_cmd += (cmd,)
        super(SshProcess, self).__init__(real_cmd)
        self.host = host
        self.pty = make_connection_params(connection_params).get('pty')
        """For ssh processes, pty is initialized by the connection params. This
        allows setting default pty behaviors in connection_params shared
        by various remote processes (this was motivated by allowing
        `execo_g5k.config.default_oarsh_oarcp_params` to set pty to
        True, because oarsh/oarcp are run sudo which forbids to send
        signals)."""
        self.name = name_from_cmdline(self.remote_cmd)

    def _args(self):
        return [ style.command(repr(self.remote_cmd)),
                 repr(self.host) ] + Process._kwargs(self) + SshProcess._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self.connection_params: kwargs.append("connection_params=%r" % (self.connection_params,))
        return kwargs

    def _infos(self):
        return [ "real cmd=%r" % (self.cmd,) ] + Process._infos(self)

class TaktukProcess(ProcessBase): #IGNORE:W0223

    r"""Dummy process similar to `execo.process.SshProcess`."""

    def __init__(self, cmd, host):
        super(TaktukProcess, self).__init__(cmd)
        self.host = Host(host)
        self.remote_cmd = cmd
        self.name = name_from_cmdline(self.remote_cmd)

    def _args(self):
        return [ style.command(repr(self.remote_cmd)),
                 repr(self.host) ] + ProcessBase._kwargs(self)

    def start(self):
        """Notify TaktukProcess of actual remote process start.

        This method is intended to be used by
        `execo.action.TaktukRemote`.
        """
        # careful placement of locked sections to allow calling
        # lifecycle handlers outside the lock
        with self._lock:
            if self.started:
                raise ValueError, "unable to start an already started process"
            self.started = True
            self.start_date = time.time()
            if self.timeout != None:
                self.timeout_date = self.start_date + self.timeout
        logger.debug(style.emph("start:") + " %s" % (str(self),))
        for handler in self.lifecycle_handlers:
            try:
                handler.start(self)
            except Exception, e:
                logger.error("process lifecycle handler %s start raised exception for process %s:\n%s" % (
                        handler, self, format_exc()))
        return self

    def _set_terminated(self, exit_code = None, error = False, error_reason = None, timeouted = None, forced_kill = None):
        """Update TaktukProcess state: set it to terminated.

        This method is intended to be used by `execo.action.TaktukRemote`.

        Update its exit_code, end_date, ended flag, and log its
        termination (INFO or WARNING depending on how it ended).
        """
        # careful placement of locked sections to allow calling
        # lifecycle handlers outside the lock
        logger.debug("set terminated %s, exit_code=%s, error=%s" % (str(self), exit_code, error))
        with self._lock:
            if not self.started:
                self.start()
            if error != None:
                self.error = error
            if error_reason != None:
                self.error_reason = error_reason
            if exit_code != None:
                self.exit_code = exit_code
            if timeouted == True:
                self.timeouted = True
            if forced_kill == True:
                self.forced_kill = True
            self.end_date = time.time()
            self.ended = True
            for h in self._stdout_files:
                self._stdout_files[h].close()
            self._stdout_files.clear()
            for h in self._stderr_files:
                self._stderr_files[h].close()
            self._stderr_files.clear()
        self._log_terminated()
        for handler in self.lifecycle_handlers:
            try:
                handler.end(self)
            except Exception, e:
                logger.error("process lifecycle handler %s end raised exception for process %s:\n%s" % (
                        handler, self, format_exc()))

def get_process(*args, **kwargs):
    """Instanciates a `execo.process.Process` or `execo.process.SshProcess`, depending on the existence of host keyword argument"""
    if kwargs.get("host") != None:
        return SshProcess(*args, **kwargs)
    else:
        del kwargs["host"]
        if "connection_params" in kwargs: del kwargs["connection_params"]
        return Process(*args, **kwargs)

class port_forwarder_stderr_handler(ProcessOutputHandler):
    """parses verbose ssh output to notify when forward port is actually open"""

    def __init__(self, local_port, bind_address):
        super(port_forwarder_stderr_handler, self).__init__()
        regex = "^debug1: Local forwarding listening on %s port %i\.\s*$" % (
            bind_address,
            local_port)
        self.pf_open_re = re.compile(regex)

    def read_line(self, process, string, eof, error):
        if self.pf_open_re.match(string):
            process.forwarding.set()

def get_port_forwarder(host,
                       connection_params,
                       remote_host,
                       remote_port,
                       local_port = None,
                       bind_address = None):
    """Create an ssh port forwarder process (ssh -L).

    This port forwarding process opens a listening socket on
    localhost, and forwards it to the given remote host / port on the
    remote side of the ssh connection.

    :param host: remote side of the ssh connection

    :param connection_params: connection params for connecting to host

    :param remote_host: the remote host to connect to on the remote
      side

    :param remote_port: the remote port to connect to on the remote
      side

    :param local_port: the port to use locally for the listening
      socket. If None (the default), a port is automatically selected
      with ``execo.utils.get_port``

    :param bind_address: the bind address to use locally for the
      listening socket. If None (the default), it uses 127.0.0.1, so
      the socket is only available to localhost.

    :returns: a tuple (SshProcess, local_port). The returned
      SshProcess executes a long sleep, and needs to be started /
      killed as usual. It is configured to show no log / error when
      killed. The returned SshProcess has an additionnal instance
      variable `forwarding` which is a `threading.Event` which can be
      waited upon to be notified when the forwarding port is actually
      open (beware: this member is not correctly reset if the
      forwarder SshProcess is reset)
    """
    if not bind_address:
        bind_address = "127.0.0.1"
    if not local_port:
        local_port = get_port()
    pf_conn_parms = make_connection_params(connection_params)
    pf_conn_parms['ssh_options'] = pf_conn_parms['ssh_options'] + (
        '-v',
        '-L',
        '%s:%i:%s:%i' % (
            bind_address,
            local_port,
            Host(remote_host).address,
            remote_port
            ))
    pf = SshProcess("sleep 31536000",
                    host,
                    connection_params = pf_conn_parms)
    pf.ignore_exit_code = pf.nolog_exit_code = True
    pf.forwarding = threading.Event()
    pf.stderr_handlers.append(port_forwarder_stderr_handler(local_port,
                                                            bind_address))
    return pf, local_port

class PortForwarder():
    """Context manager for port forwarders"""

    def __init__(self,
                 host,
                 connection_params,
                 remote_host,
                 remote_port,
                 local_port = None,
                 bind_address = None):
        """
        for params: see documentation of
        ``execo.process.get_port_forwarder``

        When entering the context, it returns a tuple host, port to
        connect to. The forwarded port is guaranteed to be
        operational.

        When leaving the context, it kills the port forwarder
        background process.
        """
        self.__host = host
        self.__connection_params = connection_params
        self.__remote_host = remote_host
        self.__remote_port = remote_port
        self.__local_port = local_port
        self.__bind_address = bind_address
        self.__port_forwarder = None
        self.__local_port = None

    def __enter__(self):
        self.__port_forwarder, self.__local_port = get_port_forwarder(
            self.__host,
            self.__connection_params,
            self.__remote_host,
            self.__remote_port,
            self.__local_port,
            self.__bind_address)
        self.__port_forwarder.start()
        self.__port_forwarder.forwarding.wait()
        return "127.0.0.1", self.__local_port

    def __exit__(self, t, v, traceback):
        self.__port_forwarder.kill()
        return False

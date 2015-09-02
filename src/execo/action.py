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

from execo.config import make_connection_params, configuration, SSH, TAKTUK, SCP, CHAINPUT
from execo.host import Host
from execo.process import get_process, STDOUT, STDERR, ExpectOutputHandler
from host import get_hosts_list, get_unique_hosts_list
from log import style, logger
from process import ProcessLifecycleHandler, SshProcess, ProcessOutputHandler, \
    TaktukProcess, Process, SerialSsh
from report import Report
from ssh_utils import get_rewritten_host_address, get_scp_command, \
    get_taktuk_connector_command, get_ssh_command
from utils import name_from_cmdline, non_retrying_intr_cond_wait, intr_event_wait, get_port, \
    singleton_to_collection
from traceback import format_exc
from substitutions import get_caller_context, remote_substitute
from time_utils import get_seconds, format_date, Timer
import threading, time, pipes, tempfile, os, shutil, stat

class ActionLifecycleHandler(object):

    """Abstract handler for `execo.action.Action` lifecycle."""

    def start(self, action):
        """Handle `execo.action.Action`'s start.

        :param action: The Action which starts.
        """
        pass

    def end(self, action):
        """Handle `execo.action.Action`'s end.

        :param action: The Action which ends.
        """
        pass

    def reset(self, action):
        """Handle `execo.action.Action`'s reset.

        :param action: the Action which is reset.
        """
        pass

class Action(object):

    """Abstract base class. A set of parallel processes.

    An `execo.action.Action` can be started
    (`execo.action.Action.start`), killed
    (`execo.action.Action.kill`). One can wait
    (`execo.action.Action.wait`) for an Action, it means waiting for
    all processes in the Action to finish. An Action can be run
    (`execo.action.Action.run`), it means start it then wait for it to
    complete.

    An Action and its subclasses can act as a context manager object,
    allowing to write code such as::

     with Remote(...).start() as r:
       [...do something...]

    When exiting the contex manager scope, the remote is automatically
    killed.
    """

    _wait_multiple_actions_condition = threading.Condition()

    def __init__(self, lifecycle_handlers = None, name = None, default_expect_timeout = None):
        """:param lifecycle_handlers: List of instances of
          `execo.action.ActionLifecycleHandler` for being notified of
          action lifecycle events.

        :param name: User-friendly name. A default is generated and
          can be changed.

        :param default_expect_timeout: The default timeout for expect
          invocations when no explicit timeout is given. Defaults to
          None, meaning no timeout.
        """
        self.started = False
        """whether this Action was started (boolean)"""
        self.ended = False
        """whether this Action has ended (boolean)"""
        # self.timeout = timeout
        # """Timeout in seconds, or None"""
        # self.factory = factory
        # """Factory which created this action, or None"""
        self.processes = []
        """Iterable of all `execo.process.ProcessBase`"""
        if name != None:
            self.name = name
            """User-friendly name. A default is generated and can be changed."""
        else:
            self.name = "%s 0x%08.8x" % (self.__class__.__name__, id(self))
        self.default_expect_timeout = default_expect_timeout
        """The default timeout for expect invocations when no explicit timeout
        is given. Defaults to None, meaning no timeout."""
        if lifecycle_handlers != None:
            self.lifecycle_handlers = list(lifecycle_handlers)
            """List of instances of `execo.action.ActionLifecycleHandler` for
            being notified of action lifecycle events.
            """
        else:
            self.lifecycle_handlers = list()
        self._end_event = threading.Event()
        self._end_event.clear()
        self._thread_local_storage = threading.local()
        self._thread_local_storage.expect_handler = None

    def _common_reset(self):
        # all methods _common_reset() of this class hierarchy contain
        # the code common to the constructor and reset() to
        # reinitialize an object. _common_reset() of child classes
        # must explicitely call _common_reset() of their parent class.
        self.started = False
        self.ended = False
        self._end_event.clear()
        self._thread_local_storage.expect_handler = None
        self._init_processes()

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
        if self.default_expect_timeout != None: kwargs.append("default_expect_timeout=%r" % (self.default_expect_timeout,))
        return kwargs

    def _infos(self):
        # to be implemented in all subclasses. Must return a list with
        # all relevant infos other than those returned by _args(), for
        # use in __str__ methods.
        stats = self.stats()
        infos = [ "name=%s" % (self.name,),
                  "started=%r" % (self.started,),
                  "start_date=%r" % (format_date(stats['start_date']),),
                  "ended=%r" % (self.ended,),
                  "end_date=%r" % (format_date(stats['end_date']),),
                  "num_processes=%r" % (stats['num_processes'],),
                  "num_started=%r" % (stats['num_started'],),
                  "num_ended=%r" % (stats['num_ended'],),
                  "num_timeouts=%r" % (stats['num_timeouts'],),
                  "num_errors=%r" % (stats['num_errors'],),
                  "num_forced_kills=%r" % (stats['num_forced_kills'],),
                  "num_non_zero_exit_codes=%r" % (stats['num_non_zero_exit_codes'],),
                  "num_ok=%r" % (stats['num_ok'],),
                  "num_finished_ok=%r" % (stats['num_finished_ok'],),
                  "ok=%r" % (self.ok,) ]
        if self.default_expect_timeout != None: infos.append("default_expect_timeout=%r" % (self.default_expect_timeout,))
        return infos

    def __repr__(self):
        # implemented once for all subclasses
        return "%s(%s)" % (self.__class__.__name__, ", ".join(self._args()))

    def __str__(self):
        # implemented once for all subclasses
        return "<" + style.object_repr(self.__class__.__name__) + "(%s)>" % (", ".join(self._args() + self._infos()),)

    def _notify_terminated(self):
        with Action._wait_multiple_actions_condition:
            logger.debug(style.emph("got termination notification for:") + " %s", self)
            for handler in list(self.lifecycle_handlers):
                try:
                    handler.end(self)
                except Exception, e:
                    logger.error("action lifecycle handler %s end raised exception for action %s:\n%s" % (
                            handler, self, format_exc()))
            self.ended = True
            self._end_event.set()
            Action._wait_multiple_actions_condition.notifyAll()

    def _init_processes(self):
        self.processes = []

    def start(self):
        """Start all processes.

        return self"""
        if self.started:
            raise ValueError, "Actions may be started only once"
        self.started = True
        logger.debug(style.emph("start:") + " %s", self)
        for handler in list(self.lifecycle_handlers):
            try:
                handler.start(self)
            except Exception, e:
                logger.error("action lifecycle handler %s start raised exception for action %s:\n%s" % (
                        handler, self, format_exc()))
        return self

    def kill(self):
        """Kill all processes not yet ended.

        Returns immediately, without waiting for processes to be
        actually killed.

        return self"""
        logger.debug(style.emph("kill:") + " %s", self)
        return self

    def wait(self, timeout = None):
        """Wait for all processes to complete.

        return self"""
        logger.debug(style.emph("start waiting:") + " %s", self)
        intr_event_wait(self._end_event, get_seconds(timeout))
        logger.debug(style.emph("end waiting:") + " %s", self)
        return self

    def run(self, timeout = None):
        """Start all processes then wait for them to complete.

        return self"""
        logger.debug(style.emph("run:") + " %s", self)
        self.start()
        self.wait(timeout)
        return self

    def reset(self):
        """Reinitialize an Action so that it can later be restarted.

        If it is running, this method will first kill it then wait for
        its termination before reseting.
        """
        logger.debug(style.emph("reset:") + " %s", self)
        if self.started and not self.ended:
            self.kill()
            self.wait()
        for handler in list(self.lifecycle_handlers):
            try:
                handler.reset(self)
            except Exception, e:
                logger.error("action lifecycle handler %s reset raised exception for action %s:\n%s" % (
                        handler, self, format_exc()))
        self._common_reset()
        return self

    @property
    def ok(self):
        """Returns a boolean indicating if all processes are ok.

        refer to `execo.process.ProcessBase.ok` for detailed semantics
        of being ok for a process.
        """
        return all([process.ok for process in self.processes])

    @property
    def finished_ok(self):
        """Action has started, ended, and is not in error."""
        return self.started and self.ended and self.ok

    def stats(self):
        """Return a dict summarizing the statistics of all processes
        of this Action.

        see `execo.report.Report.stats`.
        """
        stats = Report.empty_stats()
        stats['name'] = self.name
        for process in self.processes:
            pstats = process.stats()
            if (stats['start_date'] == None
                or (pstats['start_date'] != None
                    and pstats['start_date'] < stats['start_date'])):
                stats['start_date'] = pstats['start_date']
            if (stats['end_date'] == None
                or (pstats['end_date'] != None
                    and pstats['end_date'] > stats['end_date'])):
                stats['end_date'] = pstats['end_date']
            stats['num_processes'] += pstats['num_processes']
            stats['num_started'] += pstats['num_started']
            stats['num_ended'] += pstats['num_ended']
            stats['num_errors'] += pstats['num_errors']
            stats['num_timeouts'] += pstats['num_timeouts']
            stats['num_forced_kills'] += pstats['num_forced_kills']
            stats['num_non_zero_exit_codes'] += pstats['num_non_zero_exit_codes']
            stats['num_ok'] += pstats['num_ok']
            stats['num_finished_ok'] += pstats['num_finished_ok']
        if stats['num_processes'] > stats['num_ended']:
            stats['end_date'] = None
        return stats

    def __enter__(self):
        """Context manager enter function: returns self"""
        return self

    def __exit__(self, t, v, traceback):
        """Context manager leave function: kills the action"""
        self.kill()
        return False

    def expect(self, regexes, timeout = False, stream = STDOUT, backtrack_size = 2000, start_from_current = False):
        """searches the process output stream(s) for some regex. It mimics/takes ideas from Don Libes expect, or python-pexpect, but in parallel on several processes.

        It is an easier-to-use frontend for
        `execo.process.ExpectOutputHandler`.

        It waits for a regex to match on all processes. Then it
        returns a list of tuples (process, regex index, match
        object). For processes / streams for which there was no match
        before reaching the timeout or the stream is eof or error, the
        tuple is (process, None, None). The returned list has the same
        process sort order than self.processes.

        It uses thread local storage such that concurrent expects in
        parallel threads do not interfere which each other.

        :param regexes: a regex or list of regexes. May be given as string
          or as compiled regexes.

        :param timeout: wait timeout after which it returns (None,
          None) if no match was found. If False (the default): use the
          default expect timeout. If None: no timeout.

        :param stream: stream to monitor for this process, STDOUT or
          STDERR.

        :param backtrack_size: Each time some data is received, this
          ouput handler needs to perform the regex search not only on
          the incoming data, but also on the previously received data,
          or at least on the last n bytes of the previously received
          data, because the regex may match on a boundary between what
          was received in a previous read and what is received in the
          incoming read. These n bytes are the backtrack_size. (for
          special cases: if backtrack_size == None, the regex search
          is always done on the whole received data, but beware, this
          is probably not what you want)

        :param start_from_current: boolean. If True: when a process is
          monitored by this handler for the first time, the regex
          matching is started from the position in the stream at the
          time that this output hander starts receiving data. If
          False: when a process is monitored by this handler for the
          first time, the regex matching is started from the beginning
          of the stream.

        """
        if timeout == False: timeout = self.default_expect_timeout
        countdown = Timer(timeout)
        cond = threading.Condition()
        num_found_and_list = [0, {}]
        for p in self.processes: num_found_and_list[1][p] = (None, None)
        def internal_callback(process, stream, re_index, match_object):
            num_found_and_list[0] +=1
            num_found_and_list[1][process] = (re_index, match_object)
            with cond:
                cond.notify_all()
        if self._thread_local_storage.expect_handler == None:
            self._thread_local_storage.expect_handler = ExpectOutputHandler()
        self._thread_local_storage.expect_handler.expect(regexes,
                                                         callback = internal_callback,
                                                         backtrack_size = backtrack_size,
                                                         start_from_current = start_from_current)
        with cond:
            for p in self.processes:
                if stream == STDOUT:
                    p.stdout_handlers.append(self._thread_local_storage.expect_handler)
                else:
                    p.stderr_handlers.append(self._thread_local_storage.expect_handler)
            while (countdown.remaining() == None or countdown.remaining() > 0) and num_found_and_list[0] < len(self.processes):
                non_retrying_intr_cond_wait(cond, countdown.remaining())
        retval = []
        for p in self.processes:
            if num_found_and_list[1][p][0] == None:
                p._notify_expect_fail(regexes)
            retval.append((p, num_found_and_list[1][p][0], num_found_and_list[1][p][1]))
        return retval

def wait_any_actions(actions, timeout = None):
    """Wait for any of the actions given to terminate.

    :param actions: An iterable of `execo.action.Action`.

    :param timeout: Optional timeout in any type supported by
      `execo.time_utils.get_seconds`.

    returns: iterable of `execo.action.Action` which have terminated.
    """
    timeout = get_seconds(timeout)
    if timeout != None:
        end = time.time() + timeout
    with Action._wait_multiple_actions_condition:
        finished = [action for action in actions if action.ended]
        while len(finished) == 0 and (timeout == None or timeout > 0):
            non_retrying_intr_cond_wait(Action._wait_multiple_actions_condition, get_seconds(timeout))
            finished = [action for action in actions if action.ended]
            if timeout != None:
                timeout = end - time.time()
        return finished

def wait_all_actions(actions, timeout = None):
    """Wait for all of the actions given to terminate.

    :param actions: An iterable of `execo.action.Action`.

    :param timeout: Optional timeout in any type supported by
      `execo.time_utils.get_seconds`.

    returns: iterable of `execo.action.Action` which have terminated.
    """
    timeout = get_seconds(timeout)
    if timeout != None:
        end = time.time() + timeout
    with Action._wait_multiple_actions_condition:
        finished = [action for action in actions if action.ended]
        while len(finished) != len(actions) and (timeout == None or timeout > 0):
            non_retrying_intr_cond_wait(Action._wait_multiple_actions_condition, get_seconds(timeout))
            finished = [action for action in actions if action.ended]
            if timeout != None:
                timeout = end - time.time()
        return finished

def filter_bad_hosts(action, hosts):
    """Returns the list of host filtered from any host where any process of the action has failed.

    :param action: The action from which the processes are checked

    :param hosts: The iterable of hosts or host addesses to be
      filtered and returned as a list
    """
    action_hosts_ko = set()
    for p in action.processes:
        if hasattr(p, "host") and not p.ok:
            action_hosts_ko.add(p.host)
    filtered_hosts = []
    for h in hosts:
        if isinstance(h, Host):
            if h not in action_hosts_ko:
                filtered_hosts.append(h)
        else:
            if Host(h) not in action_hosts_ko:
                filtered_hosts.append(h)
    return filtered_hosts

class ActionNotificationProcessLH(ProcessLifecycleHandler):

    def __init__(self, action, total_processes):
        super(ActionNotificationProcessLH, self).__init__()
        self.action = action
        self.total_processes = total_processes
        self.terminated_processes = 0

    def end(self, process):
        self.terminated_processes += 1
        logger.debug("%i/%i processes terminated in %s",
            self.terminated_processes,
            self.total_processes,
            self.action)
        if self.terminated_processes == self.total_processes:
            self.action._notify_terminated()

    def action_reset(self):
        self.terminated_processes = 0

class Remote(Action):

    """Launch a command remotely on several host, with ``ssh`` or a similar remote connection tool.

    One ssh process is launched for each connection.
    """

    def __init__(self, cmd, hosts, connection_params = None, process_args = None, **kwargs):
        """:param cmd: the command to run remotely. Substitions
          described in `execo.substitutions.remote_substitute` will be
          performed.

        :param hosts: iterable of `execo.host.Host` to which to
          connect and run the command.

        :param connection_params: a dict similar to
          `execo.config.default_connection_params` whose values will
          override those in default_connection_params for connection.

        :param process_args: Dict of keyword arguments passed to
          instanciated processes.
        """
        self.cmd = cmd
        """The command to run remotely. substitions described in
        `execo.substitutions.remote_substitute` will be performed."""
        if not kwargs.has_key("name"):
            kwargs.update({"name": name_from_cmdline(self.cmd)})
        super(Remote, self).__init__(**kwargs)
        self.connection_params = connection_params
        """A dict similar to `execo.config.default_connection_params` whose values
        will override those in default_connection_params for connection."""
        if process_args != None:
            self.process_args = process_args
            """Dict of keyword arguments passed to instanciated processes."""
        else:
            self.process_args = {}
        self.hosts = hosts
        """Iterable of `execo.host.Host` to which to connect and run the command."""
        self._caller_context = get_caller_context(['get_remote'])
        self._init_processes()

    @property
    def hosts(self):
        return self._hosts

    @hosts.setter
    def hosts(self, v):
        self._hosts = get_hosts_list(singleton_to_collection(v))

    def _args(self):
        return [ repr(self.cmd),
                 repr(self.hosts) ] + Action._args(self) + Remote._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self.connection_params: kwargs.append("connection_params=%r" % (self.connection_params,))
        if len(self.process_args) > 0: kwargs.append("process_args=%r" % (self.process_args,))
        return kwargs

    def _infos(self):
        infos = []
        if self.connection_params: infos.append("connection_params=%r" % (self.connection_params,))
        if len(self.process_args) > 0: infos.append("process_args=%r" % (self.process_args,))
        return infos

    def _init_processes(self):
        self.processes = []
        processlh = ActionNotificationProcessLH(self, len(self.hosts))
        for (index, host) in enumerate(self.hosts):
            p = SshProcess(remote_substitute(self.cmd, self.hosts, index, self._caller_context),
                           host = host,
                           connection_params = self.connection_params,
                           **self.process_args)
            p.lifecycle_handlers.append(processlh)
            self.processes.append(p)

    def start(self):
        retval = super(Remote, self).start()
        if len(self.processes) == 0:
            logger.debug("%s contains 0 processes -> immediately terminated", self)
            self._notify_terminated()
        else:
            for process in self.processes:
                process.start()
        return retval

    def kill(self):
        retval = super(Remote, self).kill()
        for process in self.processes:
            if process.running:
                process.kill()
        return retval

    def write(self, s):
        """Write on the Remote processes standard inputs

        Allows Remote instances to behave as file-like objects. You
        can for example print to a Remote.
        """
        for process in self.processes:
            process.write(s)

class _TaktukRemoteOutputHandler(ProcessOutputHandler):

    """Parse taktuk output."""

    def __init__(self, taktukaction):
        super(_TaktukRemoteOutputHandler, self).__init__()
        self.taktukaction = taktukaction

    def _log_unexpected_output(self, string):
        logger.critical("%s: Taktuk unexpected output parsing. Please report this message. Line received:", self.__class__.__name__)
        logger.critical(self._describe_taktuk_output(string))

    def _describe_taktuk_output(self, string):
        s = "%s: " % (self.__class__.__name__,)
        try:
            if len(string) > 0:
                header = ord(string[0])
                (position, _, line) = string[2:].partition(" # ")
                position = int(position)
                if position == 0:
                    host_address = "localhost"
                else:
                    host_address = self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[position-1]].host.address
                if header in (65, 66, 67, 70, 71, 72):
                    if header == 65: t = "stdout"
                    elif header == 66: t = "stderr"
                    elif header == 67: t = "status"
                    elif header == 70: t = "info"
                    elif header == 71: t = "taktuk"
                    elif header == 72: t = "message"
                    s += "%s - host = %s - line = %s" % (t, host_address, line[:-1])
                elif header in (68, 69):
                    (peer_position, _, line) = line.partition(" # ")
                    peer_host_address = None
                    try:
                        peer_position = int(peer_position)
                        if peer_position == 0:
                            peer_host_address = "localhost"
                        else:
                            peer_host_address = self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[peer_position-1]].host.address
                    except ValueError:
                        pass
                    if header == 68:
                        s += "connector - host = %s - peer = %s - line = %s" % (host_address, peer_host_address, line[:-1])
                    elif header == 69:
                        (_, _, state_msg) = line.partition(" # ")
                        s += "state - host = %s - peer = %s - state = %s" % (host_address, peer_host_address, state_msg[:-1])
                elif header == 73:
                    (taktuktype, _, line) = line.partition(" # ")
                    s += "message type = %s - host = %s - line = %s" % (taktuktype, host_address, line[:-1])
                else:
                    s += "unexpected string = %s" % (string[:-1])
            else:
                s += "empty string"
            return s
        except Exception, e: #IGNORE:W0703
            logger.critical("%s: Unexpected exception %s while parsing taktuk output. Please report this message.", self.__class__.__name__, e)
            logger.critical("line received = %s", string.rstrip('\n'))
            return s

    def read_line(self, process, stream, string, eof, error):
        # my taktuk output protocol:
        #  stream    format                                                    header normal?
        #  output    "A $position # $line"                                     65     YES
        #  error     "B $position # $line"                                     66     YES
        #  status    "C $position # $line"                                     67     YES
        #  connector "D $position # $peer_position # $line"                    68     YES
        #  state     "E $position # $peer_position # $line # event_msg($line)" 69     YES
        #  info      "F $position # $line"                                     70     NO
        #  taktuk    "G $position # $line"                                     71     SOMETIMES
        #  message   "H $position # $line"                                     72     NO
        #  default   "I $position # $type # $line"                             73     NO
        try:
            if len(string) > 0:
                #logger.debug("taktuk: %s", self._describe_taktuk_output(string))
                header = ord(string[0])
                (position, _, line) = string[2:].partition(" # ")
                position = int(position)
                if header >= 65 and header <= 67: # stdout, stderr, status
                    if position == 0:
                        self._log_unexpected_output(string)
                        return
                    else:
                        process = self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[position-1]]
                        if header == 65: # stdout
                            process._handle_stdout(line, eof, error)
                        elif header == 66: # stderr
                            process._handle_stderr(line, eof, error)
                        else: # 67: status
                            process._set_terminated(exit_code = int(line))
                elif header in (68, 69): # connector, state
                    (peer_position, _, line) = line.partition(" # ")
                    if header == 68: # connector
                        peer_position = int(peer_position)
                        process = self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[peer_position-1]]
                        process._handle_stderr(line, False, False)
                    else: # state
                        (state_code, _, _) = line.partition(" # ")
                        state_code = int(state_code)
                        if state_code == 6 or state_code == 7: # command started or remote command exec failed
                            process = self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[position-1]]
                            if state_code == 6: # command started
                                process.start()
                            else: # 7: remote command exec failed
                                process._set_terminated(error = True, error_reason = "taktuk remote command execution failed")
                        elif state_code == 3 or state_code == 5: # connection failed or lost
                            peer_position = int(peer_position)
                            process = self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[peer_position-1]]
                            if state_code == 3: # connection failed
                                process._set_terminated(error = True, error_reason = "taktuk connection failed")
                            else: # 5: connection lost
                                process._set_terminated(error = True, error_reason = "taktuk connection lost")
                        elif state_code in (0, 1, 2, 4, 8, 19):
                            pass
                        else:
                            self._log_unexpected_output(string)
                else:
                    self._log_unexpected_output(string)
        except Exception, e: #IGNORE:W0703
            logger.critical("%s: Unexpected exception %s while parsing taktuk output. Please report this message.", self.__class__.__name__, e)
            logger.critical("line received = %s", string.rstrip('\n'))
            raise

class _TaktukLH(ProcessLifecycleHandler):

    """Notify `execo.process.TaktukProcess` of their real taktuk process lifecyle."""

    def __init__(self, taktukremote):
        super(_TaktukLH, self).__init__()
        self.taktukremote = taktukremote

    def end(self, process):
        error = None
        error_reason = None
        timeouted = None
        forced_kill = None
        if process.error == True:
            error = True
        if process.error_reason != None:
            error_reason = "error of taktuk process: %s" % process.error_reason
        if process.timeouted == True:
            timeouted = True
        if process.forced_kill == True:
            forced_kill = True
        for taktukprocess in self.taktukremote.processes:
            if not taktukprocess.ended:
                taktukprocess._set_terminated(error = error,
                                              error_reason = error_reason,
                                              timeouted = timeouted,
                                              forced_kill = forced_kill)

def _escape_brackets_in_taktuk_options(s):
    for c in [ '[', ']' ]:
        s = s.replace( c, '\\' + c )
    return s

class TaktukRemote(Action):

    """Launch a command remotely on several host, with ``taktuk``.

    One taktuk instance is ran, which itself connects to hosts through
    an ``ssh`` tree.

    Behavior should be identical to `execo.action.Remote`. Current
    limitation are:

    - we can provide per-host user with taktuk, but we cannot provide
      per-host port or keyfile, so a check is made that all hosts and
      connection_params have the same port / keyfile (or None). If not,
      an exception is raised during initialization.

    - remote processes are not killed when killing the
      TaktukRemote. See 'hanged commands' in
      http://taktuk.gforge.inria.fr/taktuk.html#bugs. With ssh the
      workaround is to pass options -tt but passing these options to
      taktuk connector causes immediate closing of the connector upon
      connection.
    """

    def __init__(self, cmd, hosts, connection_params = None, process_args = None, **kwargs):
        """
        :param cmd: the command to run remotely. substitions
          described in `execo.substitutions.remote_substitute` will be
          performed.

        :param hosts: iterable of `execo.host.Host` to which to
          connect and run the command.

        :param connection_params: a dict similar to
          `execo.config.default_connection_params` whose values will
          override those in default_connection_params for connection.
        """
        self.cmd = cmd
        """The command to run remotely. substitions described in
        `execo.substitutions.remote_substitute` will be performed."""
        if not kwargs.has_key("name"):
            kwargs.update({"name": name_from_cmdline(self.cmd)})
        super(TaktukRemote, self).__init__(**kwargs)
        self.connection_params = connection_params
        """A dict similar to `execo.config.default_connection_params` whose values
        will override those in default_connection_params for connection."""
        if process_args != None:
            self.process_args = process_args
            """Dict of keyword arguments passed to instanciated processes."""
        else:
            self.process_args = {}
        self.hosts = hosts
        """Iterable of `execo.host.Host` to which to connect and run the command."""
        self._caller_context = get_caller_context(['get_remote'])
        self._taktuk_stdout_output_handler = _TaktukRemoteOutputHandler(self)
        self._taktuk_stderr_output_handler = self._taktuk_stdout_output_handler
        self._init_processes()

    @property
    def hosts(self):
        return self._hosts

    @hosts.setter
    def hosts(self, v):
        self._hosts = get_hosts_list(singleton_to_collection(v))

    def _args(self):
        return [ repr(self.cmd),
                 repr(self.hosts) ] + Action._args(self) + TaktukRemote._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self.connection_params: kwargs.append("connection_params=%r" % (self.connection_params,))
        if len(self.process_args) > 0: kwargs.append("process_args=%r" % (self.process_args,))
        return kwargs

    def _gen_taktukprocesses(self):
        processlh = ActionNotificationProcessLH(self, len(self.hosts))
        for (index, host) in enumerate(self.hosts):
            p = TaktukProcess(remote_substitute(self.cmd, self.hosts, index, self._caller_context),
                              host = host,
                              **self.process_args)
            p.lifecycle_handlers.append(processlh)
            p._taktuk_remote = self
            self.processes.append(p)

    def _gen_taktuk_commands(self, hosts_with_explicit_user):
        for (index, host) in [ (idx, h) for (idx, h) in enumerate(self.hosts) if h not in hosts_with_explicit_user ]:
            self._taktuk_commands += ("-m", get_rewritten_host_address(host.address, self.connection_params), "-[", "exec", "[", _escape_brackets_in_taktuk_options(self.processes[index].cmd), "]", "-]",)
            self._taktuk_hosts_order.append(index)
            self.processes[index]._taktuk_index = index
        for (index, host) in [ (idx, h) for (idx, h) in enumerate(self.hosts) if h in hosts_with_explicit_user ]:
            self._taktuk_commands += ("-l", host.user, "-m", get_rewritten_host_address(host.address, self.connection_params), "-[", "exec", "[", _escape_brackets_in_taktuk_options(self.processes[index].cmd), "]", "-]",)
            self._taktuk_hosts_order.append(index)
            self.processes[index]._taktuk_index = index

    def _init_processes(self):
        # taktuk code common to TaktukRemote and subclasses TaktukGet
        # TaktukPut
        self.processes = []
        self._taktuk_hosts_order = []
        self._taktuk_commands = ()
        self._taktuk = None
        if len(self.hosts) > 0:
            # we can provide per-host user with taktuk, but we cannot
            # provide per-host port or keyfile, so check that all hosts
            # and connection_params have the same port / keyfile (or None)
            actual_connection_params = make_connection_params(self.connection_params)
            check_default_port = actual_connection_params['port']
            check_default_keyfile = actual_connection_params['keyfile']
            check_keyfiles = set()
            check_ports = set()
            hosts_with_explicit_user = set()
            for host in self.hosts:
                if host.user != None:
                    hosts_with_explicit_user.add(host)
                if host.keyfile != None:
                    check_keyfiles.add(host.keyfile)
                else:
                    check_keyfiles.add(check_default_keyfile)
                if host.port != None:
                    check_ports.add(host.port)
                else:
                    check_ports.add(check_default_port)
            if len(check_keyfiles) > 1 or len(check_ports) > 1:
                raise ValueError, "unable to provide more than one keyfile / port for taktuk remote connection"
            global_keyfile = None
            global_port = None
            if len(check_keyfiles) == 1:
                global_keyfile = list(check_keyfiles)[0]
            if len(check_ports) == 1:
                global_port = list(check_ports)[0]
            self._gen_taktukprocesses()
            self._gen_taktuk_commands(hosts_with_explicit_user)
            #self._taktuk_commands += ("quit",)
            #handler = _TaktukRemoteOutputHandler(self)
            taktuk_options_filehandle, taktuk_options_filename = tempfile.mkstemp(prefix = 'tmp_execo_taktuk_')
            self._taktuk_commands = " ".join(self._taktuk_commands)
            os.write(taktuk_options_filehandle, self._taktuk_commands + "\n")
            os.close(taktuk_options_filehandle)
            logger.debug("generated taktuk tmp cmd file %s with content:\n%s", taktuk_options_filename, self._taktuk_commands)
            real_taktuk_cmdline = (actual_connection_params['taktuk'],)
            real_taktuk_cmdline += ("-E", "!")
            real_taktuk_cmdline += ("-o", 'output="A $position # $line\\n"',
                                    "-o", 'error="B $position # $line\\n"',
                                    "-o", 'status="C $position # $line\\n"',
                                    "-o", 'connector="D $position # $peer_position # $line\\n"',
                                    "-o", 'state="E $position # $peer_position # $line # ".event_msg($line)."\\n"',
                                    "-o", 'info="F $position # $line\\n"',
                                    "-o", 'taktuk="G $position # $line\\n"',
                                    "-o", 'message="H $position # $line\\n"',
                                    "-o", 'default="I $position # $type > $line\\n"')
            real_taktuk_cmdline += actual_connection_params['taktuk_options']
            real_taktuk_cmdline += ("-c", " ".join(
                get_taktuk_connector_command(keyfile = global_keyfile,
                                             port = global_port,
                                             connection_params = self.connection_params)))
            real_taktuk_cmdline += ("-F", taktuk_options_filename)
            real_taktuk_cmdline = " ".join([pipes.quote(arg) for arg in real_taktuk_cmdline])
            real_taktuk_cmdline += " && rm -f " + taktuk_options_filename
            self._taktuk = Process(real_taktuk_cmdline)
            #self._taktuk.close_stdin = False
            self._taktuk.shell = True
            #self._taktuk.default_stdout_handler = False
            #self._taktuk.default_stderr_handler = False
            self._taktuk.stdout_handlers.append(self._taktuk_stdout_output_handler)
            #self._taktuk.stderr_handlers.append(self._taktuk_stderr_output_handler)
            self._taktuk.lifecycle_handlers.append(_TaktukLH(self))

    def start(self):
        retval = super(TaktukRemote, self).start()
        if len(self.processes) == 0:
            logger.debug("%s contains 0 processes -> immediately terminated", self)
            self._notify_terminated()
        else:
            self._taktuk.start()
        return retval

    def kill(self):
        retval = super(TaktukRemote, self).kill()
        if self._taktuk:
            for process in self.processes:
                if process.running:
                    process.killed = True
            self._taktuk.write("broadcast kill\n\n")
            self._taktuk.write("quit\n\n")
        return retval

    def wait(self, timeout = None):
        retval = super(TaktukRemote, self).wait(timeout)
        if self._taktuk:
            self._taktuk.wait()
        return retval

    def _notify_terminated(self):
        try:
            self._taktuk.write("quit\n\n")
        except:
            pass
        super(TaktukRemote, self)._notify_terminated()

class Put(Remote):

    """Copy local files to several remote host, with ``scp`` or a similar connection tool."""

    def __init__(self, hosts, local_files, remote_location = ".", connection_params = None, **kwargs):
        """
        :param hosts: iterable of `execo.host.Host` onto which to copy
          the files.

        :param local_files: an iterable of string of file
          paths. substitions described in
          `execo.substitutions.remote_substitute` will be performed.

        :param remote_location: the directory on the remote hosts were
          the files will be copied. substitions described in
          `execo.substitutions.remote_substitute` will be performed.

        :param connection_params: a dict similar to
          `execo.config.default_connection_params` whose values will
          override those in default_connection_params for connection.
        """
        self.hosts = hosts
        """Iterable of `execo.host.Host` onto which to copy the files."""
        if not kwargs.has_key("name"):
            kwargs.update({"name": "%s to %i hosts" % (self.__class__.__name__, len(self.hosts))})
        super(Remote, self).__init__(**kwargs)
        self.local_files = local_files
        """An iterable of string of file paths. substitions described in
        `execo.substitutions.remote_substitute` will be performed."""
        self.remote_location = remote_location
        """The directory on the remote hosts were the files will be
        copied. substitions described in `execo.substitutions.remote_substitute`
        will be performed."""
        self.connection_params = connection_params
        """A dict similar to `execo.config.default_connection_params` whose values
        will override those in default_connection_params for connection."""
        self._caller_context = get_caller_context(['get_fileput'])
        self._init_processes()

    def _args(self):
        return [ repr(self.hosts),
                 repr(self.local_files) ] + Action._args(self) + Put._kwargs(self)

    def _kwargs(self):
        kwargs = []
        kwargs.append("remote_location=%r" % (self.remote_location,))
        if self.connection_params: kwargs.append("connection_params=%r" % (self.connection_params,))
        return kwargs

    def _infos(self):
        infos = []
        if self.connection_params: infos.append("connection_params=%r" % (self.connection_params,))
        return infos

    def _init_processes(self):
        self.processes = []
        if len(self.local_files) > 0:
            processlh = ActionNotificationProcessLH(self, len(self.hosts))
            for (index, host) in enumerate(self.hosts):
                real_command = list(get_scp_command(host.user, host.keyfile, host.port, self.connection_params)) + [ remote_substitute(local_file, self.hosts, index, self._caller_context) for local_file in self.local_files ] + ["%s:%s" % (get_rewritten_host_address(host.address, self.connection_params), remote_substitute(self.remote_location, self.hosts, index, self._caller_context)),]
                real_command = ' '.join(real_command)
                p = Process(real_command)
                p.shell = True
                p.lifecycle_handlers.append(processlh)
                p.host = host
                self.processes.append(p)

class Get(Remote):

    """Copy remote files from several remote host to a local directory, with ``scp`` or a similar connection tool."""

    def __init__(self, hosts, remote_files, local_location = ".", connection_params = None, **kwargs):
        """
        :param hosts: iterable of `execo.host.Host` from which to get
          the files.

        :param remote_files: an iterable of string of file
          paths. substitions described in
          `execo.substitutions.remote_substitute` will be performed.

        :param local_location: the local directory were the files will
          be copied. substitions described in
          `execo.substitutions.remote_substitute` will be performed.

        :param connection_params: a dict similar to
          `execo.config.default_connection_params` whose values will
          override those in default_connection_params for connection.
        """
        self.hosts = hosts
        """Iterable of `execo.host.Host` from which to get the files."""
        if not kwargs.has_key("name"):
            kwargs.update({"name": "%s from %i hosts" % (self.__class__.__name__, len(self.hosts))})
        super(Remote, self).__init__(**kwargs)
        self.remote_files = remote_files
        """Iterable of string of file paths. substitions described in
        `execo.substitutions.remote_substitute` will be performed."""
        self.local_location = local_location
        """The local directory were the files will be copied. substitions described
        in `execo.substitutions.remote_substitute` will be performed."""
        self.connection_params = connection_params
        """Dict similar to `execo.config.default_connection_params` whose values
        will override those in default_connection_params for connection."""
        self._caller_context = get_caller_context(['get_fileget'])
        self._init_processes()

    def _args(self):
        return [ repr(self.hosts),
                 repr(self.remote_files) ] + Action._args(self) + Get._kwargs(self)

    def _kwargs(self):
        kwargs = []
        kwargs.append("local_location=%r" % (self.local_location,))
        if self.connection_params: kwargs.append("connection_params=%r" % (self.connection_params,))
        return kwargs

    def _infos(self):
        infos = []
        if self.connection_params: infos.append("connection_params=%r" % (self.connection_params,))
        return infos

    def _init_processes(self):
        self.processes = []
        if len(self.remote_files) > 0:
            processlh = ActionNotificationProcessLH(self, len(self.hosts))
            for (index, host) in enumerate(self.hosts):
                remote_specs = ()
                for path in self.remote_files:
                    remote_specs += ("%s:%s" % (get_rewritten_host_address(host.address, self.connection_params), remote_substitute(path, self.hosts, index, self._caller_context)),)
                real_command = get_scp_command(host.user, host.keyfile, host.port, self.connection_params) + remote_specs + (remote_substitute(self.local_location, self.hosts, index, self._caller_context),)
                real_command = ' '.join(real_command)
                p = Process(real_command)
                p.shell = True
                p.lifecycle_handlers.append(processlh)
                p.host = host
                self.processes.append(p)

class _TaktukPutOutputHandler(_TaktukRemoteOutputHandler):

    """Parse taktuk output."""

    def _update_taktukprocess_end_state(self, process):
        if process._num_transfers_started > 0 and not process.started:
            process.start()
        if process._num_transfers_failed + process._num_transfers_terminated >= len(self.taktukaction.local_files):
            if process._num_transfers_failed > 0:
                process._set_terminated(error = True, error_reason = "taktuk file reception failed")
            else:
                process._set_terminated(exit_code = 0)

    def read_line(self, process, stream, string, eof, error):
        try:
            if len(string) > 0:
                header = ord(string[0])
                (position, _, line) = string[2:].partition(" # ")
                position = int(position)
                if header in (68, 69): # connector, state
                    (peer_position, _, line) = line.partition(" # ")
                    if header == 68: # connector
                        peer_position = int(peer_position)
                        process = self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[peer_position-1]]
                        process._handle_stderr(line, False, False)
                    else: # state
                        (state_code, _, _) = line.partition(" # ")
                        state_code = int(state_code)
                        if state_code in (13, 14, 15): # file reception started, failed, terminated
                            process = self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[position-1]]
                            if state_code == 13: # file reception started
                                process._num_transfers_started += 1
                            elif state_code == 14: # file reception failed
                                process._num_transfers_failed += 1
                            else: # 15: file reception terminated
                                process._num_transfers_terminated += 1
                            self._update_taktukprocess_end_state(process)
                        elif state_code == 16: # file send failed
                            if position > 0:
                                processes = [ self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[position-1]] ]
                            else:
                                processes = self.taktukaction.processes
                            for process in processes:
                                process._num_transfers_started += 1
                                process._num_transfers_failed += 1
                                self._update_taktukprocess_end_state(process)
                        elif state_code == 3 or state_code == 5: # connection failed or lost
                            peer_position = int(peer_position)
                            process = self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[peer_position-1]]
                            if state_code == 3: # connection failed
                                process._set_terminated(error = True, error_reason = "taktuk connection failed")
                            else: # 5: connection lost
                                process._set_terminated(error = True, error_reason = "taktuk connection lost")
                        elif state_code in (0, 1, 2, 4):
                            pass
                        else:
                            self._log_unexpected_output(string)
                elif header == 71: # to ignore "Error No such file or directory". Don't know if it can occur in other situations
                    if position > 0:
                        processes = [ self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[position-1]] ]
                    else:
                        processes = self.taktukaction.processes
                    for process in processes:
                        process._handle_stderr(line, False, False)
                else:
                    self._log_unexpected_output(string)
        except Exception, e: #IGNORE:W0703
            logger.critical("%s: Unexpected exception %s while parsing taktuk output. Please report this message.", self.__class__.__name__, e)
            logger.critical("line received = %s", string.rstrip('\n'))

class TaktukPut(TaktukRemote):

    """Copy local files to several remote host, with ``taktuk``."""

    def __init__(self, hosts, local_files, remote_location = ".", connection_params = None, **kwargs):
        """
        :param hosts: iterable of `execo.host.Host` onto which to copy
          the files.

        :param local_files: an iterable of string of file
          paths. substitions described in
          `execo.substitutions.remote_substitute` will not be
          performed, but taktuk substitutions can be used (see
          http://taktuk.gforge.inria.fr/taktuk.html#item_put__2a_src__2a__2a_dest__2a)

        :param remote_location: the directory on the remote hosts were
          the files will be copied. substitions described in
          `execo.substitutions.remote_substitute` will not be
          performed, but taktuk substitutions can be used (see
          http://taktuk.gforge.inria.fr/taktuk.html#item_put__2a_src__2a__2a_dest__2a)

        :param connection_params: a dict similar to
          `execo.config.default_connection_params` whose values will
          override those in default_connection_params for connection.
        """
        self.hosts = hosts
        """Iterable of `execo.host.Host` onto which to copy the files."""
        if not kwargs.has_key("name"):
            kwargs.update({"name": "%s to %i hosts" % (self.__class__.__name__, len(self.hosts))})
        super(TaktukRemote, self).__init__(**kwargs)
        self.local_files = local_files
        """Iterable of string of file paths. substitions described in
        `execo.substitutions.remote_substitute` will not be performed,
        but taktuk substitutions can be used (see
        http://taktuk.gforge.inria.fr/taktuk.html#item_put__2a_src__2a__2a_dest__2a)"""
        self.remote_location = remote_location
        """The directory on the remote hosts were the files will be
        copied. substitions described in `execo.substitutions.remote_substitute`
        will not be performed, but taktuk substitutions can be used (see
        http://taktuk.gforge.inria.fr/taktuk.html#item_put__2a_src__2a__2a_dest__2a)"""
        self.connection_params = connection_params
        """Dict similar to `execo.config.default_connection_params` whose values
        will override those in default_connection_params for connection."""
        self.process_args = {}
        self._caller_context = get_caller_context(['get_fileput'])
        self._taktuk_stdout_output_handler = _TaktukPutOutputHandler(self)
        self._taktuk_stderr_output_handler = self._taktuk_stdout_output_handler
        self._init_processes()

    def _args(self):
        return [ repr(self.hosts),
                 repr(self.local_files) ] + Action._args(self) + TaktukPut._kwargs(self)

    def _kwargs(self):
        kwargs = []
        kwargs.append("remote_location=%r" % (self.remote_location,))
        if self.connection_params: kwargs.append("connection_params=%r" % (self.connection_params,))
        return kwargs

    def _gen_taktukprocesses(self):
        processlh = ActionNotificationProcessLH(self, len(self.hosts))
        for (index, host) in enumerate(self.hosts):
            process = TaktukProcess("",
                                    host = host,
                                    **self.process_args)
            process.lifecycle_handlers.append(processlh)
            process._num_transfers_started = 0
            process._num_transfers_terminated = 0
            process._num_transfers_failed = 0
            self.processes.append(process)

    def _gen_taktuk_commands(self, hosts_with_explicit_user):
        self._taktuk_hosts_order = []
        for (index, host) in [ (idx, h) for (idx, h) in enumerate(self.hosts) if h not in hosts_with_explicit_user ]:
            self._taktuk_commands += ("-m", get_rewritten_host_address(host.address, self.connection_params))
            self._taktuk_hosts_order.append(index)
        for (index, host) in [ (idx, h) for (idx, h) in enumerate(self.hosts) if h in hosts_with_explicit_user ]:
            self._taktuk_commands += ("-l", host.user, "-m", get_rewritten_host_address(host.address, self.connection_params))
            self._taktuk_hosts_order.append(index)
        for src in self.local_files:
            self._taktuk_commands += ("broadcast", "put", "[", src, "]", "[", self.remote_location, "]", ";")

class _TaktukGetOutputHandler(_TaktukRemoteOutputHandler):

    """Parse taktuk output."""

    def _update_taktukprocess_end_state(self, process):
        if process._num_transfers_started > 0 and not process.started:
            process.start()
        if process._num_transfers_failed + process._num_transfers_terminated >= len(self.taktukaction.remote_files):
            if process._num_transfers_failed > 0:
                process._set_terminated(error = True, error_reason = "taktuk file reception failed")
            else:
                process._set_terminated(exit_code = 0)

    def read_line(self, process, stream, string, eof, error):
        try:
            if len(string) > 0:
                header = ord(string[0])
                (position, _, line) = string[2:].partition(" # ")
                position = int(position)
                if header in (68, 69): # connector, state
                    (peer_position, _, line) = line.partition(" # ")
                    if header == 68: # connector
                        peer_position = int(peer_position)
                        process = self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[peer_position-1]]
                        process._handle_stderr(line, False, False)
                    else: # state
                        (state_code, _, _) = line.partition(" # ")
                        state_code = int(state_code)
                        if state_code in (13, 14, 15): # file reception started, failed, terminated
                            peer_position = int(peer_position)
                            process = self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[peer_position-1]]
                            if state_code == 13: # file reception started
                                process._num_transfers_started += 1
                            elif state_code == 14: # file reception failed
                                process._num_transfers_failed += 1
                            else: # 15: file reception terminated
                                process._num_transfers_terminated += 1
                            self._update_taktukprocess_end_state(process)
                        elif state_code == 16: # file send failed
                            if position > 0:
                                processes = [ self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[position-1]] ]
                            else:
                                processes = self.taktukaction.processes
                            for process in processes:
                                process._num_transfers_started += 1
                                process._num_transfers_failed += 1
                                self._update_taktukprocess_end_state(process)
                        elif state_code == 3 or state_code == 5: # connection failed or lost
                            peer_position = int(peer_position)
                            process = self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[peer_position-1]]
                            if state_code == 3: # connection failed
                                process._set_terminated(error = True, error_reason = "taktuk connection failed")
                            else: # 5: connection lost
                                process._set_terminated(error = True, error_reason = "taktuk connection lost")
                        elif state_code in (0, 1, 2, 4, 19):
                            pass
                        else:
                            self._log_unexpected_output(string)
                elif header == 71: # to ignore "Error No such file or directory". Don't know if it can occur in other situations
                    if position > 0:
                        processes = [ self.taktukaction.processes[self.taktukaction._taktuk_hosts_order[position-1]] ]
                    else:
                        processes = self.taktukaction.processes
                    for process in processes:
                        process._handle_stderr(line, False, False)
                else:
                    self._log_unexpected_output(string)
        except Exception, e: #IGNORE:W0703
            logger.critical("%s: Unexpected exception %s while parsing taktuk output. Please report this message.", self.__class__.__name__, e)
            logger.critical("line received = %s", string.rstrip('\n'))

class TaktukGet(TaktukRemote):

    """Copy remote files from several remote host to a local directory, with ``taktuk``."""

    def __init__(self, hosts, remote_files, local_location = ".", connection_params = None, **kwargs):
        """
        :param hosts: iterable of `execo.host.Host` from which to get
          the files.

        :param remote_files: an iterable of string of file
          paths. Substitions described in
          `execo.substitutions.remote_substitute` will not be
          performed, but taktuk substitutions can be used (see
          http://taktuk.gforge.inria.fr/taktuk.html#item_get__2a_src__2a__2a_dest__2a)

        :param local_location: the local directory were the files will
          be copied. Substitions described in
          `execo.substitutions.remote_substitute` will not be
          performed, but taktuk substitutions can be used (see
          http://taktuk.gforge.inria.fr/taktuk.html#item_get__2a_src__2a__2a_dest__2a)

        :param connection_params: a dict similar to
          `execo.config.default_connection_params` whose values will
          override those in default_connection_params for connection.
        """
        self.hosts = hosts
        """Iterable of `execo.host.Host` from which to get the files."""
        if not kwargs.has_key("name"):
            kwargs.update({"name": "%s from %i hosts" % (self.__class__.__name__, len(self.hosts))})
        super(TaktukRemote, self).__init__(**kwargs)
        self.remote_files = remote_files
        """Iterable of string of file paths. Substitions described in
        `execo.substitutions.remote_substitute` will not be performed, but
        taktuk substitutions can be used (see
        http://taktuk.gforge.inria.fr/taktuk.html#item_get__2a_src__2a__2a_dest__2a)"""
        self.local_location = local_location
        """The local directory were the files will be copied. Substitions described
        in `execo.substitutions.remote_substitute` will not be performed, but
        taktuk substitutions can be used (see
        http://taktuk.gforge.inria.fr/taktuk.html#item_get__2a_src__2a__2a_dest__2a)"""
        self.connection_params = connection_params
        """Dict similar to `execo.config.default_connection_params` whose values
        will override those in default_connection_params for connection."""
        self.process_args = {}
        self._caller_context = get_caller_context(['get_fileget'])
        self._taktuk_stdout_output_handler = _TaktukGetOutputHandler(self)
        self._taktuk_stderr_output_handler = self._taktuk_stdout_output_handler
        self._init_processes()

    def _args(self):
        return [ repr(self.hosts),
                 repr(self.remote_files) ] + Action._args(self) + TaktukGet._kwargs(self)

    def _kwargs(self):
        kwargs = []
        kwargs.append("local_location=%r" % (self.local_location,))
        if self.connection_params: kwargs.append("connection_params=%r" % (self.connection_params,))
        return kwargs

    def _gen_taktukprocesses(self):
        processlh = ActionNotificationProcessLH(self, len(self.hosts))
        for (index, host) in enumerate(self.hosts):
            process = TaktukProcess("",
                                    host = host,
                                    **self.process_args)
            process.lifecycle_handlers.append(processlh)
            process._num_transfers_started = 0
            process._num_transfers_terminated = 0
            process._num_transfers_failed = 0
            self.processes.append(process)

    def _gen_taktuk_commands(self, hosts_with_explicit_user):
        self._taktuk_hosts_order = []
        for (index, host) in [ (idx, h) for (idx, h) in enumerate(self.hosts) if h not in hosts_with_explicit_user ]:
            self._taktuk_commands += ("-m", get_rewritten_host_address(host.address, self.connection_params))
            self._taktuk_hosts_order.append(index)
        for (index, host) in [ (idx, h) for (idx, h) in enumerate(self.hosts) if h in hosts_with_explicit_user ]:
            self._taktuk_commands += ("-l", host.user, "-m", get_rewritten_host_address(host.address, self.connection_params))
            self._taktuk_hosts_order.append(index)
        for src in self.remote_files:
            self._taktuk_commands += ("broadcast", "get", "[", src, "]", "[", self.local_location, "]", ";")

class Local(Action):

    """Launch a command localy."""

    def __init__(self, cmd, process_args = None, **kwargs):
        """:param cmd: the command to run.

        :param process_args: Dict of keyword arguments passed to
          instanciated processes.
        """
        self.cmd = cmd
        """the command to run"""
        if not kwargs.has_key("name"):
            kwargs.update({"name": name_from_cmdline(self.cmd)})
        super(Local, self).__init__(**kwargs)
        if process_args != None:
            self.process_args = process_args
            """Dict of keyword arguments passed to instanciated processes."""
        else:
            self.process_args = {}
        self._init_processes()

    def _args(self):
        return [ repr(self.cmd) ] + Action._args(self) + Local._kwargs(self)

    def _kwargs(self):
        return []

    def _init_processes(self):
        self.processes = []
        p = Process(self.cmd, **self.process_args)
        processlh = ActionNotificationProcessLH(self, 1)
        p.lifecycle_handlers.append(processlh)
        self.processes.append(p)

    def start(self):
        retval = super(Local, self).start()
        [ p.start() for p in self.processes ]
        return retval

    def kill(self):
        retval = super(Local, self).kill()
        [ p.kill() for p in self.processes if p.running ]
        return retval

class ParallelSubActionLH(ActionLifecycleHandler):

    def __init__(self, parallel_action):
        super(ParallelSubActionLH, self).__init__()
        self.parallel_action = parallel_action
        self._num_terminated = 0

    def reset(self, action):
        self._num_terminated = 0

    def end(self, action):
        self._num_terminated += 1
        logger.debug("%i/%i subactions terminated in %s",
            self._num_terminated,
            len(self.parallel_action.actions),
            self.parallel_action)
        if self._num_terminated == len(self.parallel_action.actions):
            self.parallel_action._notify_terminated()

class ParallelActions(Action):

    """An `execo.action.Action` running several sub-Actions in parallel.

    Will start, kill, wait, run every action in parallel.
    """

    def __init__(self, actions, **kwargs):
        self.actions = actions
        if not kwargs.has_key("name"):
            kwargs.update({"name": "%s %i actions" % (self.__class__.__name__, len(self.actions))})
        super(ParallelActions, self).__init__(**kwargs)
        self.hide_subactions = False
        """Wether to hide sub actions in stats."""
        self._init_actions()

    def _args(self):
        return [ repr(self.actions) ] + Action._args(self) + ParallelActions._kwargs(self)

    def _kwargs(self):
        return []

    def _init_actions(self):
        subactionslh = ParallelSubActionLH(self)
        for action in self.actions:
            action.lifecycle_handlers = [ lh for lh in action.lifecycle_handlers if not isinstance(lh, ParallelSubActionLH) ]
            action.lifecycle_handlers.append(subactionslh)

    def start(self):
        retval = super(ParallelActions, self).start()
        if len(self.actions) == 0:
            logger.debug("%s contains 0 actions -> immediately terminated", self)
            self._notify_terminated()
        else:
            for action in self.actions:
                action.start()
        return retval

    def kill(self):
        retval = super(ParallelActions, self).kill()
        for action in self.actions:
            action.kill()
        return retval

    def reset(self):
        retval = super(ParallelActions, self).reset()
        for action in self.actions:
            action.reset()
        self._init_actions()
        return retval

    @property
    def processes(self):
        p = []
        for action in self.actions:
            p.extend(action.processes)
        return p

    @processes.setter
    def processes(self, v):
        pass

    def stats(self):
        stats = Report.empty_stats()
        stats['name'] = self.name
        stats['sub_stats'] = [action.stats() for action in self.actions]
        s = Report.aggregate_stats(stats)
        if self.hide_subactions:
            s['sub_stats'] = []
        return s

class SequentialSubActionLH(ActionLifecycleHandler):

    def __init__(self, sequential_action):
        super(SequentialSubActionLH, self).__init__()
        self.sequential_action = sequential_action
        self._num_terminated = 0

    def reset(self, action):
        self._num_terminated = 0

    def end(self, action):
        self._num_terminated += 1
        logger.debug("%i/%i subactions terminated in %s",
            self._num_terminated,
            len(self.sequential_action.actions),
            self.sequential_action)
        if self._num_terminated < len(self.sequential_action.actions):
            self.sequential_action.actions[self._num_terminated].start()
        else:
            self.sequential_action._notify_terminated()

class SequentialActions(Action):

    """An `execo.action.Action` running several sub-actions sequentially.

    Will start, kill, wait, run every Action sequentially.
    """

    def __init__(self, actions, **kwargs):
        self.actions = actions
        if not kwargs.has_key("name"):
            kwargs.update({"name": "%s %i actions" % (self.__class__.__name__, len(self.actions))})
        super(SequentialActions, self).__init__(**kwargs)
        self.hide_subactions = False
        """Wether to hide sub actions in stats."""
        self._init_actions()

    def _args(self):
        return [ repr(self.actions) ] + Action._args(self) + SequentialActions._kwargs(self)

    def _kwargs(self):
        return []

    def _init_actions(self):
        subactionslh = SequentialSubActionLH(self)
        for action in self.actions:
            action.lifecycle_handlers = [ lh for lh in action.lifecycle_handlers if not isinstance(lh, SequentialSubActionLH) ]
            action.lifecycle_handlers.append(subactionslh)

    def start(self):
        retval = super(SequentialActions, self).start()
        if len(self.actions) == 0:
            logger.debug("%s contains 0 actions -> immediately terminated", self)
            self._notify_terminated()
        else:
            self.actions[0].start()
        return retval

    def kill(self):
        retval = super(SequentialActions, self).kill()
        for action in self.actions:
            action.kill()
        return retval

    def reset(self):
        retval = super(SequentialActions, self).reset()
        for action in self.actions:
            action.reset()
        self._init_actions()
        return retval

    @property
    def processes(self):
        p = []
        for action in self.actions:
            p.extend(action.processes)
        return p

    @processes.setter
    def processes(self, v):
        pass

    def stats(self):
        stats = Report.empty_stats()
        stats['name'] = self.name
        stats['sub_stats'] = [action.stats() for action in self.actions]
        s = Report.aggregate_stats(stats)
        if self.hide_subactions:
            s['sub_stats'] = []
        return s

# class _ChainPutActionHostFilteringLH(ActionLifecycleHandler):

#     def __init__(self, chainput, filtered_actions):
#         super(_ChainPutActionHostFilteringLH, self).__init__()
#         self.chainput = chainput
#         self.filtered_actions = filtered_actions

#     def end(self, action):
#         bad_hosts = [ p.host for p in
#                       action.processes
#                       if not p.ok ]
#         logger.debug(
#             "_ChainPutActionHostFilteringLH: action %s finished. bad hosts = %s" % (
#                 action, bad_hosts))
#         # partie qui pose souci
#         # for a in self.filtered_actions:
#         #     logger.debug(
#         #         "removing hosts %s from action %s" % (
#         #             set(a.hosts).intersection(bad_hosts), a))
#         #     a.hosts = list(set(a.hosts).difference(bad_hosts))
#         #     a.reset()
#         # fin partie qui pose souci
#         self.chainput.bad_hosts.update(bad_hosts)
#         self.chainput.good_hosts.difference(bad_hosts)
#         logger.debug(
#             "%s:\ngood_hosts = %s\nbad_hosts = %s" % (
#                 self.chainput,
#                 self.chainput.good_hosts,
#                 self.chainput.bad_hosts))

class _ChainPutCopyTaktukProcessLH(ProcessLifecycleHandler):

    def __init__(self, num_processes, action_to_start):
        self.num_processes = num_processes
        self.started_processes = 0
        self.action_to_start = action_to_start

    def start(self, process):
        self.started_processes += 1
        if self.started_processes == self.num_processes:
            self.action_to_start.start()

class _ChainPutCopy(ParallelActions):

    def __init__(self, local, remote):
        self._local = local
        self._remote = remote
        super(_ChainPutCopy, self).__init__([local, remote])

    def start(self):
        if isinstance(self._remote, TaktukRemote):
            wait_all_taktuk_processes_start = _ChainPutCopyTaktukProcessLH(
                len(self._remote.processes),
                self._local)
            for p in self._remote.processes:
                p.lifecycle_handlers.append(wait_all_taktuk_processes_start)
            retval = super(ParallelActions, self).start()
            self._remote.start()
            return retval
        else:
            return super(_ChainPutCopy, self).start()

_execo_chainput = os.path.abspath(os.path.join(os.path.dirname(__file__), "execo-chainput"))
if not os.path.isfile(_execo_chainput): _execo_chainput = None

class ChainPut(SequentialActions):

    """Broadcast local files to several remote host, with an unencrypted, unauthenticated chain of host to host copies (idea taken from `kastafior <https://gforge.inria.fr/plugins/scmgit/cgi-bin/gitweb.cgi?p=kadeploy3/kadeploy3.git;a=tree;f=addons/kastafior;h=e5472ce9e800c80d9f54d1097ebbcba77f8ccd7a;hb=3.1.7>`_).

    Each broadcast is performed with a chain copy (simultaneously:
    host0 sending to host1, host1 sending to host2, ... hostN to
    hostN+1)

    ChainPut relies on:

    - running a bourne shell and netcat being available both on remote
      hosts and on localhost.

    - direct TCP connections allowed between any nodes among localhost
      and remote hosts. The exact chain of TCP connections is: localhost
      to first remote host, first remote host to second remote host, and
      so on up to the last remote host.

    On the security side, data transfers are not crypted, and ChainPut
    briefly opens a TCP server socket on each remote host, accepting
    any data without authentication. Insecure temporary files are
    used. It is thus intended to be used in a secured network
    environment.
    """

    def __init__(self, hosts, local_files, remote_location = ".", connection_params = None, **kwargs):
        """
        :param hosts: iterable of `execo.host.Host` onto which to copy
          the files.

        :param local_file: iterable of source file (local pathes).

        :param remote_location: destination directory (remote path).

        :param connection_params: a dict similar to
          `execo.config.default_connection_params` whose values will
          override those in default_connection_params for connection.
        """
        self.hosts = hosts
        # self.good_hosts = set(self.hosts)
        # self.bad_hosts = set()
        self.local_files = local_files
        self.remote_location = remote_location
        self.connection_params = connection_params
        if not kwargs.has_key("name"):
            kwargs.update({"name": "%s to %i hosts" % (self.__class__.__name__, len(self.hosts))})
        super(ChainPut, self).__init__([], **kwargs)
        self.hide_subactions = True

    @property
    def hosts(self):
        return self._hosts

    @hosts.setter
    def hosts(self, v):
        self._hosts = get_unique_hosts_list(singleton_to_collection(v))

    def _init_actions(self):
        if len(self.hosts) > 0:
            actual_connection_params = make_connection_params(self.connection_params)
            chain_retries = actual_connection_params['chainput_chain_retry']
            if isinstance(chain_retries, float):
                chain_retries = int(chain_retries * len(self.hosts))

            chainhosts_handle, chainhosts_filename = tempfile.mkstemp(prefix = 'tmp_execo_chainhosts_')
            chainhosts = "\n".join([h.address for h in self.hosts]) + "\n"
            os.write(chainhosts_handle, chainhosts)
            os.close(chainhosts_handle)

            chainscript_filename = tempfile.mktemp(prefix = 'tmp_execo_chainscript_')
            if not _execo_chainput:
                raise EnvironmentError, "unable to find execo-chainput"
            shutil.copy2(_execo_chainput, chainscript_filename)
            os.chmod(chainscript_filename, os.stat(chainscript_filename).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH )

            preparechain = TaktukPut(self.hosts,
                                     [ chainhosts_filename, chainscript_filename ],
                                     "/tmp",
                                     actual_connection_params
                                     )

            #previous_action = preparechain
            chains = []
            port = get_port()
            for findex, f in enumerate(self.local_files):

                autoremoveopt = ""
                if findex + 1 >= len(self.local_files):
                    autoremoveopt = " --autoremove"

                fwdcmd = [ "%s '%s' '%s' '%s' %i %i %i %i %i %i %i '%s'%s" % (
                        chainscript_filename,
                        f,
                        self.remote_location,
                        actual_connection_params['nc'],
                        actual_connection_params['chainput_nc_client_timeout'],
                        actual_connection_params['chainput_nc_server_timeout'],
                        port,
                        actual_connection_params['chainput_host_retry'],
                        chain_retries,
                        actual_connection_params['chainput_try_delay'],
                        idx+1,
                        chainhosts_filename,
                        autoremoveopt,
                        ) for idx, host in enumerate(self.hosts) ]

                fwd = TaktukRemote("{{fwdcmd}}",
                                   self.hosts,
                                   actual_connection_params)

                send = Local("%s '%s' '%s' '%s' %i %i %i %i %i %i %i '%s'%s" % (
                        chainscript_filename,
                        f,
                        self.remote_location,
                        actual_connection_params['nc'],
                        actual_connection_params['chainput_nc_client_timeout'],
                        actual_connection_params['chainput_nc_server_timeout'],
                        port,
                        actual_connection_params['chainput_host_retry'],
                        chain_retries,
                        actual_connection_params['chainput_try_delay'],
                        0,
                        chainhosts_filename,
                        autoremoveopt,
                        ))

                chain = _ChainPutCopy(send, fwd)
                #previous_action.lifecycle_handlers.insert(0, _ChainPutActionHostFilteringLH(self, [fwd]))
                #previous_action = chain
                chains.append(chain)

            self.actions = [ preparechain ] + chains
        else:
            self.actions = []
        super(ChainPut, self)._init_actions()

class RemoteSerial(Remote):

    """Open a serial port on several hosts in parallel through ``ssh`` or a similar remote connection tool.

    The serial port can be read (standard output) and written to (standard input).
    """

    def __init__(self, hosts, device, speed, connection_params = None, process_args = None, **kwargs):
        """:param hosts: iterable of `execo.host.Host` to which to
          connect and open the serial device.

        :param device: The Path to the serial device on the remote
          hosts (for example: ``/dev/ttyUSB1``). Substitions described
          in `execo.substitutions.remote_substitute` will be
          performed.

        :param speed: the speed of the serial port (for example:
          115200)

        :param connection_params: a dict similar to
          `execo.config.default_connection_params` whose values will
          override those in default_connection_params for connection.

        :param process_args: Dict of keyword arguments passed to
          instanciated processes.
        """
        self.hosts = hosts
        """Iterable of `execo.host.Host` to which to connect and run the command."""
        if not kwargs.has_key("name"):
            kwargs.update({"name": "%s to %i hosts" % (self.__class__.__name__, len(self.hosts))})
        super(Remote, self).__init__(**kwargs)
        self.connection_params = connection_params
        """A dict similar to `execo.config.default_connection_params` whose values
        will override those in default_connection_params for connection."""
        self.device = device
        """Path to the serial devices on the remote hosts. (for example:
        ``/dev/ttyUSB1``). Substitions described in
        `execo.substitutions.remote_substitute` will be performed."""
        self.speed = speed
        """The speed of the serial port (for example: 115200)"""
        if process_args != None:
            self.process_args = process_args
            """Dict of keyword arguments passed to instanciated processes."""
        else:
            self.process_args = {}
        self._caller_context = get_caller_context()
        self._init_processes()

    def _args(self):
        return [ repr(self.hosts),
                 repr(self.device),
                 repr(self.speed) ] + Action._args(self) + RemoteSerial._kwargs(self)

    def _kwargs(self):
        kwargs = []
        if self.connection_params: kwargs.append("connection_params=%r" % (self.connection_params,))
        return kwargs

    def _init_processes(self):
        self.processes = []
        processlh = ActionNotificationProcessLH(self, len(self.hosts))
        for (index, host) in enumerate(self.hosts):
            p = SerialSsh(host,
                          remote_substitute(self.device, self.hosts, index, self._caller_context),
                          self.speed,
                          connection_params = self.connection_params,
                          **self.process_args)
            p.lifecycle_handlers.append(processlh)
            self.processes.append(p)

class ActionFactory:
    """Instanciate multiple remote process execution and file copies using configurable connector tools: ``ssh``, ``scp``, ``taktuk``"""

    def __init__(self, remote_tool = None, fileput_tool = None, fileget_tool = None):
        """
        :param remote_tool: can be `execo.config.SSH` or
          `execo.config.TAKTUK`

        :param fileput_tool: can be `execo.config.SCP`,
          `execo.config.TAKTUK` or `execo.config.CHAINPUT`

        :param fileget_tool: can be `execo.config.SCP` or
          `execo.config.TAKTUK`
        """

        if remote_tool == None:
            remote_tool = configuration.get("remote_tool")
        if fileput_tool == None:
            fileput_tool = configuration.get("fileput_tool")
        if fileget_tool == None:
            fileget_tool = configuration.get("fileget_tool")
        self.remote_tool = remote_tool
        self.fileput_tool = fileput_tool
        self.fileget_tool = fileget_tool

    def get_remote(self, *args, **kwargs):
        """Instanciates a `execo.action.Remote` or `execo.action.TaktukRemote`"""
        if self.remote_tool == SSH:
            return Remote(*args, **kwargs)
        elif self.remote_tool == TAKTUK:
            return TaktukRemote(*args, **kwargs)
        else:
            raise KeyError, "no such remote tool: %s" % self.remote_tool

    def get_fileput(self, *args, **kwargs):
        """Instanciates a `execo.action.Put`, `execo.action.TaktukPut` or `execo.action.ChainPut`"""
        if self.fileput_tool == SCP:
            return Put(*args, **kwargs)
        elif self.fileput_tool == TAKTUK:
            return TaktukPut(*args, **kwargs)
        elif self.fileput_tool == CHAINPUT:
            return ChainPut(*args, **kwargs)
        else:
            raise KeyError, "no such fileput tool: %s" % self.fileput_tool

    def get_fileget(self, *args, **kwargs):
        """Instanciates a `execo.action.Get` or `execo.action.TaktukGet`"""
        if self.fileget_tool == SCP:
            return Get(*args, **kwargs)
        elif self.fileget_tool == TAKTUK:
            return TaktukGet(*args, **kwargs)
        else:
            raise KeyError, "no such fileget tool: %s" % self.fileget_tool

default_action_factory = ActionFactory()

def get_remote(*args, **kwargs):
    """Instanciates a `execo.action.Remote` or `execo.action.TaktukRemote` with the default factory"""
    return default_action_factory.get_remote(*args, **kwargs)

def get_fileput(*args, **kwargs):
    """Instanciates a `execo.action.Put` or `execo.action.TaktukPut` with the default factory"""
    return default_action_factory.get_fileput(*args, **kwargs)

def get_fileget(*args, **kwargs):
    """Instanciates a `execo.action.Get` or `execo.action.TaktukGet` with the default factory"""
    return default_action_factory.get_fileget(*args, **kwargs)

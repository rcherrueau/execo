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

from log import style, logger, logger_handler
from time_utils import format_unixts
from utils import compact_output
from config import configuration
import Queue, errno, fcntl, logging, os, select, \
  signal, sys, thread, threading, time, traceback, \
  subprocess, resource

# assuming import of this module is triggered from "the main" thread,
# the following call is intended to workaround python issue #7980
# http://bugs.python.org/issue7980
# (see also http://bugs.python.org/issue11108)
try:
    time.strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
except:
    pass

# max number of bytes read when reading asynchronously from a pipe
try:
    _MAXREAD = int(subprocess.Popen(["getconf", "_POSIX_SSIZE_MAX"], stdout=subprocess.PIPE).communicate()[0])
except:
    _MAXREAD = 32767

DEFAULT_MAXFD = 1024

if sys.platform.startswith('darwin') or sys.platform.startswith('win'):

    # minimal abstraction of poll over select allowing the conductor
    # to run on platforms lacking poll: macosx, windows. poll is
    # removed from python on macos due to an unclear (buggy or
    # unusual?) behavior.  this *minimal* abstraction only goes as far
    # as needed to allow the conductor to run, this is probably *not*
    # a full emulation of poll over select.

    POLLIN=1
    POLLPRI=2
    POLLOUT=4
    POLLERR=8
    POLLHUP=16
    POLLNVAL=32

    def dict_item_and(dic, item, field):
        if dic.has_key(item):
            dic[item] |= field
        else:
            dic[item] = field

    class poll(object):

        def __init__(self):
            self._readfds = list()
            self._writefds = list()
            self._exceptfds = list()

        def register(self, fd, eventmask = POLLIN | POLLPRI | POLLOUT):
            if eventmask & POLLIN or eventmask & POLLPRI:
                readfds_set = set(self._readfds)
                readfds_set.add(fd)
                self._readfds = list(readfds_set)
            if eventmask & POLLOUT:
                writefds_set = set(self._writefds)
                writefds_set.add(fd)
                self._writefds = list(writefds_set)
            if eventmask & POLLIN or eventmask & POLLOUT:
                exceptfds_set = set(self._exceptfds)
                exceptfds_set.add(fd)
                self._exceptfds = list(exceptfds_set)

        def unregister(self, fd):
            try:
                self._readfds.remove(fd)
            except ValueError:
                pass
            try:
                self._writefds.remove(fd)
            except ValueError:
                pass
            try:
                self._exceptfds.remove(fd)
            except ValueError:
                pass

        def poll(self, timeout = None):
            if timeout != None: timeout = timeout / 1000.0
            (ready_readfds,
             ready_writefds,
             ready_exceptfds) = select.select(
                self._readfds,
                self._writefds,
                self._exceptfds, timeout)
            fd_events = dict()
            for fd in ready_readfds: dict_item_and(fd_events, fd, POLLIN)
            for fd in ready_writefds: dict_item_and(fd_events, fd, POLLOUT)
            for fd in ready_exceptfds: dict_item_and(fd_events, fd, POLLERR)
            return list(fd_events.iteritems())

else:
    from select import poll, POLLIN, POLLPRI, POLLOUT, POLLERR, POLLHUP, POLLNVAL

def _event_desc(event):
    """For debugging: user friendly representation of the event bitmask returned by poll()."""
    desc = ""
    first = True
    for t in ('POLLIN', 'POLLPRI', 'POLLOUT', 'POLLERR', 'POLLHUP', 'POLLNVAL'):
        if event & globals()[t]:
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
            eof = True
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
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        # this lock and conditions are used for:
        #
        # - mutual exclusion and synchronization beetween sections of
        # code in _Conductor.__io_loop (conductor thread), and in
        # Process.start() and Process.wait() (main thread)
        #
        # - mutual exclusion beetween sections of code in
        #   _Conductor.__io_loop() (conductor thread) and in
        #   _Conductor.__reaper_thread_func() (reaper thread)

        self.__io_thread = threading.Thread(target = self.__io_thread_func, name = "I/O")
        self.__io_thread.setDaemon(True)
        # thread will terminate automatically when the main thread
        # exits.  once in a while, this can trigger an exception, but
        # this seems to be safe and to be related to this issue:
        # http://bugs.python.org/issue1856
        self.__rpipe, self.__wpipe = os.pipe()  # pipe used to wakeup
                                                # the conductor thread
                                                # from the main thread
                                                # when needed
        _set_fd_nonblocking(self.__rpipe)   # the reading function
                                            # _read_asmuch() relies on
                                            # file descriptors to be non
                                            # blocking
        self.__poller = poll()   # asynchronous I/O with all
                                 # subprocesses filehandles
        self.__poller.register(self.__rpipe,
                               POLLIN
                               | POLLERR)
        self.__processes = set()    # the set of `Process` handled by
                                    # this `_Conductor`
        self.__fds = dict() # keys: the file descriptors currently polled by
                            # this `_Conductor`
                            #
                            # values: tuples (`Process`, `Process`'s
                            # function to handle activity for this
                            # descriptor)
        self.__pids = dict()    # keys: the pids of the subprocesses
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
        self.__reaper_thread_running = False
                                # to keep track wether reaper thread is
                                # running
        signal.set_wakeup_fd(self.__wpipe)
        self.pgrp = self.__start_pgrp()

    def __str__(self):
        return "<" + style.object_repr("Conductor") + "(num processes=%i, num fds=%i, num pids=%i, timeline length=%i)>" % (len(self.__processes), len(self.__fds), len(self.__pids), len(self.__timeline))

    def __start_pgrp(self):
        # start a dedicated dummy process, having its own process
        # group, in order to group all processes handled by this
        # conductor
        ppid = os.getpid()
        pid = os.fork()
        if pid == 0:
            os.setpgid(0, 0)
            os.chdir("/")
            os.umask(0)
            maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
            if (maxfd == resource.RLIM_INFINITY):
                if (os.sysconf_names.has_key("SC_OPEN_MAX")):
                    maxfd = maxfd = os.sysconf("SC_OPEN_MAX")
                else:
                    maxfd = DEFAULT_MAXFD
            for fd in range(0, maxfd):
                try: os.close(fd)
                except OSError: pass
            while True:
                # poll each second that my parent is still alive. If
                # not, die. Optionnaly kill all instanciated childs.
                if os.getppid() != ppid:
                    if configuration['kill_childs_at_end']:
                        os.killpg(0, signal.SIGTERM)
                    os._exit(0)
                time.sleep(1)
        else:
            return pid

    def __wakeup(self):
        # wakeup the I/O thread
        os.write(self.__wpipe, ".")

    def start(self):
        """Start the conductor thread."""
        self.__io_thread.start()
        return self

    def terminate(self):
        """Close the conductor thread."""
        # the closing of the pipe will wake the conductor which will
        # detect this closing and self stop
        logger.debug("terminating I/O thread of %s", self)
        os.close(self.__wpipe)

    def start_process(self, process):
        """Register a new `execo.process.Process` to be started and handled by the conductor.

        Intended to be called from main thread.
        """
        self.__process_actions.put_nowait((self.__handle_start_process, (process,)))
        self.__wakeup()

    def update_process(self, process):
        """Update `execo.process.Process` to the conductor.

        Intended to be called from main thread.

        Currently: only update the timeout. This is related to the way
        the system for forcing SIGKILL on processes not killing
        cleanly is implemented.
        """
        self.__process_actions.put_nowait((self.__handle_update_process, (process,)))
        self.__wakeup()

    def remove_process(self, process, exit_code = None):
        """Remove a `execo.process.Process` from the conductor.

        Intended to be called from main thread.
        """
        self.__process_actions.put_nowait((self.__handle_remove_process, (process, exit_code)))
        self.__wakeup()

    def notify_process_terminated(self, pid, exit_code):
        """Tell the conductor thread that a `execo.process.Process` has terminated.

        Intended to be called from the reaper thread.
        """
        self.__process_actions.put_nowait((self.__handle_notify_process_terminated, (pid, exit_code)))
        self.__wakeup()

    def __handle_start_process(self, process):
        assert(not process.started and not process.ended)
        assert(process not in self.__processes)
        process._actual_start()
        if not process.ended:
            fileno_stdout = process.stdout_fd
            fileno_stderr = process.stderr_fd
            self.__processes.add(process)
            _set_fd_nonblocking(fileno_stdout)
            _set_fd_nonblocking(fileno_stderr)
            self.__fds[fileno_stdout] = (process, process._handle_stdout)
            self.__fds[fileno_stderr] = (process, process._handle_stderr)
            self.__poller.register(fileno_stdout,
                                   POLLIN
                                   | POLLERR)
            self.__poller.register(fileno_stderr,
                                   POLLIN
                                   | POLLERR)
            self.__pids[process.pid] = process
            if process.timeout_date != None:
                self.__timeline.append((process.timeout_date, process))
            if self.__reaper_thread_running == False:
                self.__reaper_thread_running = True
                reaper_thread = threading.Thread(target = self.__reaper_thread_func, name = "Reaper")
                reaper_thread.setDaemon(True)
                reaper_thread.start()

    def __handle_update_process(self, process):
        # intended to be called from conductor thread
        # Currently: only update the timeout. This is related to the
        # way the system for forcing SIGKILL on processes not killing
        # cleanly is implemented.
        logger.fdebug("update timeouts of %s in %s", str(process), self)
        if process not in self.__processes:
            return  # this will frequently occur if the process kills
                    # quickly because the process will already be
                    # killed and reaped before __handle_update_process
                    # is called
        if process.timeout_date != None:
            self.__timeline.append((process.timeout_date, process))

    def __handle_remove_process(self, process, exit_code = None):
        # intended to be called from conductor thread
        # unregister a Process from conductor
        logger.fdebug("removing %s from %s", str(process), self)
        if process not in self.__processes:
            raise ValueError, "trying to remove a process which was not yet added to conductor"
        self.__timeline = [ x for x in self.__timeline if x[1] != process ]
        del self.__pids[process.pid]
        fileno_stdout = process.stdout_fd
        fileno_stderr = process.stderr_fd
        last_bytes = ""
        if self.__fds.has_key(fileno_stdout):
            del self.__fds[fileno_stdout]
            self.__poller.unregister(fileno_stdout)
            # read the last data that may be available on stdout of
            # this process
            try:
                (last_bytes, _) = _read_asmuch(fileno_stdout)
            except OSError, e:
                if e.errno == errno.EBADF: last_bytes = ""
                else: raise e
        process._handle_stdout(last_bytes, True, False)
        last_bytes = ""
        if self.__fds.has_key(fileno_stderr):
            del self.__fds[fileno_stderr]
            self.__poller.unregister(fileno_stderr)
            # read the last data that may be available on stderr of
            # this process
            try:
                (last_bytes, _) = _read_asmuch(fileno_stderr)
            except OSError, e:
                if e.errno == errno.EBADF: last_bytes = ""
                else: raise e
        process._handle_stderr(last_bytes, True, False)
        self.__processes.remove(process)
        if exit_code != None:
            process._set_terminated(exit_code = exit_code)

    def __get_next_timeout(self):
        """Return the remaining time until the smallest timeout date of all registered `execo.process.Process`."""
        next_timeout = None
        if len(self.__timeline) != 0:
            self.__timeline.sort(key = lambda x: x[0])
            next_timeout = (self.__timeline[0][0] - time.time())
        return next_timeout

    def __check_timeouts(self):
        """Iterate all registered `execo.process.Process` whose timeout is reached, kill them gently.

        And remove them from the timeline.
        """
        now = time.time()
        remove_in_timeline = []
        for i in xrange(0, len(self.__timeline)):
            process = self.__timeline[i][1]
            if now >= process.timeout_date:
                logger.debug("timeout on %s", str(process))
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
        exit_pid, exit_code = _checked_waitpid(- self.pgrp, os.WNOHANG)
        while exit_pid != 0:
            process = self.__pids[exit_pid]
            logger.fdebug("process pid %s terminated: %s", exit_pid, str(process))
            self.__handle_remove_process(process, exit_code)
            exit_pid, exit_code = _checked_waitpid(- self.pgrp, os.WNOHANG)

    def __handle_notify_process_terminated(self, pid, exit_code):
        # intended to be called from conductor thread
        # remove a process based on its pid (as reported by waitpid)
        process = self.__pids.get(pid)
        if process:
            self.__handle_remove_process(process, exit_code)

    def __remove_handle(self, fd):
        # remove a file descriptor both from our member(s) and from
        # the Poll object
        del self.__fds[fd]
        self.__poller.unregister(fd)

    def __io_thread_func(self):
        # wrapper around the actual io loop func for
        # exception handling
        try:
            self.__io_loop()
        except Exception: #IGNORE:W0703
            print "exception in conductor I/O loop thread"
            traceback.print_exc()
            thread.interrupt_main()

    def __io_loop(self):
        # conductor thread infinite I/O loop
        finished = False
        while not finished:
            descriptors_events = []
            delay = self.__get_next_timeout()   # poll timeout will be
                                                # the delay until the
                                                # first of our
                                                # registered processes
                                                # reaches its timeout
            logger.fdebug("polling %i descriptors (+ rpipe) with timeout %s", len(self.__fds), "%.3fs" % delay if delay != None else "None")
            if delay == None or delay > 0: # don't even poll if poll timeout is <= 0
                if delay != None: delay *= 1000 # poll needs delay in millisecond
                descriptors_events = self.__poller.poll(delay)
            logger.fdebug("len(descriptors_events) = %i", len(descriptors_events))
            event_on_rpipe = None   # we want to handle any event on
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
                        logger.fdebug("event %s on fd %s, process %s", _event_desc(event), fd, str(process))
                        if event & POLLIN:
                            (string, eof) = _read_asmuch(fd)
                            stream_handler_func(string, False, False)
                            if eof:
                                self.__remove_handle(fd)
                        #if event & POLLHUP:
                        #    stream_handler_func('', True, False)
                        #    self.__remove_handle(fd)
                        if event & POLLERR:
                            stream_handler_func('', False, True)
                            self.__remove_handle(fd)
            self.__check_timeouts()
            if event_on_rpipe != None:
                logger.fdebug("event %s on inter-thread pipe", _event_desc(event_on_rpipe))
                if event_on_rpipe & POLLIN:
                    (string, eof) = _read_asmuch(self.__rpipe)
                    if eof:
                        # pipe closed -> auto stop the thread
                        finished = True
                if event_on_rpipe & POLLHUP:
                    finished = True
                if event_on_rpipe & POLLERR:
                    finished = True
                    raise IOError, "Error on inter-thread communication pipe"
            with self.lock:
                while True:
                    try:
                        # call (in the right order!) all functions
                        # enqueued from other threads
                        func, args = self.__process_actions.get_nowait()
                    except Queue.Empty:
                        break
                    func(*args)
                self.__update_terminated_processes()
                self.condition.notifyAll()
        self.__poller.unregister(self.__rpipe)
        os.close(self.__rpipe)
        os.close(self.__wpipe)

    def __reaper_thread_func(self):
        # run func for the reaper thread, whose role is to wait to be
        # notified by the operating system of terminated processes
        while True:
            exit_pid, exit_code = _checked_waitpid(- self.pgrp, 0)
            with self.lock:
                # this lock is needed to ensure that:
                #
                # - Conductor.__update_terminated_processes() won't be
                #   called before the process has been registered to
                #   the conductor
                #
                # - the following code cannot run while
                #    __handle_start_process() is running
                if (exit_pid, exit_code) == (0, 0):
                    if len(self.__processes) == 0:
                        # no more child processes, we stop this thread
                        # (another instance will be restarted as soon as
                        # another process is started)
                        self.__reaper_thread_running = False
                        break
                    # (exit_pid, exit_code) can be == (0, 0) and
                    # len(self.__processes) > 0 when and only when
                    # _checked_waitpid has returned because there are
                    # no more child and at the same time (but before
                    # entering the locked section)
                    # __handle_start_process() just added a process.
                else:
                    logger.debug("process with pid=%s terminated, exit_code=%s", exit_pid, exit_code)
                    self.notify_process_terminated(exit_pid, exit_code)

the_conductor = _Conductor().start()
"""The **one and only** `execo.conductor._Conductor` instance."""

#------------------------------------------------------------------------
#
# debug tools
#
#------------------------------------------------------------------------

def debug_dump_processes():
    with the_conductor.lock:
        print >> sys.stderr, "\n===== %s dump current %i conductor handled processes:\n" % (format_unixts(time.time()), len(the_conductor._Conductor__processes),)
        for process in the_conductor._Conductor__processes:
            print >> sys.stderr, "====="
            print >> sys.stderr, str(process)
            print >> sys.stderr, "stdout:\n" + compact_output(process.stdout)
            print >> sys.stderr, "stderr:\n" + compact_output(process.stderr)
            print >> sys.stderr
        print >> sys.stderr, "=====\n"

_debug_thread = None
_debug_thread_id = None
_debug_thread_lock = threading.Lock()

def debug_dump_threads():
    print >> sys.stderr, "\n===== %s dump thread stack frames. %i threads. conductor lock = %s:\n" % (
        format_unixts(time.time()),
        len(sys._current_frames()),
        the_conductor.lock)
    idx=0
    for thread_id, frame in sys._current_frames().iteritems():
        print >> sys.stderr, "===== thread #%i [%#x] refcount = %s" % (idx, thread_id, sys.getrefcount(frame))
        if thread_id != _debug_thread_id:
            traceback.print_stack(frame, file = sys.stderr)
        else:
            print >> sys.stderr, "  debug thread, skipping"
        idx += 1
    print >> sys.stderr, "=====\n"

class _DebugThread(threading.Thread):

    def __init__(self, interval = 30):
        super(_DebugThread, self).__init__(name = "debug")
        self.interval = interval
        self.terminate = threading.Event()
        self.setDaemon(True)

    def run(self):
        while True:
            if self.terminate.is_set():
                break
            debug_dump_threads()
            time.sleep(self.interval)

    def stop(self):
        self.terminate.set()

def debug_toggle_thread(interval = 30):

    global _debug_thread, _debug_thread_id

    with _debug_thread_lock:
        if _debug_thread:
            _debug_thread.stop()
            _debug_thread = None
            _debug_thread_id = None
        else:
            _debug_thread = _DebugThread(interval)
            _debug_thread.start()
            _debug_thread_id = _debug_thread.ident

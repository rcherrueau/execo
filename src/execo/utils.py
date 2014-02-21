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

from config import configuration
import pipes, subprocess, os, time, sys, traceback, re

def comma_join(*args):
    return ", ".join([ arg for arg in args if len(arg) > 0 ])

def compact_output(s):
    thresh = configuration.get('compact_output_threshold')
    if thresh == 0 or len(s) <= thresh: return s
    return s[:thresh/2] + "\n[...]\n" + s[(thresh/2)-thresh:]

def str_from_cmdline(cmdline):
    if hasattr(cmdline, '__iter__'):
        return " ".join([ pipes.quote(arg) for arg in cmdline ])
    else:
        return cmdline

def name_from_cmdline(cmdline):
    cmdline = str_from_cmdline(cmdline)
    cmdline = cmdline.strip().replace("\n", " ")
    cmdline = re.sub('\s+', ' ', cmdline, flags = re.MULTILINE)
    if len(cmdline) > 39:
        cmdline = cmdline[0:36] + "..."
    return cmdline

def find_files(*args):
    """run find utility with given path(es) and parameters, return the result as a list"""
    find_args = "find " + " ".join([pipes.quote(arg) for arg in args])
    p = subprocess.Popen(find_args, shell = True,
                         stdout = subprocess.PIPE,
                         stderr = subprocess.PIPE)
    (stdout, stderr) = p.communicate()
    p.wait()
    return [ p for p in stdout.split("\n") if p ]

def which(name):
    """return full path of executable on the $PATH"""
    p = subprocess.Popen("which " + name, shell = True,
                         stdout = subprocess.PIPE,
                         stderr = subprocess.PIPE)
    (stdout, stderr) = p.communicate()
    p.wait()
    stdout = stdout.rstrip()
    if len(stdout) > 0:
        return stdout
    else:
        return None

def find_exe(name):
    """search an executable in whole execo directory. If not found, get it on the $PATH."""
    path = None
    for exe_path in find_files(os.path.join(os.path.abspath(os.path.dirname(os.path.realpath(__file__))), "..", ".."), "-name", name):
        path = exe_path
        break
    if not path:
        path = which(name)
    return path

def checked_min(a, b):
    """a min function which ignores an argument if its value is None"""
    if a == None: return b
    if b == None: return a
    return min(a, b)

def intr_event_wait(event, timeout = None):
    """``threading.event.wait`` wrapper

    For use in main thread. Periodically wakes up to allow main thread
    to receive signals (such as ctrl-c)
    """
    start = current = time.time()
    while True:
        if timeout == None:
            remaining = None
        else:
            remaining = min(timeout - (current - start), 0)
        t = checked_min(configuration['intr_period'], remaining)
        event.wait(float(t) if t else None)
        eventflag = event.is_set()
        if eventflag or t == remaining:
            return eventflag
        current = time.time()

def intr_cond_wait(cond, timeout = None):
    """``threading.condition.wait`` wrapper

    For use in main thread. Periodically wakes up to allow main thread
    to receive signals (such as ctrl-c)
    """
    t = checked_min(configuration['intr_period'], timeout)
    return cond.wait(float(t) if t else None)

def format_exc():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    return "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))

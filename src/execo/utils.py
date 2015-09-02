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

from config import configuration
import pipes, subprocess, os, time, sys, traceback, re, functools, threading, random

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

def non_retrying_intr_cond_wait(cond, timeout = None):
    """``threading.condition.wait`` wrapper

    For use in main thread. Periodically wakes up to allow main thread
    to receive signals (such as ctrl-c)
    """
    t = checked_min(configuration['intr_period'], timeout)
    return cond.wait(float(t) if t else None)

def memoize(obj):
    """memoizing decorator

    works on functions, methods, or classes, and exposes the cache
    publicly. From
    https://wiki.python.org/moin/PythonDecoratorLibrary#Alternate_memoize_as_nested_functions
    warning: not thread-safe, but as dict is a primitive type, worst
    case race condition would be that the underlying obj is called two
    times for the same args/kwargs, and the cache is updated serially
    with both (necessarily identical) values for the same key."""
    cache = obj.cache = {}
    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]
    return memoizer

_port_lock = threading.RLock()

def get_port():
    """Thread safely returns a round-robbed port in the range ``configuration['port_range']``"""
    with _port_lock:
        if not hasattr(get_port, "current"):
            get_port.current = random.randrange(configuration['port_range'][0], configuration['port_range'][1])
        else:
            get_port.current += 1
            if get_port.current >= configuration['port_range'][1]:
                get_port.current = configuration['port_range'][0]
        return get_port.current

def singleton_to_collection(arg):
    """Converts single object (including strings) to list of length one containing it"""
    if not hasattr(arg, "__iter__"):
        return [ arg ]
    else:
        return arg

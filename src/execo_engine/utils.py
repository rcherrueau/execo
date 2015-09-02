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

from subprocess import MAXFD
import os, unicodedata, re, sys, ctypes, signal

def _redirect_fd(fileno, filename):
    # create and open file filename, and redirect open file fileno to it
    f = os.open(filename, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0644)
    os.dup2(f, fileno)
    os.close(f)

def _disable_sigs(sigs):
    # Disable a list of signals with sigprocmask. This makes a bunch
    # of assumptions about various libc structures and as such, is
    # fragile and not portable. Code taken and modified from
    # http://stackoverflow.com/questions/3791398/how-to-stop-python-from-propagating-signals-to-subprocesses#3792294
    libc = ctypes.CDLL('libc.so.6')
    SIGSET_NWORDS = 1024 / (8 *  ctypes.sizeof(ctypes.c_ulong))
    class SIGSET(ctypes.Structure):
        _fields_ = [
            ('val', ctypes.c_ulong * SIGSET_NWORDS)
            ]
    sigset = (ctypes.c_ulong * SIGSET_NWORDS)()
    for sig in sigs:
        ulongindex = sig / ctypes.sizeof(ctypes.c_ulong)
        ulongoffset = sig % ctypes.sizeof(ctypes.c_ulong)
        sigset[ulongindex] |= 1 << (ulongoffset - 1)
    mask = SIGSET(sigset)
    SIG_BLOCK = 0
    libc.sigprocmask(SIG_BLOCK, ctypes.pointer(mask), 0)

def _tee_fd(fileno, filename):
    # create and open file filename, and duplicate open file fileno to it
    pr, pw = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.dup2(pr, 0)
        os.dup2(fileno, 1)
        if (os.sysconf_names.has_key("SC_OPEN_MAX")):
            maxfd = os.sysconf("SC_OPEN_MAX")
        else:
            maxfd = MAXFD
        # close all unused fd
        os.closerange(3, maxfd)
        # disable signals on tee to allow getting last outputs of engine.
        # tee will close anyway on broken pipe
        _disable_sigs([signal.SIGINT, signal.SIGTERM])
        os.execv('/usr/bin/tee', ['tee', '-a', filename])
    else:
        os.close(pr)
        os.dup2(pw, fileno)
        os.close(pw)

def redirect_outputs(stdout_filename, stderr_filename):
    """Redirects, and optionnaly merge, stdout and stderr to files"""
    sys.stdout.flush()
    sys.stderr.flush()
    _redirect_fd(1, stdout_filename)
    _redirect_fd(2, stderr_filename)
    # additionnaly force stdout unbuffered by reopening stdout
    # file descriptor with write mode
    # and 0 as the buffer size (unbuffered)
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

def copy_outputs(stdout_filename, stderr_filename):
    """Copy, and optionnaly merge, stdout and stderr to file(s)"""
    sys.stdout.flush()
    sys.stderr.flush()
    _tee_fd(1, stdout_filename)
    _tee_fd(2, stderr_filename)
    # additionnaly force stdout unbuffered by reopening stdout
    # file descriptor with write mode
    # and 0 as the buffer size (unbuffered)
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

def slugify(value):
    """
    Normalizes string representation, converts to lowercase, removes
    non-alpha characters, and converts spaces to hyphens.

    Intended to convert any object having a relevant string
    representation to a valid filename.

    more or less inspired / copy pasted from django (see
    http://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename-in-python)
    """
    value = unicode(str(value))
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
    value = unicode(re.sub('[-\s]+', '-', value))
    return value

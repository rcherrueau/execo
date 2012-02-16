# Copyright 2009-2012 INRIA Rhone-Alpes, Service Experimentation et
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
import logging
import optparse
import os
import sys
import time
import execo.log

"""execo run Engine template base."""

class ArgsOptionParser(optparse.OptionParser):

    """optparse.OptionParser subclass which keeps tracks of arguments for proper help string generation."""

    def __init__(self, *args, **kwargs):
        self.arguments = []
        optparse.OptionParser.__init__(self, *args, **kwargs)

    def add_argument(self, arg_name, description):
        """Add an arg_name to the arg_name list."""
        self.arguments.append((arg_name, description))

    def set_usage(self, usage):
        """Automatic generation of usage string with arguments."""
        if usage is None:
            self.usage = "%prog [options] <arguments>"
            if len(self.arguments) > 0:
                self.usage += "\narguments:\n"
                for (argument, description) in self.arguments:
                    self.usage += "  <%s> : %s\n" % (argument, description)
        elif usage is optparse.SUPPRESS_USAGE:
            self.usage = None
        else:
            self.usage = usage

def _redirect_fd(fileno, filename):
    # create and open file filename, and redirect open file fileno to it
    f = os.open(filename, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0644)
    os.dup2(f, fileno)
    os.close(f)

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
        os.closerange(3, maxfd)
        os.execv('/usr/bin/tee', ['tee', '-a', filename])
    else:
        os.close(pr)
        os.dup2(pw, fileno)
        os.close(pw)

class Engine(object):

    """Base class for execo Engine."""

    def create_result_dir(self):
        if not os.path.isdir(self.result_dir):
            os.makedirs(self.result_dir)

    def _redirect_outputs(self, merge_stdout_stderr):
        """Redirects, and optionnaly merge, stdout and stderr to file(s) in experiment directory."""

        self.create_result_dir()
        if merge_stdout_stderr:
            stdout_redir_filename = self.result_dir + "/stdout+stderr"
            stderr_redir_filename = self.result_dir + "/stdout+stderr"
            self.logger.info("redirect stdout / stderr to %s" % (stdout_redir_filename,))
        else:
            stdout_redir_filename = self.result_dir + "/stdout"
            stderr_redir_filename = self.result_dir + "/stderr"
            self.logger.info("redirect stdout / stderr to %s and %s" % (stdout_redir_filename, stderr_redir_filename))
        sys.stdout.flush()
        sys.stderr.flush()
        _redirect_fd(1, stdout_redir_filename)
        _redirect_fd(2, stderr_redir_filename)
        # additionnaly force stdout unbuffered by reopening stdout
        # file descriptor with write mode
        # and 0 as the buffer size (unbuffered)
        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

    def _copy_outputs(self, merge_stdout_stderr):
        """Copy, and optionnaly merge, stdout and stderr to file(s) in experiment directory."""

        self.create_result_dir()
        if merge_stdout_stderr:
            stdout_redir_filename = self.result_dir + "/stdout+stderr"
            stderr_redir_filename = self.result_dir + "/stdout+stderr"
        else:
            stdout_redir_filename = self.result_dir + "/stdout"
            stderr_redir_filename = self.result_dir + "/stderr"
        sys.stdout.flush()
        sys.stderr.flush()
        _tee_fd(1, stdout_redir_filename)
        _tee_fd(2, stderr_redir_filename)
        # additionnaly force stdout unbuffered by reopening stdout
        # file descriptor with write mode
        # and 0 as the buffer size (unbuffered)
        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
        if merge_stdout_stderr:
            self.logger.info("dup stdout / stderr to %s" % (stdout_redir_filename,))
        else:
            self.logger.info("dup stdout / stderr to %s and %s" % (stdout_redir_filename, stderr_redir_filename))
            
    def __init__(self):
        self.logger = logging.getLogger("execo." + self.__class__.__name__)
        # the experiment logger
        self.engine_dir = os.path.abspath(os.path.dirname(os.path.realpath(sys.modules[self.__module__].__file__)))
        # full path of the Engine directory
        self.options_parser = ArgsOptionParser()
        # command line option parser
        self.options_parser.add_option(
            "-l", dest = "log_level", type = "int", default = 20,
            help = "log level (10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL). Default = %default")
        self.options_parser.add_option(
            "-L", dest = "output_mode", action="store_const", const = "copy", default = False,
            help = "copy stdout / stderr to log files in the experiment result directory. Default = %default")
        self.options_parser.add_option(
            "-R", dest = "output_mode", action="store_const", const = "redirect", default = False,
            help = "redirect stdout / stderr to log files in the experiment result directory. Default = %default")
        self.options_parser.add_option(
            "-M", dest = "merge_outputs", action="store_true", default = False,
            help = "when copying or redirecting outputs, merge stdout / stderr in a single file. Default = %default")
        self.options_parser.add_option(
            "-c", dest = "continue_dir", default = None, metavar = "DIR",
            help = "continue experiment in DIR")
        self.options_parser.set_description(self.__class__.__name__)
        # default options parser configuration
        self.options = None
        # will contain the options given when the command line is parsed
        self.args = None
        # will contain the arguments given when the command line is parsed
        self.run_name = None
        # will contain the name of the current experiment run
        self.result_dir = None
        # will contain the full path of the current experiment run result directory

    def start(self):
        """Start the experiment.

        Properly init the experiment Engine instance, then pass the
        control to the overriden run() method of the requested
        experiment Engine.
        """
        del sys.argv[1]
        (self.options, self.args) = self.options_parser.parse_args()
        self.logger.setLevel(self.options.log_level)
        self.setup_run_name()
        if self.options.continue_dir:
            if not os.path.isdir(self.options.continue_dir):
                print >> sys.stderr, "ERROR: unable to find experiment dir %s" % (self.options.continue_dir,)
                exit(1)
            self.result_dir = self.options.continue_dir
        else:
            self.setup_result_dir()
        if self.options.output_mode:
            if self.options.output_mode == "copy":
                self._copy_outputs(self.options.merge_outputs)
            elif self.options.output_mode == "redirect":
                self._redirect_outputs(self.options.merge_outputs)
        if self.options.continue_dir:
            self.logger.info("continue experiment in %s" % (self.options.continue_dir,))
        self.run()

    # ------------------------------------------------------------------
    #
    # below: methods that inherited real experiment engines can
    # override
    #
    # ------------------------------------------------------------------

    def setup_run_name(self):
        """Set the experiment run name.

        Default implementation: concatenation of class name and date.
        """
        self.run_name = self.__class__.__name__ + "_" + time.strftime("%Y%m%d_%H%M%S_%z")

    def setup_result_dir(self):
        """Set the experiment run result directory name.

        Default implementation: subdirectory with the experiment run
        name in the current directory.
        """
        self.result_dir = os.path.abspath(self.run_name)

    def run(self):
        """Experiment run method

        Override this method with the experiment code. Default
        implementation does nothing.
        """
        pass

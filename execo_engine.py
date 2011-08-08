# Copyright 2009-2011 INRIA Rhone-Alpes, Service Experimentation et
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

"""execo run engine template base."""

import optparse, sys, time, logging, os

class MyOptionParser(optparse.OptionParser):

    def __init__(self,
                 usage = None,
                 option_list = None,
                 option_class = optparse.Option,
                 version = None,
                 conflict_handler = "error",
                 description = None,
                 formatter = None,
                 add_help_option = True,
                 prog = None,
                 epilog = None):
        optparse.OptionParser.__init__(self,
                                       usage = usage,
                                       option_list = option_list,
                                       option_class = option_class,
                                       version = version,
                                       conflict_handler = conflict_handler,
                                       description = description,
                                       formatter = formatter,
                                       add_help_option = add_help_option,
                                       prog = prog,
                                       epilog = epilog)
        self.arguments = []

    def add_argument(self, argument, description):
        self.arguments.append((argument, description))

    def format_help(self, formatter = None):
        if len(self.arguments) > 0:
            usage = self.get_usage()
            usage += "\narguments:\n"
            for (argument, description) in self.arguments:
                usage += "  <%s> : %s\n" % (argument, description)
            self.set_usage(usage)
        return optparse.OptionParser.format_help(self, formatter = formatter)

class execo_engine(object):

    """Base class for execo engine."""

    def __init__(self):
        self.logger = logging.getLogger("execo." + self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.engine_dir = os.path.abspath(os.path.dirname(sys.modules[self.__module__].__file__))
        self.options_parser = MyOptionParser()
        self.arguments = []
        self.run_name_suffix = None

    def redirect_outputs(self, merge_stdout_stderr):

        def redirect_fd(fileno, filename):
            f = os.open(filename, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0644)
            os.dup2(f, fileno)
            os.close(f)

        os.makedirs(self.result_dir)
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
        redirect_fd(1, stdout_redir_filename)
        redirect_fd(2, stderr_redir_filename)

    def copy_outputs(self, merge_stdout_stderr):

        def tee_fd(fileno, filename): 
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

        os.makedirs(self.result_dir)
        if merge_stdout_stderr:
            stdout_redir_filename = self.result_dir + "/stdout+stderr"
            stderr_redir_filename = self.result_dir + "/stdout+stderr"
        else:
            stdout_redir_filename = self.result_dir + "/stdout"
            stderr_redir_filename = self.result_dir + "/stderr"
        sys.stdout.flush()
        sys.stderr.flush()
        tee_fd(1, stdout_redir_filename)
        tee_fd(2, stderr_redir_filename)
        if merge_stdout_stderr:
            self.logger.info("dup stdout / stderr to %s" % (stdout_redir_filename,))
        else:
            self.logger.info("dup stdout / stderr to %s and %s" % (stdout_redir_filename, stderr_redir_filename))
            
    def start(self):
        self.configure_options_parser()
        self.parse_arguments()
        self.logger.setLevel(self.options.log_level)
        self.configure()
        self.setup_run_name()
        self.setup_result_dir()
        if self.options.output_mode:
            if self.options.output_mode == "copy":
                self.copy_outputs(self.options.merge_outputs)
            elif self.options.output_mode == "redirect":
                self.redirect_outputs(self.options.merge_outputs)
        self.run()

    def configure_options_parser(self):
        self.options_parser.add_option("-l", dest = "log_level", type = "int", default = 20,
                                       help ="log level (10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL). Default = %default")
        self.options_parser.add_option("-L", dest = "output_mode", action="store_const", const = "copy", default = False,
                                       help ="copy stdout / stderr to log files in the experiment result directory. Default = %default")
        self.options_parser.add_option("-R", dest = "output_mode", action="store_const", const = "redirect", default = False,
                                       help ="redirect stdout / stderr to log files in the experiment result directory. Default = %default")
        self.options_parser.add_option("-M", dest = "merge_outputs", action="store_true", default = False,
                                       help ="when copying or redirecting outputs, merge stdout / stderr in a single file. Default = %default")

    def parse_arguments(self):
        del sys.argv[1]
        (self.options, self.args) = self.options_parser.parse_args()
        
    def configure(self):
        pass

    def setup_run_name(self):
        self.run_name = self.__class__.__name__
        if self.run_name_suffix:
            self.run_name += "_%s" % (self.run_name_suffix,)
        self.run_name += "_" + time.strftime("%Y%m%d_%H%M%S_%z")

    def setup_result_dir(self):
        self.result_dir = os.path.abspath(self.run_name)

    def run(self):
        pass

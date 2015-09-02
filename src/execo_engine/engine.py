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

from log import logger
import optparse, os, sys, time, inspect, pipes
from utils import redirect_outputs, copy_outputs

_engineargs = sys.argv[1:]

class ArgsOptionParser(optparse.OptionParser):

    """optparse.OptionParser subclass which keeps tracks of arguments for proper help string generation.

    This class is a rather quick and dirty hack. Using ``argparse``
    would be better but it's only available in python 2.7.
    """

    def __init__(self, *args, **kwargs):
        self.arguments = []
        optparse.OptionParser.__init__(self, *args, **kwargs)

    def add_argument(self, arg_name, description):
        """Add an arg_name to the arg_name list."""
        self.arguments.append((arg_name, description))

    def num_arguments(self):
        """Returns the number of expected arguments."""
        return len(self.arguments)

    def format_arguments(self, formatter=None):
        if formatter is None:
            formatter = self.formatter
        result = []
        result.append(formatter.format_heading("Arguments"))
        formatter.indent()
        for argument in self.arguments:
            result.append(formatter._format_text("%s: %s" % (argument[0], argument[1])))
            result.append("\n")
        formatter.dedent()
        result.append("\n")
        return "".join(result)

    def format_help(self, formatter=None):
        if formatter is None:
            formatter = self.formatter
        result = []
        if self.usage:
            result.append(self.get_usage() + "\n")
        if self.description:
            result.append(self.format_description(formatter) + "\n")
        result.append(self.format_arguments(formatter))
        result.append(self.format_option_help(formatter))
        result.append(self.format_epilog(formatter))
        return "".join(result)

def run_meth_on_engine_ancestors(instance, method_name):
    engine_ancestors = [ cls for cls in inspect.getmro(instance.__class__) if issubclass(cls, Engine) ]
    for cls in engine_ancestors:
        meth = cls.__dict__.get(method_name)
        if meth is not None:
            meth(instance)

class Engine(object):

    """Basic class for execo Engine.

    Subclass it to develop your own engines, possibly reusable.

    This class offers basic facilities:

    - central handling of options and arguments

    - automatic experiment directory creation

    - various ways to handle stdout / stderr

    - support for continuing a previously stopped experiment

    - log level selection

    A subclass of Engine can access the following member variables
    which are automatically defined and initialized at the right time
    by the base class `execo_engine.engine.Engine`:

    - `execo_engine.engine.Engine.engine_dir`

    - `execo_engine.engine.Engine.result_dir`

    - `execo_engine.engine.Engine.options_parser`

    - `execo_engine.engine.Engine.options`

    - `execo_engine.engine.Engine.args`

    - `execo_engine.engine.Engine.run_name`

    - `execo_engine.engine.Engine.result_dir`

    A subclass of Engine can override the following methods:

    - `execo_engine.engine.Engine.init`

    - `execo_engine.engine.Engine.run`

    - `execo_engine.engine.Engine.setup_run_name`

    - `execo_engine.engine.Engine.setup_result_dir`

    A typical, non-reusable engine would override
    `execo_engine.engine.Engine.init`, adding options / arguments to
    `execo_engine.engine.Engine.options_parser`, and override
    `execo_engine.engine.Engine.run`, putting all the experiment code
    inside it, being sure that all initializations are done when
    ``run`` is called: results directory is initialized and created
    (possibly reusing a previous results directory, to restart from a
    previously stopped experiment), log level is set, stdout / stderr
    are redirected as needed, and options and arguments are in
    `execo_engine.engine.Engine.options` and
    `execo_engine.engine.Engine.args`.

    A typical usage of a `execo_engine.utils.ParamSweeper` in an
    engine would be to intialize one at the beggining of
    `execo_engine.engine.Engine.run`, using a persistence file in the
    results directory. example code::

     def run(self):
         sweeps = sweep({<parameters and their values>})
         sweeper = ParamSweeper(sweeps, os.path.join(self.result_dir, "sweeps"))
         [...]

    """

    def _create_result_dir(self):
        """Ensure the engine's result dir exists. Create it if needed."""
        try:
            os.makedirs(self.result_dir)
        except os.error:
            pass

    def __init__(self):
        self.engine_dir = None
        """Full path of the engine directory, if available. May not be
        available if engine code is run interactively.
        """
        mymodule = sys.modules[self.__module__]
        if hasattr(mymodule, "__file__"):
            self.engine_dir = os.path.abspath(os.path.dirname(os.path.realpath(mymodule.__file__)))
        self.options_parser = ArgsOptionParser(usage = "usage: <program> [options] <arguments>")
        """An instance of
        `execo_engine.engine.ArgsOptionParser`. Subclasses of
        `execo_engine.engine.Engine` can register options and args to
        this options parser in `execo_engine.engine.Engine.init`.
        """
        self.options_parser.add_option(
            "-l", dest = "log_level", default = None,
            help = "log level (int or string). Default = inherit execo logger level")
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
            "-c", dest = "use_dir", default = None, metavar = "DIR",
            help = "use experiment directory DIR")
        self.options_parser.set_description("engine: " + self.__class__.__name__)
        self.options = None
        """Options given on the command line. Available after the
        command line has been parsed, in
        `execo_engine.engine.Engine.run` (not in
        `execo_engine.engine.Engine.init`)
        """
        self.args = None
        """Arguments given on the command line. Available after the
        command line has been parsed, in
        `execo_engine.engine.Engine.run` (not in
        `execo_engine.engine.Engine.init`)
        """
        self.run_name = None
        """Name of the current experiment. If you want to modify it,
        override `execo_engine.engine.Engine.setup_run_name`
        """
        self.result_dir = None
        """full path to the current engine's execution results
        directory, where results should be written, where stdout /
        stderr are output, where `execo_engine.utils.ParamSweeper`
        persistence files should be written, and where more generally
        any file pertaining to a particular execution of the
        experiment should be located.
        """

    def start(self, args = _engineargs):
        """Start the engine.

        Properly initialize the experiment Engine instance, then call
        the init() method of all subclasses, then pass the control to
        the overridden run() method of the requested experiment
        Engine.
        """
        (self.options, self.args) = self.options_parser.parse_args(args = args)
        # _engineargs hack: to make running an engine from ipython
        # more convenient: if ipython is run from command line as
        #
        # $ ipython -i <enginefile.py> -- [engine options]
        #
        # iptyhon will hack sys.argv when loading enginefile, hiding
        # its own args, but then in the interactive shell, we get back
        # the real sys.argv, including ipython args, so if we
        # instanciate or start the engine interactively, we need to
        # parse the right args from sys.argv which is thus saved at
        # engine load time.
        if self.options.log_level != None:
            try:
                log_level = int(self.options.log_level)
            except:
                log_level = self.options.log_level
            logger.setLevel(log_level)
        if len(self.args) < self.options_parser.num_arguments():
            self.options_parser.print_help(sys.stderr)
            exit(1)
        self.setup_run_name()
        if self.options.use_dir:
            self.result_dir = self.options.use_dir
        else:
            self.setup_result_dir()
        self._create_result_dir()
        if self.options.output_mode:
            if self.options.merge_outputs:
                stdout_fname = self.result_dir + "/stdout+stderr"
                stderr_fname = self.result_dir + "/stdout+stderr"
            else:
                stdout_fname = self.result_dir + "/stdout"
                stderr_fname = self.result_dir + "/stderr"
            if self.options.output_mode == "copy":
                copy_outputs(stdout_fname, stderr_fname)
                logger.info("dup stdout / stderr to %s and %s", stdout_fname, stderr_fname)
            elif self.options.output_mode == "redirect":
                redirect_outputs(stdout_fname, stderr_fname)
                logger.info("redirect stdout / stderr to %s and %s", stdout_fname, stderr_fname)
        logger.info("command line arguments: %s" % (sys.argv,))
        logger.info("command line: " + " ".join([pipes.quote(arg) for arg in sys.argv]))
        logger.info("run in directory %s", self.result_dir)
        run_meth_on_engine_ancestors(self, "init")
        run_meth_on_engine_ancestors(self, "run")

    # ------------------------------------------------------------------
    #
    # below: methods that inherited real experiment engines can
    # override
    #
    # ------------------------------------------------------------------

    def setup_run_name(self):
        """Set the experiment run name.

        Default implementation: concatenation of class name and
        date. Override this method to change the name of the
        experiment. This method is called before
        `execo_engine.engine.Engine.run` and
        `execo_engine.engine.Engine.init`
        """
        self.run_name = self.__class__.__name__ + "_" + time.strftime("%Y%m%d_%H%M%S_%z")

    def setup_result_dir(self):
        """Set the experiment run result directory name.

        Default implementation: subdirectory with the experiment run
        name in the current directory. Override this method to change
        the name of the result directory. This method is called before
        `execo_engine.engine.Engine.run` and
        `execo_engine.engine.Engine.init`. Note that if option ``-c``
        is given to the engine, the name given on command line will
        take precedence, and this method won't be called.
        """
        self.result_dir = os.path.abspath(self.run_name)

    def init(self):
        """Experiment init method

        Override this method with the experiment init code. Default
        implementation does nothing.

        The base class `execo_engine.engine.Engine` takes care that
        all `execo_engine.engine.Engine.init` methods of its subclass
        hierarchy are called, in the order ancestor method before
        subclass method. This order is chosen so that generic engines
        inheriting from `execo_engine.engine.Engine` can easily
        implement common functionnalities. For example a generic
        engine can declare its own options and arguments in ``init``,
        which will be executed before a particular experiment subclass
        ``init`` method.
        """
        pass

    def run(self):
        """Experiment run method

        Override this method with the experiment code. Default
        implementation does nothing.

        The base class `execo_engine.engine.Engine` takes care that
        all `execo_engine.engine.Engine.run` methods of its subclass
        hierarchy are called, in the order ancestor method before
        subclass method. This order is chosen so that generic engines
        inheriting from `execo_engine.engine.Engine` can easily
        implement common functionnalities. For example a generic
        engine can prepare an experiment environment in its ``run``
        method, which will be executed before a particular experiment
        subclass ``run`` method.
        """
        pass

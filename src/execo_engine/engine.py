# Copyright 2009-2016 INRIA Rhone-Alpes, Service Experimentation et
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

from .log import logger
import os, sys, time, inspect, pipes
from .utils import redirect_outputs, copy_outputs
from argparse import ArgumentParser

_engineargs = sys.argv[1:]

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

    - `execo_engine.engine.Engine.args_parser`

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
    `execo_engine.engine.Engine.args_parser`, and override
    `execo_engine.engine.Engine.run`, putting all the experiment code
    inside it, being sure that all initializations are done when
    ``run`` is called: results directory is initialized and created
    (possibly reusing a previous results directory, to restart from a
    previously stopped experiment), log level is set, stdout / stderr
    are redirected as needed, and options and arguments are in
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
        self.args_parser = ArgumentParser(usage = "usage: <program> [options] <arguments>",
                                          description = "engine: " + self.__class__.__name__)
        """Subclasses of `execo_engine.engine.Engine` can register options and args to this options parser in `execo_engine.engine.Engine.init`."""
        self.args_parser.add_argument(
            "-l", dest = "log_level", default = None,
            help = "log level (int or string). Default = inherit execo logger level")
        self.args_parser.add_argument(
            "-L", dest = "output_mode", action="store_const", const = "copy", default = False,
            help = "copy stdout / stderr to log files in the experiment result directory. Default = %default")
        self.args_parser.add_argument(
            "-R", dest = "output_mode", action="store_const", const = "redirect", default = False,
            help = "redirect stdout / stderr to log files in the experiment result directory. Default = %default")
        self.args_parser.add_argument(
            "-M", dest = "merge_outputs", action="store_true", default = False,
            help = "when copying or redirecting outputs, merge stdout / stderr in a single file. Default = %default")
        self.args_parser.add_argument(
            "-c", dest = "use_dir", default = None, metavar = "DIR",
            help = "use experiment directory DIR")
        self.args = None
        """Arguments and options given on the command line. Available after
        the command line has been parsed, in
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

    def start(self, engineargs = _engineargs):
        """Start the engine.

        Properly initialize the experiment Engine instance, then call
        the init() method of all subclasses, then pass the control to
        the overridden run() method of the requested experiment
        Engine.
        """
        self.args = self.args_parser.parse_args(args = engineargs)
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
        if self.args.log_level != None:
            try:
                log_level = int(self.args.log_level)
            except:
                log_level = self.args.log_level
            logger.setLevel(log_level)
        if sys.version_info < (2, 7):
            if len(self.args) < self.args_parser.num_arguments():
                self.args_parser.print_help(sys.stderr)
                exit(1)
        self.setup_run_name()
        if self.args.use_dir:
            self.result_dir = self.args.use_dir
        else:
            self.setup_result_dir()
        self._create_result_dir()
        if self.args.output_mode:
            if self.args.merge_outputs:
                stdout_fname = self.result_dir + "/stdout+stderr"
                stderr_fname = self.result_dir + "/stdout+stderr"
            else:
                stdout_fname = self.result_dir + "/stdout"
                stderr_fname = self.result_dir + "/stderr"
            if self.args.output_mode == "copy":
                copy_outputs(stdout_fname, stderr_fname)
                logger.info("dup stdout / stderr to %s and %s", stdout_fname, stderr_fname)
            elif self.args.output_mode == "redirect":
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

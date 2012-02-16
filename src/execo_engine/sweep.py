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

from engine import Engine
from param_sweeper import ParamSweeper
import threading, os.path, json

class Sweep(Engine):
    
    # threadsafe and persistent iteration over parameter combinations 

    def __init__(self):
        super(Sweep, self).__init__()
        self.__lock = threading.Lock()
        self.param_combinations = None
        self.__param_file = None
        self.current_param = None
        self.__current_param_file = None
        self.parameters = {}

    def next_xp(self):
        with self.__lock:
            self.__current_param_file = os.path.join(self.result_dir, "sweep_current")
            if not self.param_combinations:
                self.create_result_dir()
                self.__param_file = os.path.join(self.result_dir, "sweep_parameters")
                # try loading state from previous run
                if os.path.isfile(self.__param_file) and os.path.isfile(self.__current_param_file):
                    with open(self.__param_file, "r") as param_file:
                        saved_parameters = json.load(param_file)
                    with open(self.__current_param_file, "r") as current_param_file:
                        self.current_param = json.load(current_param_file)
                    if json.dumps(saved_parameters) != json.dumps(self.parameters):
                        raise EnvironmentError, "parameters differ between xp runs: " \
                            "previous run params = %s current run params = %s" % (
                            saved_parameters, self.parameters)
                # save state for next runs
                else:
                    with open(self.__param_file, "w") as param_file:
                        json.dump(self.parameters, param_file)
                    self.current_param = 0
                print "parameters = %s" % (self.parameters,)
                self.param_combinations = list(ParamSweeper(self.parameters))
                print "param_combinations = %s" % (self.param_combinations,)
            if self.current_param < len(self.param_combinations):
                self.mark_current_xp_done()
                combination = self.param_combinations[self.current_param]
                self.current_param += 1
                return combination
            else:
                return None

    def mark_current_xp_done(self):
        with open(self.__current_param_file, "w") as current_param_file:
            json.dump(self.current_param, current_param_file)

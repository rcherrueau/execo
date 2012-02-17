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
import threading, os.path, cPickle

class Sweep(Engine):
    
    # threadsafe and persistent iteration over parameter combinations 

    def __init__(self):
        super(Sweep, self).__init__()
        self.__lock = threading.RLock()
        self.__combinations = None
        self.__done = None
        self.__remaining_iter = None
        self.__done_file = None
        self.parameters = {}

    def __update_remaining_iter(self):
        with self.__lock:
            remaining = self.__combinations.difference(self.__done)
            self.__remaining_iter = iter(remaining)

    def __post_init(self):
        with self.__lock:
            if not self.__combinations:
                self.__combinations = frozenset(ParamSweeper(self.parameters))
                self.create_result_dir()
                self.__done_file = os.path.join(self.result_dir, "sweep_done")
                if os.path.isfile(self.__done_file):
                    with open(self.__done_file, "r") as done_file:
                        self.__done = cPickle.load(done_file)
                else:
                    self.__done = set()
                self.__update_remaining_iter()

    def next_xp(self):
        with self.__lock:
            self.__post_init()
            try:
                return self.__remaining_iter.next()
            except StopIteration:
                return None

    def mark_xp_done(self, xp):
        with self.__lock:
            self.__post_init()
            self.__done.add(xp)
            self.__update_remaining_iter()
            with open(self.__done_file, "w") as done_file:
                cPickle.dump(self.__done, done_file)

    def num_xp_remaining(self):
        with self.__lock:
            return len(self.__combinations.difference(self.__done))

    def num_xp_total(self):
        with self.__lock:
            return len(self.__combinations)

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
from utils import ParamSweeper
import threading, os.path, cPickle

class Sweep(Engine):
    
    # threadsafe and persistent iteration over parameter combinations 

    def __init__(self):
        super(Sweep, self).__init__()
        self.__lock = threading.RLock()
        self.__done = set()
        self.__inprogress = set()
        self.__done_file = None

    def init(self):
        self.__done_file = os.path.join(self.result_dir, "sweep_done")
        if os.path.isfile(self.__done_file):
            with open(self.__done_file, "r") as done_file:
                self.__done = cPickle.load(done_file)

    def __remaining(self, parameters):
        return frozenset(ParamSweeper(parameters)).difference(self.__done).difference(self.__inprogress)

    def next_xp(self, parameters):
        with self.__lock:
            try:
                xp = iter(self.__remaining(parameters)).next()
                self.__inprogress.add(xp)
                self.logger.info("new xp: %s. %i remaining, %i in progress" % (xp, self.num_xp_remaining(parameters), len(self.__inprogress)))
                return xp
            except StopIteration:
                self.logger.info("no new xp. %i remaining, %i in progress" % (self.num_xp_remaining(parameters), len(self.__inprogress)))
                return None

    def xp_done(self, xp):
        with self.__lock:
            self.__done.add(xp)
            self.__inprogress.discard(xp)
            self.logger.info("xp done: %s" % (xp,))
            self.create_result_dir()
            with open(self.__done_file, "w") as done_file:
                cPickle.dump(self.__done, done_file)

    def reset_xp(self):
        with self.__lock:
            self.__inprogress.clear()

    def num_xp_remaining(self, parameters):
        with self.__lock:
            return len(self.__remaining(parameters))

    def num_xp_total(self, parameters):
        return len(ParamSweeper(parameters))

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

import itertools, threading, os, cPickle
from log import logger

class HashableDict(dict):

    """Hashable dictionnary. Beware: must not mutate it after its first use as a key."""

    def __key(self):
        return tuple((k,self[k]) for k in sorted(self))

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        return other and self.__key() == other.__key()

def sweeps(parameters):

    """Generates all combinations of parameters.
    
    Given a a dict associating parameters and the list of their
    values, will iterate over the cartesian product of all parameters
    combinations. In the given parameters, if instead of a list of
    values a dict is given, then will use the keys of the dict as
    possible values, and the values of the dict as parameters for a
    recursive sub-sweep.

    The returned list contains HashableDict instead of dict so that
    parameters combinations can be used as dict keys (but don't modify
    them in such cases)

    Examples:

    >>> sweeps({
    ...     "param 1": ["a", "b"],
    ...     "param 2": [1, 2]
    ...     })
    [{'param 1': 'a', 'param 2': 1}, {'param 1': 'a', 'param 2': 2}, {'param 1': 'b', 'param 2': 1}, {'param 1': 'b', 'param 2': 2}]

    >>> sweeps({
    ...     "param 1": ["a", "b"],
    ...     "param 2": {
    ...         1: {
    ...             "param 1 1": [ "x", "y" ],
    ...             "param 1 2": [ 0.0, 1.0 ]
    ...             },
    ...         2: {
    ...             "param 2 1": [ -10, 10 ]
    ...             }
    ...         }
    ...     })
    [{'param 1 2': 0.0, 'param 1 1': 'x', 'param 1': 'a', 'param 2': 1}, {'param 1 2': 0.0, 'param 1 1': 'y', 'param 1': 'a', 'param 2': 1}, {'param 1 2': 1.0, 'param 1 1': 'x', 'param 1': 'a', 'param 2': 1}, {'param 1 2': 1.0, 'param 1 1': 'y', 'param 1': 'a', 'param 2': 1}, {'param 2 1': -10, 'param 1': 'a', 'param 2': 2}, {'param 2 1': 10, 'param 1': 'a', 'param 2': 2}, {'param 1 2': 0.0, 'param 1 1': 'x', 'param 1': 'b', 'param 2': 1}, {'param 1 2': 0.0, 'param 1 1': 'y', 'param 1': 'b', 'param 2': 1}, {'param 1 2': 1.0, 'param 1 1': 'x', 'param 1': 'b', 'param 2': 1}, {'param 1 2': 1.0, 'param 1 1': 'y', 'param 1': 'b', 'param 2': 1}, {'param 2 1': -10, 'param 1': 'b', 'param 2': 2}, {'param 2 1': 10, 'param 1': 'b', 'param 2': 2}]
    """

    result = [HashableDict()]
    for key, val in parameters.items():
        newresult = []
        for i in result:
            if isinstance(val, dict):
                for subkey, subval in val.items():
                    for subcombs in sweeps(subval):
                        subresult = HashableDict(i)
                        subresult.update({key: subkey})
                        subresult.update(subcombs)
                        newresult.append(subresult)
            else:
                for j in val:
                    subresult = HashableDict(i)
                    subresult.update({key: j})
                    newresult.append(subresult)
        result = newresult
    return result

class ParamSweeper(object):

    """Threadsafe and persistent iteration over parameter combinations."""

    def __init__(self, persistence_file, name = None):
        self.__lock = threading.RLock()
        self.__done = set()
        self.__inprogress = set()
        self.__done_file = persistence_file
        self.__name = name
        if not self.__name:
            self.__name = os.path.basename(self.__done_file)
        if os.path.isfile(self.__done_file):
            with open(self.__done_file, "r") as done_file:
                self.__done = cPickle.load(done_file)

    def __remaining(self, parameters):
        return frozenset(sweeps(parameters)).difference(self.__done).difference(self.__inprogress)

    def get_next(self, parameters):
        with self.__lock:
            try:
                xp = iter(self.__remaining(parameters)).next()
                self.__inprogress.add(xp)
                logger.info(
                    "%s new xp: %s. %i remaining, %i in progress, %i total",
                        self.__name, xp,
                        self.num_remaining(parameters),
                        len(self.__inprogress),
                        len(list(sweeps(parameters))))
                return xp
            except StopIteration:
                logger.info(
                    "%s no new xp. %i remaining, %i in progress, %i total",
                        self.__name,
                        self.num_remaining(parameters),
                        len(self.__inprogress),
                        len(list(sweeps(parameters))))
                return None

    def done(self, xp):
        with self.__lock:
            self.__done.add(xp)
            self.__inprogress.discard(xp)
            logger.info("%s xp done: %s",
                self.__name, xp)
            with open(self.__done_file, "w") as done_file:
                cPickle.dump(self.__done, done_file)

    def reset(self):
        with self.__lock:
            logger.info("%s reset", self.__name)
            self.__inprogress.clear()

    def num_remaining(self, parameters):
        with self.__lock:
            return len(self.__remaining(parameters))

    def num_total(self, parameters):
        return len(ParamSweeper(parameters))

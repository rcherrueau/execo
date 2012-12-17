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

import threading, os, cPickle, fcntl
from log import logger

class HashableDict(dict):

    """Hashable dictionnary. Beware: must not mutate it after its first use as a key."""

    def __key(self):
        return tuple((k,self[k]) for k in sorted(self))

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        return other and self.__key() == other.__key()

def sweep(parameters):

    """Generates all combinations of parameters.

    The aim of this function is, given a list of experiment parameters
    (named factors), and for each parameter (factor), the list of
    their possible values (named levels), to generate the cartesian
    product of all parameter values (called a full factorial design in
    *The Art Of Computer Systems Performance Analysis, R. Jain, Wiley
    1991*).

    More formally: given a a dict associating factors as keys and the
    list of their possible levels as values, this function will return
    a list of dict corresponding to all cartesian product of all level
    combinations. Each dict in the returned list associates the
    factors as keys, and one of its possible levels as value.

    In the given factors dict, if for a factor X (key), the value
    associated is a dict instead of a list of levels, then it will use
    the keys of the sub-dict as levels for the factor X, and the
    values of the sub-dict must also be some dict for factor / levels
    combinations which will be explored only for the corresponding
    levels of factor X. This is kind of recursive sub-sweep and it
    allows to explore some factor / level combinations only for some
    levels of a given factor.

    The returned list contains `execo_engine.utils.HashableDict`
    instead of dict, which is a simple subclass of dict, so that
    parameters combinations can be used as dict keys (but don't modify
    them in such cases)

    Examples:

    >>> sweep({
    ...     "param 1": ["a", "b"],
    ...     "param 2": [1, 2]
    ...     })
    [{'param 1': 'a', 'param 2': 1}, {'param 1': 'a', 'param 2': 2}, {'param 1': 'b', 'param 2': 1}, {'param 1': 'b', 'param 2': 2}]

    >>> sweep({
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
        if len(val) == 0: continue
        newresult = []
        for i in result:
            if isinstance(val, dict):
                for subkey, subval in val.items():
                    for subcombs in sweep(subval):
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

# context manager for locking and getting the state of a ParamSweeper
class _ParamSweeperLockedState():

    def __init__(self, persistence_dir):
        self.__persistence_dir = persistence_dir

    def __enter__(self):
        self._done_file = open(os.path.join(self.__persistence_dir, "done"), "a+")
        fcntl.lockf(self._done_file, fcntl.LOCK_EX)
        try:
            self._done = cPickle.load(self._done_file)
            self._client_done = self._done.copy()
        except EOFError:
            self._done = None
            self._client_done = set()

        self._inprogress_file = open(os.path.join(self.__persistence_dir, "inprogress"), "a+")
        fcntl.lockf(self._inprogress_file, fcntl.LOCK_EX)
        try:
            self._inprogress = cPickle.load(self._inprogress_file)
            self._client_inprogress = self._inprogress.copy()
        except EOFError:
            self._inprogress = None
            self._client_inprogress = set()

        return (self._client_done, self._client_inprogress)

    def __exit__(self, t, v, traceback):
        if self._client_inprogress != self._inprogress:
            self._inprogress_file.truncate(0)
            cPickle.dump(self._client_inprogress, self._inprogress_file)
            self._inprogress_file.flush()
            os.fsync(self._inprogress_file.fileno())
        fcntl.lockf(self._inprogress_file, fcntl.LOCK_UN)
        self._inprogress_file.close()

        if self._client_done != self._done:
            self._done_file.truncate(0)
            cPickle.dump(self._client_done, self._done_file)
            self._done_file.flush()
            os.fsync(self._done_file.fileno())
        fcntl.lockf(self._done_file, fcntl.LOCK_UN)
        self._done_file.close()

class ParamSweeper(object):

    """Threadsafe and persistent iterable container to iterate over a list of experiment parameters (or whatever, actually).

    The aim of this class is to provide a convenient way to iterate
    over several experiment configurations (or anything else). It is
    an iterable container with the following characteristics:

    - each element of the iterable has three states:

      - *todo*

      - *done*

      - *skipped*

    - at beginning, each element is in state *todo*

    - client code can mark any element *done* or *skipped*

    - when iterating over it, you always get the next item in *todo*
      state

    - this container is threadsafe

    - this container has automatic persistence of the element states
      *done* (but not state *skipped*) to disk: If later you
      instanciate a container with the same persistence file (path to
      this file is given to constructor), then the elements of the
      container will be taken from the constructor argument, but those
      marked *done* in the last stored in the persistent file will be
      marked *done*

    This container is intended to be used in the following way: at the
    beginning of the experiment, you initialize a ParamSweeper with
    the list of experiment configurations (which can result from a
    call to `execo_engine.utils.sweep`, but not necessarily) and a
    filename for the persistence. During execution, you request
    (possibly from several threads) new experiment configurations with
    `execo_engine.utils.ParamSweeper.get_next`, mark them *done* or
    *skipped* with `execo_engine.utils.ParamSweeper.done` and
    `execo_engine.utils.ParamSweeper.skip`. At a later date, you can
    relaunch the same script, it will continue from where it left,
    also retrying the skipped configurations. This works well when
    used with `execo_engine.engine.Engine` startup option ``-c``
    (continue experiment in a given directory).
    """

    def __init__(self, sweeps, persistence_dir, name = None):
        """:param sweeps: what to iterate on

        :param persistence_dir: path to persistence directory
        """
        self.__lock = threading.RLock()
        self.__sweeps = sweeps
        self.__skipped = set()
        self.__my_inprogress = set()
        self.__cached_done = set()
        self.__cached_inprogress = set()
        self.__persistence_dir = persistence_dir
        self.__name = name
        if not self.__name:
            self.__name = os.path.basename(self.__persistence_dir)
        try:
            os.makedirs(self.__persistence_dir)
        except os.error:
            pass
        self.update()

    def update(self):
        with self.__lock:
            with _ParamSweeperLockedState(self.__persistence_dir) as (done, inprogress):
                inprogress.update(self.__my_inprogress)
                self.__cached_done = done
                self.__cached_inprogress = inprogress

    def __str__(self):
        with self.__lock:
            return "%s <%i total, %i done, %i skipped, %i in progress, %i remaining>" % (
                self.__name,
                self.num_total(),
                self.num_done(),
                self.num_skipped(),
                self.num_inprogress(),
                self.num_remaining())

    def set_sweeps(self, sweeps):
        """Change the list of what to iterate on"""
        with self.__lock:
            self.__sweeps = sweeps

    def get_next(self):
        """Return the next element which is *todo*. Returns None if reached end."""
        with self.__lock:
            try:
                with _ParamSweeperLockedState(self.__persistence_dir) as (done, inprogress):
                    inprogress.update(self.__my_inprogress)
                    remaining = frozenset(self.__sweeps).difference(done).difference(self.__skipped).difference(inprogress)
                    combination = iter(remaining).next()
                    inprogress.add(combination)
                    self.__my_inprogress.add(combination)
                    self.__cached_done = done
                    self.__cached_inprogress = inprogress
                logger.info("%s new combination: %s", self.__name, combination)
                logger.info(self)
                return combination
            except StopIteration:
                logger.info("%s no new combination", self.__name)
                logger.info(self)
                return None

    def done(self, combination):
        """mark the given element *done*"""
        with self.__lock:
            with _ParamSweeperLockedState(self.__persistence_dir) as (done, inprogress):
                inprogress.update(self.__my_inprogress)
                done.add(combination)
                inprogress.discard(combination)
                self.__my_inprogress.discard(combination)
                self.__cached_done = done
                self.__cached_inprogress = inprogress
            logger.info("%s combination done: %s", self.__name, combination)
            logger.info(self)

    def skip(self, combination):
        """mark the given element *skipped*"""
        with self.__lock:
            with _ParamSweeperLockedState(self.__persistence_dir) as (done, inprogress):
                inprogress.discard(combination)
                self.__my_inprogress.discard(combination)
                self.__skipped.add(combination)
                self.__cached_done = done
                self.__cached_inprogress = inprogress
            logger.info("%s combination skipped: %s", self.__name, combination)
            logger.info(self)

    RESET_NO_INPROGRESS=0
    RESET_MY_INPROGRESS=1
    RESET_ALL_INPROGRESS=2

    def reset(self, reset_inprogress = RESET_NO_INPROGRESS):
        """reset container: iteration will start from beginning, state *skipped* are forgotten, state *done* are **not** forgotten"""
        with self.__lock:
            self.__skipped.clear()
            with _ParamSweeperLockedState(self.__persistence_dir) as (done, inprogress):
                if reset_inprogress == RESET_MY_INPROGRESS:
                    inprogress.difference_update(self.__my_inprogress)
                    self.__my_inprogress.clear()
                elif reset_inprogress == RESET_ALL_INPROGRESS:
                    inprogress.clear()
                    self.__my_inprogress.clear()
                else:
                    inprogress.update(self.__my_inprogress)
                self.__cached_done = done
                self.__cached_inprogress = inprogress
            logger.info("%s reset", self.__name)
            logger.info(self)

    def num_total(self):
        """returns the total number of elements"""
        with self.__lock:
            return len(self.__sweeps)

    def num_skipped(self):
        """returns the current number of *skipped* elements"""
        with self.__lock:
            return len(self.__skipped)

    def num_remaining(self):
        """returns the current number of *todo* elements"""
        with self.__lock:
            return len(frozenset(self.__sweeps).difference(self.__cached_done).difference(self.__skipped).difference(self.__cached_inprogress))

    def num_inprogress(self):
        """returns the current number of elements which were obtained by a call to `execo_engine.utils.ParamSweeper.get_next`, not yet marked *done* or *skipped*"""
        with self.__lock:
            return len(self.__cached_inprogress)

    def num_done(self):
        """returns the current number of *done* elements"""
        with self.__lock:
            return self.num_total() - (self.num_remaining() + self.num_inprogress() + self.num_skipped())

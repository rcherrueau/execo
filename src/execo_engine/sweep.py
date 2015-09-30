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

import threading, os, fcntl, math
import cPickle as pickle
from log import logger

def geom(range_min, range_max, num_steps):
    """Return a geometric progression from range_min to range_max with num_steps"""
    if num_steps == 0: return []
    if num_steps == 1: return [float(range_min)]
    return ([ float(range_min) ]
            + [ float(range_min) * math.pow(math.pow(float(range_max) / float(range_min), 1.0 / (int(num_steps) - 1)), i)
                for i in range(0, int(num_steps)) ][1:-1]
            + [ float(range_max) ])

def igeom(range_min, range_max, num_steps):
    """Return an integer geometric progression from range_min to range_max with num_steps"""
    if num_steps == 0: return []
    if num_steps == 1: return [int(range_min)]
    return sorted(set([ int(range_min) ]
                      + [ int(round(x)) for x in geom(range_min, range_max, num_steps) ]
                      + [ int(range_max) ]))

class HashableDict(dict):

    """Hashable dictionnary. Beware: must not mutate it after its first use as a key."""

    def __key(self):
        return tuple((k,self[k]) for k in sorted(self))

    def __hash__(self):
        return hash(self.__key())

def sweep(parameters):

    """Generates all combinations of parameters.

    The aim of this function is, given a list of experiment parameters
    (named factors), and for each parameter (factor), the list of
    their possible values (named levels), to generate the cartesian
    product of all parameter values, for a full factorial experimental
    design (*The Art Of Computer Systems Performance Analysis,
    R. Jain, Wiley 1991*).

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

    The returned list contains `execo_engine.sweep.HashableDict`
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

# context manager for opening and locking files
# beware: for locking purpose, the file is always opened in mode "a+"
# which is the only mode allowing both locking the file and having
# read write access to it. but it forces to handle correctly file
# position and truncation
class _openlock():

    def __init__(self, filename):
        self.__filename = filename

    def __enter__(self):
        self.__file = open(self.__filename, "a+")
        fcntl.lockf(self.__file, fcntl.LOCK_EX)
        return self.__file

    def __exit__(self, t, v, traceback):
        self.__file.flush()
        os.fsync(self.__file.fileno())
        fcntl.lockf(self.__file, fcntl.LOCK_UN)
        self.__file.close()
        return False

class ParamSweeper(object):

    """Multi-process-safe, thread-safe and persistent iterable container to iterate over a list of experiment parameters (or whatever, actually).

    The aim of this class is to provide a convenient way to iterate
    over several experiment configurations (or anything else). It is
    an iterable container with the following characteristics:

    - each element of the iterable has four states:

      - *todo*

      - *inprogress*

      - *done*

      - *skipped*

    - at beginning, each element is in state *todo*

    - client code can mark any element *done* or *skipped*

    - when iterating over it, you always get the next item in *todo*
      state

    - this container has automatic persistence of the element states
      *done* and *inprogress* (but not state *skipped*) to disk: If
      later you instanciate a container with the same persistence
      directory (path to is given to constructor), then the elements
      of the container will be taken from the constructor argument,
      but states *done* or *inprogress* will be loaded from persistent
      state.

    - this container is thread-safe and multi-process-safe. Multiple
      threads can concurrently use a single ParamSweeper
      object. Multiple threads or processes on the same or different
      hosts can concurrently use several ParamSweeper instances
      sharing the same persistence directory. With sufficiently recent
      linux kernels and nfs servers / clients, it will work on a
      shared nfs storage (current implementation uses python flock,
      which should work since kernel 2.6.12. see
      http://nfs.sourceforge.net/#faq_d10). All threads sharing a
      ParamSweeper instance synchronize through in-process locks, and
      all threads / processes with different instances of ParamSweeper
      sharing the same persistent directory synchronize through the
      persisted state.

    This container is intended to be used in the following way: at the
    beginning of the experiment, you initialize a ParamSweeper with
    the list of experiment configurations (which can result from a
    call to `execo_engine.sweep.sweep`, but not necessarily) and a
    directory for the persistence. During execution, you request
    (possibly from several concurrent threads or processes) new
    experiment configurations with
    `execo_engine.sweep.ParamSweeper.get_next`, mark them *done* or
    *skipped* with `execo_engine.sweep.ParamSweeper.done` and
    `execo_engine.sweep.ParamSweeper.skip`. At a later date, you can
    relaunch the same script, it will continue from where it left,
    also retrying the skipped configurations. This works well when
    used with `execo_engine.engine.Engine` startup option ``-c``
    (continue experiment in a given directory).

    State *inprogress* is stored on disk to avoid concurrent processes
    to get the same elements from different ParamSweeper instances (to
    avoid duplicating work). In some cases (for example if a process
    has crashed without marking an element *done* or *skipped* or
    cancelling it), you may want to reset the *inprogress* state. This
    can be done by removing the file ``inprogress`` in the persistence
    directory (this can even be done while some ParamSweeper are
    instanciated and using it).

    ParamSweeper handle crashes in the following ways: if it crashes
    (or is killed) while synchronizing to disk, in the worst case, the
    current element marked done can be lost (i.e. other ParamSweepers
    or later instanciations will not see it marked done), or the whole
    list of inprogress elements can be lost.

    The ParamSweeper code assumes that in typical usage, there may be
    a huge number of elements to iterate on (and accordingly, the list
    of done elements will grow huge too), and that the number of
    inprogress elements will stay reasonably low. The whole iterable
    of elements is (optionnaly) written to disk only once, at
    ParamSweeper construction. The set of done elements can only grow
    and is incrementaly appended. The set of inprogress elements is
    fully read from and written to disk at each operation, thus this
    may become a bottleneck to ParamSweeper performance if the set of
    inprogress elements is big.
    """

    def __init__(self, persistence_dir, sweeps = None, save_sweeps = False, name = None):
        """
        :param persistence_dir: path to persistence directory. In this
          directory will be created to python pickle files: ``done``
          and ``inprogress`` This files can be erased if needed.

        :param sweeps: An iterable, what to iterate on. If None
          (default), try to load it from ``persistence_dir``

        :param save_sweeps: boolean. default False. If True, the
          sweeps are written to disk during initialization (this may
          take some time but occurs only once)

        :param name: a convenient name to identify an instance in
          logs. If None, compute one from persistence_dir.
        """
        self.__lock = threading.RLock()
        self.__persistence_dir = persistence_dir
        try:
            os.makedirs(self.__persistence_dir)
        except os.error:
            pass
        self.__name = name
        if not self.__name:
            self.__name = os.path.basename(self.__persistence_dir)

        self.__done = set()
        self.__inprogress = set()
        self.__skipped = set()

        self.__filtered_done = set()
        self.__filtered_inprogress = set()
        self.__filtered_skipped = set()
        # __filtered_done, __filtered_inprogress, __filtered_skipped
        # are the intersections of __sweeps and __done, __inprogress,
        # __skipped. They exist because:
        #
        # - client may call set_sweeps with different sweeps, still we
        #   want to remember the complete list of __done,
        #   __inprogress, __skipped
        #
        # - different ParamSweeper instances may share the same
        #   storage though having different individual
        #   __sweeps. __inprogress and __done on storage will be the
        #   union of all inprogress and done, so the ParamSweeper must
        #   be prepared to deal correctly with __inprogress and __done
        #   containing elements not in *its* __sweeps (and it must not
        #   discard them)
        #
        # - on the other hand, when displaying with __str__(),
        #   stats(), or when retrieving with get_done(),
        #   get_inprogress(), get_skipped(), client expects to get
        #   only lists of done, inprogress, skipped relative to the
        #   current __sweeps.
        #
        # - we could filter (do the intersection with __sweeps) only
        #   when displaying or returning the lists, but it's a costly
        #   operation to do the full intersection, whereas doing it
        #   incrementaly is fast.

        self.__remaining = set()
        self.__done_filepos = None

        self.set_sweeps(sweeps, save_sweeps)

    def set_sweeps(self, sweeps = None, save_sweeps = False):
        """Change the list of what to iterate on.

        :param sweeps: iterable

        :param save_sweeps: boolean. default False. If True, the
          sweeps are written to disk.
        """
        with self.__lock:
            if sweeps:
                self.__sweeps = set(sweeps)
                if save_sweeps:
                    with _openlock(os.path.join(self.__persistence_dir, "sweeps")) as sweeps_file:
                        sweeps_file.truncate(0)
                        pickle.dump(self.__sweeps, sweeps_file)
            else:
                with _openlock(os.path.join(self.__persistence_dir, "sweeps")) as sweeps_file:
                    sweeps_file.seek(0, os.SEEK_SET)
                    self.__sweeps = pickle.load(sweeps_file)
            self.full_update()

    def __nolock_full_update(self, done_file, inprogress_file):
        self.__done.clear()
        self.__done_filepos = 0
        done_file.seek(0, os.SEEK_SET)
        while True:
            try:
                self.__done.add(pickle.load(done_file))
                self.__done_filepos = done_file.tell()
            except:
                done_file.truncate(self.__done_filepos)
                break
        inprogress_file.seek(0, os.SEEK_SET)
        try:
            self.__inprogress = pickle.load(inprogress_file)
        except:
            inprogress_file.truncate(0)
            self.__inprogress.clear()
        self.__remaining = set(self.__sweeps).difference(self.__done, self.__skipped, self.__inprogress)
        self.__filtered_done = self.__done.intersection(self.__sweeps)
        self.__filtered_inprogress = self.__inprogress.intersection(self.__sweeps)
        self.__filtered_skipped = self.__skipped.intersection(self.__sweeps)

    def full_update(self):
        """Reload completely the ParamSweeper state from disk (may take some time)."""
        with self.__lock:
            with _openlock(os.path.join(self.__persistence_dir, "done")) as done_file:
                with _openlock(os.path.join(self.__persistence_dir, "inprogress")) as inprogress_file:
                    self.__nolock_full_update(done_file, inprogress_file)

    def __nolock_update(self, done_file, inprogress_file):
        done_file.seek(0, os.SEEK_END)
        new_done_filepos = done_file.tell()
        if new_done_filepos >= self.__done_filepos:
            if new_done_filepos > self.__done_filepos:
                done_file.seek(self.__done_filepos, os.SEEK_SET)
                new_done = list()
                while True:
                    try:
                        new_done.append(pickle.load(done_file))
                        self.__done_filepos = done_file.tell()
                    except:
                        done_file.truncate(self.__done_filepos)
                        break
                self.__done.update(new_done)
                self.__remaining.difference_update(new_done)
                self.__filtered_done.update(set(new_done).intersection(self.__sweeps))
            inprogress_file.seek(0, os.SEEK_SET)
            try:
                self.__inprogress = pickle.load(inprogress_file)
            except:
                inprogress_file.truncate(0)
                self.__inprogress.clear()
            self.__remaining.difference_update(self.__inprogress)
            self.__filtered_inprogress.intersection_update(self.__inprogress)
        else:
            self.__nolock_full_update(done_file, inprogress_file)

    def update(self):
        """Update incrementaly the ParamSweeper state from disk

        fast, except if done file has been truncated or deleted. In
        this case, will trigger a full_reload.
        """
        with self.__lock:
            with _openlock(os.path.join(self.__persistence_dir, "done")) as done_file:
                with _openlock(os.path.join(self.__persistence_dir, "inprogress")) as inprogress_file:
                    self.__nolock_update(done_file, inprogress_file)

    def __str__(self):
        with self.__lock:
            return "%s <%i total, %i done, %i skipped, %i in progress, %i remaining>" % (
                self.__name,
                len(self.__sweeps),
                len(self.__filtered_done),
                len(self.__filtered_skipped),
                len(self.__filtered_inprogress),
                len(self.__remaining))

    def get_sweeps(self):
        """Returns the iterable of what to iterate on"""
        return self.__sweeps.copy()

    def get_skipped(self):
        """returns an iterable of current *skipped* elements

        The returned iterable is a copy (safe to use without fearing
        concurrent mutations by another thread).
        """
        return self.__filtered_skipped.copy()

    def get_remaining(self):
        """returns an iterable of current remaining *todo* elements

        The returned iterable is a copy (safe to use without fearing
        concurrent mutations by another thread).
        """
        return self.__remaining.copy()

    def get_inprogress(self):
        """returns an iterable of elements currently processed (which were obtained by a call to `execo_engine.sweep.ParamSweeper.get_next`, not yet marked *done* or *skipped*)

        The returned iterable is a copy (safe to use without fearing
        concurrent mutations by another thread).
        """
        return self.__filtered_inprogress.copy()

    def get_done(self):
        """returns an iterable of currently *done* elements

        The returned iterable is a copy (safe to use without fearing
        concurrent mutations by another thread).
        """
        return self.__filtered_done.copy()

    def get_next(self, filtr = None):
        """Return the next element which is *todo*. Returns None if reached end.

        :param filtr: a filter function. If not None, this filter
          takes the iterable of remaining elements and returns a
          filtered iterable. It can be used to filter out some
          combinations and / or control the order of iteration.
        """
        with self.__lock:
            with _openlock(os.path.join(self.__persistence_dir, "done")) as done_file:
                with _openlock(os.path.join(self.__persistence_dir, "inprogress")) as inprogress_file:
                    self.__nolock_update(done_file, inprogress_file)
                    remaining = self.__remaining
                    if filtr:
                        remaining = filtr(remaining)
                    try:
                        combination = iter(remaining).next()
                    except StopIteration:
                        logger.trace("%s no new combination", self.__name)
                        logger.trace(self)
                        return None
                    self.__remaining.discard(combination)
                    self.__inprogress.add(combination)
                    self.__filtered_inprogress.add(combination)
                    inprogress_file.truncate(0)
                    pickle.dump(self.__inprogress, inprogress_file)
            logger.trace("%s new combination: %s", self.__name, combination)
            logger.trace(self)
            return combination

    def get_next_batch(self, num_combs, filtr = None):
        """Return the next elements which are *todo*.

        :param num_combs: how much combinations to get. An array of
          combinations is returned. The size of the array is <=
          num_combs and is limited by the number of available
          remaining combinations

        :param filtr: a filter function. If not None, this filter
          takes the iterable of remaining elements and returns a
          filtered iterable. It can be used to filter out some
          combinations and / or control the order of iteration.
        """
        combinations = []
        with self.__lock:
            with _openlock(os.path.join(self.__persistence_dir, "done")) as done_file:
                with _openlock(os.path.join(self.__persistence_dir, "inprogress")) as inprogress_file:
                    self.__nolock_update(done_file, inprogress_file)
                    remaining = self.__remaining
                    if filtr:
                        remaining = filtr(remaining)
                    try:
                        for i in remaining:
                            if num_combs <= 0:
                                break
                            combinations.append(i)
                            num_combs -= 1
                    except StopIteration:
                        logger.trace("%s no new combination", self.__name)
                        logger.trace(self)
                    self.__remaining.difference_update(combinations)
                    self.__inprogress.update(combinations)
                    self.__filtered_inprogress.update(combinations)
                    inprogress_file.truncate(0)
                    pickle.dump(self.__inprogress, inprogress_file)
            logger.trace("%s new combinations: %s", self.__name, combinations)
            logger.trace(self)
            return combinations

    def done(self, combination):
        """mark the given element *done*"""
        with self.__lock:
            with _openlock(os.path.join(self.__persistence_dir, "done")) as done_file:
                with _openlock(os.path.join(self.__persistence_dir, "inprogress")) as inprogress_file:
                    self.__nolock_update(done_file, inprogress_file)
                    self.__remaining.discard(combination)
                    self.__inprogress.discard(combination)
                    self.__filtered_inprogress.discard(combination)
                    self.__done.add(combination)
                    if combination in self.__sweeps:
                        self.__filtered_done.add(combination)
                    done_file.seek(0, os.SEEK_END)
                    pickle.dump(combination, done_file)
                    inprogress_file.truncate(0)
                    pickle.dump(self.__inprogress, inprogress_file)
            logger.trace("%s combination done: %s", self.__name, combination)
            logger.trace(self)

    def done_batch(self, combinations):
        """mark the given element(s) *done*"""
        with self.__lock:
            with _openlock(os.path.join(self.__persistence_dir, "done")) as done_file:
                with _openlock(os.path.join(self.__persistence_dir, "inprogress")) as inprogress_file:
                    self.__nolock_update(done_file, inprogress_file)
                    self.__remaining.difference_update(combinations)
                    self.__inprogress.difference_update(combinations)
                    self.__filtered_inprogress.difference_update(combinations)
                    self.__done.update(combinations)
                    filtered_combinations = set(combinations)
                    filtered_combinations.intersection_update(self.__sweeps)
                    self.__filtered_done.update(filtered_combinations)
                    done_file.seek(0, os.SEEK_END)
                    for combination in combinations:
                        pickle.dump(combination, done_file)
                    inprogress_file.truncate(0)
                    pickle.dump(self.__inprogress, inprogress_file)
            logger.trace("%s combinations done: %s", self.__name, combinations)
            logger.trace(self)

    def skip(self, combination):
        """mark the given element *skipped*"""
        with self.__lock:
            with _openlock(os.path.join(self.__persistence_dir, "done")) as done_file:
                with _openlock(os.path.join(self.__persistence_dir, "inprogress")) as inprogress_file:
                    self.__nolock_update(done_file, inprogress_file)
                    self.__skipped.add(combination)
                    if combination in self.__sweeps:
                        self.__filtered_skipped.add(combination)
                    self.__inprogress.discard(combination)
                    self.__filtered_inprogress.discard(combination)
                    inprogress_file.truncate(0)
                    pickle.dump(self.__inprogress, inprogress_file)
            logger.trace("%s combination skipped: %s", self.__name, combination)
            logger.trace(self)

    def skip_batch(self, combinations):
        """mark the given element(s) *skipped*"""
        with self.__lock:
            with _openlock(os.path.join(self.__persistence_dir, "done")) as done_file:
                with _openlock(os.path.join(self.__persistence_dir, "inprogress")) as inprogress_file:
                    self.__nolock_update(done_file, inprogress_file)
                    self.__skipped.update(combinations)
                    filtered_combinations = set(combinations)
                    filtered_combinations.intersection_update(self.__sweeps)
                    self.__filtered_skipped.update(filtered_combinations)
                    self.__inprogress.difference_update(combinations)
                    self.__filtered_inprogress.difference_update(combinations)
                    inprogress_file.truncate(0)
                    pickle.dump(self.__inprogress, inprogress_file)
            logger.trace("%s combinations skipped: %s", self.__name, combinations)
            logger.trace(self)

    def cancel(self, combination):
        """cancel processing of the given combination, but don't mark it as skipped, it comes back in the *todo* queue."""
        with self.__lock:
            with _openlock(os.path.join(self.__persistence_dir, "done")) as done_file:
                with _openlock(os.path.join(self.__persistence_dir, "inprogress")) as inprogress_file:
                    self.__nolock_update(done_file, inprogress_file)
                    if combination in self.__sweeps:
                        self.__remaining.add(combination)
                    self.__inprogress.discard(combination)
                    self.__filtered_inprogress.discard(combination)
                    inprogress_file.truncate(0)
                    pickle.dump(self.__inprogress, inprogress_file)
            logger.trace("%s combination cancelled: %s", self.__name, combination)
            logger.trace(self)

    def cancel_batch(self, combinations):
        """cancel processing of the given combination(s), but don't mark it/them as skipped, they comes back in the *todo* queue."""
        with self.__lock:
            with _openlock(os.path.join(self.__persistence_dir, "done")) as done_file:
                with _openlock(os.path.join(self.__persistence_dir, "inprogress")) as inprogress_file:
                    self.__nolock_update(done_file, inprogress_file)
                    filtered_combinations = set(combinations)
                    filtered_combinations.intersection_update(self.__sweeps)
                    self.__remaining.update(filtered_combinations)
                    self.__inprogress.difference_update(combinations)
                    self.__filtered_inprogress.difference_update(combinations)
                    inprogress_file.truncate(0)
                    pickle.dump(self.__inprogress, inprogress_file)
            logger.trace("%s combinations cancelled: %s", self.__name, combinations)
            logger.trace(self)

    def reset(self, reset_inprogress = False):
        """reset container: iteration will start from beginning, state *skipped* are forgotten, state *done* are **not** forgotten.

        :param reset_inprogress: default False. If True, state
          *inprogress* is also reset.
        """
        with self.__lock:
            with _openlock(os.path.join(self.__persistence_dir, "done")) as done_file:
                with _openlock(os.path.join(self.__persistence_dir, "inprogress")) as inprogress_file:
                    self.__nolock_update(done_file, inprogress_file)
                    self.__remaining.update(self.__filtered_skipped)
                    self.__skipped.clear()
                    self.__filtered_skipped.clear()
                    if reset_inprogress:
                        self.__inprogress.clear()
                        self.__filtered_inprogress.clear()
                        inprogress_file.truncate(0)
                        pickle.dump(self.__inprogress, inprogress_file)
            logger.trace("%s reset", self.__name)
            logger.trace(self)

    def stats(self):
        """Atomically return the tuple (sweeps, remaining, skipped, inprogress, done)"""
        with self.__lock:
            sweeps = self.get_sweeps()
            remaining = self.get_remaining()
            skipped = self.get_skipped()
            inprogress = self.get_inprogress()
            done = self.get_done()
            return (sweeps,
                    remaining,
                    skipped,
                    inprogress,
                    done)

def sweep_stats(stats):
    """taking stats tuple returned by `execo_engine.sweep.ParamSweeper.stats`, and if the ParamSweeper sweeps are in the format output by `execo_engine.sweep.sweep`, returns a dict detailing number and ratios of remaining, skipped, done, inprogress combinations per combination parameter value."""

    def count(combs):
        counts = dict()
        for comb in combs:
            for k in comb:
                if not counts.has_key(k):
                    counts[k] = dict()
                if not counts[k].has_key(comb[k]):
                    counts[k][comb[k]] = 0
                counts[k][comb[k]] += 1
        return counts

    (sweeps,
     remaining,
     skipped,
     inprogress,
     done) = stats
    ctotal = count(sweeps)
    cremaining = count(remaining)
    cskipped = count(skipped)
    cinprogress = count(inprogress)
    cdone = count(done)
    remaining_ratio = dict()
    skipped_ratio = dict()
    inprogress_ratio = dict()
    done_ratio = dict()
    for k1 in ctotal:
        remaining_ratio[k1] = dict()
        skipped_ratio[k1] = dict()
        inprogress_ratio[k1] = dict()
        done_ratio[k1] = dict()
        for k2 in ctotal[k1]:
            if cremaining.has_key(k1) and cremaining[k1].has_key(k2):
                r = cremaining[k1][k2]
            else:
                r = 0
            remaining_ratio[k1][k2] = float(r) / float(ctotal[k1][k2])
            if cskipped.has_key(k1) and cskipped[k1].has_key(k2):
                s = cskipped[k1][k2]
            else:
                s = 0
            skipped_ratio[k1][k2] = float(s) / float(ctotal[k1][k2])
            if cinprogress.has_key(k1) and cinprogress[k1].has_key(k2):
                i = cinprogress[k1][k2]
            else:
                i = 0
            inprogress_ratio[k1][k2] = float(i) / float(ctotal[k1][k2])
            if cdone.has_key(k1) and cdone[k1].has_key(k2):
                d = cdone[k1][k2]
            else:
                d = 0
            done_ratio[k1][k2] = float(d) / float(ctotal[k1][k2])
    return {
        "total": ctotal,
        "remaining": cremaining,
        "remaining_ratio": remaining_ratio,
        "skipped": cskipped,
        "skipped_ratio": skipped_ratio,
        "inprogress": cinprogress,
        "inprogress_ratio": inprogress_ratio,
        "done": cdone,
        "done_ratio": done_ratio
        }

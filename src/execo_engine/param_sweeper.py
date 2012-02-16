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

import itertools

class ParamSweeper(object):
    
    """Iterable over all possible combinations of parameters.
    
    Given a a directory associating parameters and the list of their
    values, will iterate over the cartesian product of all parameters
    combinations. For example:
    
    >>> ps = ParamSweeper({
    ...   "param1": [ 0, 1 ],
    ...   "param2": [ "a", "b", "c" ]
    ...   })
    >>> list(ps)
    [{'param2': 'a', 'param1': 0}, {'param2': 'a', 'param1': 1}, {'param2': 'b', 'param1': 0}, {'param2': 'b', 'param1': 1}, {'param2': 'c', 'param1': 0}, {'param2': 'c', 'param1': 1}]
    """

    def __init__(self, parameters):
        self.__parameters = parameters

    def __iter__(self):
        return ( dict(zip(self.__parameters.keys(), values)) for
                 values in itertools.product(*self.__parameters.values()) )

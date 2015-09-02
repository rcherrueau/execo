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

class ProcessesFailed(Exception):
    """Raised when one or more `execo.process.Process` have failed."""

    def __init__(self, processes):
        """:param processes: iterable of failed processes"""
        self.processes = processes
        """iterable of failed processes"""

    def __str__(self):
        s = "<ProcessesFailed> - failed process(es):\n"
        for p in self.processes:
            s += " " + p.dump() + "\n"
        return s

class ActionsFailed(Exception):
    """Raised when one or more `execo.action.Action` have failed."""

    def __init__(self, actions):
        """:param actions: iterable of failed actions"""
        self.actions = actions
        """iterable of failed actions"""

    def __str__(self):
        s = "<ActionsFailed> - failed action(s):\n"
        for a in self.actions:
            s += " " + str(a) + "\n"
        return s

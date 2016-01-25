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

import logging
import execo.log #IGNORE:W0611 #@PydevCodeAnalysisIgnore #@UnusedImport
__default_logger = logging.getLoggerClass()
logging.setLoggerClass(execo.log.Logger)
logger = logging.getLogger("execo.engine")
logging.setLoggerClass(__default_logger)

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

"""Tools and extensions to execo suitable for use in Grid5000."""

from config import g5k_configuration, default_frontend_connexion_params, \
  default_oarsh_oarcp_params, g5k_api_params
from oar import OarSubmission, oarsub, oardel, get_current_oar_jobs, \
  get_oar_job_info, wait_oar_job_start, get_oar_job_nodes, \
  get_oar_job_subnets
from oargrid import oargridsub, oargriddel, get_current_oargrid_jobs, \
  get_oargrid_job_info, wait_oargrid_job_start, get_oargrid_job_nodes
from kadeploy import Deployment, Kadeployer, kadeploy, deploy

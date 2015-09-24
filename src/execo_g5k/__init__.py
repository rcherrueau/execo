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

"""Execo extension and tools for Grid5000."""

from config import g5k_configuration, \
    default_frontend_connection_params, default_oarsh_oarcp_params

from oar import OarSubmission, oarsub, oardel, get_current_oar_jobs, \
    get_oar_job_info, wait_oar_job_start, get_oar_job_nodes, \
    get_oar_job_subnets, get_oar_job_kavlan, oarsubgrid

from oargrid import oargridsub, oargriddel, \
    get_current_oargrid_jobs, get_oargrid_job_info, \
    get_oargrid_job_oar_jobs, wait_oargrid_job_start, \
    get_oargrid_job_nodes, get_oargrid_job_key

from kadeploy import Deployment, Kadeployer, deploy

from utils import get_kavlan_host_name, G5kAutoPortForwarder

from planning import get_planning, compute_slots, find_first_slot, \
    find_max_slot, find_free_slot, get_jobs_specs, distribute_hosts, \
    g5k_charter_time

from api_utils import get_g5k_sites, get_site_clusters, \
    get_cluster_hosts, get_g5k_clusters, get_g5k_hosts, \
    get_cluster_site, APIConnection, APIGetException, get_host_site, \
    get_host_cluster, group_hosts, get_resource_attributes, \
    get_site_network_equipments, get_host_attributes, get_cluster_attributes, \
    get_site_attributes, get_network_equipment_attributes, canonical_host_name,\
    get_network_equipment_site, get_host_shortname, get_host_longname, \
    get_cluster_network_equipments, get_site_hosts, get_host_network_equipments

from charter import g5k_charter_time, get_next_charter_period

try:
    from charter import g5k_charter_remaining
except:
    pass # no psycopg2

try:
    from topology import g5k_graph, treemap
except:
    pass # no networkx

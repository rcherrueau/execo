# Copyright 2009-2013 INRIA Rhone-Alpes, Service Experimentation et
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


from pprint import pformat
from execo.log import set_style
from execo_g5k.api_utils import get_cluster_site, get_g5k_sites, get_host_attributes

from setup_cluster import VirshCluster, get_virt_clusters, get_big_clusters, get_kavlan_sites
from state import define_vms_params, create_disks, destroy_all, install
from migration import measurements_loop, twonodes_migrations, split_vm, crossed_migrations, split_merge_migrations

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

from config import default_frontend_connexion_params
from execo_g5k.config import g5k_configuration
import re
import socket

__default_frontend = None
__default_frontend_cached = False
def get_default_frontend():
    """Return the name of the default frontend."""
    global __default_frontend, __default_frontend_cached #IGNORE:W0603
    if not __default_frontend_cached:
        __default_frontend_cached = True
        if g5k_configuration.get("default_frontend"):
            __default_frontend = g5k_configuration["default_frontend"]
        else:
            try:
                localhost = socket.gethostname()
            except socket.error:
                localhost = ""
            mo = re.search("^[^ \t\n\r\f\v\.]+\.([^ \t\n\r\f\v\.]+)\.grid5000.fr$", localhost)
            if mo:
                __default_frontend = mo.group(1)
            else:
                __default_frontend = None
    return __default_frontend

def get_frontend_connexion_params(frontend_connexion_params):
    params = default_frontend_connexion_params.copy()
    if frontend_connexion_params:
        params.update(frontend_connexion_params)
    return params

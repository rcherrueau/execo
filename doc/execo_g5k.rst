****************
:mod:`execo_g5k`
****************

.. automodule:: execo_g5k

To use execo_g5k, your code must be run from grid5000. Passwordless,
public-key based authentification must be used (either with
appropriate public / private specific g5k keys shared on all your home
directories, or with an appropriate ssh-agent forwarding
configuration).

The code can be run from a frontend or from g5k nodes (in the latter
case, of course, you would explicitely refer to the local g5k by name,
and the node running the code needs to be able to connect to all
frontends)

OAR functions
=============

OarSubmission
-------------
.. autoclass:: OarSubmission
   :members:

oarsub
------
.. autofunction:: oarsub

oardel
------
.. autofunction:: oardel

get_current_oar_jobs
--------------------
.. autofunction:: get_current_oar_jobs

get_oar_job_info
----------------
.. autofunction:: get_oar_job_info

wait_oar_job_start
------------------
.. autofunction:: wait_oar_job_start

get_oar_job_nodes
-----------------
.. autofunction:: get_oar_job_nodes

OARGRID functions
=================

oargridsub
----------
.. autofunction:: oargridsub

oargriddel
----------
.. autofunction:: oargriddel

get_current_oargrid_jobs
------------------------
.. autofunction:: get_current_oargrid_jobs

get_oargrid_job_info
--------------------
.. autofunction:: get_oargrid_job_info

wait_oargrid_job_start
----------------------
.. autofunction:: wait_oargrid_job_start

get_oargrid_job_nodes
---------------------
.. autofunction:: get_oargrid_job_nodes

kadeploy3
=========

Deployment
----------
.. autoclass:: Deployment
   :members:

Kadeployer
----------
.. inheritance-diagram:: Kadeployer
.. autoclass:: Kadeployer
   :members:
   :show-inheritance:

kadeploy
--------
.. autofunction:: kadeploy

deploy
-------------------
.. autofunction:: deploy

Utilities
=========

format_oar_date
---------------
.. autofunction:: format_oar_date

format_oar_duration
-------------------
.. autofunction:: format_oar_duration

oar_date_to_unixts
------------------
.. autofunction:: oar_date_to_unixts

oar_duration_to_seconds
-----------------------
.. autofunction:: oar_duration_to_seconds

Configuration
=============

This module may be configured at import time by defining two dicts
`g5k_configuration` and `default_frontend_connexion_params` in the
file ``~/.execo.conf.py``

The `g5k_configuration` dict contains global g5k configuration
parameters.

.. autodata:: g5k_configuration

Its default values are:

.. literalinclude:: ../execo_g5k.py
   :start-after: # _STARTOF_ g5k_configuration
   :end-before: # _ENDOF_ g5k_configuration
   :language: python

The `default_frontend_connexion_params` dict contains default
parameters for remote connexions to grid5000 frontends.

.. autodata:: default_frontend_connexion_params

Its default values are:

.. literalinclude:: ../execo_g5k.py
   :start-after: # _STARTOF_ default_frontend_connexion_params
   :end-before: # _ENDOF_ default_frontend_connexion_params
   :language: python

in default_frontend_connexion_params, the ``host_rewrite_func``
configuration variable is set to automatically map a site name to its
corresponding frontend, so that all commands are run on the proper
frontends.

`default_oarsh_oarcp_params` contains default connexion parameters
suitable to connect to grid5000 nodes with oarsh / oarcp.

.. autodata:: default_oarsh_oarcp_params

Its default values are:

.. literalinclude:: ../execo_g5k.py
   :start-after: # _STARTOF_ default_oarsh_oarcp_params
   :end-before: # _ENDOF_ default_oarsh_oarcp_params
   :language: python

OAR keys
========

Oar/oargrid by default generate job specific ssh keys. So by default,
one has to retrieve these keys and explicitely use them for connecting
to the jobs, which is painfull. Another possibility is to tell
oar/oargrid to use specific keys. Oar can automatically use the key
pointed to by the environement variable ``OAR_JOB_KEY_FILE`` if it is
defined. Oargrid does not automatically do this, but execo takes care
of telling oargrid to use the key pointed to by ``OAR_JOB_KEY_FILE``
if it is defined. So the most convenient way to use execo/oar/oargrid,
is to set ``OAR_JOB_KEY_FILE`` in your ``~/.profile`` to point to your
internal Grid5000 ssh key and export this environment variable.

Running from another host than a frontend
=========================================

Note that when running a script from another host than a frontend,
everything will work except oarsh/oarcp connexions, since these
executables only exist on frontends. One may imagine a solution
through the setup of an ssh proxying and an alias, as described in the
next section, but i never tried it.

Running from outside Grid5000
=============================

Execo scripts can be run from outside grid5000 with a subtle
configuration of both execo and ssh.

First, in ``~/.ssh/config``, declare aliases for g5k connexion
(through the access machine). For example, here is an alias ``g5k``
for connecting through the lyon access::

 Host g5k lyon.g5k
   ProxyCommand ssh access.lyon.grid5000.fr "nc -q 0 frontend  %p"
   StrictHostKeyChecking no
 Host *.g5k
   ProxyCommand ssh access.lyon.grid5000.fr "nc -q 0 `basename %h .g5k` %p"
   StrictHostKeyChecking no

Then in ``~/.execo.conf.py`` put this code::

 import re
 def _rewrite_func(host):
     host = re.sub("\.grid5000\.fr$", "", host)
     if len(host) > 0:
         host += ".g5k"
     else:
         host = "g5k"
     return host

 default_connexion_params = {
     'host_rewrite_func': _rewrite_func
     }

 default_frontend_connexion_params = {
     'host_rewrite_func': _rewrite_func
     }

 g5k_api_params = {
     'username': '<username>',
     'password': '<password>',
     }

TODO: Putting the password in this file is very bad from a security
point of view, but it works. One may ``chmod 600 ~/.execo.conf.py``,
but it's only an illusion of security (nfs is easily compromised). The
solution would be to store the password in a keystore (gnome, kde?),
or asking it on the command line or in a dialog when needed.

Now, every time execo tries to connect to a host, the host name is
rewritten as to be reached through the Grid5000 ssh proxy connexion
alias, and the same for the frontends.

This won't work, though, for taktuk actions (because the first level
of the taktuk connexion tree will work, but not the lower levels) or
oarsh/oarcp connexions (because oarsh/oarcp are not installed outside
grid5000, or even if they were, you don't have the oarsh private keys,
only user oar on grid5000 frontends have it).

The perfect grid5000 connexion configuration
============================================

* use separate ssh keys for connecting from outside to grid5000 and
  inside grid5000:

  * use your regular ssh key for connecting from outside grid5000 to
    inside grid5000 by adding your regular public key to
    ``~/.ssh/authorized_keys`` on each site's nfs.

  * generate (with ``ssh-keygen``)a specific grid5000 private/public
    key pair, without passphrase, for navigating inside
    grid5000. replicate ``~/.ssh/id_dsa`` and ``~/.ssh/id_dsa.pub``
    (for a dsa key pair, or the equivalent rsa keys for an rsa key
    pair) to ``~/.ssh/`` on each site's nfs and add
    ``~/.ssh/id_dsa.pub`` to ``~/.ssh/authorized_keys`` on each site's
    nfs.

* add ``export OAR_JOB_KEY_FILE=~/.ssh/id_dsa`` (or id_rsa) to each
  site's ``~/.bash_profile``

* Connexions should then work directly with oarsh/oarcp if you use
  `default_oarsh_oarcp_params` connexion parameters. Connexions should
  work directly with ssh (for nodes reserved with the
  allow_classic_ssh option). For deployed nodes, connexions should
  work directly (option -k passed to kadeploy3 by default).

TODO: Currently, due to an ongoing bug or misconfiguration (see
https://www.grid5000.fr/cgi-bin/bugzilla3/show_bug.cgi?id=3302), oar
fails to access the ssh keys if they are not world-readable, so you
need to make them so.

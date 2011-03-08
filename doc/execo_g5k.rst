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

Its default values are the same as `default_connexion_params`

`default_oarsh_oarcp_params` contains default connexion parameters
suitable to connect to grid5000 nodes with oarsh / oarcp.

.. autodata:: default_oarsh_oarcp_params

Its default values are:

.. literalinclude:: ../execo_g5k.py
   :start-after: # _STARTOF_ default_oarsh_oarcp_params
   :end-before: # _ENDOF_ default_oarsh_oarcp_params
   :language: python

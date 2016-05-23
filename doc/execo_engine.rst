*******************
:mod:`execo_engine`
*******************

.. automodule:: execo_engine

Overview
========

The `execo_engine` module provides tools for the development of
experiments.

Parameter sweeping
==================

sweep
-----
.. autofunction:: execo_engine.sweep.sweep

ParamSweeper
------------
.. autoclass:: execo_engine.sweep.ParamSweeper
   :members:

sweep_stats
-----------
.. autofunction:: execo_engine.sweep.sweep_stats

geom
----
.. autofunction:: execo_engine.sweep.geom

igeom
-----
.. autofunction:: execo_engine.sweep.igeom


Engine
======

The `execo_engine.engine.Engine` class hierarchy is the base for
reusable experiment engines: The class `execo_engine.engine.Engine` is
the base class for classes which act as experiment engines.

Engine
------
.. autoclass:: execo_engine.engine.Engine
   :members:
   :show-inheritance:

Misc
====

slugify
-------
.. autofunction:: execo_engine.utils.slugify

logger
------
Default and convenient logger for engines.
Inherits its properties from the execo logger.

.. data:: execo_engine.log.logger


HashableDict
------------
.. autoclass:: execo_engine.sweep.HashableDict
   :members:

redirect_outputs
----------------
.. autofunction:: execo_engine.utils.redirect_outputs

copy_outputs
------------
.. autofunction:: execo_engine.utils.copy_outputs

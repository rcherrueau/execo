*******************
:mod:`execo_engine`
*******************

.. automodule:: execo_engine

Overview
========

The `execo_engine` module provides tools for the development of
experiment.

Parameter sweeping
==================

sweep
-----
.. autofunction:: execo_engine.utils.sweep

ParamSweeper
------------
.. autoclass:: execo_engine.utils.ParamSweeper
   :members:

Engine
======

The `execo_engine.engine.Engine` class hierarchy and ``execo-run``
command-line tool are the base for reusable experiment engines. The
`execo_engine.engine.Engine` class is the base class for classes which
act as experiment engines. ``execo_run`` is the launcher for those
experiment engines.

Engine
------
.. autoclass:: execo_engine.engine.Engine
   :members:
   :show-inheritance:

Misc
====

HashableDict
------------
.. autoclass:: execo_engine.utils.HashableDict
   :members:

ArgsOptionParser
----------------
.. autoclass:: execo_engine.engine.ArgsOptionParser
   :members:

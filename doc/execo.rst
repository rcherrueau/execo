************
:mod:`execo`
************

.. automodule:: execo

Overview
========

This module offers a high level API for parallel local or remote
processes execution with the `execo.action.Action` class hierarchy,
and a lower level API with the `execo.process.Process` class
hierarchy, for handling individual local or remote subprocesses.

Process class hierarchy
=======================

A `execo.process.Process` and its subclass `execo.process.SshProcess`
are abstractions of local or remote operating system process, similar
to the `subprocess.Popen` class in the python standard library
(actually execo `execo.process.Process` use them internally). The main
differences is that a background thread (which is started when execo
module is imported) takes care of reading asynchronously stdout and
stderr, and of handling the lifecycle of the process. This allows
writing easily code in a style appropriate for conducting several
processes in parallel.

You can automatically instanciate the right class
`execo.process.Process` or `execo.process.SshProcess` with the
"factory" function `execo.process.get_process`.

`execo.process.ProcessBase` and `execo.process.TaktukProcess` are used
internally and should probably not be used, unless for developing code
similar to `execo.action.TaktukRemote`.

get_process
-----------
.. autofunction:: execo.process.get_process

ProcessBase
-----------
.. inheritance-diagram:: execo.process.ProcessBase
.. autoclass:: execo.process.ProcessBase
   :members:
   :show-inheritance:

Process
-------
.. inheritance-diagram:: execo.process.Process
.. autoclass:: execo.process.Process
   :members:
   :show-inheritance:

SshProcess
----------
.. inheritance-diagram:: execo.process.SshProcess
.. autoclass:: execo.process.SshProcess
   :members:
   :show-inheritance:

TaktukProcess
-------------
.. inheritance-diagram:: execo.process.TaktukProcess
.. autoclass:: execo.process.TaktukProcess
   :members:
   :show-inheritance:

Serial
------
.. inheritance-diagram:: execo.process.Serial
.. autoclass:: execo.process.Serial
   :members:
   :show-inheritance:

SerialSsh
---------
.. inheritance-diagram:: execo.process.SerialSsh
.. autoclass:: execo.process.SerialSsh
   :members:
   :show-inheritance:

PortForwarder
-------------
.. inheritance-diagram:: execo.process.PortForwarder
.. autoclass:: execo.process.PortForwarder
   :members:
   :show-inheritance:

ProcessLifecycleHandler
-----------------------
.. autoclass:: execo.process.ProcessLifecycleHandler
   :members:
   :show-inheritance:

ProcessOutputHandler
--------------------
.. autoclass:: execo.process.ProcessOutputHandler
   :members:
   :show-inheritance:

ExpectOutputHandler
-------------------
.. autoclass:: execo.process.ExpectOutputHandler
   :members:
   :show-inheritance:

Action class hierarchy
======================

An `execo.action.Action` is an abstraction of a set of parallel
processes. It is an abstract class. Child classes are:
`execo.action.Remote`, `execo.action.TaktukRemote`,
`execo.action.Get`, `execo.action.Put`, `execo.action.TaktukGet`,
`execo.action.TaktukPut`, `execo.action.Local`. A
`execo.action.Remote` or `execo.action.TaktukRemote` is a remote
process execution on a group of hosts. The remote connection is
performed by ssh or a similar tool. `execo.action.Remote` uses as many
ssh connections as remote hosts, whereas `execo.action.TaktukRemote`
uses taktuk internally (http://taktuk.gforge.inria.fr/) to build a
communication tree, thus is more scalable. `Put` and `Get` are actions
for copying files or directories to or from groups of hosts. The copy
is performed with scp or a similar tool. `execo.action.TaktukPut` and
`execo.action.TaktukGet` also copy files or directories to or from
groups of hosts, using taktuk (see taktuk documentation about
limitations of using taktuk for file transfers). A
`execo.action.Local` is a local process (it is a very lightweight
`execo.action.Action` on top of a single `execo.Process.Process`
instance).

`execo.action.Remote`, `execo.action.TaktukRemote`,
`execo.action.Get`, `execo.action.TaktukGet`, `execo.action.Put`,
`execo.action.TaktukPut` require a list of remote hosts to perform
their tasks. These hosts are passed as an iterable of instances of
`execo.host.Host`. The `execo.host.Host` class instances have an
address, and may have a ssh port, a ssh keyfile, a ssh user, if
needed.

A configurable `execo.action.ActionFactory` can be created to choose
which kind of actions to instanciate, ``ssh``/``scp`` or
``taktuk``. Functions `execo.action.get_remote`,
`execo.action.get_fileput`, `execo.action.get_fileget` use the default
`execo.action.ActionFactory`: `execo.action.default_action_factory`.

As an example of the usage of the `execo.action.Remote` class, let's
launch some commands on a few remote hosts::

  a = Remote(cmd='whoami ; uname -a',
             hosts=(Host('nancy'), Host('Rennes'))).start()
  b = Remote(cmd='cd ~/project ; make clean ; make'
             hosts=(Host('lille'), Host('sophia'))).start()
  a.wait()
  # do something else....
  b.wait()

Action
------
.. inheritance-diagram:: execo.action.Action
.. autoclass:: execo.action.Action
   :members:
   :show-inheritance:

Remote
------
.. inheritance-diagram:: execo.action.Remote
.. autoclass:: execo.action.Remote
   :members:
   :show-inheritance:

TaktukRemote
------------
.. inheritance-diagram:: execo.action.TaktukRemote
.. autoclass:: execo.action.TaktukRemote
   :members:
   :show-inheritance:

RemoteSerial
------------
.. inheritance-diagram:: execo.action.RemoteSerial
.. autoclass:: execo.action.RemoteSerial
   :members:
   :show-inheritance:

Put
---
.. inheritance-diagram:: execo.action.Put
.. autoclass:: execo.action.Put
   :members:
   :show-inheritance:

TaktukPut
---------
.. inheritance-diagram:: execo.action.TaktukPut
.. autoclass:: execo.action.TaktukPut
   :members:
   :show-inheritance:

ChainPut
--------
.. inheritance-diagram:: execo.action.ChainPut
.. autoclass:: execo.action.ChainPut
   :members:
   :show-inheritance:

Get
---
.. inheritance-diagram:: execo.action.Get
.. autoclass:: execo.action.Get
   :members:
   :show-inheritance:

TaktukGet
---------
.. inheritance-diagram:: execo.action.TaktukGet
.. autoclass:: execo.action.TaktukGet
   :members:
   :show-inheritance:

Local
-----
.. inheritance-diagram:: execo.action.Local
.. autoclass:: execo.action.Local
   :members:
   :show-inheritance:

ParallelActions
---------------
.. inheritance-diagram:: execo.action.ParallelActions
.. autoclass:: execo.action.ParallelActions
   :members:
   :show-inheritance:

SequentialActions
-----------------
.. inheritance-diagram:: execo.action.SequentialActions
.. autoclass:: execo.action.SequentialActions
   :members:
   :show-inheritance:

ActionLifecycleHandler
----------------------
.. inheritance-diagram:: execo.action.ActionLifecycleHandler
.. autoclass:: execo.action.ActionLifecycleHandler
   :members:
   :show-inheritance:

wait_any_actions
---------------------
.. autofunction:: execo.action.wait_any_actions

wait_all_actions
----------------
.. autofunction:: execo.action.wait_all_actions

filter_bad_hosts
----------------
.. autofunction:: execo.action.filter_bad_hosts

ActionFactory
-------------
.. autoclass:: execo.action.ActionFactory
   :members:

get_remote
----------
.. autofunction:: execo.action.get_remote

get_fileput
-----------
.. autofunction:: execo.action.get_fileput

get_fileget
-----------
.. autofunction:: execo.action.get_fileget


.. _execo-substitutions:

substitutions for Remote, TaktukRemote, Get, Put
------------------------------------------------

In the command line given for a `execo.action.Remote`,
`execo.action.TaktukRemote`, as well as in pathes given to
`execo.action.Get` and `execo.action.Put`, some patterns are
automatically substituted:

- all occurences of the literal string ``{{{host}}}`` are substituted
  by the address of the `execo.host.Host` to which execo connects to.

- all occurences of ``{{<expression>}}`` are substituted in the
  following way: ``<expression>`` must be a python expression, which
  will be evaluated in the context (globals and locals) where the
  `execo.action.Remote`, `execo.action.Put`, `execo.action.Get` is
  instanciated, and which must return a sequence. ``{{<expression>}}``
  will be replaced by ``<expression>[index % len(<expression>)]``.

For example, in the following code::

  execo.Remote('iperf -c {{[host.address for host in hosts2]}}', hosts1)

When execo runs this command on all hosts of ``hosts1``, it will
produce a different command line for each host, each command line
connecting a host from ``hosts1`` to a host from ``hosts2``

if ``hosts1`` contains six hosts ``a``, ``b``, ``c``, ``d``, ``e``,
``f`` and hosts2 contains 3 hosts ``1``, ``2``, ``3`` the mapping
between hosts1 and hosts2 could be:

- ``a`` -> ``1``

- ``b`` -> ``2``

- ``c`` -> ``3``

- ``d`` -> ``1``

- ``e`` -> ``2``

- ``f`` -> ``3``

`execo.substitutions.remote_substitute` is the function used
internally by execo to perform these substitutions:

.. autofunction:: execo.substitutions.remote_substitute

Miscellaneous classes
=====================

Host
----
.. autoclass:: execo.host.Host
   :members:
   :show-inheritance:

Report
------
.. autoclass:: execo.report.Report
   :members:
   :show-inheritance:

Timer
-----
.. autoclass:: execo.time_utils.Timer
   :members:
   :show-inheritance:

Exceptions
==========
.. autoclass:: execo.exception.ProcessesFailed
   :members:
   :show-inheritance:

Utilities
=========

sleep
-----
.. autofunction:: execo.time_utils.sleep

get_unixts
----------
.. autofunction:: execo.time_utils.get_unixts

get_seconds
-----------
.. autofunction:: execo.time_utils.get_seconds

format_date
-----------
.. autofunction:: execo.time_utils.format_date

format_duration
---------------
.. autofunction:: execo.time_utils.format_duration

get_hosts_list
--------------
.. autofunction:: execo.host.get_hosts_list

get_hosts_set
-------------
.. autofunction:: execo.host.get_hosts_set

memoize
-------
.. autofunction:: execo.utils.memoize

get_port
--------
.. autofunction:: execo.utils.get_port

Logging
=======

execo uses the standard `logging` module for logging. By default, logs
are sent to stderr, logging level is `logging.WARNING`, so no logs
should be output when everything works correctly. Some logs will
appear if some processes don't exit with a zero return code (unless
they were created with flag ignore_non_zer_exit_code), or if some
processes timeout (unless they were created with flag ignore_timeout),
or if process instanciation resulted in a operating system error
(unless they were created with flag ignore_error).

logger
------
.. data:: execo.log.logger

  The execo logger.

.. _execo-configuration:

Configuration
=============

This module may be configured at import time by modifiing execo module
variables `execo.config.configuration`,
`execo.config.default_connection_params` or by defining two dicts
`configuration` and `default_connection_params` in the file
``~/.execo.conf.py``

configuration
-------------

The `configuration` dict contains global configuration parameters.

.. autodata:: execo.config.configuration

Its default values are:

.. literalinclude:: ../src/execo/config.py
   :start-after: # _STARTOF_ configuration
   :end-before: # _ENDOF_ configuration
   :language: python

default_connection_params
-------------------------

The `default_connection_params` dict contains default parameters for
remote connections.

.. autodata:: execo.config.default_connection_params

Its default values are:

.. literalinclude:: ../src/execo/config.py
   :start-after: # _STARTOF_ default_connection_params
   :end-before: # _ENDOF_ default_connection_params
   :language: python

These default connection parameters are the ones used when no other
specific connection parameters are given to `execo.process.SshProcess`,
`execo.action.Remote`, `execo.action.TaktukRemote`,
`execo.action.Get`, `execo.action.TaktukGet`, `execo.action.Put`,
`execo.action.TaktukPut`, or given to the `execo.host.Host`. When
connecting to a remote host, the connection parameters are first taken
from the `execo.host.Host` instance to which the connection is made,
then from the ``connection_params`` given to the
`execo.process.SshProcess` / `execo.action.TaktukRemote` /
`execo.action.Remote` / `execo.action.Get` / `execo.action.TaktukGet`
/ `execo.action.Put` / `execo.action.TaktukPut`, if there are some,
then from the `default_connection_params`, which has default values
which can be changed by directly modifying its global value, or in
``~/.execo.conf.py``

ssh/scp configuration for SshProcess, Remote, TaktukRemote, Get, TaktukGet, Put, TaktukPut
------------------------------------------------------------------------------------------

For `execo.process.SshProcess`, `execo.action.Remote`,
`execo.action.TaktukRemote`, `execo.action.Get`,
`execo.action.TaktukGet`, `execo.action.Put`, `execo.action.TaktukPut`
to work correctly, ssh/scp connections need to be fully automatic: No
password has to be asked. The default configuration in execo is to
force a passwordless, public key based authentification. As this tool
is growing in a cluster/grid environment where servers are frequently
redeployed, default configuration also disables strict key checking,
and the recording of hosts keys to ``~/.ssh/know_hosts``. This may be
a security hole in a different context.

Miscellaneous notes
===================

Time: timestamps and durations
------------------------------

Two notions are used in execo for dealing with time: the notion of a
date, also called a timestamp, which uniquely identifies a moment in
time, and the notion of duration. Internally all dates are unix
timestamps, ie. number of seconds elapsed since the unix epoch
(00:00:00 on Jan 1 1970), possibly with or without subsecond precision
(float or integer). All durations are in seconds, also possibly with
or without subsecond precision.

When passing parameters to execo api, all timestamps and durations can be
expressed in various formats (see `execo.time_utils.get_unixts`,
`execo.time_utils.get_seconds`) and will be automatically converted to
dates as unix timestamps, and durations in seconds.

All dates in execo are UTC. When giving a date to execo, you can give
it with an explicit timezone or if no timezone is given, execo will
assume that ``datetime.datetime`` or dates expressed as strings are in
local timezone. For timestamps given directly as unix timestamps, it
will assume they are in UTC.

If a developer wants to take advantage of the time facilities of
execo, the main functions are `execo.time_utils.get_unixts` for
properly converting the various timestamp formats to a unix timestamp
(with proper handling of timezone), and `execo.time_utils.get_seconds`
for properly converting the various duration formats to
seconds. `execo.time_utils.format_date` and
`execo.time_utils.format_duration` can be used to pretty-print a
timestamp or a duration, expressed in any of the format supported by
`execo.time_utils.get_unixts` or `execo.time_utils.get_seconds` (they
actually call them internally before pretty-printing).

Exceptions at shutdown
----------------------

Some exceptions may sometimes be triggered at python shutdown, with
the message ``most likely raised during interpreter shutdown``. They
are most likely caused by a bug in shutdown code's handling of threads
termination, and thus can be ignored. See
http://bugs.python.org/issue1856

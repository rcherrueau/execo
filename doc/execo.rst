************
:mod:`execo`
************

.. automodule:: execo

Overview
========

This module offers a high level API for parallel local or remote
processes execution with the `Action` class hierarchy, and a lower
level API with the `Process` class hierarchy, for handling individual
local or remote subprocesses.

Process class hierarchy
=======================

A `Process` and its subclass `SshProcess` are abstractions of local or
remote operating system process, similar to the `subprocess.Popen`
class in the python standard library (actually execo `Process` use
them internally). The main differences is that a background thread
(which is started when execo module is imported) takes care of reading
asynchronously stdout and stderr, and of handling the lifecycle of the
process. This allows writing easily code in a style appropriate for
conducting several processes in parallel.

`ProcessBase` and `TaktukProcess` are used internally and should
probably not be used, unless for developing code similar to
`TaktukRemote`.

ProcessBase
-----------
.. inheritance-diagram:: ProcessBase
.. autoclass:: ProcessBase
   :members:
   :show-inheritance:

Process
-------
.. inheritance-diagram:: Process
.. autoclass:: Process
   :members:
   :show-inheritance:

SshProcess
----------
.. inheritance-diagram:: SshProcess
.. autoclass:: SshProcess
   :members:
   :show-inheritance:

TaktukProcess
-------------
.. inheritance-diagram:: TaktukProcess
.. autoclass:: TaktukProcess
   :members:
   :show-inheritance:

ProcessLifecycleHandler
-----------------------
.. autoclass:: ProcessLifecycleHandler
   :members:
   :show-inheritance:

ProcessOutputHandler
--------------------
.. autoclass:: ProcessOutputHandler
   :members:
   :show-inheritance:

Action class hierarchy
======================

An `Action` is an abstraction of a set of parallel processes. It is an
abstract class. Child classes are: `Remote`, `TaktukRemote`, `Get`,
`Put`, `TaktukGet`, `TaktukPut`, `Local`. A `Remote` or `TaktukRemote`
is a remote process execution on a group of hosts. The remote
connexion is performed by ssh or a similar tool. `Remote` uses as many
ssh connexions as remote hosts, whereas `TaktukRemote` uses taktuk
internally (http://taktuk.gforge.inria.fr/) to build a communication
tree, thus is more scalable. `Put` and `Get` are actions for copying
files or directories to or from groups of hosts. The copy is performed
with scp or a similar tool. `TaktukPut` and `TaktukGet` also copy
files or directories to or from groups of hosts, using taktuk. A
`Local` is a local process (it is a very lightweight `Action` on top
of a single `Process` instance).

`Remote`, `TaktukRemote`, `Get`, `TaktukGet`, `Put`, `TaktukPut`
require a list of remote hosts to perform their tasks. These hosts are
passed as an iterable of instances of `Host`. The `Host` class
instances have an address, and may have a ssh port, a ssh keyfile, a
ssh user, if needed.

As an example of the usage of the `Remote` class, let's launch some
commands on a few remote hosts::

  a = Remote(hosts=(Host('nancy'), Host('Rennes')),
             cmd='whoami ; uname -a').start()
  b = Remote(hosts=(Host('lille'), Host('sophia')),
             cmd='cd ~/project ; make clean ; make').start()
  a.wait()
  # do something else....
  b.wait()

Action
------
.. inheritance-diagram:: Action
.. autoclass:: Action
   :members:
   :show-inheritance:

Remote
------
.. inheritance-diagram:: Remote
.. autoclass:: Remote
   :members:
   :show-inheritance:

TaktukRemote
------------
.. inheritance-diagram:: TaktukRemote
.. autoclass:: TaktukRemote
   :members:
   :show-inheritance:

Put
---
.. inheritance-diagram:: Put
.. autoclass:: Put
   :members:
   :show-inheritance:

TaktukPut
---------
.. inheritance-diagram:: TaktukPut
.. autoclass:: TaktukPut
   :members:
   :show-inheritance:

Get
---
.. inheritance-diagram:: Get
.. autoclass:: Get
   :members:
   :show-inheritance:

TaktukGet
---------
.. inheritance-diagram:: TaktukGet
.. autoclass:: TaktukGet
   :members:
   :show-inheritance:

Local
-----
.. inheritance-diagram:: Local
.. autoclass:: Local
   :members:
   :show-inheritance:

ParallelActions
---------------
.. inheritance-diagram:: ParallelActions
.. autoclass:: ParallelActions
   :members:
   :show-inheritance:

SequentialActions
-----------------
.. inheritance-diagram:: SequentialActions
.. autoclass:: SequentialActions
   :members:
   :show-inheritance:

ActionLifecycleHandler
----------------------
.. inheritance-diagram:: ActionLifecycleHandler
.. autoclass:: ActionLifecycleHandler
   :members:
   :show-inheritance:

wait_multiple_actions
---------------------
.. autofunction:: wait_multiple_actions

wait_all_actions
----------------
.. autofunction:: wait_all_actions

substitutions for Remote, TaktukRemote, Get, Put
------------------------------------------------

In the command line given for a `Remote`, `TaktukRemote`, as well as
in pathes given to `Get` and `Put`, some patterns are automatically
substituted:

- all occurences of the literal string ``{{{host}}}`` are substituted
  by the address of the `Host` to which execo connects to.

- all occurences of ``{{<expression>}}`` are substituted in the
  following way: ``<expression>`` must be a python expression, which
  will be evaluated in the context (globals and locals) where the
  `Remote`, `Put`, `Get` is instanciated, and which must return a
  sequence. ``{{<expression>}}`` will be replaced by
  ``<expression>[index % len(<expression>)]``.

For example, in the following code::

  execo.Remote(hosts1, 'iperf -c {{[host.address for host in hosts2]}}')

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

`remote_substitute` is the function used internally by execo to
perform these substitutions:

.. autofunction:: remote_substitute

Miscellaneous classes
=====================

Host
----
.. autoclass:: Host
   :members:
   :show-inheritance:

Report
------
.. autoclass:: Report
   :members:
   :show-inheritance:

Timer
-----
.. autoclass:: Timer
   :members:
   :show-inheritance:

Utilities
=========

sleep
-----
.. autofunction:: sleep

get_unixts
----------
.. autofunction:: get_unixts

get_seconds
-----------
.. autofunction:: get_seconds

format_unixts
-------------
.. autofunction:: format_unixts

format_seconds
--------------
.. autofunction:: format_seconds

format_date
-----------
.. autofunction:: format_date

format_duration
---------------
.. autofunction:: format_duration

get_hosts_list
--------------
.. autofunction:: get_hosts_list

get_hosts_set
-------------
.. autofunction:: get_hosts_set

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
.. data:: logger

  The execo logger.

Configuration
=============

This module may be configured at import time by modifiing execo module
variables `configuration`, `default_connexion_params` or by defining
two dicts `configuration` and `default_connexion_params` in the file
``~/.execo.conf.py``

The `configuration` dict contains global configuration parameters.

.. autodata:: configuration

Its default values are:

.. literalinclude:: ../execo.py
   :start-after: # _STARTOF_ configuration
   :end-before: # _ENDOF_ configuration
   :language: python

The `default_connexion_params` dict contains default parameters for
remote connexions.

.. autodata:: default_connexion_params

Its default values are:

.. literalinclude:: ../execo.py
   :start-after: # _STARTOF_ default_connexion_params
   :end-before: # _ENDOF_ default_connexion_params
   :language: python

These default connexion parameters are the ones used when no other
specific connexion parameters are given to `SshProcess`, `Remote`,
`TaktukRemote`, `Get`, `TaktukGet`, `Put`, `TaktukPut`, or given to
the `Host`. When connecting to a remote host, the connexion parameters
are first taken from the `Host` instance to which the connexion is
made, then from the ``connexion_params`` given to the `SshProcess` /
`TaktukRemote` / `Remote` / `Get` / `TaktukGet` / `Put` / `TaktukPut`,
if there are some, then from the `default_connexion_params`, which has
default values which can be changed by directly modifying its global
value, or in ``~/.execo.conf.py``

ssh/scp configuration for SshProcess, Remote, TaktukRemote, Get, TaktukGet, Put, TaktukPut
------------------------------------------------------------------------------------------

For `SshProcess`, `Remote`, `TaktukRemote`, `Get`, `TaktukGet`, `Put`,
`TaktukPut` to work correctly, ssh/scp connexions need to be fully
automatic: No password has to be asked. The default configuration in
execo is to force a passwordless, public key based
authentification. As this tool is growing in a cluster/grid
environment where servers are frequently redeployed, default
configuration also disables strict key checking, and the recording of
hosts keys to ``~/.ssh/know_hosts``. This may be a security hole in a
different context.

Miscellaneous notes
===================

Timestamps
----------

Internally all dates are unix timestamps, ie. number of seconds
elapsed since the unix epoch (00:00:00 on Jan 1 1970), possibly with
or without subsecond precision (float or integer). All durations are
in seconds. When passing parameters to execo api, all dates and
duration can be given in various formats (see `get_unixts`,
`get_seconds`) and will be automatically converted to dates as unix
timestamps, and durations in seconds.

Exceptions at shutdown
----------------------

Some exceptions may sometimes be triggered at python shutdown, with
the message ``most likely raised during interpreter shutdown``. They
are most likely caused by a bug in shutdown code's handling of threads
termination, and thus can be ignored. See
http://bugs.python.org/issue1856

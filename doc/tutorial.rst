**************
execo tutorial
**************

Installation
============

Prerequisites: you need (debian package names, adapt for other
distributions): ``make``, ``python`` (>= 2.6), ``python-httplib2`` and
optionnaly ``python-keyring``. You also need ``ssh`` and optionnaly
``taktuk``.

To install execo: In this tutorial it is shown how to install execo in
subdirectory ``.local/`` of your home, allowing installation on
computers where you are not root or when you don't want to mix
manually installed packages with packages managed by your distribution
package manager::

 $ git clone git://scm.gforge.inria.fr/execo/execo.git
 $ cd execo
 $ make install PREFIX=$HOME/.local

To add this directory to python search path (i assume bash shell here,
adapt for other shells)::

 $ PYTHONHOMEPATH="$HOME/.local/"$(python -c "import sys,os; print os.sep.join(['lib', 'python' + sys.version[:3], 'site-packages'])")
 $ export PYTHONPATH="$PYTHONHOMEPATH${PYTHONPATH:+:${PYTHONPATH}}"

You can put these two lines in your ``~/.profile`` to have your
environment setup automatically in all shells.

Execo
=====

Core module. Handles launching of several operating system level
processes in parallel and controlling them *asynchronously*.  Handles
remote executions and file copies with ssh/scp and taktuk.

- Standalone processes: `execo.process.Process`, `execo.process.SshProcess`

- Parallel processes: `execo.action.Action`

Processes
---------

- `execo.process.Process`: abstraction of an operating system
  process. Fine grain asynchronous lifecycle handling:

  - start, wait, kill

  - stdout, stderr, error, pid, exit code

  - start date, end date

  - timeout handling

  - callbacks

  - shell; pty

- `execo.process.SshProcess`: Same thing but through ssh. Additional
  parameter: Host, ConnexionParams

  - Host: abstraction of a remote host: address, user, keyfile, port

  - ConnexionParams: connexion parameters, ssh options, ssh path,
    keyfile, port, user, etc.

Process example
---------------

On one host *host1* Start an `execo.process.SshProcess` *process_A*
running an iperf server, then wait 5 seconds, then on another host
*host2* start an `execo.process.SshProcess` *process_B* running an
iperf client, then wait wait for *process_B* termination, then kill
*process_A*::

 from execo import *
 process_A = SshProcess("./iperf -s", "host1")
 process_B = SshProcess("./iperf -c host1", "host2")
 process_A.start()
 sleep(1)
 process_B.start()
 process_B.wait()
 process_A.kill()

when running this example, the killing of process_A triggers a warning
log on the console, since this process terminates with a non-zero exit
code (it's normal since we have killed it). For situations where we
now that it's normal, we can instruct execo to ignore the exit code by
passing option ``ignore_exit_code = True`` to process instanciation.

Interactive usage
=================

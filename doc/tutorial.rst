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

Process examples
................

Local process
'''''''''''''

List all files in the root directory::

 from execo import *
 process = Process("ls /")
 process.run()
 print "process:\n%s" + str(process)
 print "process stdout:\n" + process.stdout()
 print "process stderr:\n" + process.stderr()

In this example, the ls process was directly spawned, not using a
subshell. We can ask for a subshell with option ``shell = True`` if we
need a full shell environment (for example to expand environment
variables or use pipes). For example, to find all files in /tmp
belonging to me::

 process = Process("find /tmp -user $USERNAME", shell = True).run()

In this example, a warning log was likely displayed, because if you
are not root, there are probably some directories in /tmp that find
could not visit (lack of permissions) and find does not return 0 in
this case. The default behavior of execo is to issue warning logs when
processes are in error, do not return 0, or timeout. If needed we can
instruct execo to ignore the exit code of specific processes by
passing option ``ignore_exit_code = True`` to process instanciation.

Remote process over ssh
'''''''''''''''''''''''

On one host *host1* Start an `execo.process.SshProcess` *process_A*
running an iperf server, then wait 5 seconds, then on another host
*host2* start an `execo.process.SshProcess` *process_B* running an
iperf client, then wait wait for *process_B* termination, then kill
*process_A*::

 from execo import *
 process_A = SshProcess("./iperf -s", "host1", ignore_exit_code = True)
 process_B = SshProcess("./iperf -c host1", "host2")
 process_A.start()
 sleep(1)
 process_B.start()
 process_B.wait()
 process_A.kill()

In this example we ignore the exit code of *process_A* because we know
we kill it at the end, so it always has a non-zero exit code

This example also shows the asynchronous control of processes: while a
process is running (the iperf server), the code can do something else
(run the iperf client), and later get back control of the first
process (waiting for it, or as in this example killing it).

Actions
-------

- `execo.action.Action`: abstraction of a set of parallel
  Process. Asynchronous lifecycle handling:

  - start, kill, wait

  - access to individual Process

  - callbacks

  - timeout

  - errors

- `execo.action.Local`: A set of parallel local Process

- `execo.action.Remote`: A set of parallel remote SshProcess

- `execo.action.TaktukRemote`: Same as Remote but using taktuk instead
  of plain ssh

- `execo.action.Put`, `execo.action.Get`: send files or get files in
  parallel to/from remote hosts

- `execo.action.TaktukPut`, `execo.action.TaktukGet`: same using
  taktuk

- `execo.action.Report`: aggregates the results of several Action and
  pretty-prints summary reports

Remote example
..............

Run iperf client and server simultaneously on two hosts, to generate
traffic in both directions::

 from execo import *
 hosts = [ "host1", "host2" ]
 targets = list(reversed(hosts))
 servers = Remote("./iperf -s", hosts, ignore_exit_code = True)
 clients = Remote("./iperf -c {{targets}}", hosts)
 servers.start()
 sleep(1)
 clients.run()
 servers.kill()
 print Report([ servers, clients ]).to_string()

Interactive usage
=================

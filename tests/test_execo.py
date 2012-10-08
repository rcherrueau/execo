# Copyright 2009-2011 INRIA Rhone-Alpes, Service Experimentation et
# Developpement
# This file is part of Execo, released under the GNU Lesser Public
# License, version 3 or later.

import execo, unittest, resource, shutil

workdir = '/tmp/execo.unittests.tmp'
test_hosts = [ execo.Host('graal.ens-lyon.fr'), execo.Host('g5k'), execo.Host('rennes.g5k') ]

class Test_functions(unittest.TestCase):

    def test_format_time(self):
        self.assertEqual(execo.format_date(1260000000), '2009-12-05_09:00:00_CET')

    def test_format_duration(self):
        self.assertEqual(execo.format_duration(126045), '1d11h0m45s')

    def test_sleep(self):
        timer = execo.Timer().start()
        execo.sleep(1)
        elapsed = timer.get_elapsed()
        self.assertAlmostEqual(elapsed, 1.0, 1)

class Test_Process(unittest.TestCase):

    def setUp(self):
        (soft, _) = resource.getrlimit(resource.RLIMIT_NOFILE)
        self.max_processes = int(soft/2) - 20
        # Process, by default, close stdin, so a Process consumes 2
        # file descriptors (stdout, stderr). Thus, we should be able
        # to open half the fd limit, minus a security threshold.

    def test_process_states(self):
        # basic test of Process state before and after being ran
        processes = [ execo.Process('echo test', timeout = 10) for _ in xrange(0, self.max_processes) ]
        for p in processes:
            self.assertEqual(p.cmd(), 'echo test', str(p))
            self.assertEqual(p.started(), False, str(p))
            self.assertEqual(p.start_date(), None, str(p))
            self.assertEqual(p.ended(), False, str(p))
            self.assertEqual(p.end_date(), None, str(p))
            self.assertEqual(p.pid(), None, str(p))
            self.assertEqual(p.error(), False, str(p))
            self.assertEqual(p.error_reason(), None, str(p))
            self.assertEqual(p.exit_code(), None, str(p))
            self.assertEqual(p.timeout(), 10, str(p))
            self.assertEqual(p.timeouted(), False, str(p))
            self.assertEqual(p.forced_kill(), False, str(p))
            self.assertEqual(p.stdout(), '', str(p))
            self.assertEqual(p.stderr(), '', str(p))
        map(execo.Process.start, processes)
        map(execo.Process.wait, processes)
        for p in processes:
            self.assertEqual(p.cmd(), 'echo test', str(p))
            self.assertEqual(p.started(), True, str(p))
            self.assertNotEqual(p.start_date(), None, str(p))
            self.assertEqual(p.ended(), True, str(p))
            self.assertNotEqual(p.end_date(), None, str(p))
            self.assertNotEqual(p.pid(), None, str(p))
            self.assertEqual(p.error(), False, str(p))
            self.assertEqual(p.error_reason(), None, str(p))
            self.assertEqual(p.exit_code(), 0, str(p))
            self.assertEqual(p.timeout(), 10, str(p))
            self.assertEqual(p.timeouted(), False, str(p))
            self.assertEqual(p.forced_kill(), False, str(p))
            self.assertEqual(p.stdout(), 'test\n', str(p))
            self.assertEqual(p.stderr(), '', str(p))

    def test_lots_of_processes(self):
        # run a lot of Process (close to the max possible, given file
        # descriptor limits) several times, to ensure previous file
        # descriptors resources are cleanly released
        for _ in xrange(0, 3):
            processes = None # ensure cleaning processes to avoid a
                             # not enough file descriptors error
            processes = [ execo.Process('echo start ; echo done') for _ in xrange(0, self.max_processes) ]
            map(execo.Process.start, processes)
            map(execo.Process.wait, processes)
            for p in processes:
                self.assertEqual(p.started(), True, str(p))
                self.assertEqual(p.ended(), True, str(p))
                self.assertEqual(p.error(), False, str(p))
                self.assertEqual(p.exit_code(), 0, str(p))
                self.assertEqual(p.stdout(), 'start\ndone\n', str(p))

    def test_timeouts(self):
        timeout = 5
        processes = [ execo.Process('echo start ; sleep 50 ; echo done',
                                    timeout = timeout,
                                    ignore_exit_code = True,
                                    ignore_timeout = True) for _ in xrange(0, self.max_processes) ]
        t = execo.Timer().start()
        map(execo.Process.start, processes)
        map(execo.Process.wait, processes)
        elapsed = t.get_elapsed()
        tolerance = 10 # launching self.max_processes takes some time
        self.failUnless(elapsed < timeout + tolerance, "timeout = %f, elapsed = %f" % (timeout, elapsed))
        for p in processes:
            self.assertEqual(p.started(), True, str(p))
            self.assertEqual(p.ended(), True, str(p))
            self.assertEqual(p.error(), False, str(p))
            self.assertEqual(p.timeouted(), True, str(p))
            self.assertEqual(p.stdout(), 'start\n', str(p))
        execo.sleep(execo.configuration['kill_timeout'] + 2)
        # to ensure that all references to processes are released
        processes = None

    def test_error(self):
        p = execo.Process('ceprogrammenexistepas',
                                shell = False,
                                ignore_exit_code = True,
                                ignore_error = True)
        p.run()
        self.assertEqual(p.started(), True, str(p))
        self.assertEqual(p.ended(), True, str(p))
        self.assertEqual(p.error(), True, str(p))
        self.assertEqual(p.error_reason().__repr__(), "OSError(2, 'No such file or directory')", str(p))
        # # this version of the test currently commented, it fails on my
        # # version of python due to http://bugs.python.org/issue5179
        # processes = [ execo.Process('ceprogrammenexistepas',
        #                             shell = False,
        #                             ignore_exit_code = True,
        #                             ignore_error = True) for i in xrange(0, self.max_processes / 2) ]
        # map(execo.Process.start, processes)
        # map(execo.Process.wait, processes)
        # for p in processes:
        #     self.assertEqual(p.started(), True, str(p))
        #     self.assertEqual(p.ended(), True, str(p))
        #     self.assertEqual(p.error(), True, str(p))
        #     self.assertEqual(p.error_reason().__repr__(), "OSError(2, 'No such file or directory')", str(p))
        # processes = None

class Test_Action(unittest.TestCase):

    def test_Remote(self):
        remote = execo.Remote('true', test_hosts).run()
        stats = remote.stats()
        self.assertEqual(stats['num_started'], len(test_hosts), str(remote))
        self.assertEqual(stats['num_ended'], len(test_hosts), str(remote))
        self.assertEqual(stats['num_ok'], len(test_hosts), str(remote))

    def test_Get(self):
        remote_filename = workdir + '/test_get'
        local_location = workdir + '/test_get/{{{host}}}'
        remote = execo.Remote('mkdir -p ' + workdir + ' ; echo {{{host}}} > ' + remote_filename, test_hosts).run()
        stats = remote.stats()
        self.assertEqual(stats['num_started'], len(test_hosts), str(remote))
        self.assertEqual(stats['num_ended'], len(test_hosts), str(remote))
        self.assertEqual(stats['num_ok'], len(test_hosts), str(remote))
        get = execo.Get(test_hosts, remote_files = (remote_filename,), local_location = local_location, create_dirs = True).run()
        stats = get.stats()
        self.assertEqual(stats['num_started'], len(test_hosts), str(get))
        self.assertEqual(stats['num_ended'], len(test_hosts), str(get))
        self.assertEqual(stats['num_ok'], len(test_hosts), str(get))
        for host in test_hosts:
            filename = workdir + '/test_get/' + host.address + '/test_get'
            f = open(filename, 'r')
            content = f.read()
            f.close()
            self.assertEqual(content, host.address + '\n')
        shutil.rmtree(workdir, True)
        remote = execo.Remote('rm -rf ' + remote_filename, test_hosts).run()
        stats = remote.stats()
        self.assertEqual(stats['num_started'], len(test_hosts), str(remote))
        self.assertEqual(stats['num_ended'], len(test_hosts), str(remote))
        self.assertEqual(stats['num_ok'], len(test_hosts), str(remote))

    def test_Put(self):
        local_filename = workdir + '/test_put'
        remote_location = workdir + '/test_put'
        local = execo.Local('mkdir -p ' + workdir + ' ; echo toto > ' + local_filename).run()
        stats = local.stats()
        self.assertEqual(stats['num_started'], 1, str(local))
        self.assertEqual(stats['num_ended'], 1, str(local))
        self.assertEqual(stats['num_ok'], 1, str(local))
        put = execo.Put(test_hosts, local_files = (local_filename,), remote_location = remote_location).run()
        stats = put.stats()
        self.assertEqual(stats['num_started'], len(test_hosts), str(put))
        self.assertEqual(stats['num_ended'], len(test_hosts), str(put))
        self.assertEqual(stats['num_ok'], len(test_hosts), str(put))
        remote = execo.Remote('if [ `<' + remote_location + '` == "toto" ] ; then true ; else false ; fi', test_hosts).run()
        stats = remote.stats()
        self.assertEqual(stats['num_started'], len(test_hosts), str(remote))
        self.assertEqual(stats['num_ended'], len(test_hosts), str(remote))
        self.assertEqual(stats['num_ok'], len(test_hosts), str(remote))
        remote = execo.Remote('rm ' + remote_location, test_hosts).run()
        stats = remote.stats()
        self.assertEqual(stats['num_started'], len(test_hosts), str(remote))
        self.assertEqual(stats['num_ended'], len(test_hosts), str(remote))
        self.assertEqual(stats['num_ok'], len(test_hosts), str(remote))
        shutil.rmtree(workdir, True)

if __name__ == '__main__':
    unittest.main()

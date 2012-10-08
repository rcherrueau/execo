# Copyright 2009-2011 INRIA Rhone-Alpes, Service Experimentation et
# Developpement
# This file is part of Execo, released under the GNU Lesser Public
# License, version 3 or later.

import execo, execo_g5k, unittest

class Test_execo_g5k(unittest.TestCase):

    def setUp(self):
        self.oarjobs = execo_g5k.get_current_oar_jobs(execo_g5k.api_utils.get_g5k_sites(), local=False)
        self.oargridjobs = execo_g5k.get_current_oargrid_jobs()
        if len(self.oarjobs) == 0 and len(self.oargridjobs) == 0:
            raise Exception, "no job to use"

    def test_get_oar_job_start_time(self):
        if len(self.oarjobs) == 0:
            raise Exception, "no job to use"
        for (job, site) in self.oarjobs:
            start = execo_g5k.get_oar_job_info(job, site)['start_date']
            print "oar job %i in %s: start at %s" % (job, site, execo.format_date(start))

    def test_get_oargrid_job_start_time(self):
        if len(self.oargridjobs) == 0:
            raise Exception, "no job to use"
        for job in self.oargridjobs:
            start = execo_g5k.get_oargrid_job_info(job)['start_date']
            print "oargrid job %i: start at %s" % (job, execo.format_date(start))

    def test_wait_oar_job_start(self):
        if len(self.oarjobs) == 0:
            raise Exception, "no job to use"
        for (job, site) in self.oarjobs:
            print "waiting for oar job %i in %s start..." % (job, site),
            execo_g5k.wait_oar_job_start(job, site)
            print "ok"
        
    def test_wait_oargrid_job_start(self):
        if len(self.oargridjobs) == 0:
            raise Exception, "no job to use"
        for job in self.oargridjobs:
            print "waiting for oargrid job %i start..." % (job,),
            execo_g5k.wait_oargrid_job_start(job)
            print "ok"

    def test_get_oar_job_nodes(self):
        if len(self.oarjobs) == 0:
            raise Exception, "no job to use"
        for (job, site) in self.oarjobs:
            nodes = execo_g5k.get_oar_job_nodes(job, site)
            print "oar job %i in %s has nodes: %s" % (job, site, nodes)

    def test_get_oargrid_job_nodes(self):
        if len(self.oargridjobs) == 0:
            raise Exception, "no job to use"
        for job in self.oargridjobs:
            nodes = execo_g5k.get_oargrid_job_nodes(job)
            print "oargrid job %i has nodes: %s" % (job, nodes)

if __name__ == '__main__':
    unittest.main()

import execo, execo_g5k, unittest
import simplejson # http://pypi.python.org/pypi/simplejson/
import restclient # http://pypi.python.org/pypi/py-restclient/1.2.2

# helpers functions
def links_by_title(links):
  index = {}
  for link in links:
    # index by title if present, else by rel attribute
    if 'title' in link.keys():
      index[link['title']] = link
    else:
      index[link['rel']] = link
  return index

def get_g5k_sites():
    api = restclient.Resource('https://api.grid5000.fr', transport=restclient.transport.HTTPLib2Transport())
    grid5000 = simplejson.loads( api.get('/sid/grid5000', headers={'Accept': 'application/json'}) )
    sites_link = links_by_title(grid5000['links'])['sites']['href']
    sites = simplejson.loads( api.get(sites_link, headers={'Accept': 'application/json'}, version=grid5000['version']) )
    return [site['uid'] for site in sites['items']]

g5k_sites = get_g5k_sites()

class Test_execo_g5k(unittest.TestCase):

    def setUp(self):
        self.oarjobs = execo_g5k.get_current_oar_jobs(g5k_sites, local=False)
        self.oargridjobs = execo_g5k.get_current_oargrid_jobs()
        if len(self.oarjobs) == 0 and len(self.oargridjobs) == 0:
            raise Exception, "no job to use"

    def test_get_oar_job_start_time(self):
        if len(self.oarjobs) == 0:
            raise Exception, "no job to use"
        for (job, site) in self.oarjobs:
            start = execo_g5k.get_oar_job_start_time(job, site)
            print "oar job %i in %s: start at %s" % (job, site, execo.format_time(start))

    def test_get_oargrid_job_start_time(self):
        if len(self.oargridjobs) == 0:
            raise Exception, "no job to use"
        for job in self.oargridjobs:
            start = execo_g5k.get_oargrid_job_start_time(job)
            print "oargrid job %i: start at %s" % (job, execo.format_time(start))

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

    def test_prepare_xp(self):
        (deployed_hosts, undeployed_hosts, hosts) = execo_g5k.prepare_xp(oar_job_id_tuples = self.oarjobs, oargrid_job_ids = self.oargridjobs, check_deployed_command = 'true', num_deploy_retries = 10)
        print "%i deployed hosts:    %s" % (len(deployed_hosts), deployed_hosts)
        print "%i undeployed hosts:  %s" % (len(undeployed_hosts), undeployed_hosts)
        print "%i total hosts:       %s" % (len(hosts), hosts)
        self.assertEqual(len(deployed_hosts) + len(undeployed_hosts), len(hosts))

if __name__ == '__main__':
    unittest.main()

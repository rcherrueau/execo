import optparse, sys, threading, datetime
from execo_engine import execo_engine
import g5k_api_tools, execo_g5k

class bag_of_tasks_per_cluster(execo_engine):

    def __init__(self):
        super(bag_of_tasks_per_cluster, self).__init__()
        self.deploy = False
        self.deployment = None
        self.default_num_nodes = 2
        self.default_min_nodes = 1
        self.default_walltime = "1:0:0"

    def configure_options_parser(self):
        usage = "<arguments> = <space separated list of clusters>\n" \
                "   a cluster: <clustername>[@sitename]\n" \
                "   if sitename not given, will get it from g5k api"
        self.options_parser.set_usage(self.options_parser.get_usage() + usage)
        self.options_parser.add_option("-a", dest = "all_clusters", help = "run on all clusters. Default = %default", action = "store_true", default = False)
        self.options_parser.add_option("-q", dest = "oar_queue", help = "oar queue to use. Default = %default", default = None)
        self.options_parser.add_option("-n", dest = "num_nodes", help = "number of nodes per cluster to run. Default = %default", type = "int", default = self.default_num_nodes)
        self.options_parser.add_option("-m", dest = "min_nodes", help = "minimum number of nodes to get results from. Default = %default", type = "int", default = self.default_min_nodes)
        self.options_parser.add_option("-w", dest = "walltime", help = "walltime of jobs. Default = %default", type = "string", default = self.default_walltime)
        self.options_parser.add_option("-r", dest = "reservation_date", help = "reservation date. Default = %default", type = "string", default = None)

    def configure(self):
        self.clusters = dict()
        for arg in self.args:
            arobindex = arg.find("@")
            if arobindex == -1:
                cluster = arg
                site = g5k_api_tools.get_cluster_site(cluster)
            else:
                cluster = arg[0:arobindex]
                site = arg[arobindex+1:]
            self.clusters[cluster] = site
        if self.options.all_clusters:
            for cluster in g5k_api_tools.get_g5k_clusters():
                self.clusters[cluster] = g5k_api_tools.get_cluster_site(cluster)
        unknown_clusters = [ cluster for cluster in self.clusters.keys() if self.clusters[cluster] == None ]
        if len(unknown_clusters) > 0:
            raise Exception, "unknown clusters: %s" % (unknown_clusters,)

    def cluster_run(self, oarjob, site, cluster, nodes):
        pass

    def cluster_run_threadfunc(self, oarjob, site, cluster):
        def thread_log(arg):
            self.logger.info("thread %s@%s (oarjob %s): %s" % (cluster, site, oarjob, arg))
        thread_start_date = datetime.datetime.utcnow()
        oarjob_start_date = None
        try:
            thread_log("wait oar job start")
            execo_g5k.wait_oar_job_start(oarjob, site)
            thread_log("get oar job nodes")
            nodes = execo_g5k.get_oar_job_nodes(oarjob, site)
            thread_log("nodes = %s" % (nodes,))
            if len(nodes) < self.options.min_nodes:
                thread_log("aborting, not enough nodes")
                return
            oarjob_start_date = datetime.datetime.utcnow()
            if self.deploy and self.deployment != None:
                self.deployment.hosts = nodes
                thread_log("deploying nodes")
                execo_g5k.deploy(deployment)
            thread_log("start")
            threading.currentThread().ok = self.cluster_run(oarjob, site, cluster, nodes)
            thread_log("end")
        finally:
            thread_log("deleting oar job %i" % (oarjob,))
            execo_g5k.oardel([(oarjob, site)])
            job_duration = None
            if oarjob_start_date != None:
                job_duration = datetime.datetime.utcnow() - oarjob_start_date
            real_duration = datetime.datetime.utcnow() - thread_start_date
            thread_log("end (job duration = %s, real duration = %s)" % (job_duration, real_duration))

    def run(self):
        self.oar_submissions = []
        for cluster in self.clusters.keys():
            submission = execo_g5k.OarSubmission(walltime = self.options.walltime,
                                                 resources = "nodes=%i" % self.options.num_nodes,
                                                 sql_properties = "cluster = '%s'" % (cluster,),
                                                 name = self.run_name)
            if self.deploy:
                submission.job_type = "deploy"
            else:
                submission.job_type = "allow_classic_ssh"
            if self.options.reservation_date != None:
                submission.reservation_date = self.options.reservation_date
            if self.options.oar_queue != None:
                submission.queue = self.options.oar_queue
            self.oar_submissions.append((submission, self.clusters[cluster]))

        self.logger.info("submit oar jobs")
        self.submitted_jobs = execo_g5k.oarsub(self.oar_submissions)
        self.submitted_jobs = zip([job[0] for job in self.submitted_jobs], [job[1] for job in self.submitted_jobs], self.clusters.keys())
        self.submit_failure_jobs = [ job for job in self.submitted_jobs if job[0] == None]
        self.submit_success_jobs = [ job for job in self.submitted_jobs if job[0] != None]
        for job in self.submit_failure_jobs:
            self.logger.info("submission failed for %s@%s" % (job[2], job[1]))
        for (indx, job) in enumerate(self.submit_success_jobs):
            th = threading.Thread(target = self.cluster_run_threadfunc, args = (job[0], job[1], job[2]), name = "%s_%s" % (self.run_name, job[2],))
            th.ok = False
            self.submit_success_jobs[indx] = self.submit_success_jobs[indx] + (th,)
            self.logger.info("start thread on %s@%s" % (job[2], job[1]))
            th.start()
        self.logger.info("waiting for all threads to end")
        for th in [ job[3] for job in self.submit_success_jobs ]:
            th.join()
        self.logger.info("all threads finished. Summary:")
        for job in self.submit_failure_jobs:
            self.logger.info("  %s@%s: submission failed" % (job[2], job[1]))
        for job in self.submit_success_jobs:
            if job[3].ok:
                self.logger.info("  %s@%s: ok" % (job[2], job[1]))
            else:
                self.logger.info("  %s@%s: error" % (job[2], job[1]))

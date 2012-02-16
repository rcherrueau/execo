import threading, time
from execo_engine import Engine
import execo, execo_g5k

class bag_of_tasks_per_cluster(Engine):

    def __init__(self):
        super(bag_of_tasks_per_cluster, self).__init__()
        self.deploy = False
        self.deployment = None
        self.default_num_nodes = 2
        self.default_min_nodes = 1
        self.default_walltime = "1:0:0"
        self.options_parser.add_argument("space separated list of clusters", "a cluster: <clustername>[@sitename]; if sitename not given, will get it from g5k api")
        self.options_parser.add_option("-a", dest = "all_clusters", help = "run on all clusters. Default = %default", action = "store_true", default = False)
        self.options_parser.add_option("-q", dest = "oar_queue", help = "oar queue to use. Default = %default", default = None)
        self.options_parser.add_option("-n", dest = "num_nodes", help = "number of nodes per cluster to run. Default = %default", type = "int", default = self.default_num_nodes)
        self.options_parser.add_option("-m", dest = "min_nodes", help = "minimum number of nodes to get results from. Default = %default", type = "int", default = self.default_min_nodes)
        self.options_parser.add_option("-w", dest = "walltime", help = "walltime of jobs. Default = %default", type = "string", default = self.default_walltime)
        self.options_parser.add_option("-r", dest = "reservation_date", help = "reservation date. Default = %default", type = "string", default = None)
        self.clusters = dict()
        self.oar_submissions = []
        self.submitted_jobs = []
        self.submit_failure_jobs = []
        self.submit_success_jobs = []

    def run(self):
        for arg in self.args:
            arobindex = arg.find("@")
            if arobindex == -1:
                cluster = arg
                site = execo_g5k.api_utils.get_cluster_site(cluster)
            else:
                cluster = arg[0:arobindex]
                site = arg[arobindex+1:]
            self.clusters[cluster] = site
        if self.options.all_clusters:
            for cluster in execo_g5k.api_utils.get_g5k_clusters():
                self.clusters[cluster] = execo_g5k.api_utils.get_cluster_site(cluster)
        unknown_clusters = [ cluster for cluster in self.clusters.keys() if self.clusters[cluster] == None ]
        if len(unknown_clusters) > 0:
            raise Exception, "unknown clusters: %s" % (unknown_clusters,)
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
            th.daemon = True
            th.start()
        try:
            self.logger.info("waiting for all threads to end")
            for th in [ job[3] for job in self.submit_success_jobs ]:
                while th.isAlive():
                    th.join(3600) # timeout hack for correct ctrl-c handling
        except KeyboardInterrupt:
            self.logger.info("interrupted...")
            self.logger.info("forcing deletion of all oar jobs")
            execo_g5k.oardel([ (job[0], job[1]) for job in self.submit_success_jobs])
        self.logger.info("Summary:")
        for job in self.submit_failure_jobs:
            self.logger.info("  %s@%s: submission failed" % (job[2], job[1]))
        for job in self.submit_success_jobs:
            if job[3].ok:
                self.logger.info("  %s@%s: ok" % (job[2], job[1]))
            else:
                self.logger.info("  %s@%s: error" % (job[2], job[1]))

    def cluster_run_threadfunc(self, oarjob, site, cluster):
        def thread_log(arg):
            self.logger.info("thread %s@%s (oarjob %s): %s" % (cluster, site, oarjob, arg))
        thread_start_date = time.time()
        oarjob_start_date = None
        try:
            thread_log("wait oar job start")
            def start_prediction_changed(t):
                thread_log("oar job start prediction = %s" % (execo.format_date(t),))
            if not execo_g5k.wait_oar_job_start(oarjob, site, prediction_callback = start_prediction_changed):
                thread_log("aborting, unable to wait job start")
                return
            thread_log("get oar job nodes")
            nodes = execo_g5k.get_oar_job_nodes(oarjob, site)
            thread_log("%i nodes = %s" % (len(nodes), nodes))
            if len(nodes) < self.options.min_nodes:
                thread_log("aborting, not enough nodes")
                return
            thread_log("get oar job infos")
            job_infos = execo_g5k.get_oar_job_info(oarjob, site)
            if (not job_infos.has_key("start_date")) or (not job_infos.has_key("walltime")):
                thread_log("aborting, unable to get job infos")
                return
            oarjob_end_date = job_infos["start_date"] + job_infos["walltime"]
            oarjob_start_date = time.time()
            if self.deploy and self.deployment != None:
                self.deployment.hosts = nodes
                thread_log("deploying nodes")
                execo_g5k.deploy(self.deployment)
            thread_log("start")
            threading.currentThread().ok = self.cluster_run(oarjob, site, cluster, nodes, oarjob_end_date)
            thread_log("end")
        finally:
            thread_log("deleting oar job %i" % (oarjob,))
            execo_g5k.oardel([(oarjob, site)])
            job_duration = None
            if oarjob_start_date != None:
                job_duration = time.time() - oarjob_start_date
            real_duration = time.time() - thread_start_date
            thread_log("end (job duration = %s, real duration = %s)" % (execo.format_duration(job_duration), execo.format_duration(real_duration)))

    def cluster_run(self, oarjob, site, cluster, nodes, oarjob_end_date):
        pass

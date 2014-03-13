# Copyright 2009-2014 INRIA Rhone-Alpes, Service Experimentation et
# Developpement
#
# This file is part of Execo.
#
# Execo is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Execo is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
# License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Execo.  If not, see <http://www.gnu.org/licenses/>

import datetime, os, time, threading
from execo.time_utils import get_unixts, datetime_to_unixts, sleep
from execo.utils import memoize
from traceback import format_exc
from execo_g5k.api_utils import get_host_attributes, get_cluster_hosts, get_site_clusters
from execo_g5k.config import g5k_configuration
from execo_g5k.utils import G5kAutoPortForwarder
from execo_g5k.oar import format_oar_date

try:
    import MySQLdb
except:
    MySQLdb = None

_fixed_french_holidays = [
    (1, 1),   # new year
    (5, 1),   # labour day
    (5, 8),   # ww2 victory
    (7, 14),  # bastille day
    (8, 15),  # assumption
    (11, 1),  # all saints
    (11, 11), # armistice
    (12, 25), # christmas
    ]

def _easter(year):
    # http://www.smart.net/~mmontes/oudin.html
    century = year//100
    G = year % 19
    K = (century - 17)//25
    I = (century - century//4 - (century - K)//3 + 19*G + 15) % 30
    I = I - (I//28)*(1 - (I//28)*(29//(I + 1))*((21 - G)//11))
    J = (year + year//4 + I + 2 - century + century//4) % 7
    L = I - J
    EasterMonth = 3 + (L + 40)//44
    EasterDay = L + 28 - 31*(EasterMonth//4)
    return datetime.date(year, EasterMonth, EasterDay)

def _easter_monday(year):
    return _easter(year) + datetime.timedelta(1)

def _ascension_thursday(year):
    return _easter(year) + datetime.timedelta(39)

def _whit_monday(year):
    return _easter(year) + datetime.timedelta(50)

@memoize
def french_holidays(year):
    """Returns the set of public holiday days for a given year.

    Returns a set of datetime.date. Results are cached between calls
    for speed.
    """
    holidays = set()
    holidays.update([ datetime.date(year, m, d) for (m,d) in _fixed_french_holidays ])
    holidays.add(_easter_monday(year))
    holidays.add(_ascension_thursday(year))
    holidays.add(_whit_monday(year))
    return holidays

def _work_day(d):
    # Is the given datetime.date a work day?
    if d.weekday() in [5, 6]: return False
    if d in french_holidays(d.year): return False
    return True

def _next_work_day(d):
    # returns the next working day following the given datetime.date
    while True:
        d += datetime.timedelta(1)
        if _work_day(d):
            return d

def unixts_to_oar_datetime(ts):
    """Convert a timestamp to a naive datetime (no tz attached) in the g5k oar/oargrid timezone Europe/Paris."""
    return datetime.datetime.strptime(format_oar_date(ts), "%Y-%m-%d %H:%M:%S")

def oar_datetime_to_unixts(dt):
    """Convert a naive datetime (no tz attached) in the g5k oar/oargrid timezone Europe/Paris to a unix timestamp."""
    # forking code because modifying os.environ["TZ"] and calling
    # time.tzset() is not thread-safe
    rend, wend = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.environ["TZ"] = "Europe/Paris"
        time.tzset()
        ts = datetime_to_unixts(dt)
        os.write(wend, str(ts))
        os._exit(0)
    else:
        os.close(wend)
        f = os.fdopen(rend)
        ts = float(f.read())
        f.close()
        os.waitpid(pid, 0)
        return ts

def get_oar_day_start_end(day):
    """Return unixts tuple of start and end a of day in timezone Europe/Paris."""
    dt = datetime.datetime(day.year,
                           day.month,
                           day.day)
    daystart = oar_datetime_to_unixts(dt)
    dayend = daystart + 24*3600-1
    return daystart, dayend

def g5k_charter_time(t):
    """Is the given date in a g5k charter time period ?

    Returns a boolean, True if the given date is in a period where
    the g5k charter needs to be respected, False if it is in a period
    where charter is not applicable (night, weekends, non working days)

    :param t: a date in a type supported by `execo.time_utils.get_unixts`
    """
    dt = unixts_to_oar_datetime(get_unixts(t))
    if dt.hour < 9 or dt.hour >= 19: return False
    return _work_day(dt.date())

def get_next_charter_period(start, end):
    """Return the next g5k charter time period.

    :param start: timestamp in a type supported by
      `execo.time_utils.get_unixts` from which to start searching for
      the next g5k charter time period. If start is in a g5k charter
      time period, the returned g5k charter time period starts at
      start.

    :param end: timestamp in a type supported by
      `execo.time_utils.get_unixts` until which to search for the next
      g5k charter time period. If end is in the g5k charter time
      period, the returned g5k charter time period ends at end.

    :returns: a tuple (charter_start, charter_end) of
      unix timestamps. (None, None) if no g5k charter time period
      found
    """
    start = unixts_to_oar_datetime(get_unixts(start))
    end = unixts_to_oar_datetime(get_unixts(end))
    if end <= start:
        return None, None
    elif g5k_charter_time(start):
        charter_end = start.replace(hour = 19, minute = 0, second = 0, microsecond = 0)
        return oar_datetime_to_unixts(start), datetime_to_unixts(min(end, charter_end))
    else:
        if start.hour < 9 and _work_day(start.date()):
            charter_start = start.replace(hour = 9, minute = 0, second = 0, microsecond = 0)
        else:
            charter_start = datetime.datetime.combine(_next_work_day(start.date()), datetime.time(9, 0, 0))
        charter_end = charter_start.replace(hour = 19, minute = 0, second = 0, microsecond = 0)
        return oar_datetime_to_unixts(charter_start), datetime_to_unixts(min(end, charter_end))

def _job_intersect_charter_period(job):
    return (get_next_charter_period(job['start_time'], job['stop_time']) != None, None)

@memoize
def cluster_num_cores(cluster):
    """Returns the num of cores for a grid5000 cluster

    Results are cached between calls for speed."""
    num_host_cores = get_host_attributes(cluster + "-1")["architecture"]["smt_size"]
    num_hosts = len(get_cluster_hosts(cluster))
    return num_hosts * num_host_cores

if MySQLdb:

    def _get_jobs(db, cluster, user, start, end):
        q = """(SELECT
                  jobs.job_id as job_id,
                  jobs.job_type as job_type,
                  jobs.job_user as user,
                  moldable_job_descriptions.moldable_walltime as walltime,
                  gantt_jobs_predictions_visu.start_time as start_time ,
                  (gantt_jobs_predictions_visu.start_time + moldable_job_descriptions.moldable_walltime) as stop_time,
                  count(*) as nb_resources_scheduled,
                  0 as nb_resources_actual,
                  resources.cluster as cluster
                FROM jobs,
                  moldable_job_descriptions,
                  gantt_jobs_resources_visu,
                  gantt_jobs_predictions_visu,
                  resources
                WHERE
                  jobs.job_user = '%(user)s' AND jobs.stop_time = 0 AND
                  gantt_jobs_predictions_visu.moldable_job_id = gantt_jobs_resources_visu.moldable_job_id AND
                  gantt_jobs_predictions_visu.moldable_job_id = moldable_job_descriptions.moldable_id AND
                  jobs.job_id = moldable_job_descriptions.moldable_job_id AND
                  gantt_jobs_predictions_visu.start_time < %(end)i AND
                  resources.resource_id = gantt_jobs_resources_visu.resource_id AND
                  resources.type = 'default' AND
                  gantt_jobs_predictions_visu.start_time >= %(start)i AND
                  jobs.job_id not in (SELECT job_id FROM job_types where job_id = jobs.job_id AND (type = 'besteffort' OR type = 'nfs' OR type = 'iscsi') ) AND
                  resources.cluster = '%(cluster)s'
                GROUP BY job_id,
                  job_type,
                  user,
                  walltime,
                  start_time,
                  stop_time
                ORDER BY jobs.job_user)
                UNION (SELECT
                  jobs.job_id as job_id,
                  jobs.job_type as job_type,
                  jobs.job_user as user,
                  jobs.stop_time-jobs.start_time as walltime,
                  jobs.start_time as start_time,
                  jobs.stop_time as stop_time,
                  0 as nb_resources_scheduled,
                  count(*) as nb_resources_actual,
                  resources.cluster as cluster
                FROM jobs,
                  assigned_resources,
                  resources
                WHERE
                  jobs.job_user = '%(user)s' AND jobs.stop_time != 0 AND
                  jobs.stop_time != jobs.start_time AND
                  assigned_resources.moldable_job_id = jobs.assigned_moldable_job AND
                  jobs.start_time < %(end)i AND
                  jobs.start_time >= %(start)i AND
                  jobs.job_id not in (SELECT job_id FROM job_types where job_id = jobs.job_id  and (type = 'besteffort' or type = 'nfs' or type = 'iscsi') ) AND
                  assigned_resources.resource_id = resources.resource_id AND
                  resources.type = 'default' AND
                  resources.cluster = '%(cluster)s'
                GROUP BY job_id,
                  job_type,
                  user,
                  walltime,
                  start_time,
                  stop_time
                ORDER BY jobs.job_user)""" % {"user": user, "start": start, "end": end, "cluster": cluster}
        db.query(q)
        r = db.store_result()
        return [data for data in r.fetch_row(maxrows = 0, how = 1)]

    def __site_charter_remaining(site, day):
        with G5kAutoPortForwarder(site,
                                  'mysql.' + site + '.grid5000.fr',
                                  g5k_configuration['oar_mysql_ro_port']) as (host, port):
            start, end = get_oar_day_start_end(day)
            user = g5k_configuration.get('api_username')
            if not user:
                user = os.environ['LOGNAME']
            try:
                db = MySQLdb.connect(host = host, port = port,
                                     user = g5k_configuration['oar_mysql_ro_user'],
                                     passwd = g5k_configuration['oar_mysql_ro_password'],
                                     db = g5k_configuration['oar_mysql_ro_db'])
                try:
                    for cluster in get_site_clusters(site):
                        cluster_used = 0
                        for j in _get_jobs(db, cluster, user, start, end):
                            if _job_intersect_charter_period(j):
                                cluster_used += (j["nb_resources_scheduled"] + j["nb_resources_actual"]) * j["walltime"]
                        threading.currentThread().remaining[cluster] = max(0, cluster_num_cores(cluster) * 3600 * 2 - cluster_used)
                finally:
                    db.close()
            except Exception, e:
                logger.warn("error connecting to oar database / getting planning from " + site)
                logger.trace("exception:\n" + format_exc())

    def g5k_charter_remaining(sites, day):
        """Returns the amount of time remaining per cluster for submitting jobs, according to the grid5000 charter.

        Grid5000 usage charter:
        https://www.grid5000.fr/mediawiki/index.php/Grid5000:UserCharter
        This function is only available if MySQLdb is available.

        :param sites: a list of grid5000 sites

        :param day: a `datetime.date`

        :returns: a dict, keys are cluster names, value is remaining
          time for each cluster (in seconds)
        """
        remaining = {}
        threads = {}
        for site in sites:
            t = threading.Thread(target = __site_charter_remaining,
                                 args = (site, day))
            t.remaining = {}
            threads[site] = t
            t.start()
        for site, t in threads.iteritems():
            t.join()
            remaining.update(t.remaining)
        return remaining

# Copyright 2009-2015 INRIA Rhone-Alpes, Service Experimentation et
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
from execo.time_utils import get_unixts, datetime_to_unixts, \
    format_unixts, format_seconds
from execo.utils import memoize
from traceback import format_exc
from execo_g5k.api_utils import get_site_clusters
from execo_g5k.config import g5k_configuration
from execo_g5k.utils import G5kAutoPortForwarder
from execo_g5k.oar import format_oar_date
from execo.log import logger

try:
    import psycopg2
except:
    psycopg2 = None

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
        if charter_start > end:
            return None, None
        charter_end = charter_start.replace(hour = 19, minute = 0, second = 0, microsecond = 0)
        return oar_datetime_to_unixts(charter_start), datetime_to_unixts(min(end, charter_end))

def _job_intersect_charter_period(job):
    return (get_next_charter_period(job['start_time'], job['stop_time']) != (None, None))

if psycopg2:

    def _cluster_num_available_cores(conn, cluster):
        q = "SELECT COUNT(*) AS num_available_cores FROM resources WHERE cluster = '%s' AND state != 'Dead' AND type='default'" % (cluster,)
        cur = conn.cursor()
        cur.execute(q)
        data = cur.fetchone()
        logger.trace("num available cores %s = %s" % (cluster, data[0]))
        return data[0]

    def _get_jobs(conn, cluster, user, start, end):
        q = """(
                 SELECT J.job_id, J.job_user AS user, J.state, JT.type,
                   J.job_type, R.cluster,
                   MJD.moldable_walltime AS walltime,
                   GJPV.start_time AS start_time,
                   GJPV.start_time + MJD.moldable_walltime AS stop_time,
                   COUNT(*) AS nb_resources_scheduled, 0 AS nb_resources_actual
                 FROM jobs J
                 LEFT JOIN moldable_job_descriptions MJD
                   ON MJD.moldable_job_id = J.job_id
                 LEFT JOIN gantt_jobs_predictions_visu GJPV
                   ON GJPV.moldable_job_id = MJD.moldable_id
                 INNER JOIN gantt_jobs_resources GJR
                   ON GJR.moldable_job_id = MJD.moldable_id
                 LEFT JOIN resources R
                   ON GJR.resource_id = R.resource_id
                 LEFT JOIN job_types JT
                   ON JT.job_id = J.job_id
                 WHERE J.job_user = '%(user)s' AND
                   R.cluster = '%(cluster)s' AND
                   R.type = 'default' AND
                   ( JT.type IS NULL OR JT.type NOT IN ('besteffort', 'nfs', 'iscsi') ) AND
                   ( ( GJPV.start_time >= %(start)i AND GJPV.start_time < %(end)i ) OR
                     ( stop_time >= %(start)i AND stop_time < %(end)i ) )
                 GROUP BY J.job_id, JT.type, R.cluster, walltime, GJPV.start_time, stop_time
               ) UNION (
                 SELECT J.job_id, J.job_user AS user, J.state, JT.type,
                   J.job_type, R.cluster,
                   J.stop_time - J.start_time as walltime,
                   J.start_time AS start_time,
                   J.stop_time AS stop_time,
                   0 AS nb_resources_scheduled, COUNT(*) AS nb_resources_actual
                 FROM jobs J
                 LEFT JOIN assigned_resources AR
                   ON AR.moldable_job_id = J.assigned_moldable_job
                 LEFT JOIN resources R
                   ON AR.resource_id = R.resource_id
                 LEFT JOIN job_types JT
                   ON JT.job_id = J.job_id
                 WHERE J.job_user = '%(user)s' AND
                   R.cluster = '%(cluster)s' AND
                   R.type = 'default' AND
                   ( JT.type IS NULL OR JT.type NOT IN ('besteffort', 'nfs', 'iscsi') ) AND
                   ( ( J.start_time >= %(start)i AND J.start_time < %(end)i ) OR
                     ( J.stop_time >= %(start)i AND J.stop_time < %(end)i ) )
                 GROUP BY J.job_id, JT.type, R.cluster, walltime, start_time, stop_time
               )""" % {"user": user,
                       "start": start,
                       "end": end,
                       "cluster": cluster}
        cur = conn.cursor()
        cur.execute(q)
        return [data for data in cur.fetchall()]

    def __site_charter_remaining(site, day, user = None):
        with G5kAutoPortForwarder(site,
                                  'oardb.' + site + '.grid5000.fr',
                                  g5k_configuration['oar_pgsql_ro_port']) as (host, port):
            start, end = get_oar_day_start_end(day)
            if not user:
                user = g5k_configuration.get('api_username')
                if not user:
                    user = os.environ['LOGNAME']
            try:
                conn = psycopg2.connect(host = host, port = port,
                                        user = g5k_configuration['oar_pgsql_ro_user'],
                                        password = g5k_configuration['oar_pgsql_ro_password'],
                                        database = g5k_configuration['oar_pgsql_ro_db'])
                try:
                    logger.trace("getting jobs for user %s on %s for %s" % (user, site, day))
                    OOC_total_site_used = 0
                    OOC_site_quota = 0
                    for cluster in get_site_clusters(site):
                        total_cluster_used = 0
                        for j in _get_jobs(conn, cluster, user, start, end):
                            logger.trace("%s:%s - job: start %s, end %s, walltime %s, %s" % (
                                    site, cluster, format_unixts(j[7]),
                                    format_unixts(j[8]), format_seconds(j[6]), j))
                            if _job_intersect_charter_period(j):
                                cluster_used = (j[9] + j[10]) * j[6]
                                logger.trace("%s:%s job %i intersects charter -> uses %is of cluster quota" % (
                                        site, cluster, j[0], cluster_used,))
                                total_cluster_used += cluster_used
                        #cluster_quota = cluster_num_cores(cluster) * 3600 * 2
                        cluster_quota = _cluster_num_available_cores(conn, cluster) * 3600 * 2
                        logger.trace("%s:%s total cluster used = %i (%s), cluster quota = %i (%s)" % (
                                site, cluster,
                                total_cluster_used, format_seconds(total_cluster_used),
                                cluster_quota, format_seconds(cluster_quota)))
                        threading.currentThread().remaining[cluster] = max(0, cluster_quota - total_cluster_used)
                        OOC_total_site_used += total_cluster_used
                        OOC_site_quota += cluster_quota
                    logger.trace("%s to compare with outofchart: total used = %i (%s), %i%% of site quota = %i (%s)" % (
                            site,
                            OOC_total_site_used, format_seconds(OOC_total_site_used),
                            int(float(OOC_total_site_used) / float(OOC_site_quota) * 100.0),
                            OOC_site_quota, format_seconds(OOC_site_quota)))
                finally:
                    conn.close()
            except Exception, e:
                logger.warn("error connecting to oar database / getting planning from " + site)
                logger.detail("exception:\n" + format_exc())
                

    def g5k_charter_remaining(sites, day, user = None):
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
                                 args = (site, day),
                                 kwargs = {'user': user})
            t.remaining = {}
            threads[site] = t
            t.start()
        for site, t in threads.iteritems():
            t.join()
            remaining.update(t.remaining)
        return remaining

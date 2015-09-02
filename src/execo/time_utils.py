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

from log import logger
import datetime, calendar, time, re

def timedelta_to_seconds(td):
    """Convert a ``datetime.timedelta`` to a number of seconds (float)."""
    return td.days * 86400 + td.seconds + td.microseconds / 1e6

def datetime_to_unixts(dt):
    """Convert a ``datetime.datetime`` to a utc unix timestamp (float).

    If the datetime given has an explicit timezone, it is used,
    otherwise the timestamp is assumed to be given in the locale
    timezone and is converted to utc.
    """
    if dt.tzinfo:
        return calendar.timegm(dt.utctimetuple()) + dt.microsecond / 1e6
    else:
        return time.mktime(dt.timetuple()) + dt.microsecond / 1e6

def unixts_to_datetime(ts, tz = None):
    """Convert a utc unixts (int or float) to ``datetime.datetime``.

    If no timezone is given, the returned datetime is a naive one,
    with the timestamp set in local time, without a timezone. Or the
    returned datetime is set in the given timezone if one is given.
    """
    return datetime.datetime.fromtimestamp(ts, tz)

def numeric_date_to_unixts(d):
    """Convert a numeric to unix timestamp (float)."""
    return float(d)

def numeric_duration_to_seconds(d):
    """Convert a numeric to duration in number of seconds (float)."""
    return float(d)

_num_str_date_re = re.compile("^((\d*\.)?\d+)$")
_str_date_re = re.compile("^\s*((((\d\d)?(\d\d)-)?((\d?\d)-))?(\d?\d)[Tt ])?(\d?\d):(\d?\d):(\d?\d)(\.\d+)?([Zz]|([+-])(\d?\d):(\d?\d))?\s*$")
def str_date_to_unixts(d):
    """Convert a date string to a unix timestamp (float).

    date string format can be:

    - a numeric. In this case, convert with
      `execo.time_utils.numeric_date_to_unixts`

    - a string in rfc-3339 format 'YYYY-MM-DD[Tt ]HH:MM:SS[.subsec][Zz|(+|-)HH:MM]'.
      If a timezone is given, it is used, otherwise the timestamp is
      assumed to be given in the locale timezone and is converted to
      utc.
    """
    numeric_date = _num_str_date_re.match(d)
    if numeric_date:
        return numeric_date_to_unixts(float(numeric_date.group(1)))
    str_date = _str_date_re.match(d)
    if str_date:
        now = time.localtime()
        if str_date.group(5):
            year = int(str_date.group(5))
            if str_date.group(4):
                year += 100 * int(str_date.group(4))
            else:
                if year >= 69: year += 1900
                else: year += 2000
        else:
            year = now.tm_year
        if str_date.group(7):
            month = int(str_date.group(7))
        else:
            month = now.tm_mon
        if str_date.group(8):
            day = int(str_date.group(8))
        else:
            day = now.tm_mday
        h = int(str_date.group(9))
        m = int(str_date.group(10))
        s = int(str_date.group(11))
        norm_date = "%i-%i-%i %i:%i:%i" % (year, month, day, h, m, s)
        if str_date.group(13):
            ts = calendar.timegm(time.strptime(norm_date, "%Y-%m-%d %H:%M:%S"))
        else:
            ts = time.mktime(time.strptime(norm_date, "%Y-%m-%d %H:%M:%S"))
        if str_date.group(12):
            ts += float(str_date.group(12))
        if str_date.group(13) and str_date.group(13) not in ["Z", "z"]:
            offset = int(str_date.group(15)) * 3600 + int(str_date.group(16)) * 60
            if str_date.group(14) == "+": ts -= offset
            else: ts += offset
        return ts
    raise ValueError, "unsupported date format %s" % (d,)

_num_str_duration_re = re.compile("^((\d*\.)?\d+)$")
_str_duration_re = re.compile("^(\d+):(\d?\d):(\d?\d)(\.\d+)?$")
def str_duration_to_seconds(duration):
    """Convert a duration string to a number of seconds (float).

    duration string format can be:

    - a numeric. In this case, convert with
      `execo.time_utils.numeric_duration_to_seconds`

    - a string in format 'HH:MM:SS[.MS]'
    """
    numeric_duration = _num_str_duration_re.match(duration)
    if numeric_duration:
        return numeric_duration_to_seconds(float(numeric_duration.group(1)))
    str_duration = _str_duration_re.match(duration)
    if str_duration:
        duration = (int(str_duration.group(1)) * 3600
                    + int(str_duration.group(2)) * 60
                    + int(str_duration.group(3)))
        if str_duration.group(4):
            duration += float(str_duration.group(4))
        return duration
    raise ValueError, "unsupported duration format %s" % (duration,)

def get_seconds(duration):
    """Convert a duration to a number of seconds (float).

    :param duration: a duration in one of the supported types. if
      duration == None, returns None. Supported types

      - ``datetime.timedelta``: see `execo.time_utils.timedelta_to_seconds`

      - string: see `execo.time_utils.str_duration_to_seconds`

      - numeric type: see
        `execo.time_utils.numeric_duration_to_seconds`
    """
    if duration == None:
        return None
    elif isinstance(duration, datetime.timedelta):
        return timedelta_to_seconds(duration)
    elif isinstance(duration, str):
        return str_duration_to_seconds(duration)
    return numeric_duration_to_seconds(duration)

def get_unixts(d):
    """Convert a date to a unix timestamp (float).

    :param d: a date in one of the supported types. if date == None,
      returns None. Supported types

      - ``datetime.datetime``: see `execo.time_utils.datetime_to_unixts`

      - string: see `execo.time_utils.str_date_to_unixts`

      - numeric type: see `execo.time_utils.numeric_date_to_unixts`
    """
    if d == None:
        return None
    if isinstance(d, datetime.datetime):
        return datetime_to_unixts(d)
    elif isinstance(d, str):
        return str_date_to_unixts(d)
    return numeric_date_to_unixts(d)

def _get_milliseconds_suffix(secs):
    """Return a formatted millisecond suffix, either empty if ms = 0, or dot with 3 digits otherwise.

    :param secs: a unix timestamp (integer or float)
    """
    ms_suffix = ""
    msecs = int (round(secs - int(secs), 3) * 1000)
    if msecs != 0:
        ms_suffix = ".%03i" % msecs
    return ms_suffix

def _zone3339(timetuple):
    """Return a string with the locale timezone in rfc-3339 format."""
    dst = timetuple[8]
    offs = (time.timezone, time.timezone, time.altzone)[1 + dst]
    return '%+.2d:%.2d' % (offs / -3600, abs(offs / 60) % 60)

def format_unixts(secs, showms = False):
    """Return a string with the formatted date (year, month, day, hour, min, sec, ms) in locale timezone and in rfc-3339 format for pretty printing.

    :param secs: a unix timestamp (integer or float) (or None).

    :param showms: whether to show ms or not. Default False.
    """
    if secs == None:
        return None
    t = time.localtime(secs)
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", t)
    if showms:
        formatted_time += _get_milliseconds_suffix(secs)
    formatted_time += _zone3339(t)
    return formatted_time

def format_seconds(secs, showms = False):
    """Return a string with a formatted duration (days, hours, mins, secs, ms) for pretty printing.

    :param secs: a duration in seconds (integer or float) (or None).

    :param showms: whether to show ms or not. Default False.
    """
    if secs == None:
        return None
    s = secs
    d = (s - (s % 86400)) / 86400
    s -= d * 86400
    h = (s - (s % 3600)) / 3600
    s -= h * 3600
    m = (s - (s % 60)) / 60
    s -= m * 60
    formatted_duration = ""
    if secs >= 86400: formatted_duration += "%id" % d
    if secs >= 3600: formatted_duration += "%ih" % h
    if secs >= 60: formatted_duration += "%im" % m
    if showms:
        formatted_duration += "%i%ss" % (s, _get_milliseconds_suffix(s))
    else:
        formatted_duration += "%is" % (s,)
    return formatted_duration

def format_date(d, showms = False):
    """Return a string with the formatted date (year, month, day, hour, min, sec, ms) in locale timezone and in rfc-3339 format for pretty printing.

    :param d: a date in one of the formats handled (or None) (see
      `execo.time_utils.get_unixts`).

    :param showms: whether to show ms or not. Default False.
    """
    return format_unixts(get_unixts(d), showms)

def format_duration(duration, showms = False):
    """Return a string with a formatted duration (days, hours, mins, secs, ms) for pretty printing.

    :param duration: a duration in one of the formats handled (or
      None) (see `execo.time_utils.get_seconds`).

    :param showms: whether to show ms or not. Default False.
    """
    return format_seconds(get_seconds(duration), showms)

def _safe_sleep(secs):
    """Safe sleeping: restarted if interrupted by signals.

    :param secs: time to sleep in seconds (int or float)
    """
    end = time.time() + secs
    sleep_time = secs
    while sleep_time > 0:
        time.sleep(sleep_time)
        sleep_time = end - time.time()

def sleep(delay = None, until = None):
    """Sleep until a given delay has elapsed or until a given date.

    If both present, will sleep at least for the delay and at least
    until the date.

    :param delay: the delay to sleep in one of the formats handled (or
      None) (see `execo.time_utils.get_seconds`).

    :param until: the date until which to sleep in one of the formats
      handled (or None) (see `execo.time_utils.get_unixts`).
    """
    delay = get_seconds(delay)
    until = get_unixts(until)
    sleeptime = 0
    if delay != None:
        sleeptime = delay
    if until != None:
        dt = until - time.time()
        if (sleeptime > 0 and dt > sleeptime) or (sleeptime <= 0 and dt > 0):
            sleeptime = dt
    if sleeptime > 0:
        logger.debug("sleeping %s", format_seconds(sleeptime))
        _safe_sleep(sleeptime)
        logger.debug("end sleeping")
        return sleeptime

class Timer(object):

    """Keep track of elapsed time."""

    def __init__(self, timeout = None):
        """Create and start the Timer."""
        self._start = time.time()
        self._timeout = get_seconds(timeout)

    def wait_elapsed(self, elapsed):
        """Sleep until the given amount of time has elapsed since the Timer's start.

        :param elapsed: the delay to sleep in one of the formats
          handled (or None) (see `execo.time_utils.get_seconds`).
        """
        elapsed = get_seconds(elapsed)
        really_elapsed = time.time() - self._start
        if really_elapsed < elapsed:
            sleep(elapsed - really_elapsed)
        return self

    def start_date(self):
        """Return this Timer's instance start time."""
        return self._start

    def elapsed(self):
        """Return this Timer's instance elapsed time since start."""
        return time.time() - self._start

    def remaining(self):
        """Returns the remaining time before the timeout."""
        if self._timeout != None:
            return self._start + self._timeout - time.time()
        else:
            return None

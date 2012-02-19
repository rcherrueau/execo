# Copyright 2009-2012 INRIA Rhone-Alpes, Service Experimentation et
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

from log import set_style
from time_utils import format_unixts, format_seconds
import sys

def sort_reports(reports):
    def key_func(report):
        if report.stats()['start_date'] != None:
            return report.stats()['start_date']
        else:
            return sys.maxint
    reports.sort(key = key_func)

class Report(object):

    """Human-readable summary of one or more `execo.action.Action`.

    A Report gathers the results of actions or (recursively) of other
    reports. `execo.report.Report.output` returns a formatted string
    with a human-readable summary of all actions results.

    To be able to gather transparently results both from actions and
    sub-reports, both `execo.action.Action` and `execo.report.Report`
    implement the `execo.report.Report.stats`,
    `execo.report.Report.reports`, `execo.report.Report.name` methods.

    >>> r = Report()
    >>> r
    <Report(<0 entries>, name='Report')>
    >>> sorted(r.stats().items())
    [('end_date', None), ('num_ended', 0), ('num_errors', 0), ('num_forced_kills', 0), ('num_non_zero_exit_codes', 0), ('num_ok', 0), ('num_processes', 0), ('num_started', 0), ('num_timeouts', 0), ('start_date', None)]
    """

    def __init__(self, reports = None, name = None):
        """
        :param reports: a `execo.report.Report`, an
          `execo.action.Action`, or an iterable of these, which will
          be added to this report.

        :param name a name given to this report. If None, a default
          name will be given.
        """
        if name == None:
            self._name = "%s" % (self.__class__.__name__,)
        else:
            self._name = name
        self._reports = set()
        if reports != None:
            self.add(reports)

    def add(self, reports):
        """Add some sub-`execo.report.Report` or `execo.action.Action` to this report.
        
        :param reports: an iterable of `execo.report.Report` or
          `execo.action.Action`, which will be added to this report.
        """
        self._reports.update(reports)

    def reports(self):
        """Return a sorted (by start date) copy of the list of `execo.report.Report` or `execo.action.Action` registered to this report."""
        reports = list(self._reports)
        sort_reports(reports)
        return reports

    def name(self):
        """Return the report name."""
        return self._name

    @staticmethod
    def empty_stats():
        """Return a stats dict all initialized to zero."""
        return {
            'start_date': None,
            'end_date': None,
            'num_processes': 0,
            'num_started': 0,
            'num_ended': 0,
            'num_errors': 0,
            'num_timeouts': 0,
            'num_forced_kills': 0,
            'num_non_zero_exit_codes': 0,
            'num_ok': 0,
            'num_finished_ok': 0,
            }

    def stats(self):
        """Return a dict summarizing the statistics of all `execo.action.Action` and sub-`execo.report.Report` registered to this report.

        This summary dict contains the following metrics:
        
        - ``start_date``: earliest start date (unix timestamp) of all
          `Action` or None if none have started yet.
        
        - ``end_date``: latest end date (unix timestamp) of all
          `Action` or None if not available (not all started, not all
          ended).
        
        - ``num_processes``: number of processes in all `Action`.
        
        - ``num_started``: number of processes that have started.
        
        - ``num_ended``: number of processes that have ended.
        
        - ``num_errors``: number of processes that went in error
          when started.
        
        - ``num_timeouts``: number of processes that had to be killed
          (SIGTERM) after reaching their timeout.
        
        - ``num_forced_kills``: number of processes that had to be
          forcibly killed (SIGKILL) after not responding for some
          time.
        
        - ``num_non_zero_exit_codes``: number of processes that ran
          correctly but whose return code was != 0.
        
        - ``num_ok``: number of processes which:

          - did not started

          - started and not yet ended

          - started and ended and did not went in error (or where
            launched with flag ignore_error) , did not timeout (or
            where launched with flag ignore_timeout), and had an exit
            code == 0 (or where launched with flag ignore_exit_code).

        - ``num_finished_ok``: number of processes which started,
          ended, and are ok.
        """
        stats = Report.empty_stats()
        stats['start_date'] = None
        no_end_date = False
        for report in self._reports:
            stats2 = report.stats()
            for k in stats2.keys():
                if k == 'start_date':
                    if (stats2[k] != None
                        and (stats[k] == None or stats2[k] < stats[k])):
                        stats[k] = stats2[k]
                elif k == 'end_date':
                    if stats2[k] == None:
                        no_end_date = True
                    elif stats[k] == None or stats2[k] > stats[k]:
                        stats[k] = stats2[k]
                else:
                    stats[k] += stats2[k]
        if no_end_date:
            stats['end_date'] = None
        return stats

    def __repr__(self):
        return "<Report(<%i entries>, name=%r)>" % (len(self._reports), self._name)

    def __str__(self):
        stats = self.stats()
        return "<Report(<%i entries>, name=%r, start_date=%r, end_date=%r, num_processes=%r, num_started=%r, num_ended=%r, num_timeouts=%r, num_errors=%r, num_forced_kills=%r, num_non_zero_exit_codes=%r, num_ok=%r, num_finished_ok=%r)>" % (len(self._reports), self._name, format_unixts(stats['start_date']), format_unixts(stats['end_date']), stats['num_processes'], stats['num_started'], stats['num_ended'], stats['num_timeouts'], stats['num_errors'], stats['num_forced_kills'], stats['num_non_zero_exit_codes'], stats['num_ok'], stats['num_finished_ok'])

    def output(self, wide = False, brief = False):
        """Returns a formatted string with a human-readable summary of all `Action` results.

        :param wide: if False (default), report format is designed for
          80 columns display. If True, output a (175 characters) wide
          report.

        :param brief: when True, only the Total summary is output, not
          each `Action` or `Report` summary. Default is False.
        """
        stats = self.stats()
        output = ""
        if wide:
            output += "Name                                    start               end                 length         started   ended     errors    timeouts  f.killed  badretval ok        total     \n"
            output += "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
        else:
            output += "Name                                    start               end                \n"
            output += "  length       started ended   error   timeout fkilled ret!=0  ok      total   \n"
            output += "-------------------------------------------------------------------------------\n"
        def format_line(name, stats, indent):
            result = ""
            indented_name = " " * indent + name
            length = ""
            if stats['start_date'] != None and stats['end_date'] != None:
                length = format_seconds(stats['end_date'] - stats['start_date'])
            else:
                length = ""
            if wide:
                tmpline = "%-39.39s %-19.19s %-19.19s %-15.15s%-10.10s%-10.10s%-10.10s%-10.10s%-10.10s%-10.10s%-10.10s%-10.10s\n" % (
                    indented_name,
                    format_unixts(stats['start_date']),
                    format_unixts(stats['end_date']),
                    length,
                    stats['num_started'],
                    stats['num_ended'],
                    stats['num_errors'],
                    stats['num_timeouts'],
                    stats['num_forced_kills'],
                    stats['num_non_zero_exit_codes'],
                    stats['num_ok'],
                    stats['num_processes'])
            else:
                tmpline = "%-39.39s %-19.19s %-19.19s\n" % (
                    indented_name,
                    format_unixts(stats['start_date']),
                    format_unixts(stats['end_date']),)
                tmpline += "  %-13.13s%-8.8s%-8.8s%-8.8s%-8.8s%-8.8s%-8.8s%-8.8s%-8.8s\n" % (
                    length,
                    stats['num_started'],
                    stats['num_ended'],
                    stats['num_errors'],
                    stats['num_timeouts'],
                    stats['num_forced_kills'],
                    stats['num_non_zero_exit_codes'],
                    stats['num_ok'],
                    stats['num_processes'],)
            if stats['num_ok'] < stats['num_processes']:
                if stats['num_ok'] == stats['num_ended']:
                    tmpline = set_style(tmpline, 'report_warn')
                else:
                    tmpline = set_style(tmpline, 'report_error')
            result += tmpline
            return result

        def recurse_report(report, indent):
            result = ""
            result += format_line(report.name(), report.stats(), indent)
            subreports = report.reports()
            if len(subreports) != 0:
                for subreport in subreports:
                    result += recurse_report(subreport, indent+2)
            return result

        subreports = self.reports()
        if not brief and len(subreports) != 0:
            for report in subreports:
                output += recurse_report(report, 0)
            if wide:
                output += "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
            else:
                output += "-------------------------------------------------------------------------------\n"

        output += format_line(self.name(), stats, 0)
        return output

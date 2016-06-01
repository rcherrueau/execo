# Copyright 2009-2016 INRIA Rhone-Alpes, Service Experimentation et
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

from .config import configuration, FDEBUG, IODEBUG, TRACE, DETAIL
import logging, sys, functools, os

_ansi_styles = {
    'default'    : '\033[m',
    # styles
    'bold'       : '\033[1m',
    'underline'  : '\033[4m',
    'blink'      : '\033[5m',
    'reverse'    : '\033[7m',
    'concealed'  : '\033[8m',
    # font colors
    'black'      : '\033[30m',
    'red'        : '\033[31m',
    'green'      : '\033[32m',
    'yellow'     : '\033[33m',
    'blue'       : '\033[34m',
    'magenta'    : '\033[35m',
    'cyan'       : '\033[36m',
    'white'      : '\033[37m',
    # background colors
    'on_black'   : '\033[40m',
    'on_red'     : '\033[41m',
    'on_green'   : '\033[42m',
    'on_yellow'  : '\033[43m',
    'on_blue'    : '\033[44m',
    'on_magenta' : '\033[45m',
    'on_cyan'    : '\033[46m',
    'on_white'   : '\033[47m',
    }
"""Definition of ansi escape sequences for colorized output."""

def _set_style(style, string):
    """Enclose a string with ansi color escape codes if ``execo.config.configuration['color_mode']`` is True.

    :param string: the string to enclose

    :param style: a key in dict
      ``execo.config.configuration['color_styles']``
    """
    if (configuration.get('color_mode')
        and configuration['color_styles'].get(style)):
        style_seq = ""
        for attr in configuration['color_styles'][style]:
            style_seq += _ansi_styles[attr]
        return "%s%s%s" % (style_seq, string, _ansi_styles['default'])
    else:
        return string

class Styler:
    def set(self, style, string):
        return _set_style(style, string)
    def __getattr__(self, attr):
        return functools.partial(_set_style, attr)
style = Styler()

logging.addLevelName(FDEBUG, 'FDEBUG')
logging.addLevelName(IODEBUG, 'IODEBUG')
logging.addLevelName(TRACE, 'TRACE')
logging.addLevelName(DETAIL, 'DETAIL')

# copied from logging, modified to handle cases for custom log levels
if hasattr(sys, 'frozen'): #support for py2exe
    _srcfile = "execo%slog%s" % (os.sep, __file__[-4:])
elif __file__[-4:].lower() in ['.pyc', '.pyo']:
    _srcfile = __file__[:-4] + '.py'
else:
    _srcfile = __file__
_srcfile = os.path.normcase(_srcfile)

class Logger(logging.getLoggerClass()):

    def fdebug(self, message, *args, **kwargs):
        self.log(FDEBUG, message, *args, **kwargs)

    def iodebug(self, message, *args, **kwargs):
        self.log(IODEBUG, message, *args, **kwargs)

    def trace(self, message, *args, **kwargs):
        self.log(TRACE, message, *args, **kwargs)

    def detail(self, message, *args, **kwargs):
        self.log(DETAIL, message, *args, **kwargs)

    # copied from logging, modified to handle cases for custom log levels
    if sys.version_info >= (3,):
        def findCaller(self, stack_info=False):
            """
            Find the stack frame of the caller so that we can note the source
            file name, line number and function name.
            """
            f = logging.currentframe()
            #On some versions of IronPython, currentframe() returns None if
            #IronPython isn't run with -X:Frames.
            if f is not None:
                f = f.f_back
            rv = "(unknown file)", 0, "(unknown function)", None
            while hasattr(f, "f_code"):
                co = f.f_code
                filename = os.path.normcase(co.co_filename)
                if filename == _srcfile or filename == logging._srcfile:
                    f = f.f_back
                    continue
                sinfo = None
                if stack_info:
                    sio = io.StringIO()
                    sio.write('Stack (most recent call last):\n')
                    traceback.print_stack(f, file=sio)
                    sinfo = sio.getvalue()
                    if sinfo[-1] == '\n':
                        sinfo = sinfo[:-1]
                    sio.close()
                rv = (co.co_filename, f.f_lineno, co.co_name, sinfo)
                break
            return rv
    else:
        def findCaller(self):
            """
            Find the stack frame of the caller so that we can note the source
            file name, line number and function name.
            """
            f = logging.currentframe()
            #On some versions of IronPython, currentframe() returns None if
            #IronPython isn't run with -X:Frames.
            if f is not None:
                f = f.f_back
            rv = "(unknown file)", 0, "(unknown function)"
            while hasattr(f, "f_code"):
                co = f.f_code
                filename = os.path.normcase(co.co_filename)
                if filename == _srcfile or filename == logging._srcfile:
                    f = f.f_back
                    continue
                rv = (co.co_filename, f.f_lineno, co.co_name)
                break
            return rv


# logger is the execo logging object
__default_logger = logging.getLoggerClass()
logging.setLoggerClass(Logger)
logger = logging.getLogger("execo")
"""The execo logger."""
logging.setLoggerClass(__default_logger)
logger_handler = logging.StreamHandler(sys.stdout)


class MyFormatter(logging.Formatter):
    def format(self, record):
        if logger.getEffectiveLevel() < logging.DEBUG:
            self._fmt = ( style.log_header("%(asctime)s ")
                          + "".join([_ansi_styles[attr] for attr in configuration['color_styles'][record.levelno]])
                          + "%(levelname)s " + _ansi_styles['default'] + style.log_header("%(threadName)s - %(funcName)s:")
                          + _ansi_styles['default'] + " %(message)s" )
        elif logger.getEffectiveLevel() == logging.DEBUG:
            self._fmt = ( style.log_header("%(asctime)s ")
                          + "".join([_ansi_styles[attr] for attr in configuration['color_styles'][record.levelno]])
                          + "%(levelname)s " + _ansi_styles['default'] + style.log_header("%(funcName)s:")
                          + _ansi_styles['default'] + " %(message)s" )
        else:
            self._fmt = ( style.log_header("%(asctime)s ")
                          + "".join([_ansi_styles[attr] for attr in configuration['color_styles'][record.levelno]])
                          + "%(levelname)s:"
                          + _ansi_styles['default'] + " %(message)s" )
        if sys.version_info >= (3,2):
            self._style._fmt = self._fmt
        return logging.Formatter.format(self, record)

logger_handler.setFormatter(MyFormatter())
logger.addHandler(logger_handler)
logger.setLevel(configuration.get('log_level'))

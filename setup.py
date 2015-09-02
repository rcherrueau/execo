#!/usr/bin/env python

# Copyright 2009-2015 INRIA Rhone-Alpes, Service Experimentation et
# Developpement
# This file is part of Execo, released under the GNU Lesser Public
# License, version 3 or later.

from distutils.command.clean import clean as _clean
from distutils.dir_util import remove_tree
from distutils import log
import sys, subprocess, os, textwrap, shutil, re

try:
    from setuptools import setup
    from setuptools.command.install import install as _install
    from setuptools.command.build_py import build_py as _build_py
    from setuptools.command.sdist import sdist as _sdist
except:
    from distutils.core import setup
    from distutils.command.install import install as _install
    from distutils.command.build_py import build_py as _build_py
    from distutils.command.sdist import sdist as _sdist

try:
    from sphinx.setup_command import BuildDoc
except:
    pass

def get_git_version():
    # returns git tag / sha as string
    # returns None if not available
    try:
        p = subprocess.Popen(["git", "describe", "--tags", "--dirty", "--always"],
                             stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    except EnvironmentError:
        return None
    version = p.communicate()[0].rstrip()
    if p.returncode != 0:
        return None
    return version

VERSION_PY = """
# Do not edit. This file is originally generated from git information.
# Distribution tarballs contain a pre-generated copy of this file.
__version__ = '%s'
"""

def update_version_py():
    version_filename = "src/execo/_version.py"
    version = get_git_version()
    if version or not os.path.isfile(version_filename):
        if not version: version = "UNKNOWN"
        with open(version_filename, "w") as f:
            f.write(VERSION_PY % version)

def get_version():
    gitversion = get_git_version()
    if gitversion:
        return gitversion
    try:
        with open("src/execo/_version.py") as f:
            for line in f.readlines():
                mo = re.match("__version__ = '([^']*)'", line)
                if mo:
                    version = mo.group(1)
                    return version
    except EnvironmentError:
        pass
    return "UNKNOWN"

def read_file(filename, start_marker = None, end_marker = None):
    s = ""
    with open(filename, "r") as ifh:
        insection = (start_marker == None)
        for line in ifh:
            line = line.rstrip()
            if start_marker != None and line == start_marker and not insection:
                insection = True
                continue
            if end_marker != None and line == end_marker and insection:
                insection = False
                break
            if insection:
                s += line + "\n"
    return s.rstrip()

def extract_conf(fh, source_file, marker):
    s = read_file(source_file, "# _STARTOF_ " + marker, "# _ENDOF_ " + marker)
    s = textwrap.dedent(s)
    for l in s.splitlines():
        print >> fh, "# " + l
    print >> fh, "\n"

def generate_conf_template(datadir):
    try:
        os.makedirs(os.path.join(datadir, "share", "execo"))
    except os.error:
        pass
    with open(os.path.join(datadir, "share", "execo", "execo.conf.py.sample"), "w") as fh:
        print >> fh, "# sample execo user configuration"
        print >> fh, "# copy this file to ~/.execo.conf.py and edit/modify it appropriately"
        print >> fh
        print >> fh, "# import logging, os, sys"
        print >> fh
        extract_conf(fh, os.path.join("src", "execo", "config.py"), "configuration")
        extract_conf(fh, os.path.join("src", "execo", "config.py"), "default_connection_params")
        extract_conf(fh, os.path.join("src", "execo_g5k", "config.py"), "g5k_configuration")
        extract_conf(fh, os.path.join("src", "execo_g5k", "config.py"), "default_frontend_connection_params")
        extract_conf(fh, os.path.join("src", "execo_g5k", "config.py"), "default_oarsh_oarcp_params")

def copy_additional_files(datadir):
    try:
        os.makedirs(os.path.join(datadir, "share", "execo"))
    except os.error:
        pass

class build_py(_build_py):
    def run(self):
        update_version_py()
        _build_py.run(self)

class sdist(_sdist):
    def run(self):
        update_version_py()
        _sdist.run(self)

class install(_install):
    def run(self):
        _install.run(self)
        self.execute(generate_conf_template, (self.install_data,),
                     msg = "Generate execo configuration template")
        self.execute(copy_additional_files, (self.install_data,),
                     msg = "Copying additional files")

class install_doc(_install):
    def run(self):
        self.run_command('build_doc')
        build = self.get_finalized_command('build')
        build_dir = os.path.join(os.path.abspath(build.build_base), "sphinx", "html")
        build_dir = os.path.abspath(build_dir)
        self.copy_tree(build_dir, os.path.join(self.install_data, "share", "doc", "execo", "html"))

class clean(_clean):
    def run(self):
        if self.all:
            sphinx_dir = os.path.join(self.build_base, "sphinx")
            if os.path.exists(sphinx_dir):
                remove_tree(sphinx_dir, dry_run = self.dry_run)
            else:
                log.warn("'%s' does not exist -- can't clean it",
                         sphinx_dir)
            def rm(f):
                try:
                    os.unlink(f)
                    log.info("removing " + f)
                except:
                    log.warn("can't clean " + f),
            rm("MANIFEST")
            rm("src/execo/_version.py")
        _clean.run(self)

if __name__ == "__main__":

    try:
        cmdclass = { 'build_py': build_py,
                     'sdist': sdist,
                     'install': install,
                     'build_doc': BuildDoc,
                     'install_doc': install_doc,
                     'clean': clean }
    except:
        cmdclass = { 'build_py': build_py,
                     'sdist': sdist,
                     'install': install,
                     'clean': clean }

    name = 'execo'
    version = get_version()

    setup(cmdclass = cmdclass,
          name = name,
          license = 'GNU GPL v3',
          version = version,
          description = read_file("README", "", ""), # first parapgraph delimited by two empty lines
          long_description = read_file("README"),
          author = 'Matthieu Imbert',
          author_email = 'matthieu.imbert@inria.fr',
          url = 'http://execo.gforge.inria.fr',
          package_dir = {'': 'src'},
          packages = [ 'execo', 'execo_g5k', 'execo_engine' ],
          package_data = { 'execo': ['execo-chainput'] },
          classifiers = [ 'Development Status :: 4 - Beta',
                          'Environment :: Console',
                          'Intended Audience :: Developers',
                          'Intended Audience :: Information Technology',
                          'Intended Audience :: Science/Research',
                          'Intended Audience :: System Administrators',
                          'Operating System :: POSIX :: Linux',
                          'Programming Language :: Python :: 2.5',
                          'Topic :: Software Development',
                          'Topic :: System :: Clustering',
                          'Topic :: System :: Distributed Computing'],
          platforms = [ 'unix' ]
          )

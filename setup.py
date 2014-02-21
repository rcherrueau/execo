#!/usr/bin/env python

# Copyright 2009-2014 INRIA Rhone-Alpes, Service Experimentation et
# Developpement
# This file is part of Execo, released under the GNU Lesser Public
# License, version 3 or later.

from distutils.core import setup
from distutils.command.install import install as _install
import sys, subprocess, os, textwrap, shutil

try:
    from sphinx.setup_command import BuildDoc
except:
    pass

def extract_conf(fh, source_file, marker):
    s = ""
    with open(source_file, "r") as ifh:
        insection = False
        for line in ifh:
            line = line.rstrip()
            if line == "# _STARTOF_ " + marker:
                insection = True
                continue
            if line == "# _ENDOF_ " + marker:
                insection = False
                continue
            if insection:
                s += line + "\n"
    s = textwrap.dedent(s)
    for l in s.splitlines():
        print >> fh, "# " + l
    print >> fh, "\n"

def generate_conf_template(install_base):
    try:
        os.makedirs(os.path.join(install_base, "share", "execo"))
    except os.error:
        pass
    with open(os.path.join(install_base, "share", "execo", "execo.conf.py.sample"), "w") as fh:
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

def copy_additional_files(install_base):
    try:
        os.makedirs(os.path.join(install_base, "share", "execo"))
    except os.error:
        pass
    #shutil.copytree(os.path.join("doc", "code_samples"), os.path.join(install_base, "share", "execo", "code_samples"), symlinks = True)
    #shutil.copytree(os.path.join("build", "sphinx", "html"), os.path.join(install_base, "share", "execo", "documentation"), symlinks = True)

class install(_install):
    def run(self):
        _install.run(self)
        self.execute(generate_conf_template, (self.install_base,),
                     msg = "Generate execo configuration template")
        self.execute(copy_additional_files, (self.install_base,),
                     msg = "Copying additional files")

try:
    cmdclass = { 'install': install,
                 'build_sphinx': BuildDoc }
except:
    cmdclass = { 'install': install }

name = 'execo'
version = '2.2'

with open('README') as f:
    long_description = f.read()

setup(cmdclass = cmdclass,
      name = name,
      license = 'GNU GPL v3',
      version = version,
      description = 'Execo offers a Python API for local or remote, standalone or parallel, '
      'processes execution. It is especially well suited for quickly and easily scripting '
      'workflows of parallel/distributed operations on local or remote hosts: '
      'automate a scientific workflow, conduct computer science experiments, '
      'perform automated tests, etc. The core python package is '
      '``execo``. The ``execo_g5k`` package provides a set of tools and '
      'extensions for the Grid5000 testbed [1]. The ``execo_engine`` package '
      'provides tools to ease the development of computer sciences '
      'experiments.',
      long_description = long_description,
      author = 'Matthieu Imbert',
      author_email = 'matthieu.imbert@inria.fr',
      url = 'http://execo.gforge.inria.fr',
      package_dir = {'': 'src'},
      packages = [ 'execo', 'execo_g5k', 'execo_engine' ],
      scripts = [ 'execo-chainput' ],
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
      platforms = [ 'unix' ],
      command_options={
        'build_sphinx': {
            'project': ('setup.py', name),
            'version': ('setup.py', version),
            'release': ('setup.py', version)}},
      )

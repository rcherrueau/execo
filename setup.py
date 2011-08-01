#!/usr/bin/env python

# Copyright 2009-2011 INRIA Rhone-Alpes, Service Experimentation et
# Developpement
# This file is part of Execo, released under the GNU Lesser Public
# License, version 3 or later.

from distutils.core import setup
import sys

setup(name = 'execo',
      license = 'GNU GPL v3',
      version = '1.3',
      description = 'API for parallel local or remote processes execution',
      long_description = 'This module offers a high level API for parallel'
      'local or remote processes execution with the `Action` class hierarchy,'
      'and a lower level API with the `Process` class, for handling individual'
      'subprocesses.',
      author = 'Matthieu Imbert',
      author_email = 'matthieu.imbert@inria.fr',
      url = 'http://graal.ens-lyon.fr/~mimbert/execo',
      py_modules = [ 'execo', 'execo_g5k', 'g5k_api_tools', 'execo_engine' ],
      scripts = [ 'execo-run',
                  'g5k-deploy',
                  'g5k-show-all-jobs',
                  'g5k-clean-all-jobs',
                  'g5k-show-structure' ],
      data_files = [ ('share/execo/engines',
                      [ 'engines/bag_of_tasks_per_cluster.py']) ],
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
      )

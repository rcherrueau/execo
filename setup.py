#!/usr/bin/env python

from distutils.core import setup
import sys

setup(name = 'execo',
      version = '1.3',
      description = 'API for parallel local or remote processes execution',
      long_description = 'This module offers a high level API for parallel'
      'local or remote processes execution with the `Action` class hierarchy,'
      'and a lower level API with the `Process` class, for handling individual'
      'subprocesses.',
      author = 'Matthieu Imbert',
      author_email = 'matthieu.imbert@inria.fr',
      url = 'http://graal.ens-lyon.fr/~mimbert/execo',
      py_modules = [ 'execo', 'execo_g5k', 'g5k_api_tools' ],
      scripts = [ 'g5k-deploy',
                  'g5k-show-all-jobs',
                  'g5k-clean-all-jobs',
                  'g5k-show-structure' ],
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

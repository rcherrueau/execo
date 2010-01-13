#!/usr/bin/env python

from distutils.core import setup
import sys

pg_py_modules = [
    'execo',
    ]

for i in sys.argv:
    if i == '--g5k':
        pg_py_modules.append('execo_g5k')
        sys.argv.remove(i)
        break
    
setup(name = 'execo',
      version = '1.0',
      packages = None,
      py_modules = pg_py_modules,
      author_email = 'matthieu.imbert@inria.fr',
      )

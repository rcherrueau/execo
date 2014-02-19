# Copyright 2009-2013 INRIA Rhone-Alpes, Service Experimentation et
# Developpement
# This file is part of Execo, released under the GNU Lesser Public
# License, version 3 or later.

.PHONY: all build install doc cleandoc clean dist

PREFIX=/usr/local
PYTHON=python

all: build

build:
	$(PYTHON) setup.py build

install: build
	$(PYTHON) setup.py install --prefix=$(PREFIX)

doc: sphinxdochtml
	$(PYTHON) setup.py build_sphinx

cleandoc:
	rm -rf doc/_build/ doc/_template doc/_templates/

clean: cleandoc
	rm -rf build dist MANIFEST
	find . -name '*.pyc' -exec $(RM) {} \;

dist:
	$(PYTHON) setup.py sdist

# Copyright 2009-2013 INRIA Rhone-Alpes, Service Experimentation et
# Developpement
# This file is part of Execo, released under the GNU Lesser Public
# License, version 3 or later.

.PHONY: all build install doc cleandoc sphinxdoccommon sphinxdochtml sphinxdoclatex cleansphinxdoc clean dist

PREFIX=/usr/local
PYTHON=python

all: build

build:
	$(PYTHON) setup.py build

install: build
	$(PYTHON) setup.py install --prefix=$(PREFIX)

doc: sphinxdochtml

cleandoc: cleansphinxdoc

sphinxdochtml:
	mkdir -p doc/_template
	cd doc ; sphinx-build -b html . _build/html

cleansphinxdoc:
	rm -rf doc/_build/ doc/_template doc/_templates/

clean: cleandoc
	rm -rf build dist MANIFEST
	find . -name '*.pyc' -exec $(RM) {} \;

dist: doc
	$(PYTHON) setup.py sdist

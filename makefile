# Copyright 2009-2013 INRIA Rhone-Alpes, Service Experimentation et
# Developpement
# This file is part of Execo, released under the GNU Lesser Public
# License, version 3 or later.

.PHONY: all build install doc cleandoc sphinxdoccommon sphinxdochtml sphinxdoclatex cleansphinxdoc clean dist

PREFIX=/usr/local
PYTHON=python

all: build

build: execo.conf.py.sample execo-run
	$(PYTHON) setup.py build

install: build
	$(PYTHON) setup.py install --prefix=$(PREFIX)

doc: sphinxdochtml

cleandoc: cleansphinxdoc

sphinxdochtml:
	mkdir -p doc/_template
	sphinx-build -b html doc doc/_build/html

cleansphinxdoc:
	rm -rf doc/_build/ doc/_template doc/_templates/

clean: cleandoc
	rm -rf build dist MANIFEST execo.conf.py.sample execo-run
	find . -name '*.pyc' -exec $(RM) {} \;

dist: doc
	$(PYTHON) setup.py sdist

extract = ( sed -n '/^\# _STARTOF_ $(2)/,/^\# _ENDOF_ $(2)/p' $(1) | grep -v ^\# | $(PYTHON) -c 'import sys, textwrap; print textwrap.dedent(sys.stdin.read())' | sed 's/^\(.*\)$$/\# \1/' ; echo )

execo.conf.py.sample: execo.conf.py.sample.in src/execo/config.py src/execo_g5k/config.py
	cp $< $@
	$(call extract,src/execo/config.py,configuration) >> $@
	$(call extract,src/execo/config.py,default_connection_params) >> $@
	$(call extract,src/execo_g5k/config.py,g5k_configuration) >> $@
	$(call extract,src/execo_g5k/config.py,default_frontend_connection_params) >> $@
	$(call extract,src/execo_g5k/config.py,default_oarsh_oarcp_params) >> $@

do_subst = sed -e 's,[@]prefix[@],$(PREFIX),g'

execo-run: execo-run.in
	$(do_subst) < $< > $@
	chmod a+x execo-run

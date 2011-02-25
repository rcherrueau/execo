.PHONY: all build install doc cleandoc sphinxdoccommon sphinxdochtml sphinxdoclatex cleansphinxdoc epydoc cleanepydoc check clean dist

PREFIX=/usr/local

all: build

build: execo.conf.py.sample
	python setup.py build

install: build
	python setup.py install --prefix=$(PREFIX)

doc: sphinxdochtml

cleandoc: cleansphinxdoc cleanepydoc

sphinxdoccommon:
	./g5k_deploy --help > doc/g5k_deploy.txt

sphinxdochtml: sphinxdoccommon
	mkdir -p doc/_static doc/_template
	$(MAKE) -C doc html

sphinxdoclatex: sphinxdoccommon
	mkdir -p doc/_static doc/_template
	$(MAKE) -C doc latex
	$(MAKE) -C doc/_build/latex all-pdf

cleansphinxdoc:
	$(MAKE) -C doc clean
	rm -f doc/g5k_deploy.txt

epydoc: epydoc/redirect.html

epydoc/redirect.html: execo.py execo_g5k.py g5k_api_tools.py
	epydoc --docformat "restructuredtext en" -v --html --output=epydoc execo.py execo_g5k.py g5k_api_tools.py

cleanepydoc:
	rm -rf epydoc

check:
	python execo.py
	python test_execo.py

clean: cleandoc
	rm -rf build dist *.pyc MANIFEST execo.conf.py.sample

dist: doc
	python setup.py sdist

extract = ( sed -n '/^\# _STARTOF_ $(2)/,/^\# _ENDOF_ $(2)/p' $(1) | grep -v ^\# | sed 's/^\(.*\)$$/\# \1/' ; echo )

execo.conf.py.sample: execo.conf.py.sample.in execo.py execo_g5k.py
	#echo $(call extract,execo.py,configuration)
	cp $< $@
	$(call extract,execo.py,configuration) >> $@
	$(call extract,execo.py,default_connexion_params) >> $@
	$(call extract,execo_g5k.py,g5k_configuration) >> $@
	$(call extract,execo_g5k.py,default_oarsh_oarcp_params) >> $@

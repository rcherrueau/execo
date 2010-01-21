.PHONY: all build install doc cleandoc sphinxdoc cleansphinxdoc epydoc cleanepydoc check clean dist

PREFIX=/usr/local

all: build

build:
	python setup.py build

install: build
	python setup.py install --prefix=$(PREFIX)

doc: sphinxdoc

cleandoc: cleansphinxdoc cleanepydoc

sphinxdoc:
	mkdir -p doc/_static doc/_template
	$(MAKE) -C doc html

cleansphinxdoc:
	$(MAKE) -C doc clean

epydoc: epydoc/redirect.html

epydoc/redirect.html: execo.py execo_g5k.py
	epydoc --docformat "restructuredtext en" -v --html --output=epydoc execo.py execo_g5k.py

cleanepydoc:
	rm -rf epydoc

check:
	python execo.py
	python test_execo.py

clean: cleandoc
	rm -rf build dist *.pyc MANIFEST 

dist: doc
	python setup.py sdist

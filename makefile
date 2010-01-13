.PHONY: all build install clean doc cleandoc check dist

all: build doc

build:
	python setup.py build --g5k

install: build
	python setup.py install --g5k --prefix=$(PREFIX)

doc: cleandoc
	epydoc --docformat "restructuredtext en" -v --html --output=epydoc execo.py execo_g5k.py

cleandoc:
	rm -rf epydoc

check:
	python execo.py
	python test_execo.py

clean: cleandoc
	rm -rf build dist *.pyc MANIFEST 

dist:
	python setup.py sdist

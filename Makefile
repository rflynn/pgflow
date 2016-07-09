# vim: set ts=8 noet:

test: install
	venv/bin/nosetests --with-coverage --cover-erase --cover-html

install: dep queryparser

dep: install.sh Makefile
	/bin/bash install.sh

queryparser:
	$(MAKE) -C queryparser

clean:
	$(MAKE) -C queryparser clean

.PHONY: test install dep queryparser clean

# vim: set ts=8 noet:

test:
	venv/bin/nosetests --with-coverage --cover-erase --cover-html

install: vendor

vendor:
	/bin/bash install.sh

.PHONY: test install vendor

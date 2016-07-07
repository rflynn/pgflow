# vim: set ts=8 noet:

test:
	venv/bin/nosetests --with-coverage --cover-erase --cover-html

install: vendor

vendor:
	/bin/bash install.sh

example: example.c vendor/libpg_query/libpg_query.a
	cc -Ivendor/libpg_query -Lvendor/libpg_query -lpg_query -o example example.c

queryparser:
	$(MAKE) -C queryparser

clean:
	$(MAKE) -C queryparser clean

.PHONY: test install vendor queryparser clean

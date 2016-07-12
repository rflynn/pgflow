# vim: set ts=8 noet:

test: queryparser
	venv/bin/nosetests --nocapture --with-coverage --cover-erase --cover-html tests/*

install: venv/bin/pip queryparser

venv/bin/pip: venv
	venv/bin/pip install --upgrade pip

queryparser:
	$(MAKE) -C queryparser

dep:
	$(MAKE) -C dep

venv: requirements.txt
	[ -d venv ] || { virtualenv -p python3 venv 2>/dev/null || python3 -m venv venv; }
	venv/bin/pip install -r requirements.txt
	touch requirements.txt

clean:
	$(MAKE) -C queryparser clean
	$(MAKE) -C dep clean

distclean:
	$(MAKE) -C queryparser distclean
	$(MAKE) -C dep distclean

.PHONY: test dep queryparser clean distclean

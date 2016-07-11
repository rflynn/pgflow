# vim: set ts=8 noet:

all: libpg_query/libpg_query.a

libpg_query/libpg_query.a: libpg_query
	$(MAKE) -C libpg_query -j $(sysctl -n hw.ncpu || nproc || grep -c ^processor /proc/cpuinfo || echo 1)

libpg_query: Makefile
	[ -d libpg_query ] || git clone https://github.com/lfittl/libpg_query.git

clean:
	$(MAKE) -C libpg_query clean

distclean:
	$(MAKE) -C libpg_query distclean

.PHONY: all clean distclean

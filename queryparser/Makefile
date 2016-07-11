# vim: set ts=8 noet:

PATH_LIBPG_QUERY := ../dep/libpg_query
INCLUDES_LIBPG_QUERY := -I $(PATH_LIBPG_QUERY) -I $(PATH_LIBPG_QUERY)/src -I $(PATH_LIBPG_QUERY)/src/postgres/include
CFLAGS := -W -Wall -pedantic -Os -fpic $(INCLUDES_LIBPG_QUERY) -ggdb
LDFLAGS := -I$(PATH_LIBPG_QUERY) -L$(PATH_LIBPG_QUERY) -lpg_query

CC ?= cc

queryparser: queryparser.o ../dep/libpg_query/libpg_query.a
	$(CC) -o $@ $(CFLAGS) -g queryparser.c ../dep/libpg_query/libpg_query.a

queryparser2: queryparser.o $(PATH_LIBPG_QUERY)/libpg_query.a
	$(LD) $(LDFLAGS) -o $@ queryparser.o $(PATH_LIBPG_QUERY)/libpg_query.a

queryparser.o: queryparser.c

$(PATH_LIBPG_QUERY)/libpg_query.a:
	$(MAKE) -C ../dep libpg_query/libpg_query.a

clean:
	$(RM) *.o queryparser

distclean: clean

.PHONY: clean distclean


# vim: set ts=8 noet:

PATH_LIBPG_QUERY := ../dep/libpg_query
CFLAGS := -W -Wall -Werror -pedantic -Os
LDFLAGS := -I$(PATH_LIBPG_QUERY) -L$(PATH_LIBPG_QUERY) -lpg_query

queryparser: queryparser.c $(PATH_LIBPG_QUERY)/libpg_query.a
	cc $(CFLAGS) $(LDFLAGS) -o queryparser queryparser.c

$(PATH_LIBPG_QUERY)/libpg_query.a:
	$(MAKE) -C ../dep libpg_query/libpg_query.a

clean:
	$(RM) *.o queryparser

distclean: clean

.PHONY: clean distclean


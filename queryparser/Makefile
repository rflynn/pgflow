# vim: set ts=8 noet:

PATH_LIBPG_QUERY := ../vendor/libpg_query
CFLAGS := -W -Wall -Werror -pedantic -Os
LDFLAGS := -I$(PATH_LIBPG_QUERY) -L$(PATH_LIBPG_QUERY) -lpg_query

queryparser: queryparser.c $(PATH_LIBPG_QUERY)/libpg_query.a
	cc $(CFLAGS) $(LDFLAGS) -o queryparser queryparser.c

clean:
	$(RM) *.o queryparser


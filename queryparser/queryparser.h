/* ex: set ts=4 et: */
/*
 *
 */

#include <wchar.h>
#include <stdio.h>
#include <pg_query.h>


struct pypgsql {
    char *in;
    PgQueryParseResult out;
};

void pypgsql_init   (struct pypgsql *);
void pypgsql_destroy(struct pypgsql *);



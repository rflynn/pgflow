/* ex: set ts=4 et: */
/*

Copyright (c) 2016, Ryan Flynn <parseerror@gmail.com> All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are
permitted provided that the following conditions are met:

   1. Redistributions of source code must retain the above copyright notice, this list of
      conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright notice, this list
      of conditions and the following disclaimer in the documentation and/or other materials
      provided with the distribution.

THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

The views and conclusions contained in the software and documentation are those of the
authors and should not be interpreted as representing official policies, either expressed
or implied, of Ryan Flynn.

*/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "queryparser.h"


void pypgsql_init(struct pypgsql *p)
{
    memset(p, 0, sizeof *p);
}

void pypgsql_destroy(struct pypgsql *p)
{
    free(p->in);
    p->in = NULL;
    pg_query_free_parse_result(p->out);
}

static int read_stmts_sql(struct pypgsql *p, FILE *f)
{
    char buf[BUFSIZ];
    char *input = NULL;
    size_t inlen = 0;
    pypgsql_init(p);
    while (!feof(f) && fgets(buf, sizeof buf, f)) {
        size_t buflen;
        buflen = strlen(buf);
        if (buflen > 1 && buf[0] == '-' && strcmp(buf, "-- queryparser flush\n") == 0) {
            break;
        }
        {
            char *tmp;
            tmp = realloc(input, inlen + buflen + 1);
            if (!tmp) {
                break;
            }
            input = tmp;
        }
        strcat(input, buf);
        inlen += buflen;
    }
    p->in = input;
    return !!p->in;
}

static void write_stmts_json(struct pypgsql *p, FILE *f)
{
    p->out = pg_query_parse(p->in);
    fputs(p->out.parse_tree, f);
    fputc('\n', f);
    fflush(f);
    pypgsql_destroy(p);
}

int main(void)
{
    struct pypgsql p;
    setvbuf(stdout, NULL, _IONBF, BUFSIZ);
    while (read_stmts_sql(&p, stdin))
        write_stmts_json(&p, stdout);
    return 0;
}

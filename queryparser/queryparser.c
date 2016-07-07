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
    free(p->query);
    p->query = NULL;
    free(p->in);
    p->in = NULL;
    memset(&p->out, 0, sizeof p->out);
}

static wchar_t * readinput(FILE *f)
{
    wchar_t buf[BUFSIZ];
    wchar_t *input = NULL;
    size_t inlen = 0;
    while (fgetws(buf, sizeof buf, f)) {
        size_t tmplen;
        wchar_t *tmp;
        tmplen = inlen + wcslen(buf) + 1;
        tmp = realloc(input, tmplen * sizeof *tmp);
        if (!tmp) {
            break;
        }
        input = tmp;
        wcscat(input, buf);
    }
    return input;
}

int main(void)
{
    struct pypgsql p;
    size_t maxbytes;

    pypgsql_init(&p);

    p.query = readinput(stdin);

    /* convert input string and parse */
    maxbytes = (wcslen(p.query) + 1) * 4;
    p.in = malloc(maxbytes);
    assert(p.in);
    wcstombs(p.in, p.query, maxbytes);
    p.out = pg_query_parse(p.in);

    //printf("%s\n", p.out.parse_tree);
    fputs(p.out.parse_tree, stdout);

    pypgsql_destroy(&p);

    return 0;
}

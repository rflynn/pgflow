#!/bin/bash

set -x
set -e
set -o pipefail

which python3

if ! [ -d venv ]
then
    virtualenv -p python3 venv 2>/dev/null || python3 -m venv venv
fi
venv/bin/pip3 install -r requirements.txt

mkdir -p vendor && cd vendor

echo build queryparser...
test -d queryparser || git clone https://github.com/pganalyze/queryparser.git
cd queryparser
if ! test -f queryparser
then
    echo Build PostgreSQL...
    # NOTE: happened to get e18ecb8dd973a169062b1a528aa1fbc160b2e3eb
    test -d postgresql || git clone --depth 1 https://github.com/pganalyze/postgres.git postgresql
    cd postgresql
    if ! test -d ./src/backend/postgres
    then
        ./configure
        make -j 2
    fi
    cd ..
    echo Build Queryparser binary
    ./build.sh
fi

# assert
test "$(echo 'SELECT 1' | ./queryparser --json)" = '[{"SELECT": {"distinctClause": null, "intoClause": null, "targetList": [{"RESTARGET": {"name": null, "indirection": null, "val": {"A_CONST": {"val": 1, "location": 7}}, "location": 7}}], "fromClause": null, "whereClause": null, "groupClause": null, "havingClause": null, "windowClause": null, "valuesLists": null, "sortClause": null, "limitOffset": null, "limitCount": null, "lockingClause": null, "withClause": null, "op": 0, "all": false, "larg": null, "rarg": null}}]'

echo ok

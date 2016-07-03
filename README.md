
# Run Me

```sh
/bin/bash install.sh # takes a while...
venv/bin/nosetests
```

# Summary

This project visualizes flow of data through a PostgreSQL database in real-time.

existing DDL --+
queries -------'---> query log -> qlogmon -> app -> vis


# Implementation Plan

1. confidently parse postgresql-flavored sql
2. parse DDL such as Views and Matviews to understand source/destinations
3. parse arbitrary queries, sift out applicable ones
4. condense queries to a queryx (timespan, [(source, dest, quantity)])
5. stream queryx over websocket
6. visualize results
6. document procedures for...
    1. enabling query logging
    2. enabling off-site monitoring (ssh
7. handle historical replay via a timeline of some sort


```sh

ssh -t user@remote_host tail -f /some/file
maybe mosh instead?

```


# References

1. https://en.wikipedia.org/wiki/Dataflow
2. https://www.postgresql.org/docs/9.5/static/runtime-config-logging.html
3. https://github.com/pganalyze/queryparser
4. https://wiki.postgresql.org/wiki/Query_Parsing
5. https://bost.ocks.org/mike/sankey/
6. https://github.com/vidalab/vertical_sankey


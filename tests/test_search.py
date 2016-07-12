
from pprint import pprint
import unittest

from nose.tools import assert_raises

from pgflow import SelectStmt, Stmts, WithClause


class TestSearch(unittest.TestCase):

    def test_search_false(self):
        s = Stmts.parse('select 1')
        self.assertEqual([], s.search(lambda _: False))

    def test_search_class(self):
        s = Stmts.parse('select 1')
        # print(s)
        self.assertNotEqual([], s.searchclass(SelectStmt))

    def test_search_matview(self):

        sql = '''
drop materialized view if exists matview;
create materialized view matview as
with
a as (select t1."id" id_alias from table1 t1),
b as (select 2)
select * from a
union all
select * from b;'''

        s = Stmts.parse(sql)
        w = s.searchclass(WithClause)
        # pprint(w)

        self.assertNotEqual([], w)
        self.assertEqual(['a', 'b'], [c.ctename for c in w[0].ctes])


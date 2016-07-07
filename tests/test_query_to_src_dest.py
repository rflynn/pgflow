
import unittest
from nose.tools import eq_, assert_raises

from pgflow import Stmt, sql2json, UnhandledStmt



class TestSql2Json(unittest.TestCase):

    def test_none(self):
        assert_raises(TypeError, sql2json, None)

    def test_empty(self):
        self.assertEqual([], sql2json(''))

    def test_invalid_sql(self):
        self.assertEqual([], sql2json('invalid'))


class TestQuery2Stmt(unittest.TestCase):

    maxDiff = None

    def test_update_val_1(self):
        tree = sql2json('update foo set bar=1')
        s = Stmt.from_tree(tree[0])
        #print(s)
        self.assertEqual(repr(s), """\
Update:
  relation: RangeVar:
      inhOpt: 2
      location: 7
      relname: 'foo'
      relpersistence: 'p'
  targetList: [
    ResTarget:
      location: 15
      name: 'bar'
      val: AConst:
          location: 19
          val: Integer:
              ival: 1
  ]
""")
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), [])
        self.assertEqual(s.get_dest(), ['foo'])

    def test_update_select_1(self):
        tree = sql2json("""UPDATE address SET cid = customers.id FROM customers WHERE customers.id = address.id""")
        s = Stmt.from_tree(tree[0])
        print(s)
        self.assertEqual(repr(s), """\
Update:
  fromClause: FromClause:
      [
        RangeVar:
          inhOpt: 2
          location: 43
          relname: 'customers'
          relpersistence: 'p'
      ]
  relation: RangeVar:
      inhOpt: 2
      location: 7
      relname: 'address'
      relpersistence: 'p'
  targetList: [
    ResTarget:
      location: 19
      name: 'cid'
      val: ColumnRef:
          fields: [
            String:
              str: 'customers'
            String:
              str: 'id'
          ]
          location: 25
  ]
  whereClause: AExpr:
      kind: 0
      lexpr: ColumnRef:
          fields: [
            String:
              str: 'customers'
            String:
              str: 'id'
          ]
          location: 59
      location: 72
      name: [
        String:
          str: '='
      ]
      rexpr: ColumnRef:
          fields: [
            String:
              str: 'address'
            String:
              str: 'id'
          ]
          location: 74
""")
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), ['customers'])
        self.assertEqual(s.get_dest(), ['address'])

    def test_select_select_1_select_2(self):
        tree = sql2json('select (select 1), (select 2);')
        s = Stmt.from_tree(tree[0])
        print(s)
        self.assertEqual(repr(s), """\
Select:
  op: 0
  targetList: [
    ResTarget:
      location: 7
      val: SubLink:
          location: 7
          subLinkType: 4
          subselect: Select:
              op: 0
              targetList: [
                ResTarget:
                  location: 15
                  val: AConst:
                      location: 15
                      val: Integer:
                          ival: 1
              ]
    ResTarget:
      location: 19
      val: SubLink:
          location: 19
          subLinkType: 4
          subselect: Select:
              op: 0
              targetList: [
                ResTarget:
                  location: 27
                  val: AConst:
                      location: 27
                      val: Integer:
                          ival: 2
              ]
  ]
""")
        self.assertEqual(s.maybe_src_dest(), False)
        self.assertEqual(s.get_src(), [])
        self.assertEqual(s.get_dest(), [])

    def test_insert_into_values_raw(self):
        tree = sql2json('insert into t1 values (1)')
        s = Stmt.from_tree(tree[0])
        # print(s)
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), [])
        self.assertEqual(s.get_dest(), ['t1'])

    def test_insert_into_values_subselect(self):
        tree = sql2json('insert into t1 values ((select 1 from t2))')
        s = Stmt.from_tree(tree[0])
        # print(s)
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), ['t2'])
        self.assertEqual(s.get_dest(), ['t1'])

    def test_insert_into_select_star_1(self):
        tree = sql2json('insert into t1 select * from t2')
        s = Stmt.from_tree(tree[0])
        #print(s)
        self.assertEqual(repr(s), """\
InsertStmt:
  relation: RangeVar:
      inhOpt: 2
      location: 12
      relname: 't1'
      relpersistence: 'p'
  selectStmt: Select:
      fromClause: FromClause:
          [
            RangeVar:
              inhOpt: 2
              location: 29
              relname: 't2'
              relpersistence: 'p'
          ]
      op: 0
      targetList: [
        ResTarget:
          location: 22
          val: ColumnRef:
              fields: [
                AStar:
              ]
              location: 22
      ]
""")
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), ['t2'])
        self.assertEqual(s.get_dest(), ['t1'])

    def test_create_view_1(self):
        tree = sql2json('create view v1 as select x, y, z from t1 join t2 using (foo_id) left join t3 using (bar_id)')
        s = Stmt.from_tree(tree[0])
        # print(s)
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), ['t1', 't2', 't3'])
        self.assertEqual(s.get_dest(), ['v1'])

    def test_create_matview_cte_1(self):
        sql = 'create materialized view matview as with a as (select t1."id" id_alias from table1 t1), b as (select 2) select * from a union all select * from b;'
        tree = sql2json(sql)
        s = Stmt.from_tree(tree[0])
        print(s)
        self.assertEqual(repr(s), """\
CreateTableAs:
  into: IntoClause:
      onCommit: 0
      rel: RangeVar:
          inhOpt: 2
          location: 25
          relname: 'matview'
          relpersistence: 'p'
  query: Select:
      all: True
      larg: Select:
          fromClause: FromClause:
              [
                RangeVar:
                  inhOpt: 2
                  location: 118
                  relname: 'a'
                  relpersistence: 'p'
              ]
          op: 0
          targetList: [
            ResTarget:
              location: 111
              val: ColumnRef:
                  fields: [
                    AStar:
                  ]
                  location: 111
          ]
      op: 1
      rarg: Select:
          fromClause: FromClause:
              [
                RangeVar:
                  inhOpt: 2
                  location: 144
                  relname: 'b'
                  relpersistence: 'p'
              ]
          op: 0
          targetList: [
            ResTarget:
              location: 137
              val: ColumnRef:
                  fields: [
                    AStar:
                  ]
                  location: 137
          ]
      withClause: WithClause:
          ctes: [
            CommonTableExpr:
              ctename: 'a'
              ctequery: Select:
                  fromClause: FromClause:
                      [
                        RangeVar:
                          alias: Alias:
                              aliasname: 't1'
                          inhOpt: 2
                          location: 76
                          relname: 'table1'
                          relpersistence: 'p'
                      ]
                  op: 0
                  targetList: [
                    ResTarget:
                      location: 54
                      name: 'id_alias'
                      val: ColumnRef:
                          fields: [
                            String:
                              str: 'id'
                            String:
                              str: 't1'
                          ]
                          location: 54
                  ]
              location: 41
            CommonTableExpr:
              ctename: 'b'
              ctequery: Select:
                  op: 0
                  targetList: [
                    ResTarget:
                      location: 101
                      val: AConst:
                          location: 101
                          val: Integer:
                              ival: 2
                  ]
              location: 88
          ]
          location: 36
  relkind: 22
""")
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), ['table1'])
        self.assertEqual(s.get_dest(), ['matview'])

    def test_create_matview_cte_table_twice_returned_once(self):
        sql = 'create materialized view matview as with a as (select t1.x id from table1 t1), b as (select 2 from table1) select * from b;'
        tree = sql2json(sql)
        s = Stmt.from_tree(tree[0])
        # print(s)
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), ['table1'])
        self.assertEqual(s.get_dest(), ['matview'])

    def test_create_matview_cte_two_schemas_one_tablename(self):
        sql = 'create materialized view matview as with a as (select t1.x id from schema1.table1 t1), b as (select 2 from schema2.table1) select * from b;'
        tree = sql2json(sql)
        s = Stmt.from_tree(tree[0])
        # print(s)
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), ['schema1.table1', 'schema2.table1'])
        self.assertEqual(s.get_dest(), ['matview'])

    def test_create_matview_cte_no_schema_equivalent_to_public(self):
        sql = 'create materialized view matview as with a as (select id from public.table1), b as (select id from table1) select * from a;'
        tree = sql2json(sql)
        s = Stmt.from_tree(tree[0])
        # print(s)
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), ['table1'])
        self.assertEqual(s.get_dest(), ['matview'])

    def test_create_matview_cte_subquery(self):
        sql = 'create materialized view matview as with a as (select id from (select id from (select id from public.table1) y) x) select * from a;'
        tree = sql2json(sql)
        s = Stmt.from_tree(tree[0])
        # print(s)
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), ['table1'])
        self.assertEqual(s.get_dest(), ['matview'])

    def test_matview_refresh(self):
        sql = 'refresh materialized view v1;'
        tree = sql2json(sql)
        s = Stmt.from_tree(tree[0])
        # print(s)
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), [])
        self.assertEqual(s.get_dest(), ['v1'])

    def test_unhandled_lock(self):
        sql = 'lock table foo;'
        tree = sql2json(sql)
        # print(tree)
        s = Stmt.from_tree(tree[0])
        # print(s)
        self.assertEqual(UnhandledStmt, type(s))
        self.assertEqual(s.maybe_src_dest(), False)
        self.assertEqual(s.get_src(), [])
        self.assertEqual(s.get_dest(), [])

    def test_copy_table_from_file(self):
        sql = "COPY country FROM '/path/to/foo.csv';"
        tree = sql2json(sql)
        # print(tree)
        s = Stmt.from_tree(tree[0])
        print(s)
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), ['/path/to/foo.csv'])
        self.assertEqual(s.get_dest(), ['country'])


class TestQuery2Json(unittest.TestCase):

    maxDiff = None

    def test_select_1(self):
        print(sql2json('select 1'))
        self.assertEqual(sql2json('select 1'),
                         [{'SelectStmt': {'targetList': [{'ResTarget': {'val': {'A_Const': {'val': {'Integer': {'ival': 1}}, 'location': 7}}, 'location': 7}}], 'op': 0}}])

    def test_insert_values(self):
        self.assertEqual(sql2json('insert into table1 values (1)'),
                         [{'InsertStmt': {'relation': {'RangeVar': {'inhOpt': 2,
                                           'location': 12,
                                           'relname': 'table1',
                                           'relpersistence': 'p'}},
                 'selectStmt': {'SelectStmt': {'op': 0,
                                               'valuesLists': [[{'A_Const': {'location': 27,
                                                                             'val': {'Integer': {'ival': 1}}}}]]}}}}])

    def test_insert_select(self):
        self.assertEqual(sql2json('insert into table1 select * from table2'),
                         [{'InsertStmt': {'relation': {'RangeVar': {'inhOpt': 2,
                                           'location': 12,
                                           'relname': 'table1',
                                           'relpersistence': 'p'}},
                 'selectStmt': {'SelectStmt': {'fromClause': [{'RangeVar': {'inhOpt': 2,
                                                                            'location': 33,
                                                                            'relname': 'table2',
                                                                            'relpersistence': 'p'}}],
                                               'op': 0,
                                               'targetList': [{'ResTarget': {'location': 26,
                                                                             'val': {'ColumnRef': {'fields': [{'A_Star': {}}],
                                                                                                   'location': 26}}}}]}}}}])

    def test_create_view_simple_1(self):
        eq_(sql2json("create view v1 as select foo, bar from baz where quux in ('a','b','c') or quuz is not null;"),
                    [{'ViewStmt': {'query': {'SelectStmt': {'op': 0, 'whereClause': {'BoolExpr': {'args': [{'A_Expr': {'location': 54, 'name': [{'String': {'str': '='}}], 'rexpr': [{'A_Const': {'location': 58, 'val': {'String': {'str': 'a'}}}}, {'A_Const': {'location': 62, 'val': {'String': {'str': 'b'}}}}, {'A_Const': {'location': 66, 'val': {'String': {'str': 'c'}}}}], 'lexpr': {'ColumnRef': {'location': 49, 'fields': [{'String': {'str': 'quux'}}]}}, 'kind': 6}}, {'NullTest': {'arg': {'ColumnRef': {'location': 74, 'fields': [{'String': {'str': 'quuz'}}]}}, 'nulltesttype': 1, 'location': 79}}], 'boolop': 1, 'location': 71}}, 'fromClause': [{'RangeVar': {'relname': 'baz', 'relpersistence': 'p', 'location': 39, 'inhOpt': 2}}], 'targetList': [{'ResTarget': {'location': 25, 'val': {'ColumnRef': {'location': 25, 'fields': [{'String': {'str': 'foo'}}]}}}}, {'ResTarget': {'location': 30, 'val': {'ColumnRef': {'location': 30, 'fields': [{'String': {'str': 'bar'}}]}}}}]}}, 'view': {'RangeVar': {'relname': 'v1', 'relpersistence': 'p', 'location': 12, 'inhOpt': 2}}, 'withCheckOption': 0}}])

    def test_create_matview_cte_simple_1(self):
        self.assertEqual(sql2json('create materialized view matview as with a as (select t1."id" id_alias from table1 t1), b as (select 2) select 3;'),
                         [{'CreateTableAsStmt': {'into': {'IntoClause': {'onCommit': 0, 'rel': {'RangeVar': {'relname': 'matview', 'relpersistence': 'p', 'location': 25, 'inhOpt': 2}}}}, 'relkind': 22, 'query': {'SelectStmt': {'targetList': [{'ResTarget': {'location': 111, 'val': {'A_Const': {'location': 111, 'val': {'Integer': {'ival': 3}}}}}}], 'op': 0, 'withClause': {'WithClause': {'ctes': [{'CommonTableExpr': {'ctequery': {'SelectStmt': {'targetList': [{'ResTarget': {'location': 54, 'val': {'ColumnRef': {'fields': [{'String': {'str': 't1'}}, {'String': {'str': 'id'}}], 'location': 54}}, 'name': 'id_alias'}}], 'fromClause': [{'RangeVar': {'relname': 'table1', 'relpersistence': 'p', 'alias': {'Alias': {'aliasname': 't1'}}, 'location': 76, 'inhOpt': 2}}], 'op': 0}}, 'location': 41, 'ctename': 'a'}}, {'CommonTableExpr': {'ctequery': {'SelectStmt': {'targetList': [{'ResTarget': {'location': 101, 'val': {'A_Const': {'location': 101, 'val': {'Integer': {'ival': 2}}}}}}], 'op': 0}}, 'location': 88, 'ctename': 'b'}}], 'location': 36}}}}}}])

    def test_matview_refresh(self):
        self.assertEqual(sql2json('refresh materialized view matview1;'),
                         [{'RefreshMatViewStmt': {'relation': {'RangeVar': {'inhOpt': 2,
                                                   'location': 26,
                                                   'relname': 'matview1',
                                                   'relpersistence': 'p'}}}}])

    def test_copy_table_from_file(self):
        self.assertEqual(sql2json("COPY country FROM '/usr1/proj/bray/sql/country_data';"),
                         [{"COPY": {"relation": {"RANGEVAR": {"schemaname": None, "relname": "country", "inhOpt": 2, "relpersistence": "p", "alias": None, "location": 5}}, "query": None, "attlist": None, "is_from": True, "is_program": False, "filename": "/usr1/proj/bray/sql/country_data", "options": None}}])

    def test_copy_table_from_file(self):
        print(sql2json('create view v1 as select a, b, c from t1 where c > 1'))

        self.assertEqual(sql2json('create view v1 as select a, b, c from t1 where c > 1'),
                                  [{'ViewStmt': {'view': {'RangeVar': {'relname': 'v1', 'relpersistence': 'p', 'location': 12, 'inhOpt': 2}}, 'query': {'SelectStmt': {'op': 0, 'whereClause': {'A_Expr': {'lexpr': {'ColumnRef': {'fields': [{'String': {'str': 'c'}}], 'location': 47}}, 'name': [{'String': {'str': '>'}}], 'kind': 0, 'location': 49, 'rexpr': {'A_Const': {'location': 51, 'val': {'Integer': {'ival': 1}}}}}}, 'targetList': [{'ResTarget': {'location': 25, 'val': {'ColumnRef': {'fields': [{'String': {'str': 'a'}}], 'location': 25}}}}, {'ResTarget': {'location': 28, 'val': {'ColumnRef': {'fields': [{'String': {'str': 'b'}}], 'location': 28}}}}, {'ResTarget': {'location': 31, 'val': {'ColumnRef': {'fields': [{'String': {'str': 'c'}}], 'location': 31}}}}], 'fromClause': [{'RangeVar': {'relname': 't1', 'relpersistence': 'p', 'location': 38, 'inhOpt': 2}}]}}, 'withCheckOption': 0}}])

    def test_create_table_t1(self):
        print(sql2json('create table t1 (id int);'))
        self.assertEqual(sql2json('create table t1 (id int);'),
                                  [{'CreateStmt': {'oncommit': 0, 'relation': {'RangeVar': {'location': 13, 'relname': 't1', 'inhOpt': 2, 'relpersistence': 'p'}}, 'tableElts': [{'ColumnDef': {'typeName': {'TypeName': {'names': [{'String': {'str': 'pg_catalog'}}, {'String': {'str': 'int4'}}], 'typemod': -1, 'location': 20}}, 'location': 17, 'colname': 'id', 'is_local': True}}]}}])


import unittest
from nose.tools import eq_, assert_raises

from pgflow import Stmt, sql2json, UnhandledStmt



class TestSql2Json(unittest.TestCase):

    def test_none(self):
        assert_raises(TypeError, sql2json, None)

    def test_empty(self):
        self.assertEqual(None, sql2json(''))

    def test_invalid_sql(self):
        self.assertEqual(None, sql2json('invalid'))


class TestQuery2Stmt(unittest.TestCase):

    maxDiff = None

    def test_update_val_1(self):
        tree = sql2json('update foo set bar=1')
        s = Stmt.from_tree(tree[0])
        #print(s)
        self.assertEqual(repr(s), """\
Update:
  fromClause: FromClause:
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
          val: 1
  ]
""")
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), [])
        self.assertEqual(s.get_dest(), ['foo'])

    def test_update_select_1(self):
        tree = sql2json("""UPDATE address SET cid = customers.id FROM customers WHERE customers.id = address.id""")
        s = Stmt.from_tree(tree[0])
        # print(s)
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
            'customers'
            'id'
          ]
          location: 25
  ]
  whereClause: AExpr:
      lexpr: ColumnRef:
          fields: [
            'customers'
            'id'
          ]
          location: 59
      location: 72
      name: [
        '='
      ]
      rexpr: ColumnRef:
          fields: [
            'address'
            'id'
          ]
          location: 74
""")
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), ['customers'])
        self.assertEqual(s.get_dest(), ['address'])

    def test_select_select_1_select_2(self):
        tree = sql2json('select (select 1), (select 2);')
        s = Stmt.from_tree(tree[0])
        # print(s)
        self.assertEqual(repr(s), """\
Select:
  all: False
  fromClause: FromClause:
  op: 0
  targetList: [
    ResTarget:
      location: 7
      val: Sublink:
          location: 7
          subLinkType: 4
          subselect: Select:
              all: False
              fromClause: FromClause:
              op: 0
              targetList: [
                ResTarget:
                  location: 15
                  val: AConst:
                      location: 15
                      val: 1
              ]
              valuesLists: ValuesLists:
    ResTarget:
      location: 19
      val: Sublink:
          location: 19
          subLinkType: 4
          subselect: Select:
              all: False
              fromClause: FromClause:
              op: 0
              targetList: [
                ResTarget:
                  location: 27
                  val: AConst:
                      location: 27
                      val: 2
              ]
              valuesLists: ValuesLists:
  ]
  valuesLists: ValuesLists:
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
InsertInto:
  relation: RangeVar:
      inhOpt: 2
      location: 12
      relname: 't1'
      relpersistence: 'p'
  selectStmt: Select:
      all: False
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
      valuesLists: ValuesLists:
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
        #print(s)
        self.assertEqual(repr(s), """\
CreateTableAs:
  into: IntoClause:
      onCommit: 0
      rel: RangeVar:
          inhOpt: 2
          location: 25
          relname: 'matview'
          relpersistence: 'p'
      skipData: False
  is_select_into: False
  query: Select:
      all: True
      fromClause: FromClause:
      larg: Select:
          all: False
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
          valuesLists: ValuesLists:
      op: 1
      rarg: Select:
          all: False
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
          valuesLists: ValuesLists:
      valuesLists: ValuesLists:
      withClause: WithClause:
          ctes: [
            CommonTableExpr:
              ctename: 'a'
              ctequery: Select:
                  all: False
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
                            't1'
                            'id'
                          ]
                          location: 54
                  ]
                  valuesLists: ValuesLists:
              cterecursive: False
              cterefcount: 0
              location: 41
            CommonTableExpr:
              ctename: 'b'
              ctequery: Select:
                  all: False
                  fromClause: FromClause:
                  op: 0
                  targetList: [
                    ResTarget:
                      location: 101
                      val: AConst:
                          location: 101
                          val: 2
                  ]
                  valuesLists: ValuesLists:
              cterecursive: False
              cterefcount: 0
              location: 88
          ]
          location: 36
          recursive: False
  relkind: 18
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
        print(tree)
        s = Stmt.from_tree(tree[0])
        print(s)
        self.assertEqual(UnhandledStmt, type(s))
        self.assertEqual(s.maybe_src_dest(), False)
        self.assertEqual(s.get_src(), [])
        self.assertEqual(s.get_dest(), [])

    def test_copy_table_from_file(self):
        sql = "COPY country FROM '/path/to/foo.csv';"
        tree = sql2json(sql)
        print(tree)
        s = Stmt.from_tree(tree[0])
        print(s)
        self.assertEqual(s.maybe_src_dest(), True)
        self.assertEqual(s.get_src(), ['/path/to/foo.csv'])
        self.assertEqual(s.get_dest(), ['country'])


class TestQuery2Json(unittest.TestCase):

    maxDiff = None

    def test_select_1(self):
        self.assertEqual(sql2json('select 1'),
                         [{"SELECT": {"distinctClause": None, "intoClause": None, "targetList": [{"RESTARGET": {"name": None, "indirection": None, "val": {"A_CONST": {"val": 1, "location": 7}}, "location": 7}}], "fromClause": None, "whereClause": None, "groupClause": None, "havingClause": None, "windowClause": None, "valuesLists": None, "sortClause": None, "limitOffset": None, "limitCount": None, "lockingClause": None, "withClause": None, "op": 0, "all": False, "larg": None, "rarg": None}}])

    def test_insert_values(self):
        self.assertEqual(sql2json('insert into table1 values (1)'),
                         [{"INSERT INTO": {"relation": {"RANGEVAR": {"schemaname": None, "relname": "table1", "inhOpt": 2, "relpersistence": "p", "alias": None, "location": 12}}, "cols": None, "selectStmt": {"SELECT": {"distinctClause": None, "intoClause": None, "targetList": None, "fromClause": None, "whereClause": None, "groupClause": None, "havingClause": None, "windowClause": None, "valuesLists": [[{"A_CONST": {"val": 1, "location": 27}}]], "sortClause": None, "limitOffset": None, "limitCount": None, "lockingClause": None, "withClause": None, "op": 0, "all": False, "larg": None, "rarg": None}}, "returningList": None, "withClause": None}}])

    def test_insert_select(self):
        self.assertEqual(sql2json('insert into table1 select * from table2'),
                         [{"INSERT INTO": {"relation": {"RANGEVAR": {"schemaname": None, "relname": "table1", "inhOpt": 2, "relpersistence": "p", "alias": None, "location": 12}}, "cols": None, "selectStmt": {"SELECT": {"distinctClause": None, "intoClause": None, "targetList": [{"RESTARGET": {"name": None, "indirection": None, "val": {"COLUMNREF": {"fields": [{"A_STAR": {}}], "location": 26}}, "location": 26}}], "fromClause": [{"RANGEVAR": {"schemaname": None, "relname": "table2", "inhOpt": 2, "relpersistence": "p", "alias": None, "location": 33}}], "whereClause": None, "groupClause": None, "havingClause": None, "windowClause": None, "valuesLists": None, "sortClause": None, "limitOffset": None, "limitCount": None, "lockingClause": None, "withClause": None, "op": 0, "all": False, "larg": None, "rarg": None}}, "returningList": None, "withClause": None}}])

    def test_create_view_simple_1(self):
        eq_(sql2json("create view v1 as select foo, bar from baz where quux in ('a','b','c') or quuz is not null;"),
                     [{'VIEWSTMT': {'replace': False, 'aliases': None, 'options': None, 'view': {'RANGEVAR': {'relpersistence': 'p', 'inhOpt': 2, 'relname': 'v1', 'schemaname': None, 'location': 12, 'alias': None}}, 'query': {'SELECT': {'distinctClause': None, 'larg': None, 'rarg': None, 'op': 0, 'intoClause': None, 'lockingClause': None, 'all': False, 'sortClause': None, 'withClause': None, 'targetList': [{'RESTARGET': {'indirection': None, 'val': {'COLUMNREF': {'fields': ['foo'], 'location': 25}}, 'location': 25, 'name': None}}, {'RESTARGET': {'indirection': None, 'val': {'COLUMNREF': {'fields': ['bar'], 'location': 30}}, 'location': 30, 'name': None}}], 'limitCount': None, 'groupClause': None, 'windowClause': None, 'valuesLists': None, 'fromClause': [{'RANGEVAR': {'relpersistence': 'p', 'inhOpt': 2, 'relname': 'baz', 'schemaname': None, 'location': 39, 'alias': None}}], 'limitOffset': None, 'havingClause': None, 'whereClause': {'AEXPR OR': {'lexpr': {'AEXPR IN': {'lexpr': {'COLUMNREF': {'fields': ['quux'], 'location': 49}}, 'rexpr': [{'A_CONST': {'val': 'a', 'location': 58}}, {'A_CONST': {'val': 'b', 'location': 62}}, {'A_CONST': {'val': 'c', 'location': 66}}], 'location': 54, 'name': ['=']}}, 'rexpr': {'NULLTEST': {'arg': {'COLUMNREF': {'fields': ['quuz'], 'location': 74}}, 'nulltesttype': 1, 'argisrow': False}}, 'location': 71}}}}, 'withCheckOption': 0}}])

    def test_create_matview_cte_simple_1(self):
        self.assertEqual(sql2json('create materialized view matview as with a as (select t1."id" id_alias from table1 t1), b as (select 2) select 3;'),
                         [{"CREATE TABLE AS": {"query": {"SELECT": {"distinctClause": None, "intoClause": None, "targetList": [{"RESTARGET": {"name": None, "indirection": None, "val": {"A_CONST": {"val": 3, "location": 111}}, "location": 111}}], "fromClause": None, "whereClause": None, "groupClause": None, "havingClause": None, "windowClause": None, "valuesLists": None, "sortClause": None, "limitOffset": None, "limitCount": None, "lockingClause": None, "withClause": {"WITHCLAUSE": {"ctes": [{"COMMONTABLEEXPR": {"ctename": "a", "aliascolnames": None, "ctequery": {"SELECT": {"distinctClause": None, "intoClause": None, "targetList": [{"RESTARGET": {"name": "id_alias", "indirection": None, "val": {"COLUMNREF": {"fields": ["t1", "id"], "location": 54}}, "location": 54}}], "fromClause": [{"RANGEVAR": {"schemaname": None, "relname": "table1", "inhOpt": 2, "relpersistence": "p", "alias": {"ALIAS": {"aliasname": "t1", "colnames": None}}, "location": 76}}], "whereClause": None, "groupClause": None, "havingClause": None, "windowClause": None, "valuesLists": None, "sortClause": None, "limitOffset": None, "limitCount": None, "lockingClause": None, "withClause": None, "op": 0, "all": False, "larg": None, "rarg": None}}, "location": 41, "cterecursive": False, "cterefcount": 0, "ctecolnames": None, "ctecoltypes": None, "ctecoltypmods": None, "ctecolcollations": None}}, {"COMMONTABLEEXPR": {"ctename": "b", "aliascolnames": None, "ctequery": {"SELECT": {"distinctClause": None, "intoClause": None, "targetList": [{"RESTARGET": {"name": None, "indirection": None, "val": {"A_CONST": {"val": 2, "location": 101}}, "location": 101}}], "fromClause": None, "whereClause": None, "groupClause": None, "havingClause": None, "windowClause": None, "valuesLists": None, "sortClause": None, "limitOffset": None, "limitCount": None, "lockingClause": None, "withClause": None, "op": 0, "all": False, "larg": None, "rarg": None}}, "location": 88, "cterecursive": False, "cterefcount": 0, "ctecolnames": None, "ctecoltypes": None, "ctecoltypmods": None, "ctecolcollations": None}}], "recursive": False, "location": 36}}, "op": 0, "all": False, "larg": None, "rarg": None}}, "into": {"INTOCLAUSE": {"rel": {"RANGEVAR": {"schemaname": None, "relname": "matview", "inhOpt": 2, "relpersistence": "p", "alias": None, "location": 25}}, "colNames": None, "options": None, "onCommit": 0, "tableSpaceName": None, "viewQuery": None, "skipData": False}}, "relkind": 18, "is_select_into": False}}])

    def test_matview_refresh(self):
        self.assertEqual(sql2json('refresh materialized view matview1;'),
                         [{"REFRESHMATVIEWSTMT": {"concurrent": False, "skipData": False, "relation": {"RANGEVAR": {"schemaname": None, "relname": "matview1", "inhOpt": 2, "relpersistence": "p", "alias": None, "location": 26}}}}])

    def test_copy_table_from_file(self):
        self.assertEqual(sql2json("COPY country FROM '/usr1/proj/bray/sql/country_data';"),
                         [{"COPY": {"relation": {"RANGEVAR": {"schemaname": None, "relname": "country", "inhOpt": 2, "relpersistence": "p", "alias": None, "location": 5}}, "query": None, "attlist": None, "is_from": True, "is_program": False, "filename": "/usr1/proj/bray/sql/country_data", "options": None}}])

    def test_copy_table_from_file(self):
        self.assertEqual(sql2json('create view v1 as select a, b, c from t1 where c > 1'),
                                  [{"VIEWSTMT": {"view": {"RANGEVAR": {"schemaname": None, "relname": "v1", "inhOpt": 2, "relpersistence": "p", "alias": None, "location": 12}}, "aliases": None, "query": {"SELECT": {"distinctClause": None, "intoClause": None, "targetList": [{"RESTARGET": {"name": None, "indirection": None, "val": {"COLUMNREF": {"fields": ["a"], "location": 25}}, "location": 25}}, {"RESTARGET": {"name": None, "indirection": None, "val": {"COLUMNREF": {"fields": ["b"], "location": 28}}, "location": 28}}, {"RESTARGET": {"name": None, "indirection": None, "val": {"COLUMNREF": {"fields": ["c"], "location": 31}}, "location": 31}}], "fromClause": [{"RANGEVAR": {"schemaname": None, "relname": "t1", "inhOpt": 2, "relpersistence": "p", "alias": None, "location": 38}}], "whereClause": {"AEXPR": {"name": [">"], "lexpr": {"COLUMNREF": {"fields": ["c"], "location": 47}}, "rexpr": {"A_CONST": {"val": 1, "location": 51}}, "location": 49}}, "groupClause": None, "havingClause": None, "windowClause": None, "valuesLists": None, "sortClause": None, "limitOffset": None, "limitCount": None, "lockingClause": None, "withClause": None, "op": 0, "all": False, "larg": None, "rarg": None}}, "replace": False, "options": None, "withCheckOption": 0}}])

    def test_create_table_t1(self):
        self.assertEqual(sql2json('create table t1 (id int);'),
                                  [{"CREATESTMT": {"relation": {"RANGEVAR": {"schemaname": None, "relname": "t1", "inhOpt": 2, "relpersistence": "p", "alias": None, "location": 13}}, "tableElts": [{"COLUMNDEF": {"colname": "id", "typeName": {"TYPENAME": {"names": ["pg_catalog", "int4"], "typeOid": 0, "setof": False, "pct_type": False, "typmods": None, "typemod": -1, "arrayBounds": None, "location": 20}}, "inhcount": 0, "is_local": True, "is_not_null": False, "is_from_type": False, "storage": None, "raw_default": None, "cooked_default": None, "collClause": None, "collOid": 0, "constraints": None, "fdwoptions": None, "location": 17}}], "inhRelations": None, "ofTypename": None, "constraints": None, "options": None, "oncommit": 0, "tablespacename": None, "if_not_exists": False}}])

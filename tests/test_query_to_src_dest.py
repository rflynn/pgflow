
import unittest

from stmt import Stmt, sql2json



class TestQuery2Stmt(unittest.TestCase):

    maxDiff = None

    def test_select_select_1_select_2(self):
        tree = sql2json('select (select 1), (select 2);')
        s = Stmt.from_tree(tree[0])
        self.assertEqual(repr(s), """Select:
{   'all': False,
    'op': 0,
    'targetList': [   {   'indirection': None,
                          'location': 7,
                          'name': None,
                          'val': {   'location': 7,
                                     'operName': None,
                                     'subLinkType': 4,
                                     'subselect': Select:
{   'all': False,
    'op': 0,
    'targetList': [   {   'indirection': None,
                          'location': 15,
                          'name': None,
                          'val': AConst:
{'location': 15, 'val': 1}}]},
                                     'testexpr': None}},
                      {   'indirection': None,
                          'location': 19,
                          'name': None,
                          'val': {   'location': 19,
                                     'operName': None,
                                     'subLinkType': 4,
                                     'subselect': Select:
{   'all': False,
    'op': 0,
    'targetList': [   {   'indirection': None,
                          'location': 27,
                          'name': None,
                          'val': AConst:
{'location': 27, 'val': 2}}]},
                                     'testexpr': None}}]}""")



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

    def test_matview_cte_simple(self):
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


from .util import flatten


def sql2json(sqlstr):

    import json
    import shlex
    from subprocess import Popen, PIPE

    # NOTE: queryparser is the thinnest of commandline wrappers;
    # to speed this up we'll need to avoid re-launching the program by
    # modify queryparser to stay open and process multiple commands so we
    # can keep the pipe open...
    # or better yet build a proper python c module over it
    p = Popen(['/Users/ryanflynn/src/pgflow/vendor/queryparser/queryparser', '--json'],
              stdin=PIPE, stderr=PIPE, stdout=PIPE)
    try:
        outs, errs = p.communicate((sqlstr + '\n').encode('utf8'))
        return json.loads(outs.decode('utf8')) if not p.returncode else None
    except:
        p.kill()
        raise


class Stmt:

    tree = None

    @staticmethod
    def from_tree(tree):
        # print('from_tree', tree)
        # recursively process tree picking out things we know and passing the
        # rest through unchanged
        if isinstance(tree, dict):
            if len(tree) == 1:
                # unpack dict
                k, v = list(tree.items())[0]
                return Stmt.from_tree_key(k, v)
            else:
                return {k: Stmt.from_tree_key(k, v) for k, v in tree.items()}
        elif isinstance(tree, list):
            return [Stmt.from_tree(t) for t in tree]
        else:
            return tree

    @staticmethod
    def from_tree_key(k, v):
        # print('from_tree_key', k, v)
        c = Stmt.kwclass.get(k)
        if c:
            s = c(v)
        elif isinstance(v, list):
            return [Stmt.from_tree(t) for t in v]
        elif isinstance(v, dict):
            s = Stmt.kwclass.get(k, Stmt.from_tree)(v)
        else:
            s = v
        return s

    def __init__(self, tree):
        if isinstance(tree, dict):
            tree_ = {k: Stmt.from_tree_key(k, v) for k, v in tree.items()}
            for k, v in tree_.items():
                setattr(self, k, v)
        elif isinstance(tree, list):
            tree_ = [Stmt.from_tree(t) for t in tree]
        else:
            tree_ = tree
        self.tree = tree_

    def maybe_src_dest(self):
        return False

    def get_src(self):
        return None

    def get_dest(self):
        return None

    def __repr__(self):
        return self.dump()

    def dump(self, indent=0):
        indentstr = ' ' * 2 * indent
        s = '{}:\n'.format(self.__class__.__name__)
        indent += 1
        indentstr = indentstr + '  '
        if hasattr(self.tree, 'items'):
            for k, v in sorted(self.tree.items()):
                if v is None:
                    continue
                s += '{}{}: '.format(indentstr, k)
                if isinstance(v, Stmt):
                    s += v.dump(indent=indent+1)
                elif isinstance(v, list):
                    if not v:
                        s += '[]\n'
                    else:
                        s += '[\n'
                        if all(isinstance(x, Stmt) for x in v):
                            for x in sorted(v, key=repr):
                                s += indentstr + '  ' + x.dump(indent=indent+1)
                        else:
                            for x in v:
                                if isinstance(x, Stmt):
                                    s += indentstr + '  ' + x.dump(indent=indent+1)
                                else:
                                    s += indentstr + '  ' + repr(x) + '\n'
                        s += indentstr + ']\n'
                elif isinstance(v, dict):
                    s += '\n'
                    for k1, v1 in sorted(v.items()):
                        s += '{}{}: '.format(indentstr, k1)
                        if isinstance(v1, Stmt):
                            s += v1.dump(indent=indent+1)
                        else:
                            s += repr(v1) + '\n'
                else:
                    s += repr(v) + '\n'
        else:
            if not self.tree:
                s += indentstr + '[]\n'
            else:
                s += indentstr + '[\n'
                for x in self.tree:
                    if isinstance(x, Stmt):
                        s += indentstr + '  ' + x.dump(indent=indent+1)
                    else:
                        s += indentstr + '  ' + repr(x) + '\n'
                s += indentstr + ']\n'
        return s

    def filter_tree(self):
        if isinstance(self.tree, dict):
            return {k: v for k, v in self.tree.items() if v is not None}
        else:
            return self.tree


class AConst(Stmt): pass
class Alias(Stmt): pass
class AExprIn(Stmt): pass
class AStar(Stmt): pass
class ColumnRef(Stmt): pass
class CommonTableExpr(Stmt): pass
class Copy(Stmt): pass
class CreateMaterializedView(Stmt): pass
class CreateTable(Stmt): pass

class CreateTableAs(Stmt):
    """
    create materialized view foo as...
    """
    def maybe_src_dest(self):
        return True
    def get_src(self):
        if self.query.withClause:
            return sorted(set(repr(r) for r in
                        flatten(c.ctequery.fromClause.enumerate_tables()
                            for c in self.query.withClause.ctes)))
        return None
    def get_dest(self):
        return [self.into.rel.relname]

class CreateView(Stmt):
    def maybe_src_dest(self):
        return True

class FromClause(Stmt):
    def __iter__(self):
        return iter(self.tree)
    def enumerate_tables(self):
        if isinstance(self.tree, list):
            return flatten(fc.enumerate_from() for fc in self.tree)
        return []

class FromClauseSubquery(Stmt): pass

class InsertInto(Stmt):
    def maybe_src_dest(self):
        return True
    def get_src(self):
        if self.selectStmt:
            return [r.relname
                        for r in self.selectStmt.fromClause
                            if r.relpersistence == 'p']
        return None
    def get_dest(self):
        return [self.relation.relname]

class IntoClause(Stmt): pass
class NullTest(Stmt): pass

class RangeVar(Stmt):
    def enumerate_from(self):
        return TableName(self.relname, schema=self.schemaname)

class RangeSubselect(Stmt):
    def enumerate_from(self):
        if self.subquery:
            return self.subquery.enumerate_from()
        return []

class RefreshMatView(Stmt):
    def maybe_src_dest(self):
        return True
    def get_src(self):
        # src and dest and defined by the matview itself, not the refresh query...
        # to implement this we need to understand the shape of the matview
        raise NotImplementedError
    def get_dest(self):
        raise NotImplementedError

class ResTarget(Stmt): pass

class Select(Stmt):
    def enumerate_from(self):
        return self.fromClause.enumerate_tables()

class Subquery(Stmt):
    def enumerate_from(self):
        if 'SELECT' in self.tree:
            return self.tree['SELECT'].enumerate_from()
        return []

class UnknownStmt(Stmt): pass

class Update(Stmt):
    def maybe_src_dest(self):
        return True
    def get_src(self):
        if self.fromClause:
            return [repr(t)
                        for t in self.fromClause.enumerate_tables()] or None
        return None
    def get_dest(self):
        x = self.relation.relname if self.relation.relpersistence == 'p' else None
        return [x] if x else None

class WithClause(Stmt): pass


# once all Stmt-derived classes are defined
# build mapping within Stmt so it can use them
Stmt.kwclass = {
    # ref: https://www.postgresql.org/docs/9.5/static/sql-commands.html
    'A_CONST': AConst,
    'A_STAR': AStar,
    'ALIAS': Alias,
    'AEXPR IN': AExprIn,
    'COPY': Copy,
    'COLUMNREF': ColumnRef,
    'COMMONTABLEEXPR': CommonTableExpr,
    'CREATE MATERIALIZED VIEW': CreateMaterializedView,
    'CREATESTMT': CreateTable,
    'CREATE TABLE AS': CreateTableAs,
    'fromClause': FromClause,
    'INSERT INTO': InsertInto,
    'INTOCLAUSE': IntoClause,
    'NULLTEST': NullTest,
    'RANGEVAR': RangeVar,
    'RANGESUBSELECT': RangeSubselect,
    'REFRESHMATVIEWSTMT': RefreshMatView,
    'RESTARGET': ResTarget,
    'SELECT': Select,
    'subquery': Subquery,
    'UPDATE': Update,
    'VIEWSTMT': CreateView,
    'WITHCLAUSE': WithClause,
}


class TableName(object):
    def __init__(self, name, schema=None):
        self.name = name
        self.schema = schema
        self.fqname = (schema + '.' if schema else '') + name
        self.normname = (schema + '.'
                            if schema and schema != 'public' else '') + name
    def __repr__(self):
        return self.normname
    def __eq__(self, other):
        return self.normname == other.normname
    def __lt__(self, other):
        return self.normname < other.normname

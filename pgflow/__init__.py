
from .util import flatten, flatten1


class Stmts:

    def __init__(self, stmts):
        self.stmts = stmts

    def search(self, matchfunc):
        return flatten1(s.search(matchfunc) for s in self.stmts)

    def searchclass(self, cls):
        return self.search(lambda x: isinstance(x, cls))

    def __repr__(self):
        return self.__class__.__name__ + ': ' + repr(self.stmts)

    @staticmethod
    def parse(sqlstr):
        return Stmts(Stmt.from_tree(sql2json(sqlstr)))


class Stmt:

    def __init__(self, tree):
        if isinstance(tree, dict):
            tree = {k: Stmt.from_tree_key(k, v) for k, v in tree.items()}
            for k, v in tree.items():
                setattr(self, k, v)
        elif isinstance(tree, list):
            tree = [Stmt.from_tree(t) for t in tree]
        self.tree = tree

    @staticmethod
    def from_tree(tree):
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

    @staticmethod
    def from_tree_key(k, v):
        c = Stmt.kwclass.get(k)
        if c:
            return c(v)
        elif k and k[0] == k[0].upper() and isinstance(v, dict):
            return UnhandledStmt(k, v)
        elif isinstance(v, list):
            return [Stmt.from_tree(t) for t in v]
        elif isinstance(v, dict):
            return Stmt.kwclass.get(k, Stmt.from_tree)(v)
        return v

    def maybe_src_dest(self):
        return False

    def get_src(self):
        return []

    def get_dest(self):
        return []

    def __repr__(self):
        return self.dump()

    def dump(self, indent=0):
        indents = '  '
        indent += 1
        indentstr = indents * indent
        s = self.__class__.__name__ + ':\n'
        if isinstance(self.tree, list):
            s += indentstr + self._dumplist(self.tree, indent=indent)
        elif hasattr(self.tree, 'items'):
            for k, v in sorted(self.tree.items()):
                s += indentstr + k + ': '
                if isinstance(v, Stmt):
                    s += v.dump(indent=indent+1)
                elif isinstance(v, list):
                    s += self._dumplist(v, indent=indent)
                else:
                    s += repr(v) + '\n'
        return s

    def _dumplist(self, v, indent=0):
        indents = '  '
        indentstr = indents * indent
        s = '[\n'
        if all(isinstance(x, Stmt) for x in v):
            s += ''.join(indentstr + indents + x.dump(indent=indent+1)
                            for x in sorted(v))
        else:
            s += ''.join(indentstr + indents + repr(x) + '\n'
                            for x in v)
        s += indentstr + ']\n'
        return s

    @staticmethod
    def searchlist(val, matchfunc):
        if hasattr(val, '__iter__') and not isinstance(val, str):
            matches = []
            for x in val:
                if isinstance(x, Stmt):
                    if matchfunc(x):
                        matches.append(x)
                    else:
                        matches.extend(x.search(matchfunc))
                elif hasattr(x, '__iter__') and not isinstance(val, str):
                    matches.extend(Stmt.searchlist(x, matchfunc))
            return matches
        return []

    def search(self, matchfunc):
        if matchfunc(self):
            return [self]
        elif hasattr(self.tree, 'items'):
            matches = []
            for k, v in sorted(self.tree.items()):
                if isinstance(v, Stmt):
                    if matchfunc(v):
                        matches.append(v)
                    else:
                        matches.extend(v.search(matchfunc))
                elif hasattr(v, '__iter__') and not isinstance(v, str):
                    matches.extend(Stmt.searchlist(self, matchfunc))
                elif matchfunc(v):
                    matches.append(v)
            return matches
        elif hasattr(self.tree, '__iter__'):
            return Stmt.searchlist(self, matchfunc)


class AConst(Stmt): pass
class Alias(Stmt): pass
class AExpr(Stmt): pass
class AExprIn(Stmt): pass
class AStar(Stmt): pass
class ColumnRef(Stmt): pass
class CommonTableExpr(Stmt):
    def __lt__(self, other):
        return self.tree.items() < other.tree.items()

class Copy(Stmt):
    def maybe_src_dest(self):
        return True
    def get_src(self):
        return [self.filename]
    def get_dest(self):
        return [self.relation.relname]

class CreateTable(Stmt): pass

class CreateTableAs(Stmt):
    """
    create materialized view foo as...
    """
    def maybe_src_dest(self):
        return True
    def get_src(self):
        return RelNames.norm(c.ctequery.fromClause.enumerate_tables()
                                if hasattr(c.ctequery, 'fromClause') else []
                                    for c in self.query.withClause.ctes)
    def get_dest(self):
        return [self.into.rel.relname]

class CreateView(Stmt):
    def maybe_src_dest(self):
        return True
    def get_src(self):
        return self.query.enumerate_from()
    def get_dest(self):
        return [repr(RelName.fromRangeVar(self.view))]

class DropStmt(Stmt): pass

class FromClause(Stmt):
    def enumerate_tables(self):
        return RelNames.norm(t.enumerate_from()
                                for t in flatten(self.tree))

class FromClauseSubquery(Stmt): pass

class InsertStmt(Stmt):
    def maybe_src_dest(self):
        return True
    def get_src(self):
        if self.selectStmt:
            return RelNames.norm(self.selectStmt.enumerate_from())
    def get_dest(self):
        return [self.relation.relname]

class Integer(Stmt): pass
class IntoClause(Stmt): pass
class JoinExpr(Stmt):
    def enumerate_from(self):
        return [self.larg.enumerate_from()] + [self.rarg.enumerate_from()]

class NullTest(Stmt): pass

class RangeVar(Stmt):
    def enumerate_from(self):
        return RelName.fromRangeVar(self)

class RangeSubselect(Stmt):
    def enumerate_from(self):
        return self.subquery.enumerate_from()

class RefreshMatView(Stmt):
    def maybe_src_dest(self):
        return True
    def get_src(self):
        return []
    def get_dest(self):
        return [repr(RelName.fromRangeVar(self.relation))]

class ResTarget(Stmt):
    def __lt__(self, other):
        return self.tree.items() < other.tree.items()

class SelectStmt(Stmt):
    def enumerate_from(self):
        fc = self.fromClause.enumerate_tables() if hasattr(self, 'fromClause') else []
        vl = self.valuesLists.enumerate_tables() if hasattr(self, 'valuesLists') else []
        return sorted(set(fc + vl))

class String(Stmt):
    def __lt__(self, other):
        return self.str < other.str

class SubLink(Stmt): pass

class Subquery(Stmt):
    def enumerate_from(self):
        return self.SelectStmt.enumerate_from()

class UnhandledStmt(Stmt):
    def __init__(self, k, v):
        # print(self.__class__.__name__, k, v)
        super().__init__(v)
        self.command = k
        self.v = v

class Update(Stmt):
    def maybe_src_dest(self):
        return True
    def get_src(self):
        if hasattr(self, 'fromClause'):
            return self.fromClause.enumerate_tables()
        return []
    def get_dest(self):
        x = self.relation.relname if self.relation.relpersistence == 'p' else None
        return [x] if x else []

class ValuesLists(Stmt):
    def enumerate_tables(self):
        tbls = [(v.fromClause.enumerate_from()
                    if hasattr(v, 'fromClause')
                    else v.subselect.enumerate_from()
                    if hasattr(v, 'subselect')
                    else None)
                    for v in flatten1(self.tree)]
        return flatten1([t for t in tbls if t])

class WithClause(Stmt): pass


# once all Stmt-derived classes are defined
# build mapping within Stmt so it can use them
Stmt.kwclass = {
    # ref: https://www.postgresql.org/docs/9.5/static/sql-commands.html
    'A_Const': AConst,
    'A_Star': AStar,
    'A_Expr': AExpr,
    'A_ExprIn': AExprIn,
    'Alias': Alias,
    'ColumnRef': ColumnRef,
    'CommonTableExpr': CommonTableExpr,
    'CopyStmt': Copy,
    'CreateStmt': CreateTable,
    'CreateTableAsStmt': CreateTableAs,
    'DropStmt': DropStmt,
    'InsertStmt': InsertStmt,
    'Integer': Integer,
    'IntoClause': IntoClause,
    'JoinExpr': JoinExpr,
    'NullTest': NullTest,
    'RangeSubselect': RangeSubselect,
    'RangeVar': RangeVar,
    'RefreshMatViewStmt': RefreshMatView,
    'ResTarget': ResTarget,
    'SelectStmt': SelectStmt,
    'String': String,
    'SubLink': SubLink,
    'UpdateStmt': Update,
    'ViewStmt': CreateView,
    'WithClause': WithClause,
    'fromClause': FromClause,
    'subquery': Subquery,
    'valuesLists': ValuesLists,
}


def sql2json(sqlstr):
    import json
    from subprocess import Popen, PIPE
    try:
        p = Popen(['./queryparser/queryparser', '--json'],
                    bufsize=0, stdin=PIPE, stderr=PIPE, stdout=PIPE)
        instr = (sqlstr + '\n-- queryparser flush\n').encode('utf8')
        outs, errs = p.communicate(instr)
        return json.loads(outs.decode('utf8')) if not p.returncode else None
    except:
        p.kill()
        raise


class RelName:

    def __init__(self, name, schema=None):
        self.name = name
        self.schema = schema
        self.fqname = (schema + '.' if schema else '') + name
        self.normname = (schema + '.'
                            if schema and schema != 'public' else '') + name

    def __repr__(self):
        return self.normname

    @staticmethod
    def fromRangeVar(rv):
        return RelName(rv.relname,
                       schema=rv.schemaname if hasattr(rv, 'schemaname') else None)

    @staticmethod
    def fromNorm(name):
        if '.' in name:
            schema, name = name.split('.')
        else:
            schema = 'public'
        return RelName(name, schema=schema)


class RelNames:

    @staticmethod
    def norm(l):
        flat = [x for x in flatten(l) if x is not None]
        return sorted(set(repr(r) if isinstance(r, RelName) else r for r in flat))

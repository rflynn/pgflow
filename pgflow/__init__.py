
from .util import flatten, flatten1


def sql2json(sqlstr):

    import json
    from subprocess import Popen, PIPE

    # NOTE: queryparser is the thinnest of commandline wrappers;
    # to speed this up we'll need to avoid re-launching the program by
    # modify queryparser to stay open and process multiple commands so we
    # can keep the pipe open...
    # or better yet build a proper python c module over it
    p = Popen(['./vendor/queryparser/queryparser',
               '--json'],
              stdin=PIPE, stderr=PIPE, stdout=PIPE)
    try:
        outs, errs = p.communicate((sqlstr + '\n').encode('utf8'))
        return json.loads(outs.decode('utf8')) if not p.returncode else None
    except:
        p.kill()
        raise


class Stmt:

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
            return c(v)
        elif k == k.upper() and isinstance(v, dict):
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

    def dumplist(self, v, indent=0):
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

    def dump(self, indent=0):
        indents = '  '
        indent += 1
        indentstr = indents * indent
        s = self.__class__.__name__ + ':\n'
        if hasattr(self.tree, 'items'):
            for k, v in sorted(self.tree.items()):
                if v is None:
                    continue
                s += indentstr + k + ': '
                if isinstance(v, Stmt):
                    s += v.dump(indent=indent+1)
                elif isinstance(v, list):
                    s += self.dumplist(v, indent=indent)
                elif isinstance(v, dict):
                    s += '\n'
                    for k1, v1 in sorted(v.items()):
                        s += indentstr + k1 + ': '
                        if isinstance(v1, Stmt):
                            s += v1.dump(indent=indent+1)
                        else:
                            s += repr(v1) + '\n'
                else:
                    s += repr(v) + '\n'
        elif isinstance(self.tree, list):
            s += indentstr + self.dumplist(self.tree, indent=indent)
        return s

    @staticmethod
    def tree_filter(t, func):
        '''
        recursively search tree for nodes x that func(x)
        '''
        match = []
        if isinstance(t, dict):
            for k, v in t.items():
                if func(v):
                    match.append(v)
                elif isinstance(v, (dict, list, tuple)):
                    match.extend(Stmt.tree_filter(v, func))
        elif isinstance(t, list):
            for v in t:
                if func(v):
                    match.append(v)
                elif isinstance(v, (dict, list, tuple)):
                    match.extend(Stmt.tree_filter(v, func))
        return match
            



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

class FromClause(Stmt):
    def enumerate_tables(self):
        if isinstance(self.tree, list):
            return RelNames.norm(
                fc.enumerate_from()
                    for fc in Stmt.tree_filter(self.tree,
                        lambda x: hasattr(x, 'enumerate_from')))
        return []

class FromClauseSubquery(Stmt): pass

class InsertInto(Stmt):
    def maybe_src_dest(self):
        return True
    def get_src(self):
        #print('ii2', self.selectStmt, 'ii1', self)
        if self.selectStmt:
            return RelNames.norm(self.selectStmt.enumerate_from())
    def get_dest(self):
        return [self.relation.relname]

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
        # FIXME: src and dest and defined by the matview itself, not the refresh
        # query... to implement this we need to understand the shape of the matview
        return []
    def get_dest(self):
        return [repr(RelName.fromRangeVar(self.relation))]

class ResTarget(Stmt):
    def __lt__(self, other):
        return self.tree.items() < other.tree.items()

class Select(Stmt):
    def enumerate_from(self):
        return sorted(set(self.fromClause.enumerate_tables() +
                          self.valuesLists.enumerate_tables()))
        

class Sublink(Stmt): pass
class Subquery(Stmt):
    def enumerate_from(self):
        return self.tree['SELECT'].enumerate_from()

class UnhandledStmt(Stmt):
    def __init__(self, k, v):
        super().__init__(v)
        self.command = k

class Update(Stmt):
    def maybe_src_dest(self):
        return True
    def get_src(self):
        return self.fromClause.enumerate_tables()
    def get_dest(self):
        x = self.relation.relname if self.relation.relpersistence == 'p' else None
        return [x] if x else []

class ValuesLists(Stmt):
    def enumerate_tables(self):
        if self.tree:
            #print('valueslist flttened', flatten1(self.tree))
            tbls = [(v.fromClause.enumerate_from()
                        if hasattr(v, 'fromClause')
                        else v.subselect.enumerate_from()
                        if hasattr(v, 'subselect')
                        else None)
                        for v in flatten1(self.tree)]
            return flatten1([t for t in tbls if t])
        return []


class WithClause(Stmt): pass


# once all Stmt-derived classes are defined
# build mapping within Stmt so it can use them
Stmt.kwclass = {
    # ref: https://www.postgresql.org/docs/9.5/static/sql-commands.html
    'AEXPR IN': AExprIn,
    'AEXPR': AExpr,
    'ALIAS': Alias,
    'A_CONST': AConst,
    'A_STAR': AStar,
    'COLUMNREF': ColumnRef,
    'COMMONTABLEEXPR': CommonTableExpr,
    'COPY': Copy,
    'CREATE TABLE AS': CreateTableAs,
    'CREATESTMT': CreateTable,
    'INSERT INTO': InsertInto,
    'INTOCLAUSE': IntoClause,
    'JOINEXPR': JoinExpr,
    'NULLTEST': NullTest,
    'RANGESUBSELECT': RangeSubselect,
    'RANGEVAR': RangeVar,
    'REFRESHMATVIEWSTMT': RefreshMatView,
    'RESTARGET': ResTarget,
    'SELECT': Select,
    'SUBLINK': Sublink,
    'UPDATE': Update,
    'VIEWSTMT': CreateView,
    'WITHCLAUSE': WithClause,
    'fromClause': FromClause,
    'subquery': Subquery,
    'valuesLists': ValuesLists,
}


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
        return RelName(rv.relname, schema=rv.schemaname)

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

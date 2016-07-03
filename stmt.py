
from pprint import pformat


def sql2json(sqlstr):

    import json
    import shlex
    from subprocess import Popen, PIPE, TimeoutExpired

    p = Popen(['/Users/ryanflynn/src/pgflow/vendor/queryparser/queryparser', '--json'],
              stdin=PIPE, stderr=PIPE, stdout=PIPE)
    try:
        outs, errs = p.communicate((sqlstr + '\n').encode('utf8'), timeout=1)
        return json.loads(outs.decode('utf8'))
    except TimeoutExpired:
        p.kill()


class Stmt:

    tree = None

    @staticmethod
    def from_tree(tree):
        # print('from_tree', tree)
        # recursively process tree picking out things we know and passing the
        # rest through unchanged
        if isinstance(tree, dict):
            if len(tree) == 1:
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
        if isinstance(v, list):
            return [Stmt.from_tree(t) for t in v]
        elif isinstance(v, dict):
            # ref: https://www.postgresql.org/docs/9.5/static/sql-commands.html
            if k == 'SELECT':
                s = Select(v)
            elif k == 'INSERT INTO':
                s = InsertInto(v)
            elif k == 'UPDATE':
                s = Update(v)
            elif k == 'COPY':
                s = Copy(v)
            elif k == 'CREATESTMT':
                s = CreateTable(v)
            elif k == 'VIEWSTMT':
                s = CreateView(v)
            elif k == 'CREATE MATERIALIZED VIEW':
                s = CreateMaterializedView(v)
            elif k == 'REFRESHMATVIEWSTMT':
                s = RefreshMatView(v)
            elif k == 'A_CONST':
                s = AConst(v)
            else:
                s = Stmt.from_tree(v)
        else:
            s = v
        return s

    def __init__(self, tree):
        if isinstance(tree, list):
            tree_ = [Stmt.from_tree(t) for t in tree]
        elif isinstance(tree, dict):
            tree_ = {k: Stmt.from_tree_key(k, v) for k, v in tree.items()}
            for k, v in tree_.items():
                setattr(self, k, v)
        else:
            tree_ = tree
        self.tree = tree_

    def has_src_dest(self):
        return True

    def get_src(self):
        return None

    def get_dest(self):
        return None

    def __repr__(self):
        return '{}:\n{}'.format(self.__class__.__name__, pformat(self.filter_tree(), indent=4))

    def filter_tree(self):
        if isinstance(self.tree, dict):
            return {k: v for k, v in self.tree.items() if v is not None}
        elif isinstance(self.tree, list):
            return [x for x in self.tree if x] or None
        else:
            return self.tree


class UnknownStmt(Stmt): pass

class Select(Stmt): pass

class AConst(Stmt): pass

class InsertInto(Stmt): pass

class Update(Stmt): pass

class CreateTable(Stmt): pass

class CreateView(Stmt): pass

class RefreshMatView(Stmt): pass


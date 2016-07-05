
from . import RelName


class QuerySrcDest:
    def __init__(self, src, dest):
        self.src = src
        self.dest = dest


class NamedEntityType:
    UNKNOWN = 0
    TABLE = 1
    VIEW = 2
    MATVIEW = 3
    TEMP_TABLE = 4
    CTE_QUERY = 5
    DATA_FILE = 6
    DBDUMP_FILE = 7

    @staticmethod
    def is_persistent(t):
        if t == NamedEntityType.UNKNOWN:
            return None
        return t in (NamedEntityType.TABLE,
                     NamedEntityType.DATA_FILE)


class NamedEntity:

    def __init__(self, name, netype=NamedEntityType.UNKNOWN):
        self.rename = RelName.fromNorm(name)
        self._netype = netype

    def get_type(self):
        return self._netype

    def is_persistent(self):
        return NamedEntityType.is_persistent(self._netype)

    def resolve_persistence(self):
        raise NotImplementedError


class DDLSource:

    '''
    base class of a source of DDL information
    '''

    def __init__(self):
        pass

    def load_entities(self):
        raise NotImplementedError


class LiveConn(DDLSource):

    def __init__(self, dbconn):
        super().__init__()
        self.dbconn = dbconn

    def load_entities(self):
        raise NotImplementedError


class PGDump(DDLSource):

    def __init__(self, filepath):
        super().__init__()
        self.filepath = filepath

    def load_entities(self):
        raise NotImplementedError

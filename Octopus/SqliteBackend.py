# -*- coding: UTF-8 -*-

import os, os.path
import sqlite3
import threading

from itertools import *
from .FileBasedLock import *

class Backend(object):
    schema={ }
    index={ }
    per_thread = threading.local() # Thread-local data are data whose values are thread specific.

    def __init__(self):
        self.cols = None
    
    def get_cols(self):
        if self.cols!=None: return self.cols
        self._init_cols()
        return self.cols
    
    def _init_cols(self):
        # FIXME: add a lock here
        if self.cols!=None: return
        cn=self.get_connection(self.get_filename(), self._init_db)
        c=cn.cursor()
        cols={}
        for table, create_sql in self.schema.items():
            # Get the field names of the table
            c.execute("SELECT * FROM %s LIMIT 1" % table)
            cols[table] = set([tuple[0] for tuple in c.description])
        self.cols=cols

    @classmethod
    def get_connection(class_name, filename, init):
        need_init=False
        if not hasattr(class_name.per_thread, 'cn'): class_name.per_thread.cn = {}
        cn = class_name.per_thread.cn.get(filename, None)
        if not cn:
            if not os.path.exists(filename):
                need_init=True
                try: os.makedirs(os.path.dirname(filename))
                except OSError: pass
            cn = sqlite3.connect(filename,timeout=30000,isolation_level=None)
            cn.row_factory=sqlite3.Row #serves as a highly optimized row_factory for Connection objects
            class_name.per_thread.cn[filename]=cn
            if need_init:
                with FileBasedLock(filename+".lock"):
                    init(cn)
        return cn

    @staticmethod
    def got_table(cn, table):
        r=cn.execute("SELECT name FROM sqlite_master WHERE TYPE='table' AND name=?", (table,)).fetchone()
        return r!=None

    def get_filename(self):
        raise NotImplemented

    def _init_db(self, cn):
            c=cn.cursor()
            for k,v in self.schema.items():
                if self.got_table(cn, k): continue
                c.execute(v)
                if self.index.has_key(k):
                    for i in self.index[k]: c.execute('CREATE INDEX ix_%(table)s_%(col)s ON %(table)s (%(col)s)' % {'table':k, 'col':i})

    def execute(self, *a, **kw):
        cn=self.get_connection(self.get_filename(), self._init_db)
        c=cn.cursor()
        c.execute(*a, **kw)
        return c.rowcount, c.lastrowid

    def query(self, *a, **kw):
        cn=self.get_connection(self.get_filename(), self._init_db)
        c=cn.cursor()
        # NOTE: the code below is intended to be imap not a custom mapper
        return imap(lambda r: dict(r), c.execute(*a, **kw))
        
    def insert(self, table, params):
        return self.execute(
            'INSERT INTO %s (%s) VALUES (%s)' % (
                table, ', '.join(params.keys()),
                ', '.join(['?']*len(params.keys()))
                ),
            params.values())

class PartitionedBackend(Backend):
    def __init__(self, mapper=imap, chainer=chain, reducer=None):
        Backend.__init__(self)
        self._mapper = mapper
        self._reducer = reducer
        self._chainer = chainer

    def get_partition_filename(self, key):
        raise NotImplemented

    def execute(self, key, *a, **kw):
        cn=self.get_connection(self.get_partition_filename(key), self._init_db)
        c=cn.cursor()
        c.execute(*a, **kw)
        return c.rowcount, c.lastrowid

    def map_execute(self, keys, *a,  **kw):
        mapper=self._mapper
        chainer=self._chainer
        return chainer(mapper(lambda key: self.execute(key, *a, **kw), keys))

    def insert(self, key, table, params):
        return self.execute(key,
            'INSERT INTO %s (%s) VALUES (%s)' % (
                table, ', '.join(params.keys()),
                ', '.join(['?']*len(params.keys()))
                ),
            params.values())

    def query(self, key, *a, **kw):
        # FIXME: allow passing the reducer from params to override properties
        reducer = self._reducer
        # the function below adds a column "_key" to each row
        def _to_dict_with_key(r):
            r=dict(r)
            r['_key']=key
            return r
        cn=self.get_connection(self.get_partition_filename(key), self._init_db)
        c=cn.cursor()
        g=imap(_to_dict_with_key, c.execute(*a, **kw)) # NOTE: this is intented to be imap not custom mapper
        if not reducer: return g
        return reducer(g)

    def map_query(self, keys, *a,  **kw):
        # FIXME: allow passing the 2 below from params to override properties
        mapper = self._mapper
        chainer = self._chainer
        l=mapper(lambda key: self.query(key, *a, **kw), keys)
        return chainer(*l)


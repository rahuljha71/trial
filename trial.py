import os
import uuid
import sqlite3
import cPickle as pickle
import unittest

class PersistentDictionary(object):
    """
    A sqlite based key,value storage.
    The value can be any pickleable object.
    Similar interface to Python dict
    Supports the GLOB syntax in methods keys(),items(), __delitem__()
    Usage Example:
    >>> p = PersistentDictionary(path='test.sqlite')
    >>> key = 'test/' + p.uuid()
    >>> p[key] = {'a': 1, 'b': 2}
    >>> print p[key]
    {'a': 1, 'b': 2}
    >>> print len(p.keys('test/*'))
    1
    >>> del p[key]
    """

    CREATE_TABLE = "CREATE TABLE persistence (pkey, pvalue)"
    SELECT_KEYS = "SELECT pkey FROM persistence WHERE pkey GLOB ?"
    SELECT_VALUE = "SELECT pvalue FROM persistence WHERE pkey GLOB ?"
    INSERT_KEY_VALUE = "INSERT INTO persistence(pkey, pvalue) VALUES (?,?)"
    UPDATE_KEY_VALUE = "UPDATE persistence SET pvalue = ? WHERE pkey = ?"
    DELETE_KEY_VALUE = "DELETE FROM persistence WHERE pkey LIKE ?"
    SELECT_KEY_VALUE = "SELECT pkey,pvalue FROM persistence WHERE pkey GLOB ?"

    def __init__(self,
                 path='persistence.sqlite',
                 autocommit=True,
                 serializer=pickle):
        self.path = path
        self.autocommit = autocommit
        self.serializer = serializer
        create_table = not os.path.exists(path)
        self.connection  = sqlite3.connect(path)
        self.connection.text_factory = str # do not use unicode
        self.cursor = self.connection.cursor()
        if create_table:
            self.cursor.execute(self.CREATE_TABLE)
            self.connection.commit()

    def uuid(self):
        return str(uuid.uuid4())

    def keys(self,pattern='*'):
        "returns a list of keys filtered by a pattern, * is the wildcard"
        self.cursor.execute(self.SELECT_KEYS,(pattern,))
        return [row[0] for row in self.cursor.fetchall()]

    def __contains__(self,key):
        return True if self.get(key)!=None else False

    def __iter__(self):
        for key in self:
            yield key

    def __setitem__(self,key, value):
        if key in self:
            if value is None:
                del self[key]
            else:
                svalue = self.serializer.dumps(value)
                self.cursor.execute(self.UPDATE_KEY_VALUE, (svalue, key))
        else:
            svalue = self.serializer.dumps(value)
            self.cursor.execute(self.INSERT_KEY_VALUE, (key, svalue))
        if self.autocommit: self.connection.commit()

    def get(self,key):
        self.cursor.execute(self.SELECT_VALUE, (key,))
        row = self.cursor.fetchone()
        return self.serializer.loads(row[0]) if row else None

    def __getitem__(self, key):
        self.cursor.execute(self.SELECT_VALUE, (key,))
        row = self.cursor.fetchone()
        if not row: raise KeyError
        return self.serializer.loads(row[0])

    def __delitem__(self, pattern):
        self.cursor.execute(self.DELETE_KEY_VALUE, (pattern,))
        if self.autocommit: self.connection.commit()

    def items(self,pattern='*'):
        self.cursor.execute(self.SELECT_KEY_VALUE, (pattern,))
        return [(row[0], self.serializer.loads(row[1])) \
                    for row in self.cursor.fetchall()]

    def dumps(self,pattern='*'):
        self.cursor.execute(self.SELECT_KEY_VALUE, (pattern,))
        rows = self.cursor.fetchall()
        return self.serializer.dumps(dict((row[0], self.serializer.loads(row[1]))
                                          for row in rows))

    def loads(self, raw):
        data = self.serializer.loads(raw)
        for key, value in data.iteritems():
            self[key] = value

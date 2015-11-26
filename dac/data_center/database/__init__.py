# coding:utf-8
"""
    tcc-dac.data_center.database
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac data_center database package.
"""
# from functools import wraps
from pymongo import MongoClient
# from pymongo.database import Database
from dac.config import mongo_host, mongo_port, mongo_db


def _connect_mongo(host, port, username=None, password=None, db=None):
    """ A util for making a connection to mongodb """
    db = db or 'test'
    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn[db]

db = _connect_mongo(mongo_host, mongo_port, db=mongo_db)


# def close_db(*dbs):
#     if dbs is None or len(dbs) == 0:
#         dbs = ['db']
#
#     def _deco(func):
#         @wraps(func)
#         def _call(*args, **kwargs):
#             result = func(*args, **kwargs)
#             for db in dbs:
#                 try:
#                     func.func_globals[db].connection.disconnect()
#                     print('{} disconnected'.format(db))
#                 except KeyError:
#                     pass
#             return result
#         return _call
#     return _deco
#
#
# class Mongodb(object):
#     def __init__(self, host='localhost', port=27017, database='test', max_pool_size=10, timeout=10):
#         self.host = host
#         self.port = port
#         self.database = database
#         self.max_pool_size = max_pool_size
#         self.timeout = timeout
#
#     @staticmethod
#     def init_mongodb():
#         return Mongodb(host=mongo_host, port=mongo_port, database=mongo_db)
#
#     @property
#     def connect(self):
#         return MongoClient(self.host, self.port, maxPoolSize=self.max_pool_size,
#                            connectTimeoutMS=60 * 60 * self.timeout)
#
#     def __getitem__(self, collection):
#         return self.__getattr__(collection)
#
#     def __getattr__(self, collection_or_func):
#         l_db = self.connect[self.database]
#         if collection_or_func in Database.__dict__:
#             # if collection_or_func is Database 's func
#             return getattr(l_db, collection_or_func)
#         # else called Collection
#         return Collection(l_db, collection_or_func)
#
#
# class Collection(object):
#     def __init__(self, db, collection):
#         self.collection = getattr(db, collection)
#
#     @close_db
#     def __getattr__(self, operation):
#         # control_type = ['disconnect', 'insert', 'update', 'find', 'find_one']
#         # if operation in control_type:
#         #     return getattr(self.collection, operation)
#         # raise AttributeError(operation)
#         return getattr(self.collection, operation)


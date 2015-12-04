# coding: utf-8
"""
    tcc-dac.dac.config
    ~~~~~~~~~~~~~~~

    tcc-dac dac config module.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
SECRET_KEY = "v\x187\xa0\x01\x82DY?\x1d\xcd\x0f\x83\x92\xb0\xc6\xdd\xe5\x93w\xd9vQX"
PERMANENT_SESSION_LIFETIME = 24 * 60 * 60

MONGODB_HOST = '192.168.1.91'
MONGODB_PORT = 20000
MONGODB_DB = 'tccdevdb'

REDIS_HOST = '192.168.1.91'
REDIS_PORT = 6379
REDIS_PASSWORD = 'redisredis'

API_VERSION = 'v1.0'

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

API_VERSION = 'v1.0'

# for uploads
MAXIMUM_UPLOADS_SIZE = 64 * 1024 * 1024
LINE_CONFIG_UPLOADS_DEFAULT_URL = "dac/static/configs/"
LINE_DATA_UPLOADS_DEFAULT_URL = "dac/static/schedules/"


MONGODB_HOST = '192.168.1.91'
MONGODB_PORT = 20000
MONGODB_DB = 'tccdevdb'

REDIS_HOST = '192.168.1.91'
REDIS_PORT = 6379
REDIS_PASSWORD = 'redisredis'


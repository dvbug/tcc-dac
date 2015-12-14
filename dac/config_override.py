# coding: utf-8
"""
    tcc-dac.dac.config_override
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac dac config_override module.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
API_VERSION = 'v1.0'

# for uploads
MAXIMUM_UPLOADS_SIZE = 64 * 1024 * 1024
LINE_CONFIG_UPLOADS_DEFAULT_URL = "dac/static/configs/"
LINE_DATA_UPLOADS_DEFAULT_URL = "dac/static/schedules/"


MONGODB_HOST = '192.168.1.118'
MONGODB_PORT = 20000
MONGODB_DB = 'tccdevdb'

REDIS_HOST = '192.168.1.118'
REDIS_PORT = 6379
REDIS_PASSWORD = 'redisredis'
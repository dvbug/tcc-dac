# coding: utf-8
"""
    tcc-dac.data_center.cache
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac data_center cache package.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
from redis import StrictRedis
from ...config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

redis_client = StrictRedis(host=REDIS_HOST, port=REDIS_PORT,
                           password=REDIS_PASSWORD,
                           charset='GBK',
                           decode_responses=True)

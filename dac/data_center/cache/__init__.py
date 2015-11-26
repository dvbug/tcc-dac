# coding: utf-8
"""
    tcc-dac.data_center.cache
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac data_center cache package.
"""
from redis import StrictRedis
from ...config import redis_host, redis_port, redis_password

redis_client = StrictRedis(host=redis_host, port=redis_port,
                           password=redis_password,
                           charset='GBK',
                           decode_responses=True)

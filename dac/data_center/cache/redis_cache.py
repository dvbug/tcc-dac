# coding: utf-8
"""
    tcc-dac.data_center.cache.redis_cache
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac data_center cache for redis module.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
import json
import pandas as pd
from abc import ABCMeta, abstractmethod
from . import redis_client


class RedisCache(object, metaclass=ABCMeta):
    def __init__(self):
        pass

    @abstractmethod
    def get_keys(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_raw_data(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_pandas_data(self, *args, **kwargs):
        pass

    @staticmethod
    def set_redis_data(key, value):
        redis_client.set(key, value)

    @staticmethod
    def get_redis_data(key):
        # TODO if not exists, need try loading from mongodb than set into redis. -- NOT DO.
        return redis_client.get(key)  # .decode()

    @staticmethod
    def key_exist(key):
        return redis_client.exists(key)


class ScheduleCache(RedisCache):
    def __init__(self):
        super(ScheduleCache, self).__init__()

    @staticmethod
    def get_schedule_type(key: str):
        return key.split('_')[1]

    @staticmethod
    def get_keys(line_no, date, plan_or_real):
        """:param plan_or_real: str, 'PLAN' or 'REAL' or 'PLAN&REAL' """
        data_type = plan_or_real.upper() or 'PLAN'
        # KEY: LINE01_PLAN_20140702
        keys = ['LINE{}_{}_{}'.format(line_no, dtype, date) for dtype in data_type.split('&')]
        return keys

    def get_raw_data(self, line_no, date, plan_or_real='plan'):
        keys = self.get_keys(line_no, date, plan_or_real)
        ret = list(map(self._get_redis_data, keys))
        return ret

    def get_pandas_data(self, line_no, date, plan_or_real='plan'):
        raw_data = self.get_raw_data(line_no, date, plan_or_real)
        return pd.DataFrame.from_dict(raw_data, orient='index')

    @staticmethod
    def _get_redis_data(key):
        json_data = RedisCache.get_redis_data(key) or "{}"
        data = json.loads(json_data, encoding='GBK')
        return key, data

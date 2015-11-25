# coding: utf-8
import json
import pandas as pd
from abc import ABCMeta, abstractmethod
from . import redis_client


class RedisCache(object, metaclass=ABCMeta):
    def __init__(self):
        pass

    @abstractmethod
    def get_key(self, *args, **kwargs):
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
    def get_key(line_no, date, plan_or_real='plan'):
        data_type = plan_or_real.upper() or 'PLAN'
        key = 'LINE{}_{}_{}'.format(line_no, data_type, date)
        return key

    def get_raw_data(self, line_no, date, plan_or_real='plan'):
        key = self.get_key(line_no, date, plan_or_real)
        json_data = RedisCache.get_redis_data(key)
        data = json.loads(json_data,encoding='GBK')
        return data

    def get_pandas_data(self, line_no, date, plan_or_real='plan'):
        raw_data = self.get_raw_data(line_no, date, plan_or_real)
        return pd.DataFrame.from_dict(raw_data, orient='index')

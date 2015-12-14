# coding: utf-8
"""
    tcc-dac.data_center.cache
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac data_center cache package.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
from redis import StrictRedis


class RedisClient(object):
    def __init__(self, app=None):
        self.redis_host = '127.0.0.1'
        self.redis_port = '9319'
        self.redis_password = None
        self._strict_redis = None
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        if "REDIS_HOST" in app.config:
            self.redis_host = app.config["REDIS_HOST"]

        if "REDIS_PORT" in app.config:
            self.redis_port = app.config["REDIS_PORT"]

        if "REDIS_PASSWORD" in app.config:
            self.redis_password = app.config["REDIS_PASSWORD"]

        self._strict_redis = StrictRedis(host=self.redis_host,
                                         port=self.redis_port,
                                         password=self.redis_password,
                                         charset='GBK',
                                         decode_responses=True)

    @property
    def client(self):
        return self._strict_redis

    def __getattr__(self, item):
        """Returns self.attribute or self._strict_redis.attribute"""
        try:
            return object.__getattribute__(item)
        except:
            return getattr(self._strict_redis, item)


redis_client = RedisClient()

# coding: utf-8
from flask_restful import abort, request
from ..data_center.cache.redis_cache import ScheduleCache
from ..data_center.database.reader import LineConfigMongodbReader


def abort_error_resp(http_status_code, **kwargs):
    abort(http_status_code,
          status=http_status_code,
          url=request.url,
          **kwargs)


class ScheduleMixin(object):

    @staticmethod
    def if_not_exists(line_no, date, plan_or_real):
        key = ScheduleCache.get_key(line_no, date, plan_or_real)
        if not ScheduleCache.key_exist(key):
            abort_error_resp(410, lineNo=line_no, date=date, datatype=plan_or_real, message="Resource does not exist")


class LineConfigMixin(object):

    @staticmethod
    def if_not_exists(line_no):
        if not LineConfigMongodbReader().exists(line_no):
            abort_error_resp(410, line_no=line_no, message="Resource does not exist")

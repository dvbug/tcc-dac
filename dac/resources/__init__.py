# coding: utf-8
"""
    tcc-dac.resources
    ~~~~~~~~~~~~~~~~~

    tcc-dac resources packages.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
from flask_restful import abort, request
from dac.data_center.cache.redis_cache import ScheduleCache
from dac.data_center.database.reader import LineConfigMongodbReader, PlanScheduleMongodbReader
from dac.config import API_VERSION


def abort_error_resp(http_status_code, **kwargs):
    if 'message' not in kwargs:
        kwargs['message'] = "Resource does not exist."

    data = kwargs
    data['url'] = request.url
    abort(http_status_code,
          status=http_status_code,
          error=True,
          data=data)


def make_json_response(http_status_code, **kwargs):
    data = kwargs
    return {
        'data': data,
        'version': API_VERSION,
        'status': http_status_code
    }


class ScheduleMixin(object):

    @staticmethod
    def if_not_exists(line_no, date, plan_or_real):
        key = ScheduleCache.get_key(line_no, date, plan_or_real)
        if not ScheduleCache.key_exist(key):
            # TODO if not exists, need try loading from mongodb than set into redis. -DONE
            plan_schedule_reader = PlanScheduleMongodbReader()
            plan_schedule_reader.load_frame(line_no, date)
            # plan_schedule_reader.to_redis()
            # plan_schedule_reader.data_frame_result.to_json(orient='index')
            # from multiprocessing.dummy import Pool as ThreadPool
            # pool = ThreadPool()
            # pool.apply_async(plan_schedule_reader.to_redis)
            # pool.close()
            # pool.join()
            import os
            try:
                pid = os.fork()
                if pid == 0:
                    print('run sub process to fill data to redis.')
                    plan_schedule_reader.to_redis()
                else:
                    print('run main process to return json data.')
                    return plan_schedule_reader.data_frame_result.to_json(orient='index')
            except OSError:
                abort_error_resp(410, lineNo=line_no, date=date, datatype=plan_or_real)


class LineConfigMixin(object):

    @staticmethod
    def if_not_exists(line_no):
        if not LineConfigMongodbReader().exists(line_no):
            abort_error_resp(410, line_no=line_no)

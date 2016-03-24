# coding: utf-8
"""
    tcc-dac.resources
    ~~~~~~~~~~~~~~~~~

    tcc-dac resources packages.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
from flask_restful import abort, request
from dac.common.exceptions import NoDataError
from dac.data_center.cache.redis import ScheduleCache
from dac.data_center.database.reader import LineConfigMongodbReader, ScheduleMongodbReader
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


def make_json_response_2(http_status_code, **kwargs):
    return dict({
        'version': API_VERSION,
        'status': http_status_code
    }, **kwargs)


class ScheduleMixin(object):

    @staticmethod
    def if_not_exists(line_no, date, plan_or_real):
        keys = ScheduleCache.get_keys(line_no, date, plan_or_real)
        for key in keys:
            if not ScheduleCache.key_exist(key):
                # TODO if not exists, need try loading from mongodb than set into redis. -DONE
                schedule_reader = ScheduleMongodbReader()
                schedule_type = ScheduleCache.get_schedule_type(key)
                try:
                    schedule_reader.load_frame(line_no, date, schedule_type)
                except NoDataError:
                    print('no data in db. KEY:{}'.format(key))
                    return

                import os
                try:
                    pid = os.fork()
                    if pid == 0:
                        print('if_not_exists -> run sub process to fill data to redis.')
                        schedule_reader.to_redis()
                        os._exit(0)
                    # else:
                    #     print('run main process to return json data.')
                    #     return schedule_reader.data_frame_result.to_json(orient='index')
                except OSError:
                    abort_error_resp(410, lineNo=line_no, date=date, datatype=schedule_type)


class LineConfigMixin(object):

    @staticmethod
    def if_not_exists(line_no):
        if not LineConfigMongodbReader().exists(line_no):
            abort_error_resp(410, line_no=line_no)


class SectionMixin(object):
    @staticmethod
    def if_not_exists(line_no, date, sections_data):
        if len(sections_data) == 0:
            abort_error_resp(410, line_no=line_no, date=date)

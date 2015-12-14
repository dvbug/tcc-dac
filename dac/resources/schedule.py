# coding: utf-8
"""
    tcc-dac.resources.schedule
    ~~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac resources schedule module.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
import os
from flask_restful import Resource, current_app, reqparse
from flask_json import as_json_p
from . import ScheduleMixin, make_json_response, make_json_response_2
from dac.data_center.cache.redis_cache import ScheduleCache
from dac.common.exceptions import NoDataError

data_cache = ScheduleCache()

post_parser = reqparse.RequestParser()
post_parser.add_argument(
    'fname', dest='fname',
    # location='form',
    type=str,
    required=True, help='The plan or real schedule\'s file name',
)
post_parser.add_argument(
    'file', dest='file',
    # location='form',
    type=str,
    required=True, help='The plan or real schedule\'s file',
)


class DateScheduleList(Resource, ScheduleMixin):
    @as_json_p
    def post(self, date, plan_or_real):
        """Post schedule data, by date & plan_or_real type. like:
        POST: /api/v1.0/schedules/20140702/plan --data "file=file content&fname=file origin name"-->
        resp: {
            "message": "success or failed",
            "status": 200, or 410
            "version": "v1.0"
        };
        of course, it also support JSONP request.
        url: /api/v1.0/schedules/20140702/01/plan?callback=?
        """
        args = post_parser.parse_args()
        file = args.file
        file_orgin_name = args.fname
        file_name = 'TEMP_{}_{}-{}.csv'.format(plan_or_real, date, file_orgin_name)  # TEMP_PLAN_20150101.csv

        upload_dir = current_app.config['LINE_DATA_UPLOADS_DEFAULT_URL'] or 'dac/static/schedules/'
        full_file_name = os.path.join(upload_dir, file_name)
        f = open(full_file_name, mode='w')
        f.write(file)
        f.close()
        try:
            pid = os.fork()
            if pid == 0:
                from dac.data_center.database import Mongodb
                from dac.data_center.csv.reader import ScheduleCSVReader
                m_db = Mongodb(app=current_app)
                schedule_reader = ScheduleCSVReader(full_file_name, plan_or_real)
                schedule_reader.to_mongodb(database=m_db.db)
                m_db.close()
                print('{} schedule to mongodb done.'.format(plan_or_real))
                os._exit(0)
            else:
                return make_json_response_2(200, message="File < {} > upload success. Refresh to reload."
                                            .format(file_name)), 200
        except OSError:
            return make_json_response_2(410, message="File < {} > upload failed.".format(file_name)), 410


class ScheduleList(Resource, ScheduleMixin):

    @as_json_p
    def get(self, line_no, date, plan_or_real):
        """Returns trains schedules json response, by date & lineNo & plan_or_real type. like:
        GET: /api/v1.0/schedules/20140702/01/plan -->
        resp: {
            "data":{
                "plan":{
                    "trip1":{},
                    "trip2":{},
                    ...,
                    "tripN":{}
                }
            },
            "status": 200,
            "version": "v1.0"
        }
        or,
        GET: /api/v1.0/schedules/20140702/01/plan&real -->
        resp: {
            "data":{
                "plan":{
                    "trip1":{},
                    "trip2":{},
                    ...,
                    "tripN":{}
                },
                "real":{
                    "trip1":{},
                    "trip2":{},
                    ...,
                    "tripN":{}
                }
            },
            "status": 200,
            "version": "v1.0"
        };
        of course, it also support JSONP request.
        url: /api/v1.0/schedules/20140702/01/plan?callback=?
        """
        try:
            self.if_not_exists(line_no, date, plan_or_real)
            raw_data_all = data_cache.get_raw_data(line_no, date, plan_or_real)
        except NoDataError as e:
            print(e)
            raw_data_all = {}

        results_schedules = dict()
        for key, raw_data in raw_data_all:
            schedule_type = ScheduleCache.get_schedule_type(key).lower()
            results_schedules[schedule_type] = raw_data

        return make_json_response(200, **results_schedules), 200


class Schedule(Resource, ScheduleMixin):

    @as_json_p
    def get(self, line_no, date, trip, plan_or_real='plan'):
        """Returns trains schedules json response, by date & lineNo & plan_or_real type & trips.like:
        url: /api/v1.0/schedules/20140702/01/plan/1023 -->
        resp: {
            "data":{
                "plan":{
                    "1023":{
                        "direction": "1",
                        "stop|<stationName>|<trip>|*|<orderIndex>": "20140702063225",
                        ...,
                        ...,
                        "trip": "1023",
                        "type": "B"
                    }
                }
            }
        }
        or,
        url: /api/v1.0/schedules/20140702/01/plan/1023&1024 -->
        resp: {
            "data":{
                "plan":{
                    "1023":{...},
                    "1024":{...}
                }
            }
        }
        or,
        url: /api/v1.0/schedules/20140702/01/plan&real/1023&1024 -->
        resp: {
            "data":{
                "plan":{
                    "1023":{...},
                    "1024":{...}
                },
                "real":{
                    "1023":{...},
                    "1024":{...}
                }
            }
        };
        of course, it also support JSONP request.
        url: /api/v1.0/schedules/20140702/01/plan/1023?callback=?
        """
        try:
            self.if_not_exists(line_no, date, plan_or_real)
            # data_frame = data_cache.get_pandas_data(line_no, date, plan_or_real)
            # """:type data_frame: pd.DataFrame"""
            #
            # found_row = data_frame[data_frame['trip'] == trip]
            # if len(found_row) == 1:
            #     data = found_row.to_dict(orient='index')
            #     # found_row = found_row.iloc(0)[0]
            #     # data = found_row.to_dict()
            #     return data, 200
            raw_data_all = data_cache.get_raw_data(line_no, date, plan_or_real)
        except NoDataError as e:
            print(e)
            raw_data_all = []

        results_schedules = dict()
        for key, raw_data in raw_data_all:
            schedule_type = ScheduleCache.get_schedule_type(key).lower()
            results_schedules[schedule_type] = dict()
            for t in trip.split('&'):
                results_schedules[schedule_type][t] = self._find_trip_row(raw_data, t)

        return make_json_response(200, **results_schedules), 200

    @staticmethod
    def _find_trip_row(collection, trip):
        if trip in collection:
            return collection[trip]
        else:
            return {}

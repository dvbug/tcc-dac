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
    'file', dest='file',
    # location='form',
    type=str,
    required=True, help='The plan or real schedule\'s file',
)


class ScheduleList(Resource, ScheduleMixin):

    @as_json_p
    def get(self, line_no, date, plan_or_real='plan'):
        """Returns trains schedules json response, by date & lineNo & plan_or_real type. like:
        GET: /api/v1.0/schedules/20140702/01/plan -->
        resp: {
            "data":{
                "schedules":{
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
            data = data_cache.get_raw_data(line_no, date, plan_or_real)
        except NoDataError as e:
            print(e)
            data = {}

        return make_json_response(200, schedules=data), 200

    @as_json_p
    def post(self, date, plan_or_real):
        args = post_parser.parse_args()
        file = args.file
        file_name = 'TEMP_{}_{}.csv'.format(plan_or_real, date)  # TEMP_PLAN_20150101.csv

        upload_dir = current_app.config['LINE_DATA_UPLOADS_DEFAULT_URL'] or 'dac/static/schedules/'
        full_file_name = os.path.join(upload_dir, file_name)
        f = open(full_file_name, mode='w')
        f.write(file)
        f.close()
        try:
            pid = os.fork()
            if pid == 0:
                from dac.data_center.database import create_new_conn_db
                from dac.data_center.csv.reader import ScheduleCSVReader
                conn, db = create_new_conn_db()
                schedule_reader = ScheduleCSVReader(full_file_name, plan_or_real)
                schedule_reader.to_mongodb(database=db)
                conn.close()
                print('{} schedule to mongodb done.'.format(plan_or_real))
                os._exit(0)
            else:
                return make_json_response_2(200, message="File < {} > upload success. Refresh to reload."
                                            .format(file_name)), 200
        except OSError:
            return make_json_response_2(410, message="File < {} > upload failed.".format(file_name)), 410


class Schedule(Resource, ScheduleMixin):

    @as_json_p
    def get(self, line_no, date, trip, plan_or_real='plan'):
        """Returns trains schedules json response, by date & lineNo & plan_or_real type & trips.like:
        url: /api/v1.0/schedules/20140702/01/plan/1023 -->
        resp: {
            "data":{
                "schedules":{
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
                "schedules":{
                    "1023":{
                        "direction": "1",
                        "stop|<stationName>|<trip>|*|<orderIndex>": "20140702063225",
                        ...,
                        ...,
                        "trip": "1023",
                        "type": "B"
                    },
                    "1024":{
                        "direction": "1",
                        "stop|<stationName>|<trip>|*|<orderIndex>": "20140702063455",
                        ...,
                        ...,
                        "trip": "1024",
                        "type": "B"
                    }
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
            raw_data = data_cache.get_raw_data(line_no, date, plan_or_real)
        except NoDataError as e:
            print(e)
            raw_data = {}

        results_schedules = dict()

        if '&' in trip:
            trips = trip.split('&')
            for trip in trips:
                if len(trip) > 0:
                    results_schedules[trip] = self._find_trip_row(raw_data, trip)
        else:
            results_schedules[trip] = self._find_trip_row(raw_data, trip)

        return make_json_response(200, schedules=results_schedules), 200

    @staticmethod
    def _find_trip_row(collection, trip):
        if trip in collection:
            return collection[trip]
        else:
            return {}

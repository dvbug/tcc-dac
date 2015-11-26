# coding: utf-8
from flask_restful import Resource
from flask_json import as_json_p
from . import ScheduleMixin, make_json_response
from ..data_center.cache.redis_cache import ScheduleCache

data_cache = ScheduleCache()


class ScheduleList(Resource, ScheduleMixin):
    """Returns trains schedules json response, by date & lineNo & plan_or_real type. like:
    url: /api/v1.0/schedules/20140702/01/plan/1023 -->
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
    @as_json_p
    def get(self, line_no, date, plan_or_real='plan'):
        self.if_not_exists(line_no, date, plan_or_real)
        data = data_cache.get_raw_data(line_no, date, plan_or_real)

        return make_json_response(200, schedules=data), 200


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

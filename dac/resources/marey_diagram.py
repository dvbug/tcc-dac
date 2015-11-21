# coding: utf-8
from flask_restful import Resource, abort
from . import MareyDiagramMixin
from ..data_center.cache.redis_cache import MareyDiagramCache

data_cache = MareyDiagramCache()


class TrainMareyDiagramList(Resource, MareyDiagramMixin):

    def get(self, line_no, date, plan_or_real='plan'):
        self.if_not_exists(line_no, date, plan_or_real)
        data = data_cache.get_raw_data(line_no, date, plan_or_real)
        return data


class TrainMareyDiagram(Resource, MareyDiagramMixin):

    def get(self, line_no, date, trip, plan_or_real='plan'):

        self.if_not_exists(line_no, date, plan_or_real)
        data_frame = data_cache.get_pandas_data(line_no, date, plan_or_real)
        """:type data_frame: pd.DataFrame"""

        found_row = data_frame[data_frame['trip'] == trip]
        if len(found_row) == 1:
            data = found_row.to_dict(orient='index')
            # found_row = found_row.iloc(0)[0]
            # data = found_row.to_dict()
            return data
        else:
            abort(404, lineNo=line_no, date=date, trip=trip, datatype=plan_or_real, message="Resource does not exist.")

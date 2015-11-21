# coding: utf-8
from flask_restful import abort
from ..data_center.cache.redis_cache import MareyDiagramCache
from ..data_center.database.reader import HeaderMongodbReader


class MareyDiagramMixin(object):

    @staticmethod
    def if_not_exists(line_no, date, plan_or_real):
        key = MareyDiagramCache.get_key(line_no, date, plan_or_real)
        if not MareyDiagramCache.key_exist(key):
            abort(404, lineNo=line_no, date=date, datatype=plan_or_real, message="Resource does not exist.")


class LineHeaderConfigMixin(object):

    @staticmethod
    def if_not_exists(line_no):
        if not HeaderMongodbReader().exists(line_no):
            abort(404, line_no=line_no, message="Resource does not exist.")

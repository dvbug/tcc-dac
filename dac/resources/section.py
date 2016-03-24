# coding:utf-8
"""
    tcc-dac.resources.section
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac resources section module.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
import datetime
from flask_restful import Resource, current_app
from flask_json import as_json_p
from . import SectionMixin, make_json_response
from dac.data_center.database.section import SectionDataGenerator, SectionMongodbReader, SectionTripCounter


TYPES = SectionMongodbReader.__types__


class SectionTripCounterResource(Resource, SectionMixin):

    @as_json_p
    def get(self, line_no, date, direction=None):
        sd = SectionDataGenerator.get_sections_data(line_no, date)

        self.if_not_exists(line_no, date, sd)

        stc = SectionTripCounter(date, line_no, sd)
        results = stc.get_count(datetime.timedelta(hours=1), direction)
        return make_json_response(200, **results), 200

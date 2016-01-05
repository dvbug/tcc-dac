# coding:utf-8
"""
    tcc-dac.resources.section
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac resources section module.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
from flask_restful import Resource, current_app
from flask_json import as_json_p
from . import SectionMixin
from dac.data_center.database.section import SectionDataGenerator, SectionMongodbReader


TYPES = SectionMongodbReader.__types__

class SectionTripCount(Resource, SectionMixin):

    @as_json_p
    def post(self, line_no, date):
        sd = SectionDataGenerator.get_sections_data(line_no, date)

        self.if_not_exists(line_no, date, sd)


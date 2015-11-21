# coding: utf-8
from flask_restful import Resource, reqparse
from ..data_center.database.reader import HeaderMongodbReader
from ..data_center.csv.reader import HeaderCSVReader
from . import LineHeaderConfigMixin

_header_mongodb_reader = HeaderMongodbReader()

post_parser = reqparse.RequestParser()
post_parser.add_argument(
    'file', dest='file',
    # location='form',
    type=object,
    required=True, help='The line header config\'s file',
)


class LineHeaderConfig(Resource, LineHeaderConfigMixin):
    def get(self, line_no):
        _header_mongodb_reader.load_frame(line_no)

        self.if_not_exists(line_no)

        return _header_mongodb_reader.get_raw_data()

    def post(self, line_no):
        args = post_parser.parse_args()
        file = args.file
        file_name = 'LINE{}_STN_CFG.csv'.format(line_no)
        full_file_name = 'static/config/{}'.format(file_name)
        file.save(full_file_name)

        header_reader = HeaderCSVReader(line_no,full_file_name)
        header_reader.to_mongodb()

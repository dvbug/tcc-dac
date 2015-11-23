# coding: utf-8
from flask_restful import Resource, reqparse
from dac.data_center.database.reader import LineConfigMongodbReader
from dac.data_center.csv.reader import LineConfigCSVReader
from . import LineConfigMixin, make_json_response

_header_mongodb_reader = LineConfigMongodbReader()

post_parser = reqparse.RequestParser()
post_parser.add_argument(
    'file', dest='file',
    # location='form',
    type=object,
    required=True, help='The line header config\'s file',
)


class LineConfig(Resource, LineConfigMixin):
    def get(self, line_no):
        _header_mongodb_reader.load_frame(line_no)
        self.if_not_exists(line_no)
        data = _header_mongodb_reader.get_raw_data()
        return make_json_response(200, data), 200

    def post(self, line_no):
        args = post_parser.parse_args()
        file = args.file
        file_name = 'LINE{}_STN_CFG.csv'.format(line_no)
        full_file_name = 'static/config/{}'.format(file_name)
        file.save(full_file_name)

        header_reader = LineConfigCSVReader(line_no, full_file_name)
        header_reader.to_mongodb()
        # TODO need implement POST method.

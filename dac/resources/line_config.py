# coding: utf-8
"""
    tcc-dac.resources.line_config
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac resources line_config module.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
import os
from flask_restful import Resource, reqparse
from flask_json import as_json_p
from dac.data_center.database.reader import LineConfigMongodbReader
from dac.data_center.csv.reader import LineConfigCSVReader
# from dac.common.exceptions import NoDataError
from . import LineConfigMixin, make_json_response, make_json_response_2

_header_mongodb_reader = LineConfigMongodbReader()

post_parser = reqparse.RequestParser()
post_parser.add_argument(
    'file', dest='file',
    # location='form',
    type=str,
    required=True, help='The line header config\'s file',
)


class LineConfig(Resource, LineConfigMixin):

    @as_json_p
    def get(self, line_no):
        _header_mongodb_reader.load_frame(line_no)
        self.if_not_exists(line_no)
        data = _header_mongodb_reader.get_raw_data()

        return make_json_response(200, configs={line_no: data}), 200

    def post(self, line_no):
        args = post_parser.parse_args()
        file = args.file
        file_name = 'LINE{}_STN_CFG.csv'.format(line_no)
        upload_dir = 'dac/static/config'

        full_file_name = '{}/{}'.format(upload_dir, file_name)
        # with open(full_file_name, mode='w') as f:
        #     f.write(file)
        f = open(full_file_name, mode='w')
        f.write(file)
        f.close()

        try:
            # pid = os.fork()
            # if pid == 0:
                from dac.data_center.database import create_new_conn_db
                header_reader = LineConfigCSVReader(line_no, full_file_name)
                header_reader.to_mongodb(database=create_new_conn_db())
                print('line config to mongodb done.')
                # os._exit(0)
            # else:
                return make_json_response_2(200, message="File < {} > upload success.".format(file_name)), 200
        except OSError:
            return make_json_response_2(410, message="File < {} > upload failed.".format(file_name)), 410

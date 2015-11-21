# coding:utf-8
import json
import pandas as pd
from abc import ABCMeta, abstractmethod
from ..database import db


class CSVReader(object, metaclass=ABCMeta):
    def __init__(self, file):
        self.file = file
        self.data_frame = None

    def read_csv(self, **kwargs):
        if 'encoding' in kwargs:
            kwargs.pop('encoding')
        if 'dtype' in kwargs:
            kwargs.pop('dtype')

        self.data_frame = pd.read_csv(self.file, encoding='utf-8', dtype=object, **kwargs)

    @abstractmethod
    def to_mongodb(self): pass

    @abstractmethod
    def to_string(self): pass

    @abstractmethod
    def __repr__(self):
        pass

    @abstractmethod
    def __str__(self):
        pass


class HeaderCSVReader(CSVReader):
    __collection__ = 'stn_conf'

    def __init__(self, line_no, file):
        super(HeaderCSVReader, self).__init__(file)
        self.line_no = line_no
        self.load()

    def load(self):
        self.read_csv()
        if 'line_no' not in self.data_frame.columns:
            self.data_frame['line_no'] = self.line_no

    def to_mongodb(self):
        collection = db[self.__collection__]
        data = self.data_frame.to_dict(orient='records')

        # all the data's columns are str type,
        # need change 'area', 'seq' to int type before save to mongodb
        for row in data:
            row['seq'] = int(row['seq'])
            row['area'] = int(row['area'])

        key = {'line_no': self.data_frame.iloc[0]['line_no']}
        if collection.count(key) > 0:
            collection.remove(key)

        collection.insert(data)

    def to_string(self):
        header_header = 'trip,type,direction,'
        header_item = 'stop|%s|%s|%s|%s|%s'	 # stop|station name|station id|distance|A or D
        header = header_header
        for index, row in self.data_frame.iterrows():
            header_item1 = header_item % (row['stn_name'], row['stn_id'], row['distance'], row['area'], 'A')
            header_item2 = header_item % (row['stn_name'], row['stn_id'], row['distance'], row['area'], 'D')

            header = header + header_item1 + ','
            header = header + header_item2 + ','

        header = header[0:header.rfind(',')]
        # header_list = header.split(',')
        return header

    __str__ = to_string
    __repr__ = to_string


class TrainPlanCSVReader(CSVReader):
    __collection__ = 'train_plan'
    COLUMNS = ['line_no', 'date', 'A', 'B', 'trip', 'terminal_stn', 'idx', 'stn_id', 'arr_time', 'dep_time', 'direction'
               ]

    def __init__(self, file):
        super(TrainPlanCSVReader, self).__init__(file)
        self.load()

    def load(self):
        self.read_csv(header=None)
        self.data_frame.columns = self.COLUMNS

    def to_string(self):
        return '{}:{}'.format(type(self), self.file)

    def to_mongodb(self):
        collection = db[self.__collection__]
        data = self.data_frame.to_dict(orient='records')

        # all the data's columns are str type,
        # need change 'idx' to int type before save to mongodb
        for row in data:
            row['idx'] = int(row['idx'])

        key = {'date': self.data_frame.iloc[0]['date']}
        if collection.count(key) > 0:
            collection.remove(key)

        collection.insert(data)

    __str__ = to_string
    __repr__ = to_string


# class CSVTrainReader(CSVReader):
#     COLUMNS = ['line_no', 'date', 'A', 'B', 'trip', 'C', 'D', 'stn_id', 'arr_time', 'dep_time', 'direction']
#     __collection__ = 'stn_conf'
#
#     def __init__(self):
#         """
#         :type line_no:str
#         :type file: str
#         :type header_file: str
#         """
#         super(CSVTrainReader, self).__init__()
#         self.data_info = {}
#         # self.file = file
#         # self.header_file = header_file
#         self.orderd_stn = []
#         self._gen_data_info()
#         self._read_header()
#         self._read_data()
#
#     def _gen_data_info(self):
#         # TEMP_PLAN_201407020000_20140702074500.csv
#         file_name = self.file[:self.file.rfind('.')]
#         tmp = file_name.split('_')
#         if len(tmp) != 4:
#             raise ValueError('file name must like : TEMP_PLAN_201407020000_20140702074500.csv')
#
#         self.data_info['type'] = tmp[1]
#         self.data_info['start_time'] = tmp[2]
#         self.data_info['end_time'] = tmp[3]
#
#     def _read_data(self):
#         data_frame = pd.read_csv(self.file, encode='utf-8', dtype=str, header=None)
#         data_frame.columns = CSVTrainReader.COLUMNS
#         # get line_no 's data
#         data_frame = data_frame.groupby(CSVTrainReader.COLUMNS[0]).get_group(self.data_info['line_no'])
#         self._gen_data(data_frame)
#
#     def _gen_data(self, data_frame):
#         data = pd.DataFrame(columns=self.header)
#         groups = data_frame.groupby(CSVTrainReader.COLUMNS[4])
#         for trip, train_group in groups:
#             record_list = [trip, 'B', train_group[:1]['direction']]
#             time_list = self._gen_train_times(train_group, self.orderd_stn)
#             record_list.extend(time_list)
#             print(record_list)
#
#     @staticmethod
#     def _gen_train_times(train_group, orderd_stn):
#         """:type train_group: pd.DataFrame
#             :type orderd_stn: list
#         """
#         time_list = []
#         for stn in orderd_stn:
#             if stn in train_group['stn_id']:
#                 for index, row in train_group.iterrows():
#                     if row['stn_id'] == stn:
#                         if row['direction'] == '1':
#                             time_list.extend([row['dep_time'], row['arr_time']])
#                         else:
#                             time_list.extend([row['arr_time'], row['dep_time']])
#             else:
#                 time_list.append('-')
#                 time_list.append('-')
#
#         return time_list
#
#     def _read_header(self):
#         self.header = gen_header(self.header_file)
#         self.orderd_stn = gen_ordered_stns(self.header_file)
#


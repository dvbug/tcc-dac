# coding:utf-8
"""
    tcc-dac.data_center.csv.reader
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac data_center csv reader module.
    Loads from csv file, than fill into mongodb.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
import pandas as pd
from abc import ABCMeta, abstractmethod
from dac.data_center.database import db


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


class LineConfigCSVReader(CSVReader):
    __collection__ = 'line_conf'

    def __init__(self, line_no, file):
        super(LineConfigCSVReader, self).__init__(file)
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


class PlanScheduleCSVReader(CSVReader):
    __collection__ = 'plan_schedule'
    COLUMNS = ['line_no', 'date', 'A', 'B', 'trip', 'terminal_stn', 'idx', 'stn_id', 'arr_time', 'dep_time', 'direction'
               ]

    def __init__(self, file):
        super(PlanScheduleCSVReader, self).__init__(file)
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


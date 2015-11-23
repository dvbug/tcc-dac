# coding:utf-8
import os, time
import pandas as pd
from abc import ABCMeta, abstractmethod
# from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool
from .import db
from ..cache.redis_cache import RedisCache


class MongodbReader(object, metaclass=ABCMeta):
    def __init__(self):
        self.data_frame = None

    def __load_frame__(self, collection, *args, **kwargs):
        _collection = db[collection]
        ret = list(_collection.find(*args, **kwargs))
        self.data_frame = pd.DataFrame(ret, dtype=object)
        try:
            del self.data_frame['_id']
        except KeyError:
            pass

    @staticmethod
    def __exists__(collection, *args, **kwargs):
        _collection = db[collection]
        result = _collection.count(*args, **kwargs)
        return result > 0

    @abstractmethod
    def load_frame(self, *args, **kwargs): pass


class LineConfigMongodbReader(MongodbReader):
    __collection__ = 'line_conf'

    def __init__(self):
        super(LineConfigMongodbReader, self).__init__()

    def load_frame(self, line_no):
        self.__load_frame__(self.__collection__, {'line_no': line_no})
        try:
            self.data_frame = self.data_frame.sort_values('seq', ascending=True)
        except KeyError:
            pass

    def get_header_list(self):
        header_header = 'trip,type,direction,'
        header_item = 'stop|%s|%s|%s|%s|%s|%s'	 # stop|station name|station id|distance|area|A or D|index
        header = header_header
        for index, row in self.data_frame.iterrows():
            header_item1 = header_item % (row['stn_name'], row['stn_id'], row['distance'], row['area'], 'A', index+1)
            header_item2 = header_item % (row['stn_name'], row['stn_id'], row['distance'], row['area'], 'D', index+1)

            header = header + header_item1 + ','
            header = header + header_item2 + ','

        header = header[0:header.rfind(',')]
        header_list = header.split(',')
        return header_list

    def get_ascending_stations(self):
        """Returns [(seq,stn_id), ...]"""
        # data_frame is ordered, ascending
        return self.data_frame.loc[:, ['seq', 'stn_id']].to_records(index=False)

    @staticmethod
    def exists(line_no):
        return MongodbReader.__exists__(LineConfigMongodbReader.__collection__, {'line_no': line_no})

    def get_raw_data(self):
        data = self.data_frame.to_dict(orient='records')
        return data


class PlanScheduleMongodbReader(MongodbReader):
    __collection__ = 'plan_schedule'
    _type = 'PLAN'

    def __init__(self):
        self._line_no = None
        self._date = None
        self._data_list_result = None
        self._data_frame_result = None
        self._header_reader = LineConfigMongodbReader()
        self.header = []
        self.ordered_stn = []
        super(PlanScheduleMongodbReader, self).__init__()

    def load_frame(self, line_no, date):
        """
        :type line_no: str
        :type date: str
        """
        self._line_no = line_no
        self._date = date
        self.__load_frame__(self.__collection__, {'$and': [
            {'line_no': line_no},
            {'date': date}
        ]})
        self._load_header(line_no)
        self._get_data()

    @property
    def data_frame_result(self):
        return self._data_frame_result

    @property
    def data_list_result(self):
        return self._data_list_result

    def to_redis(self):
        dfr = self.data_frame_result
        """:type dfr: pd.DataFrame"""
        data = dfr.to_json(orient='index')
        RedisCache.set_redis_data('LINE{}_{}_{}'.format(self._line_no, self._type, self._date), data)

    def to_csv(self):
        file_path = 'dac/static/data/LINE{}_{}_{}.tcsv'.format(self._line_no, self._type, self._date)
        self.data_frame_result.to_csv(file_path, index=False)

    def _load_header(self, line_no):
        self._header_reader.load_frame(line_no)
        self.header = self._header_reader.get_header_list()
        self.ordered_stn = self._header_reader.get_ascending_stations()

    def _get_data(self):
        # pool = ThreadPool(10)
        pool = Pool(4)

        line_trains_records = list()

        trip_groups = self.data_frame.groupby('trip')

        # for trip, train_frame in trip_groups:
        #     record_list = self._gen_row(trip, train_frame)
        #     line_trains_records.append(record_list)

        results = pool.map_async(self._gen_row, trip_groups).get(1020)
        pool.close()
        pool.join()
        line_trains_records.extend(results)

        self._data_list_result = line_trains_records
        # self._data_list_result.append(self.header)
        self._data_frame_result = pd.DataFrame(line_trains_records)  # , columns=self.header
        self._data_frame_result.columns = self.header
        dfr = self._data_frame_result
        """:type dfr:pd.DataFrame"""
        dfr.index = dfr['trip']

    def _gen_row(self, *args):
        trip, train_frame = args[0]
        print('{} - PPID: {}-PID: {} - TRIP: {}'.format(time.time(), os.getppid(), os.getpid(), trip))
        # train_frame.iloc(0)[0]['direction']:  get first row's direction column value
        record_list = [trip, 'B', train_frame.iloc(0)[0]['direction']]
        time_list = self._gen_train_times(train_frame, self.ordered_stn)
        record_list.extend(time_list)
        return record_list

    @staticmethod
    def _gen_train_times(train_frame, ordered_stn):
        """:type train_frame: pd.DataFrame
            :type ordered_stn: list
        """
        time_list = []
        stn_col = train_frame['stn_id']

        for _, stn in ordered_stn:
            # find row in train_frame where stn_id == stn
            found_row = train_frame[stn_col == stn]
            if len(found_row) == 1:
                # for index, row in train_frame.iterrows():
                #     if row['stn_id'] == stn:
                #         if row['direction'] == '1':
                #             time_list.extend([row['dep_time'], row['arr_time']])
                #         else:
                #             time_list.extend([row['arr_time'], row['dep_time']])
                found_row = found_row.iloc(0)[0]

                if found_row['direction'] == '1':
                    time_list.extend([found_row['dep_time'], found_row['arr_time']])
                else:
                    time_list.extend([found_row['arr_time'], found_row['dep_time']])
            else:
                time_list.append('-')
                time_list.append('-')

        return time_list

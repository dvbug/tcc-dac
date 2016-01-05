# coding:utf-8
"""
    tcc-dac.data_center.database.section
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac data_center database section.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
import datetime
import time
import os
import multiprocessing as mp
from itertools import chain
from . import MongodbReader
from .reader import ScheduleMongodbReader, LineConfigMongodbReader


class SectionTripData(object):
    def __init__(self, trip, str_stn1_dep, str_stn2_arr):
        self._trip = trip
        time_format = "%Y%m%d%H%M%S"
        self._stn1_dep = datetime.datetime.strptime(str_stn1_dep, time_format)
        self._stn2_arr = datetime.datetime.strptime(str_stn2_arr, time_format)

    @property
    def trip(self):
        return self._trip

    @property
    def start_time(self):
        return self._stn1_dep

    @property
    def end_time(self):
        return self._stn2_arr

    def __str__(self):
        return 'trip[{}], dep[{}], arr[{}]'.format(self.trip, self._stn1_dep, self._stn2_arr)

    __repr__ = __str__


class Section(object):
    def __init__(self, line, direction, section_name):
        self._line = line
        self._direction = direction
        self._section_name = section_name
        self._trip_data = []

    @property
    def line(self):
        return self._line

    @property
    def direction(self):
        return self._direction

    def add_section_trip_data(self, std):
        # if len(self._trip_data) == 0:
        #     self._trip_data.insert(0, std)
        # # order data list
        # for index, item in enumerate(self._trip_data):
        #     if item.start_time > std.start_time:
        #         break
        # self._trip_data.insert(index, std)
        self._trip_data.append(std)
        # self._trip_data.sort(key=lambda x: x.start_time)

    def get_trip_count_from_period(self, dt1, dt2):
        count = 0
        for std in self._trip_data:
            if std.start_time >= dt1 and std.end_time <= dt2:
                count += 1
        return count

    def __str__(self):
        return self._section_name

    __repr__ = __str__


class SectionLine(object):
    def __init__(self, line):
        self._line = line
        self._sections_by_direction = {}  # index by direction

    def add_record(self, trip, stn1, stn2, direction, stn1dep, stn2arr):
        section_name = '{}-{}'.format(stn1, stn2)
        section = self.get_section(section_name, direction)
        std = SectionTripData(trip, stn1dep, stn2arr)
        section.add_section_trip_data(std)
        self._sections_by_direction[direction][section_name] = section

    def get_section(self, section_name, direction):
        if direction in self._sections_by_direction:
            sections_by_name = self._sections_by_direction[direction]
            if section_name in sections_by_name:
                return sections_by_name[section_name]
        else:
            self._sections_by_direction[direction] = {}

        return Section(self._line, direction, section_name)

    def get_all_sections_from_direction(self, direction):
        if direction in self._sections_by_direction:
            return self._sections_by_direction[direction]
        return {}


class SectionManager(object):

    def __init__(self, lines=None):
        self._line_sections = {}
        self.init_lines(lines)

    def init_lines(self, lines):
        for line in lines or []:
            self._line_sections[line] = SectionLine(line)

    def add_record(self, line, trip, stn1, stn2, direction, stn1dep, stn2arr):
        if line not in self._line_sections:
            self._line_sections[line] = SectionLine(line)
        s1 = self._line_sections[line]
        s1.add_record(trip, stn1, stn2, direction, stn1dep, stn2arr)

    def get_section_line(self, line):
        if line in self._line_sections:
            return self._line_sections[line]
        return None


def section_generator(stations, loop=False):
    import collections
    if isinstance(stations, collections.Iterable):
        stations = list(stations)

    count = len(stations)
    for i in range(0, count-1, 1):
        yield stations[i], stations[i+1]
    if loop:
        yield stations[-1], stations[0]


class SectionMongodbReader(MongodbReader):
    __collections__ = ScheduleMongodbReader.__collections__
    __types__ = list(__collections__.keys())

    """Section data reader."""
    def __init__(self):
        super(SectionMongodbReader, self).__init__()

    def load_frame(self, line_no, date):
        header_reader = LineConfigMongodbReader()
        header_reader.init_db(self._db)
        header_reader.load_frame(line_no)
        ordered_stations = header_reader.get_ascending_stations()
        sections = list(section_generator(ordered_stations))
        del header_reader

        data_frames = {}
        for _type in self.__types__:
            data_frames[_type] = self.__load_frame__(self.__collections__[_type], {'$and': [
                {'line_no': line_no},
                {'date': date}
            ]})
        return sections, data_frames


class SectionDataGenerator(object):

    __cache__ = {}
    __reader__ = SectionMongodbReader()

    def __init__(self, line_no, section_stations, data_frames):
        self._section_stations = section_stations
        self._data_frames = data_frames
        self._data_sections = {}  # key : _type PLAN, REAL
        self.__types__ = SectionMongodbReader.__types__
        for _type in self.__types__:
            self._data_sections[_type] = SectionManager()
        self._gen_data(line_no)

    @staticmethod
    def get_sections_data(line_no, date):
        if line_no not in SectionDataGenerator.__cache__:
            SectionDataGenerator.__cache__[line_no] = {}
        if date not in SectionDataGenerator.__cache__[line_no]:
            SectionDataGenerator.__cache__[line_no][date] = {}

        sd = SectionDataGenerator.__cache__[line_no][date]
        if len(sd) == 0:
            d = SectionDataGenerator.__reader__.load_frame(line_no, date)
            generator = SectionDataGenerator(line_no, *d)
            sd = SectionDataGenerator.__cache__[line_no][date] = generator.data_sections
        return sd

    @property
    def data_sections(self):
        return self._data_sections

    def _gen_data(self, line_no):
        pool = mp.Pool(8)
        for _type in self.__types__:
            df_trip_groups = self._data_frames[_type].groupby('trip')
            sm = self._data_sections[_type]
            """:type sm: SectionManager"""

            trips_data = []
            results = pool.map(self._gen_trip_data, df_trip_groups)
            trips_data.extend(results)
            trips_data = list(chain(*trips_data))
            for d in trips_data:
                sm.add_record(line_no, *d)
        pool.close()
        pool.join()
        print('Done')

    def _gen_trip_data(self, trip_group):
        trip, train_frame = trip_group
        print('{} - PPID: {}-PID: {} - TRIP: {}'.format(time.time(), os.getppid(), os.getpid(), trip))
        trip_records = self._gen_trip_record_data(trip, train_frame)
        return trip_records

    def _gen_trip_record_data(self, trip, train_frame):
        stn_col = train_frame['stn_id']
        result = []
        for tuple_stn1, tuple_stn2 in self._section_stations:
            idx1, stn1 = tuple_stn1
            idx2, stn2 = tuple_stn2
            # find row in train_frame where stn_id == stn1 / stn2
            stn1_row = train_frame[stn_col == stn1]
            stn2_row = train_frame[stn_col == stn2]
            if len(stn1_row) == 1 and len(stn2_row) == 1:
                stn1_row = stn1_row.iloc(0)[0]
                stn2_row = stn2_row.iloc(0)[0]
                if stn1_row['direction'] == '1':
                    result.append(
                        (trip, stn1, stn2, stn1_row['direction'], stn1_row['dep_time'], stn2_row['arr_time']))
                else:
                    result.append(
                        (trip, stn2, stn1, stn2_row['direction'], stn2_row['dep_time'], stn1_row['arr_time']))
        return result


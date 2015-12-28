# coding:utf-8
"""
    tcc-dac.data_center.database.section
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac data_center database section.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""
import datetime


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
        if len(self._trip_data) == 0:
            self._trip_data.insert(0, std)
        # order data list
        for index, item in enumerate(self._trip_data):
            if item.start_time > std.start_time:
                break
        self._trip_data.insert(index, std)

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

    def __init__(self, lines):
        self._line_sections = {}
        for line in lines:
            self._line_sections[line] = SectionLine(line)

    def add_record(self, line, trip, stn1, stn2, direction, stn1dep, stn2arr):
        if line in self._line_sections:
            s1 = self._line_sections[line]
            s1.add_record(trip, stn1, stn2, direction, stn1dep, stn2arr)

    def get_section_line(self, line):
        if line in self._line_sections:
            return self._line_sections[line]
        return None


# coding: utf-8
"""
    tcc-dac.test_data_center
    ~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac website test data_center module.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""


def __update_time_field__(self, days=0, seconds=0, minutes=0, hours=0):
    import datetime
    delta = datetime.timedelta(days=days, seconds=seconds, minutes=minutes, hours=hours)

    data_frame = self.__getattribute__('data_frame')
    """:type data_frame: pd.DataFrame"""
    for i, row in data_frame.iterrows():
        raw_time = datetime.datetime.strptime(str(row["arr_time"]), "%Y%m%d%H%M%S")
        row["arr_time"] = (raw_time + delta).strftime("%Y%m%d%H%M%S")

        raw_time = datetime.datetime.strptime(str(row["dep_time"]), "%Y%m%d%H%M%S")
        row["dep_time"] = (raw_time + delta).strftime("%Y%m%d%H%M%S")


def to_csv(cls, path):
    data_frame = cls.__getattribute__('data_frame')
    data_frame.to_csv(path, index=False)


class CSVTimeGenMetaclass(type):

    def __new__(mcs, name, bases, attrs):
        update_methods = {
            "update_time": __update_time_field__,
            "to_csv": to_csv,
        }

        attrs.update(update_methods)

        return super().__new__(mcs, name, bases, attrs)


if __name__ == '__main__':
    # from dac.data_center.csv.reader import LineConfigCSVReader
    # h_csv_reader = LineConfigCSVReader('01', 'dac/static/configs/LINE01_STN_CFG.csv')
    # h_csv_reader.to_string()
    # h_csv_reader.to_mongodb()
    #

    # from dac.data_center.csv.reader import ScheduleCSVReader
    # plan_reader = ScheduleCSVReader('dac/static/schedules/TEMP_PLAN_201407020000_20140702074500.csv', 'PLAN')
    # plan_reader.to_mongodb()
    #

    # from dac.data_center.database.reader import LineConfigMongodbReader
    # h_mongodb_reader = LineConfigMongodbReader()
    # h_mongodb_reader.load_frame('01')
    # header = h_mongodb_reader.get_header_list()
    # print(header)
    #

    # from dac.data_center.database.reader import ScheduleMongodbReader
    # train_reader = ScheduleMongodbReader()
    # train_reader.load_frame('01', '20140702', 'REAL')
    # # train_reader.to_csv()
    # train_reader.to_redis()
    #

    # from dac.data_center.csv.reader import ScheduleCSVReader
    # from abc import ABCMeta
    # CSVTimeGenABCMeta = type("CSVTimeGenABCMeta", (ABCMeta, CSVTimeGenMetaclass), {})
    #
    # class UpdaterScheduleCSVReader(ScheduleCSVReader, metaclass=CSVTimeGenABCMeta):
    #     pass
    # reader = UpdaterScheduleCSVReader('dac/static/schedules/TEMP_PLAN_201407020000_20140702074500.csv', 'REAL')
    # reader.update_time(days=0, hours=0, minutes=2, seconds=0)
    # reader.to_csv("dac/static/schedules/TEMP_REAL_201407020000_20140702074500.csv")
    #

    pass

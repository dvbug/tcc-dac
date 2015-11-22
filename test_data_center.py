# coding: utf-8


if __name__ == '__main__':
    from dac.data_center.csv.reader import LineConfigCSVReader
    h_csv_reader = LineConfigCSVReader('01', 'dac/static/config/LINE01_STN_CFG.csv')
    h_csv_reader.to_string()
    h_csv_reader.to_mongodb()

    from dac.data_center.csv.reader import PlanScheduleCSVReader
    plan_reader = PlanScheduleCSVReader('dac/static/data/TEMP_PLAN_201407020000_20140702074500.csv')
    plan_reader.to_mongodb()

    from dac.data_center.database.reader import LineConfigMongodbReader
    h_mongodb_reader = LineConfigMongodbReader()
    h_mongodb_reader.load_frame('01')
    header = h_mongodb_reader.get_header_list()
    print(header)

    from dac.data_center.database.reader import PlanScheduleMongodbReader
    train_reader = PlanScheduleMongodbReader()
    train_reader.load_frame('01', '20140702')
    train_reader.to_csv()
    train_reader.to_redis()

# coding: utf-8


if __name__ == '__main__':
    from dac.data_center.csv.reader import HeaderCSVReader
    h_csv_reader = HeaderCSVReader('01', 'static/config/LINE01_STN_CFG.csv')
    h_csv_reader.to_string()
    h_csv_reader.to_mongodb()

    from dac.data_center.csv.reader import TrainPlanCSVReader
    plan_reader = TrainPlanCSVReader('static/data/TEMP_PLAN_201407020000_20140702074500.csv')
    plan_reader.to_mongodb()

    from dac.data_center.database.reader import HeaderMongodbReader
    h_mongodb_reader = HeaderMongodbReader()
    h_mongodb_reader.load_frame('01')
    header = h_mongodb_reader.get_header_list()
    print(header)

    from dac.data_center.database.reader import TrainPlanMongodbReader
    train_reader = TrainPlanMongodbReader()
    train_reader.load_frame('01', '20140702')
    train_reader.to_csv()
    train_reader.to_redis()

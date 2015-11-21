# coding:utf-8
import csv
from .tcsv_header_generator import gen_header, gen_ordered_stns

'''
input csv data example:
01,20140702,0000,0070,2302,0125,1,0103,20140702182208,20140702182308,2
01,20140702,0000,0070,2302,0125,2,0104,20140702182648,20140702182718,2

output csv data example:
2301,B,S,-,20140702052208,20140702052648,20140702052718,20140702052948,20140702053018,20140702053248,20140702053513,20140702053808,20140702054058,20140702054313,20140702054538,20140702054808,20140702055038,20140702055225,20140702055512,20140702055727,20140702055923,20140702060121,20140702060311,20140702060544,20140702060823,20140702061003,20140702061248,20140702061548,20140702061903
'''


# 适配模板算法
def _process_station_records(trip, ordered_stns, path, direction):
    time_list = []

    for stn in ordered_stns:

        if stn in path:
            time_pair = path[stn]
            if len(time_pair) != 1:
                print('Trip %s has duplicated stations at %s. ' % (trip, stn))

            temp_time_pair = time_pair[0]
            if direction == '1':                # time convert
                temp_time_pair = [time_pair[0][1], time_pair[0][0]]
            time_list.extend(temp_time_pair)
        else:
            print('Trip: %s has skipped stations at %s. ' % (trip, stn))
            time_list.append('-')
            time_list.append('-')

    return time_list


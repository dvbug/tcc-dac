# coding:utf-8

import csv

'''
trip,type,direction,stop|苹果园|0103|0|1|A,stop|苹果园|0103|0|1|D,stop|古城路|0104|40|1|A,stop|古城路|0104|40|1|D,stop|八角游乐园|0105|124|1|A,stop|八角游乐园|0105|124|1|D,stop|八宝山|0106|178|1|A,stop|玉泉路|0107|216|1|A,stop|五棵松|0108|249|2|A,stop|万寿路|0109|280|2|A,stop|公主坟（1号线）|0110|307|2|A,stop|军事博物馆（1号线）|0111|334|2|A,stop|木樨地|0112|354|2|A,stop|南礼士路|0113|391|2|A,stop|复兴门（1号线）|0114|428|2|A,stop|西单（1号线）|0115|458|2|A,stop|天安门西|0116|499|2|A,stop|天安门东|0117|543|3|A,stop|王府井|0118|574|3|A,stop|东单（1号线）|0119|604|3|A,stop|建国门（1号线）|0120|649|3|A,stop|永安里|0121|676|3|A,stop|国贸（1号线）|0122|702|3|A,stop|大望路|0123|731|3|A,stop|四惠（1号线）|0124|757|4|A,stop|四惠东（1号线）|0125|805|4|A
'''

HEADER_HEADER = 'trip,type,direction,'
HEADER_ITEM = 'stop|%s|%s|%d|%d|%s'					# stop|station name|staion id|distance|A or D

STN_SEQ = ['0103','0104','0105','0106','0107','0108','0109','0110','0111','0112','0113','0114','0115','0116','0117','0118','0119','0120','0121','0122','0123','0124','0125']

STN_MAP = {
    '0103':	[u'苹果园', 0, 1],
    '0104':	[u'古城路', 40, 1],
    '0105':	[u'八角游乐园', 80, 1],
    '0106':	[u'八宝山', 120, 1],
    '0107':	[u'玉泉路', 160, 1],
    '0108':	[u'五棵松', 200, 1],
    '0109':	[u'万寿路', 240, 1],
    '0110':	[u'公主坟（1号线）', 280, 1],
    '0111':	[u'军事博物馆（1号线）', 320, 1],
    '0112':	[u'木樨地', 360, 1],
    '0113':	[u'南礼士路', 400, 1],
    '0114':	[u'复兴门（1号线）', 440, 1],
    '0115':	[u'西单（1号线）', 480, 1],
    '0116':	[u'天安门西', 520, 1],
    '0117':	[u'天安门东', 560, 1],
    '0118':	[u'王府井', 600, 1],
    '0119':	[u'东单（1号线）', 640, 1],
    '0120':	[u'建国门（1号线）', 680, 1],
    '0121':	[u'永安里', 720, 1],
    '0122':	[u'国贸（1号线）', 760, 1],
    '0123':	[u'大望路', 800, 1],
    '0124':	[u'四惠（1号线）', 840, 1],
    '0125':	[u'四惠东（1号线）', 880, 1]
}


def gen_header(csv_file):

    stn_seq = []
    stn_list = {}

    with open(csv_file, 'rb') as f:
        csv_reader = csv.reader(f, delimiter=',')
        for row in csv_reader:
            if row[0] == u'seq':
                continue
            stn_id = row[1]
            stn_name = row[2]
            distance = int(row[3])
            area = int(row[4])

            stn_seq.append(stn_id)
            stn_list[stn_id] = [stn_name, distance, area]

    header = HEADER_HEADER

    for stn_id in stn_seq:

        header_item1 = HEADER_ITEM % (stn_list[stn_id][0], stn_id, stn_list[stn_id][1], stn_list[stn_id][2], 'A')
        header_item2 = HEADER_ITEM % (stn_list[stn_id][0], stn_id, stn_list[stn_id][1], stn_list[stn_id][2], 'D')

        header = header + header_item1 + ','
        header = header + header_item2 + ','

    # print header
    # print header[0:header.rfind(',')]
    header = header[0:header.rfind(',')]
    header_list = header.split(',')
    return header_list


def gen_ordered_stns(csv_file):
    stn_seq = []

    with open(csv_file, 'rb') as f:
        csv_reader = csv.reader(f, delimiter=',')
        for row in csv_reader:
            if row[0] == u'seq':
                continue
            stn_id = row[1]
            stn_seq.append(stn_id)

    return stn_seq

if __name__ == '__main__':
    csv_file_name = 'LINE01_STN_CFG.csv'
    print(gen_header(csv_file_name))

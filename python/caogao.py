# coding:utf-8

import time,re
import jieba

import jieba.posseg as pseg
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkContext, SparkConf, SQLContext
from py4j.protocol import Py4JJavaError


# 环境
env = sys.argv[1]

con = SparkConf().set("spark.rpc.message.maxSize","256")
sc = SparkContext(appName="getData_"+env, conf=con)
sqlContext = SQLContext(sc)
sc.setLogLevel("WARN")

# 一段时间内的热销(unicode)
timefield = '190'
now = time.time()
print now
before = now-24 * 60 * 60 * int(timefield)
print before
timenode = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(before)).split()[0]+' 00:00:00.0'
print timenode
time0 = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now)).split()[0]
time1 = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now-24 * 60 * 60 *1)).split()[0]
time2 = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now-24 * 60 * 60 *2)).split()[0]
time3 = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now-24 * 60 * 60 *3)).split()[0]

POS = ('n', 'v', 'nr', 'vn', 'ns', 'an', 'nz', 'nt', 'a', 'd', 'eng', 'j', 'l', 'nz', 'ns', 'nt',)


def try_load(path):
    try:
        rdd = sc.textFile(path)
        rdd.first()
        return rdd
    except Py4JJavaError as e:
        print e
        return sc.emptyRDD()




itempaths = [
            '/item/env=' + env + '/*/*/*'
        ]


def load_dict(x):
    # 调整词库
    jieba.load_userdict("/python2.7/site-packages/jieba/item_dict")
    return x


def word_cut(x):

    word_lis = []
    if not x[0] and x[1]:
        x[0] = x[1]
    # if x[0].isdigit():
    #     x[0] = x[1]
    # t_lis = re.findall('[A-Za-z0-9\x80-\xff]+', x[0])
    # t = ''.join(t_lis)
    seg_list = pseg.cut(x[0])

    for w in seg_list:
        # 过滤中文标点
        if w.flag not in ('w', 'x', 'c', 'e', 'r', 'y'):
            #w.flag in POS:除开标点符号, 'w', 'x', 'c', 'e', 'r', 'y'
            # (' ','。','？','！','，','、','；','：','“','”','’','‘',
            #              '（','）','【','】','——','……','-','·','《','》')
            word_lis.append(w.word.encode('utf-8'))
    # keywordList
    return ','.join(word_lis)


item = sc.union([try_load(path) for path in itempaths]) \
         .map(lambda line: line.split('\001')) \
         .map(lambda x: (x[0].encode('utf-8'), x[4].encode('utf-8'),
                         x[6].encode('utf-8'), x[7].encode('utf-8')))
item1 = item.map(lambda x: ((x[0], x[1]), [x[2], x[3]]))
print 'item***************************'
df_item = sqlContext.createDataFrame(item, ['itemid', 'tenantid', 'itemname', 'simplename'])


item3 = item1.mapPartitions(load_dict)
word = item3.mapValues(word_cut)
print 'word***************************'
item2 = word.map(lambda x: x[0][0] + '\001' + x[0][1] + '\001' + x[1])
print 'item2**************************'
writepath = '/data/itemKeyword/' + env + '/cdate=' + time0
try:
    if not item2.isEmpty():
        item2.saveAsTextFile(writepath)
except Py4JJavaError as e:
    print e
print 'save**************************'



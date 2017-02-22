# coding:utf-8

import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkContext, SQLContext
from py4j.protocol import Py4JJavaError

# 环境
env = sys.argv[1]

# 一段时间内的热销(unicode)
timefield = sys.argv[2]
now = time.time()
print now
before = now-24 * 60 * 60 * int(timefield)
print before
timenode = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(before)).split()[0]+' 00:00:00.0'
print timenode
current = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now)).split()[0]

sc = SparkContext(appName="getItem_"+env+"_"+timefield)
sqlContext = SQLContext(sc)
sc.setLogLevel("WARN")

# 获得商品id列表
path = '/data/tradeitemtime/'+env+'/*/*'

try:
    rd_iic = sc.textFile(path).map(lambda line: line.split(","))\
                   .filter(lambda x: x[2].encode('utf-8') > timenode)\
                   .map(lambda x: (x[0].encode('utf-8'), [x[1].encode('utf-8')]))

    rd_tradeitem = rd_iic.reduceByKey(lambda x,y:x+y).mapValues(set)
    print '********************************'
    # 每一行为一个订单的购买的商品列表，无重复值
    rd_input = rd_tradeitem.map(lambda x: ' '.join(list(x[1])))
    print 'orderitem********************************'

    writepath = '/source/test/tradeitem/'+env+'/cdate='+current+'_'+ timefield
    rd_input.coalesce(20).saveAsTextFile(writepath)
except Py4JJavaError as e:
    print Py4JJavaError, ":", e
    # sys.exit()



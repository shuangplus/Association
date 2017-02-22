# coding:utf-8
'''
寻找中位数
'''
from __future__ import division
import datetime
import numpy as np
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkContext, SparkConf
from py4j.protocol import Py4JJavaError

# 环境
env = sys.argv[1]
# support值
# sup = sys.argv[2]
# 一段时间内的热销(unicode)
timefield = sys.argv[2]

con = SparkConf().set("spark.driver.maxResultSize","2g")
sc = SparkContext(appName="findAve"+env+"_"+timefield,conf=con)
sc.setLogLevel("WARN")
current = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print current

try:
    data_lis = sc.textFile('/source/test/freqItemsets/' + env + '/*' + timefield + '/*') \
                 .map(lambda line: line.split(': ')) \
                 .map(lambda x: int(x[1])).collect()
except Py4JJavaError as e:
    print Py4JJavaError, ":", e
    sys.exit()


n = len(data_lis)
# ave = np.mean(data_lis)
if n > 1200000 and timefield != '180':
    c = (1 - round(1000000/n, 4))*100
    med = np.percentile(data_lis, c) + 1
    lis = [med]
    sc.parallelize(lis).saveAsTextFile('/source/test/freqItemsets/' + env + \
                                       '/support/cdate=' + current.split()[0] + '_' + timefield)
elif n > 1500000 and timefield == '180':
    c = (1 - round(1000000 / n, 4)) * 100
    med = np.percentile(data_lis, c) + 1
    lis = [med]
    sc.parallelize(lis).saveAsTextFile('/source/test/freqItemsets/' + env + \
                                       '/support/cdate=' + current.split()[0] + '_' + timefield)
else:
    med = 0
print '*******************************'
print med
print '*******************************'
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")






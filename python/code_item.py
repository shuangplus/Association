# coding:utf-8
'''
实现几个表的join
'''
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkContext, SQLContext

# 环境
env = sys.argv[1]

sc = SparkContext(appName="joinTables_"+env)
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)

# 一段时间内的热销(unicode)
timefield = '190'
now = time.time()
print now
before = now-24 * 60 * 60 * int(timefield)
print before
timenode = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(before)).split()[0]+' 00:00:00.0'
print timenode
time0 = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now)).split()[0]


# 读取表的数据
def get_data(headerpath, detailpath, skupath, itempath):

    # 从自己的目录读入
    rd_header = sc.textFile(headerpath).map(lambda line: line.split("\001"))\
                .filter(lambda y: len(y)==2)\
                .filter(lambda x: x[1].encode('utf-8') > timenode)
    print rd_header.take(10)
    df_header = sqlContext.createDataFrame(rd_header, ['id', 'createdate'])
    print df_header.show(10)
    # print df_header.count()
    print 'header***************************'

    rd_detail_ = sc.textFile(detailpath).map(lambda line: line.split("\001"))\
                .filter(lambda y: len(y)==5) \
                .filter(lambda x: x[0].encode('utf-8') > timenode)
    rd_detail = rd_detail_.filter(lambda x: x[3] != 'null').map(lambda x:(x[0],x[1],x[2],x[3]))
    rd_de_null = rd_detail_.filter(lambda x: x[3] == 'null').map(lambda x:(x[0],x[1],x[2],x[4]))

    print 'null***************************'
    print rd_detail.take(10)
    df_detail = sqlContext.createDataFrame(rd_detail, ['createdate', 'tenantid', 'pid', 'skuid'])
    df_de_null = sqlContext.createDataFrame(rd_de_null, ['createdate', 'tenantid', 'pid', 'platitemname'])
    print df_de_null.show(10)
    print df_detail.show(10)

    # 从官方目录读入

    rd_sku = sc.textFile(skupath).map(lambda line: line.split("\001")) \
        .map(lambda x: (x[0], x[4], x[5])) \
        .filter(lambda y: len(y) == 3)
    df_sku = sqlContext.createDataFrame(rd_sku, ['skuid', 'tenantid', 'itemid'])
    print df_sku.show(10)
    # print df_sku.count()
    print 'sku***************************'

    rd_item = sc.textFile(itempath).map(lambda line: line.split("\001")) \
        .map(lambda x: (x[0], x[4], x[6])) \
        .filter(lambda y: len(y) == 3)
    df_item = sqlContext.createDataFrame(rd_item, ['itemid', 'tenantid', 'itemname'])
    print df_item.show(10)
    # print df_item.count()
    print 'item***************************'

    return df_header, df_detail, df_de_null, df_sku, df_item


# 连接没sku的表
def table_join_null(df_header, df_de_null, df_item):

    df_sv = df_header.join(df_de_null, df_header.id == df_de_null.pid).select(df_header.id,df_header.createdate,df_de_null.platitemname)
    df_itemorder = df_sv.join(df_item, df_sv.platitemname == df_item.itemname).select(df_sv.id,df_item.itemid,df_sv.createdate)
    print df_itemorder.show(10)
    print 'orderitemtenant***************************************'

    return df_itemorder


# 连接有sku的三张表
def table_join(df_header, df_detail, df_sku):

    df_sv = df_header.join(df_detail, df_header.id == df_detail.pid).select(df_header.id,df_header.createdate,df_detail.skuid)
    print df_sv.show(10)
    print 'order***************************************'
    df_itemvip = df_sv.join(df_sku, df_sv.skuid == df_sku.skuid).select(df_sv.id,df_sku.itemid,df_sv.createdate)
    print df_itemvip.show(10)
    print 'orderitemtenant***************************************'

    return df_itemvip


# 获得商品id列表
def combine(df_itemvip, df_itemorder, writepath):

    # 将两个部分union
    df_tradeitem = df_itemvip.union(df_itemorder)
    df_tradeitem.write.csv(writepath)


def fun():
    headerpath = '/data/header/'+env+'/cdate='+time0+'/*'

    detailpath = '/data/detail/'+env+'/cdate='+time0+'/*'

    skupath = '/sku/env='+env+'/*/*/*'

    itempath = '/item/env=' + env + '/*/*/*'

    writepath = '/data/tradeitemtime/'+env+'/cdate=' + time0

    df_header, df_detail,df_de_null, df_sku, df_item = get_data(headerpath, detailpath, skupath, itempath)
    df_itemvip = table_join(df_header, df_detail, df_sku)
    df_itemorder = table_join_null(df_header, df_de_null, df_item)
    combine(df_itemvip, df_itemorder, writepath)


if __name__ == '__main__':
    fun()








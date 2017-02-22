# coding:utf-8
'''
频繁项集入库
'''
from __future__ import division
import MySQLdb, time
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
now = time.time()
base = time.mktime(time.strptime("2017-01-10 00:00:00", '%Y-%m-%d %H:%M:%S'))
flag = int((now-base)/(3*24*60*60))
current = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now))
before = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(base + flag*3*24*60*60))   # 三天前


con = SparkConf().set("spark.driver.maxResultSize", "2g")
sc = SparkContext(appName="toGroup_"+env+"_"+timefield, conf=con)
sc.setLogLevel("WARN")


# 将商品组合与商家id对应
def get_data(datapath, itempath):
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    try:
        data_lis = sc.textFile(datapath) \
            .map(lambda line: line.split(': ')) \
            .map(lambda x: [x[0][1:-1].encode('utf-8'), int(x[1])]).collect()
        print 'data_lis*********************'+ time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        if not data_lis:
            print 'Dataset is empty!'
            sys.exit()

        item = sc.textFile(itempath)\
                     .map(lambda line: line.split('\001')) \
                     .map(lambda x:(int(x[0].encode('utf-8')), int(x[1].encode('utf-8')), x[2].encode('utf-8')))\
                     # .collect()

        print 'item**************************'+ time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    except Py4JJavaError as e:
        print Py4JJavaError, ":", e
        sys.exit()

    return item, data_lis


# 将item拆分
def item_split(item, j, spli):
    print '**********************\nItem Split\n*************************'
    # 按租户id来划分
    if spli == True:
        item_lis = item.filter(lambda x:(x[1] % 3 == j)).collect()
        it_dic = {x[0]: x[1] for x in item_lis}
        in_dic = {x[0]: x[2].split(',') for x in item_lis}
    else:
        item_lis = item.collect()
        it_dic = {x[0]: x[1] for x in item_lis}
        in_dic = {x[0]: x[2].split(',') for x in item_lis}

    return it_dic, in_dic


def item_group(it_dic, in_dic, data_lis, spli):
    print '**********************\nIteration\n*************************'
    try:
        for index, v in enumerate(data_lis):
            # print index
            tid = []
            name_lis = []
            # n = data_lis[index][1]
            # data_lis[index][1] = str(n)

            for i in v[0].split(','):
                if it_dic.has_key(int(i)):
                    tid.append(it_dic[int(i)])
                    name_lis.extend(in_dic[int(i)])

            if len(set(tid)) == 1:
                # tenant_id
                data_lis[index].append(tid[0])
                # 环境，时间段
                data_lis[index].append(env)
                data_lis[index].append(timefield)
                # keywords
                name = {}.fromkeys(name_lis).keys()   # 去重
                data_lis[index].append(','.join(name))
                data_lis[index].append(current)

            else:
                data_lis[index].extend(['','','','',''])
            if index % 10000 == 0:
                print index
            # else:
            #     n = Counter(tid)      # 经常一起购买但是这个商家没有的商品
            #     data_lis[index][-1] = ','.join(i_li)
            #     data_lis[index][-2] = Counter(tid).most_common(1)[0]
            # if len(data_lis[index]) != 7:
            #     print index
            #     print data_lis[index]
        print 'Loop is ok!************************'+ time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        if spli == True:
            res_lis = filter(lambda x: x[2] != '', data_lis)  # 取存入数据库的部分
            print len(res_lis)
            print 'ok'
            data_lis = filter(lambda x: x[2] == '', data_lis)  # 取剩下的部分
            data_lis = map(lambda x: [x[0], x[1]], data_lis)
            print len(data_lis)

            return res_lis, data_lis
        else:
            return data_lis

    except Exception as e:
        print 'Error is : ',e
        sys.exit()




# 存入文件
def to_file(data_lis, j):
    print "**************************\nSave!\n****************************"
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    data_save = sc.parallelize(data_lis, 20).map(lambda x: (x[0], str(x[1]), str(x[2]), x[5], x[6])).map(lambda x: '\001'.join(x))
    data_save.saveAsTextFile('/source/test/itemGroup/' + env + '/cdate=' + current.split()[0] + '_'+ j + '_' + timefield)
    print 'save************************' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))


# 存入数据库
def to_database(data_lis, db, table):
    try:
        conn = MySQLdb.connect(host='',
                               user='', passwd='', port=3306,
                               charset="utf8")
        cur = conn.cursor()

        print "**************************\nConnection is OK!\n****************************"

        conn.select_db(db)
        print "**************************\nDB is OK!\n****************************"


        cur.execute("select count(0) from " + table + " where timefield = '%s' and time < '%s';" % (timefield, before))
        results = cur.fetchall()
        if results[0][0] != 0:
            cur.execute("delete from "+table+" where timefield = '%s' and time < '%s';" % (timefield, before))
            print "**************************\nDelete is OK!\n****************************"


        cur.executemany("insert into " + table + "(itemidList, frequency, tenantid, env, timefield, itemname, time) "+
                                                 "values(%s,%s,%s,%s,%s,%s,%s)", data_lis)
        print "**************************\nInserting data is OK!\n****************************"

        conn.commit()
        cur.close()
        conn.close()

    except MySQLdb.Error, e:
        print e


def try_load(path):
    try:
        rdd = sc.textFile(path)
        rdd.first()
        return rdd
    except Py4JJavaError as e:
        print e
        return sc.emptyRDD()


def fun():
    rd = try_load('/source/test/freqItemsets/'+env+'/support/*'+timefield)
    if rd.isEmpty():
        readpath = '/source/test/freqItemsets/' + env + '/*' + timefield + '/*'
    else:
        readpath = '/source/test/freqItemsets/' + env + '/result/*' + timefield + '/*'

    itempath = '/data/itemKeyword/'+env+'/*/*'
    item, ig_data_lis = get_data(readpath, itempath)
    print 'get_data**************************'+time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    if env == "demo":
        print '*********************\ndemo\n*********************'
        datab = ''
        table = ''
    elif flag % 2 == 0:
        print '*********************\nItem group\n*********************'
        datab = ''
        table = '' + env
    else:
        print '*********************\nItem group 1\n*********************'
        datab = ''
        table = '' + env + '_1'

    if len(ig_data_lis) > 1000000 and env == '':
        for j in (0,1,2):
            itdic, indic = item_split(item, j, True)
            res_lis, data2_li = item_group(itdic, indic, ig_data_lis, True)
            ig_data_lis = data2_li

            to_database(res_lis, datab, table)
            to_file(res_lis, str(j))
    else:
        itdic, indic = item_split(item, 0, False)
        res_lis, data2_li = item_group(itdic, indic, ig_data_lis, False)
        to_database(res_lis, datab, table)
        to_file(res_lis, '')


if __name__ == '__main__':
    fun()




# coding:utf-8
'''
关联规则入库
'''
import MySQLdb,datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkContext, SQLContext

# 环境
env = sys.argv[1]
# support值
# sup = sys.argv[2]
# 一段时间内的热销(unicode)
timefield = sys.argv[2]
current = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

sc = SparkContext(appName="toTenant_"+env)
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)


# 对商品关联排序
def rules_sort(env):

    respath = '/source/test/associationRules/'+env+'/*'+timefield+'/*'
    data = sc.textFile(respath)
    item1 = data.map(lambda line:line.split(': ')).map(lambda x:(x[0],float(x[1])))
    # .filter(lambda x:len(x[0])<42)

    item_sortbyval = item1.sortBy(lambda x: x[1],ascending=False).\
                     map(lambda y:y[0].encode('utf-8')+': '+str(y[1]))

    return item_sortbyval


# 将关联规则与商家id对应
def item_tenant(item_sortbyval):

    data = item_sortbyval.map(lambda line: line.split('}'))\
           .map(lambda x: (x[0][1:], x[1][5:], x[2][2:]))

    temp = data.map(lambda x: ','.join(x).split(',')).collect()
    print temp[:5]
    print 'temp**********************************'

    data1 = data.map(lambda x: [x[0], x[1], float(x[2]), 'none', 'False'])
    print 'data1**********************************'
    data_lis = data1.collect()

    item = sc.textFile('/item/env='+env+'/*/*/*') \
                       .map(lambda line: line.split('\001')) \
                       .map(lambda x:(x[4].encode('utf-8'), x[0].encode('utf-8')))

    it_lis = item.map(lambda x:(x[1],x[0])).collect()
    it_dic = {x[0]:x[1] for x in it_lis}

    print it_lis[:5]

    for index,v in enumerate(temp):
        tid = []
        # 环境，时间段
        data_lis[index].append(env)
        data_lis[index].append(timefield)
        for i in v[:-1]:
            tid.append(it_dic[i])
        if len(set(tid)) == 1:
            data_lis[index][3] = tid[0]
            data_lis[index][4] = 'True'
        data_lis[index].append(current)
        # else:
        #     n = Counter(tid)      # 经常一起购买但是这个商家没有的商品
        #     data_lis[index][-1] = ','.join(i_li)
        #     data_lis[index][-2] = Counter(tid).most_common(1)[0]
        print data_lis[index]

    data_save = sc.parallelize(data_lis)
    data_save.saveAsTextFile('/source/test/itemtenant/'+env+'/cdate='+current.split()[0]+'_'+timefield)

    return data_lis


# 存入数据库
def to_database(data_lis, table):
    try:
        conn = MySQLdb.connect(host='',
                               user='', passwd='', port=3306)
        cur = conn.cursor()

        print "**************************\nConnection is OK!\n****************************"

        conn.select_db('')
        print "**************************\nDB is OK!\n****************************"

        cur.execute("select count(env) from " + table + " where env = '%s' and timefield = '%s'" % (env,timefield))
        results = cur.fetchall()
        if results[0][0] != 0:
            # cur.execute('truncate table ' + table)
            cur.execute("delete from "+table+" where env = '%s' and timefield = '%s'" % (env,timefield))
            print "**************************\nDelete is OK!\n****************************"

        cur.executemany('insert into ' + table + ' values(%s,%s,%s,%s,%s,%s,%s,%s)', data_lis)
        print "**************************\nInserting data is OK!\n****************************"

        conn.commit()
        cur.close()
        conn.close()

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])


def fun():
    item_sortbyval = rules_sort(env)
    it_data_lis = item_tenant(item_sortbyval)
    to_database(it_data_lis, '')


if __name__ == '__main__':
    fun()
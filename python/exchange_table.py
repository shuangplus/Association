# coding:utf-8

import MySQLdb,time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# 环境
env = sys.argv[1]

tablename = 'item_group_'+env

now = time.time()
base = time.mktime(time.strptime("2017-01-10 00:00:00",'%Y-%m-%d %H:%M:%S'))
flag = int((now-base)/(3*24*60*60))
curbase = flag*(3*24*60*60)+base
curday = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(curbase))   # 当前更新日期
current = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now)).split()[0]


def to_database(db, table):

    try:
        conn = MySQLdb.connect(host='',
                               user='', passwd='', port=3306,
                               charset="utf8")
        cur = conn.cursor()

        print "**************************\nConnection is OK!\n****************************"

        conn.select_db(db)
        print "**************************\nDB is OK!\n****************************"

        # 查询数据是否有更新
        cur.execute("select count(0) from " + table + " where time >= '%s' and timefield = '7';" % curday)
        n1 = cur.fetchall()[0][0]
        cur.execute("select count(0) from " + table + " where time >= '%s' and timefield = '30';" % curday)
        n2 = cur.fetchall()[0][0]
        cur.execute("select count(0) from " + table + " where time >= '%s' and timefield = '90';" % curday)
        n3 = cur.fetchall()[0][0]
        cur.execute("select count(0) from " + table + " where time >= '%s' and timefield = '180';" % curday)
        n4 = cur.fetchall()[0][0]
        if n1 != 0 and n2 != 0 and n3 != 0 and n4 != 0:
            cur.execute("update table_select set sign = '"+table+"' where tablename = '"+tablename+"';")
            print "**************************\nInserting data is OK!\n****************************"

        print "**************************\nUpdate is OK!\n****************************"

        conn.commit()
        cur.close()
        conn.close()

    except MySQLdb.Error, e:
        print e


def fun():
    if flag % 2 == 0:
        table = ''+env
        print '*********************\n'+table+'\n*********************'
    else:
        table = ''+env+'_1'
        print '*********************\n' + table + '\n*********************'

    to_database('', table)


if __name__ == '__main__':
    fun()

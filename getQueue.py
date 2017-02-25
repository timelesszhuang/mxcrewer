# coding=utf-8
# 从queue里边读取数据
import threading
import time

from pymysql.err import OperationalError

from MxInfo import MxManage
from getWwwwInfo import wwwInfo
from mongodbManage import MONGODB


class getQueue(threading.Thread):
    # 继承父类threading.Thread

    def __init__(self, threadID, name, q, queueLock, coll, mxSuffix, contacttool_info, getMxFlag=False,
                 getWwwFlag=False, getContactFlag=False):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.queueLock = queueLock
        self.coll = coll
        self.getWwwFlag = getWwwFlag
        self.getMxFlag = getMxFlag
        self.mxSuffix = mxSuffix
        self.getContactFlag = getContactFlag
        # 需不需要获取
        self.contacttool_info = contacttool_info

    '''
    处理 数据遍历
    '''

    def run(self):
        while True:
            self.queueLock.acquire()
            if self.q.qsize() > 0:
                collection = self.coll[0]
                data = self.q.get()
                self.queueLock.release()
                if self.getWwwFlag:
                    self.manageWwwInfo(data, collection)
                if self.getMxFlag:
                    self.manageMxInfo(data, collection)
            else:
                self.queueLock.release()
                # print("消费者未取到数据" + str(self.q.qsize()))
                # time.sleep(1)

    '''
    处理wwwtitle 想关的信息
    '''

    def manageWwwInfo(self, data, collection):
        # 获取网站的标题
        domain_name = data['domain_name']
        wwwtitle = ''
        if data.has_key('wwwtitle'):
            wwwtitle = data['wwwtitle']
        htmlInfo = wwwInfo.start_parse(domain_name, self.contacttool_info, self.getContactFlag)
        title = ''
        # description = None
        # keywords = None
        if 'title' in htmlInfo:
            title = htmlInfo['title']
        if title and title != 'Redirect' and title != wwwtitle:
            # print '该域名已经更新网站标题'
            mongodb = MONGODB()
            mongodb.connect()
            mongodb.getdb('mxmanage')
            mongodb.getcollection(collection)
            mongodb.updateOne(
                {"_id": data['_id']},
                {
                    '$set': {
                        'wwwtitle': title
                    }
                }
            )
            print self.name + data['domain_name'] + 'change title '
            mongodb.close()
        if not self.getContactFlag:
            return
        brandinfo = {}
        if 'contacttool' in htmlInfo:
            brandinfo = htmlInfo['contacttool']
        if brandinfo:
            self.manageContacttoolInfo(data, collection, brandinfo)

    '''
    管理相关咨询工具 比如七鱼等信息的变化
    '''

    def manageContacttoolInfo(self, data, collection, brandinfo):
        mongodbWhere = {"_id": data['_id']}
        domain_name = data['domain_name']
        # mongodb 操作对象初始化
        mongodb = MONGODB()
        mongodb.connect()
        mongodb.getdb('mxmanage')
        mongodb.getcollection(collection)
        # 比对文章分类
        if data.has_key('contacttool'):
            contacttool_info = data['contacttool']
            pre_brand = contacttool_info['brand_id']
            if pre_brand != brandinfo['brand_id']:
                perdata = {
                    '$set': {
                        'contacttool': brandinfo,
                        'contacttool_changetime': int(time.time())
                    },
                    '$push': {
                        # mx 历史记录 不包含当前所属品牌
                        'contacttoollist': contacttool_info
                    }
                }
                mongodb.updateOne(mongodbWhere, perdata)
                print self.name + data['domain_name'] + ' change contact tool '
        else:
            # 直接追加
            perdata = {
                '$set': {
                    'contacttool': brandinfo,
                    'contacttool_changetime': int(time.time())
                }
            }
            print self.name + data['domain_name'] + ' add new contact tool'
            mongodb.updateOne(mongodbWhere, perdata)
        mongodb.close()

    '''
    更新MX 相关的数据
    '''

    def manageMxInfo(self, data, collection):
        domain_name = data['domain_name']
        mx_info = MxManage.startParseMx(domain_name)
        if mx_info.has_key('mxsuffix'):
            mongodbWhere = {"_id": data['_id']}
            # mongodb 操作对象初始化
            mongodb = MONGODB()
            mongodb.connect()
            mongodb.getdb('mxmanage')
            mongodb.getcollection(collection)
            if self.mxSuffix.has_key(mx_info['mxsuffix']):
                mx_brand_info = self.mxSuffix[mx_info['mxsuffix']]
                # 更新mx
                # 首先需要添加
                if data.has_key('mx'):
                    # 表示存在包含数据 匹配下是不是一致  不一致需要更新数据
                    pre_mx = data['mx']  # 之前的所有mx 信息 包含品牌 品牌id mx 以及优先级
                    if pre_mx['brand_id'] != mx_brand_info['brand_id']:
                        # 表示品牌不一样
                        if pre_mx['mx'] != mx_info['mx']:
                            # 表示已经变更 mx 信息包括品牌 还有mx都变更了
                            perdata = {
                                '$set': {
                                    'mx': {
                                        'mx': mx_info['mx'],
                                        'priority': mx_info['priority'],
                                        'brand_id': mx_brand_info['brand_id'],
                                        'brand_name': mx_brand_info['brand_name']
                                    },
                                    'mx_changetime': int(time.time())
                                },
                                '$push': {
                                    # mx 历史记录 不包含当前所属品牌
                                    'mxlist': data['mx']
                                }
                            }
                        else:
                            # 有可能更新品牌信息 但是mx 没有变更
                            perdata = {
                                '$set': {
                                    'mx': {
                                        'mx': mx_info['mx'],
                                        'priority': mx_info['priority'],
                                        'brand_id': mx_brand_info['brand_id'],
                                        'brand_name': mx_brand_info['brand_name']
                                    },
                                    'mx_changetime': int(time.time())
                                }
                            }
                        print self.name + data['domain_name'] + 'change MX'
                        mongodb.updateOne(mongodbWhere, perdata)
                else:
                    mongodb.updateOne(
                        mongodbWhere,
                        {
                            '$set':
                                {
                                    'mx': {
                                        'mx': mx_info['mx'],
                                        'priority': mx_info['priority'],
                                        'brand_id': mx_brand_info['brand_id'],
                                        'brand_name': mx_brand_info['brand_name']
                                    },
                                    'mx_changetime': int(time.time())
                                }
                        }
                    )
                    print self.name + data['domain_name'] + ' add MX'
            else:
                not_classified_suffix = mx_info['mxsuffix']
                # 需要分类
                # print "获取到没有分类：" + not_classified_suffix
                # 存储到数据库中
                # 查询 是不是已经有了
                try:
                    # 该操作是把 没有分类的 存储到数据库中
                    # db = DB()
                    # db.connect()
                    # # sql = "select s.mxsuffix,s.brand_id,b.name from sm_mx_suffix as s left join sm_mx_brand as b on b.id=s.brand_id"
                    # where = " where host = '" + not_classified_suffix + "'"
                    # sql = "select * from sm_mx_suffix_notclassified " + where
                    # # print "查询未分类的mx"+sql
                    # stepCursor = db.query(sql)
                    # mx_notclassified = stepCursor.fetchone()
                    # # print mx_notclassified
                    # # {u'count': 0, u'addtime': 1487381057, u'host': u'dragonparking.com', u'id': 7}
                    # if mx_notclassified:
                    #     updateSql = "update sm_mx_suffix_notclassified set count=" + str(
                    #         mx_notclassified['count'] + 1) + where
                    #     db.update(updateSql)
                    # else:
                    #     insertSql = "insert into sm_mx_suffix_notclassified(`host`, `count`, `addtime`) VALUE ('" + not_classified_suffix + "','1','" + str(
                    #         int(time.time())) + "') "
                    #     db.update(insertSql)
                    # stepCursor.close()
                    # db.close()
                    # 表示没有获取到分类 需要更新数据
                    # 重新来更新数据
                    mongodb.updateOne(
                        mongodbWhere,
                        {
                            '$set':
                                {
                                    'mx': {
                                        'mx': mx_info['mx'],
                                        'priority': mx_info['priority'],
                                        'brand_id': 0,
                                        'brand_name': '',
                                    },
                                    'mx_changetime': int(time.time())
                                }
                        }
                    )
                    print self.name + data['domain_name'] + 'add not classified MX'
                    mongodb.close()
                except OperationalError as ex:
                    pass

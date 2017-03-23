# coding=utf-8
# 从queue里边读取数据
import threading
import time

from pymysql.err import OperationalError

from MxInfo import MxManage
from addCrmData import addCrmData
from getWwwwInfo import wwwInfo
from mongodbManage import MONGODB
from mysqlManage import DB


class getQueue(threading.Thread):
    # 继承父类threading.Thread

    def __init__(self, threadID, name, q, queueLock, coll, mxSuffix, contacttool_info, mx_blacklist_suffix,
                 mailSelfBuildInfo, getMxFlag=True, getWwwFlag=True, getContactFlag=True, addMailCusFlag=True,
                 getSelfBuildFlag=True):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.queueLock = queueLock
        self.coll = coll
        self.getWwwFlag = getWwwFlag
        self.getMxFlag = getMxFlag
        self.mxSuffix = mxSuffix
        self.mailSelfBuildInfo = mailSelfBuildInfo
        self.getContactFlag = getContactFlag
        # 需不需要获取
        self.contacttool_info = contacttool_info
        self.addMailCusFlag = addMailCusFlag
        self.mx_blacklist_suffix = mx_blacklist_suffix
        self.getSelfBuildFlag = getSelfBuildFlag

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
                # 获取下是不是自建的
                if self.getSelfBuildFlag:
                    self.manageMailInfo(data, collection)
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
            print self.name + data['domain_name'] + ' change title '
            data['wwwtitle'] = title
            mongodb.close()
        if not self.getContactFlag:
            return
        brandinfo = {}
        if 'contacttool' in htmlInfo:
            brandinfo = htmlInfo['contacttool']
        if brandinfo:
            self.manageContacttoolInfo(data, collection, brandinfo)

    '''
    自建的项目相关
    '''

    def manageMailInfo(self, data, collection):
        mongodbWhere = {"_id": data['_id']}
        # 获取网站的标题
        domainName = data['domain_name']
        # 域名信息
        brandInfo = wwwInfo.startParseMailIndex(domainName, self.mailSelfBuildInfo)
        # 判断下是不是包含 title
        mongodb = MONGODB()
        mongodb.connect()
        mongodb.getdb('mxmanage')
        mongodb.getcollection(collection)
        if brandInfo.has_key('title') and brandInfo['title'] != '' and brandInfo['title'] != 'LNMP一键安装包 by Licess' and \
                        brandInfo['title'] != None and '卖' not in brandInfo['title'] and \
                        '售' not in brandInfo['title'] and '错' not in brandInfo['title'] and \
                        '404' not in brandInfo['title'] and brandInfo['title'] != 'IIS7' and \
                        '无法' not in brandInfo['title'] and '赌' not in brandInfo['title'] and \
                        'sale' not in brandInfo['title'] and '娱乐' not in brandInfo['title']:
            print domainName + ' get mailtitle ' + brandInfo['title']
            # 更新mailtitle
            mongodb.updateOne(
                mongodbWhere,
                {
                    '$set': {
                        'mailtitle': brandInfo['title']
                    }
                }
            )

        if brandInfo.has_key('brandInfo') and len(brandInfo['brandInfo']) != 0:
            print domainName + ' get self build mail info'
            mongodb.updateOne(
                mongodbWhere,
                {
                    '$set': {
                        'mailselfbuild': brandInfo['brandInfo']
                    }
                }
            )

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
                    print self.name + domain_name + ' change contact tool '
            else:
                # 直接追加
                perdata = {
                    '$set': {
                        'contacttool': brandinfo,
                        'contacttool_changetime': int(time.time())
                    }
                }
                print self.name + domain_name + ' add new contact tool'
                mongodb.updateOne(mongodbWhere, perdata)
            mongodb.close()

        '''
        更新MX 相关的数据
        '''

        def manageMxInfo(self, data, collection):
            domain_name = data['domain_name']
            mx_info = MxManage.startParseMx(domain_name)
            if not mx_info.has_key('mxsuffix'):
                return
            if not mx_info.has_key('mxrecord'):
                return
            mongodbWhere = {"_id": data['_id']}
            # mongodb 操作对象初始化
            mongodb = MONGODB()
            mongodb.connect()
            mongodb.getdb('mxmanage')
            mongodb.getcollection(collection)
            mx_brand_info = {}
            brand_id = 0
            brand_name = ''
            # 是不是已经存在之前的mx记录  用于以后判断 mx 变更
            if not data.has_key('mxrecord'):
                mxrecord = mx_info['mxrecord']
                perdata = {
                    '$set': {
                        'mxrecord': mxrecord
                    }
                }
                mongodb.updateOne(mongodbWhere, perdata)
                print self.name + data['domain_name'] + ' append mxrecord'
            else:
                mxrecord = mx_info['mxrecord']
                # 匹配下 原始的 mx 记录
                pre_mxrecord = data['mxrecord']
                if set(pre_mxrecord).issubset(set(mxrecord)):
                    return
            # 后缀在 黑名单中 直接返回  有些后缀天天变化
            if mx_info['mxsuffix'] in self.mx_blacklist_suffix:
                print self.name + data['domain_name'] + ' mx suffix in blacklist'
                return

            if self.mxSuffix.has_key(mx_info['mxsuffix']):
                mx_brand_info = self.mxSuffix[mx_info['mxsuffix']]
                brand_id = mx_brand_info['brand_id']
                brand_name = mx_brand_info['brand_name']
            else:
                # mx 后缀没有 需要添加到数据库中
                try:
                    # 表示没有该brand信息
                    not_classified_suffix = mx_info['mxsuffix']
                    db = DB()
                    db.connect()
                    where = " where host = '" + not_classified_suffix + "'"
                    sql = "select * from sm_mx_suffix_notclassified " + where
                    # print "查询未分类的mx"+sql
                    stepCursor = db.query(sql)
                    mx_notclassified = stepCursor.fetchone()
                    # print mx_notclassified
                    # {u'count': 0, u'addtime': 1487381057, u'host': u'dragonparking.com', u'id': 7}
                    if mx_notclassified:
                        updateSql = "update sm_mx_suffix_notclassified set count=" + str(
                            mx_notclassified['count'] + 1) + where
                        db.update(updateSql)
                    else:
                        insertSql = "insert into sm_mx_suffix_notclassified(`host`, `count`, `addtime`) VALUE ('" + not_classified_suffix + "','1','" + str(
                            int(time.time())) + "') "
                        db.update(insertSql)
                    stepCursor.close()
                    db.close()
                except OperationalError as ex:
                    pass

            # 首先需要添加
            if data.has_key('mx'):
                # 表示存在包含数据 匹配下是不是一致  不一致需要更新数据
                pre_mx = data['mx']  # 之前的所有mx 信息 包含品牌 品牌id mx 以及优先级
                now_mx = mx_info['mx']
                # 表示品牌不一样
                if brand_id != 0 and pre_mx['brand_id'] != 0 and pre_mx['brand_id'] == brand_id:
                    # 之前跟现在的品牌 都存在 但是相等的情况下 直接返回
                    return
                if MxManage.subMxSuffix(pre_mx['mx']) != MxManage.subMxSuffix(now_mx):
                    perdata = {
                        '$set': {
                            'mx': {
                                'mx': mx_info['mx'],
                                'priority': mx_info['priority'],
                                'brand_id': brand_id,
                                'brand_name': brand_name,
                                'addtime': int(time.time())
                            },
                            'mx_changetime': int(time.time())
                        },
                        '$push': {
                            # mx 历史记录 不包含当前所属品牌
                            'mxlist': data['mx']
                        }
                    }
                    mongodb.updateOne(mongodbWhere, perdata)
                    print self.name + data['domain_name'] + ' change MX'
                    if self.addMailCusFlag:
                        addCrmData.addMailCustomer(data, mx_info, mx_brand_info, collection, 'update')
                else:
                    # 这种情况是 可能有些 之前mx后缀没有匹配的 后来又匹配到了
                    if brand_id and data['mx']['brand_id'] == 0:
                        perdata = {
                            '$set': {
                                'mx': {
                                    'mx': mx_info['mx'],
                                    'priority': mx_info['priority'],
                                    'brand_id': brand_id,
                                    'brand_name': brand_name,
                                    'addtime': int(time.time())
                                },
                                'mx_changetime': int(time.time())
                            }
                        }
                        print self.name + data['domain_name'] + ' update brand info'
                        mongodb.updateOne(mongodbWhere, perdata)
            else:
                # 有可能更新品牌信息 但是mx 没有变更
                perdata = {
                    '$set': {
                        'mx': {
                            'mx': mx_info['mx'],
                            'priority': mx_info['priority'],
                            'brand_id': brand_id,
                            'brand_name': brand_name,
                            'addtime': int(time.time()),
                        },
                        'mx_changetime': int(time.time())
                    }
                }
                print self.name + data['domain_name'] + ' add MX'
                mongodb.updateOne(mongodbWhere, perdata)
                if self.addMailCusFlag:
                    addCrmData.addMailCustomer(data, mx_info, mx_brand_info, collection, 'add')

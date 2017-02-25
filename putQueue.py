# coding=utf-8
# 向 queue 中写数据

import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import threading
import time
from mongodbManage import MONGODB


class putQueue(threading.Thread):
    # 继承父类threading.Thread

    def __init__(self, threadID, name, q, qCount, queueLock, coll, flag):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.queueLock = queueLock
        self.coll = coll
        self.qCount = qCount
        self.flag = flag

    def run(self):  # 把要执行的代码写到run函数里面 线程在创建后会直接运行 run 函数
        mongodb = MONGODB()
        mongodb.connect()
        mongodb.getdb('mxmanage')
        num_coll = 'mxmanage_stopnum'
        while True:
            self.queueLock.acquire()
            if self.q.qsize() == 0:
                # 当前客户
                print("producer are producing data")
                current_coll = self.coll[0]
                # 从数据库中取数据
                mongodb.getcollection(num_coll)
                flagWhere = {"flag": self.flag}
                stop_info = mongodb.findOne(flagWhere)
                if not stop_info:
                    continue
                start = stop_info['start']
                stop = stop_info['stop']
                collection = stop_info['collection']
                if start >= stop and start != 0:
                    # 取数据失败 需要换个地区重新获取
                    del self.coll[0]
                    # 如果没有
                    if self.coll:
                        mongodb.getcollection(num_coll)
                        mongodb.updateOne(flagWhere, {"$set": {"stop": 0, "start": 0, "collection": self.coll[0]}})
                    else:
                        # 表示便利完成数据
                        exit()
                    self.queueLock.release()
                    continue
                if collection != current_coll:
                    # 更新数据库中的 公司信息 从上次的断点位置继续遍历数据
                    length = len(self.coll)
                    for item in range(length):
                        if self.coll[0] == collection:
                            break
                        else:
                            del self.coll[0]
                if stop == 0:
                    # 重新 获取下总的数量
                    mongodb.getcollection(collection)
                    # 重新来更新数据
                    stopnum = mongodb.count()
                    # print(stopnum)
                    mongodb.getcollection(num_coll)
                    mongodb.updateOne(flagWhere, {"$set": {"stop": int(stopnum)}})
                # 从数据库中获取 上次已经获取到哪了 这次从哪开始   有个问题是如果 数据有变动 会取到重复的值
                mongodb.getcollection(collection)
                print "producing data" + collection
                mongodb.findMany(self.q, start, self.qCount)
                print "produced data" + collection
                # 更新mongodb 中的 start 数据 取数据成功
                mongodb.getcollection(num_coll)
                mongodb.updateOne(flagWhere, {"$set": {"start": int(start + self.qCount)}})
                mongodb.close()
                self.queueLock.release()
            else:
                self.queueLock.release()
                # print("生产者不需要生产" + str(self.q.qsize()))
                time.sleep(1)

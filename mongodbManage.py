# coding=utf-8
# mongodbManage  mongodb 数据库相关操作
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.mongo_client import MongoClient


class MONGODB:
    # 数据库链接对象
    client = None
    # 某一个数据库操作对象
    db = None
    # 数据库中的集合操作对象
    collection = None

    '''
    连接mongodb数据库
    '''

    def connect(self):
        user = "admin"
        pwd = "qiangbi123"
        server = "115.28.161.44"
        port = '27017'
        # 表示 用于授权的
        db_name = "admin"
        uri = 'mongodb://' + user + ':' + pwd + '@' + server + ':' + port + '/' + db_name
        try:
            self.client = MongoClient(uri)
        except ServerSelectionTimeoutError as ex:
            pass

    '''
    获取数据库操作对象 dbname  mxmanage
    '''

    def getdb(self, dbname):
        self.db = self.client.get_database(dbname)

    '''
    获取指定集合的操作对象
    '''

    def getcollection(self, coll_name):
        self.collection = getattr(self.db, coll_name)

    '''
    多条数据插入集合操作
    '''

    def insert(self, data):
        self.collection.insert(data)

    '''
    单条数据插入集合
    '''

    def insertOne(self, data):
        self.collection.insert_one(data)

    '''
    集合中数据删除操作
    '''

    def delete(self):
        pass

    '''
    集合中数据更新操作
    condition 要更新的查询条件
    data 要更新成为什么字段
    '''

    def updateOne(self, condition, data):
        # 修改聚集内的记录
        # self.collection.update({"UserName": "urling"}, {"$set": {'AccountID': random.randint(20, 50)}})
        # db.test.update_one({'x': 1}, {'$inc': {'x': 3}})
        # print(data)
        try:
            self.collection.update_one(condition, data)
        except Exception as e:
            pass

    '''
    查询一个
    '''

    def findOne(self, condition):
        try:
            return self.collection.find_one(condition)
        except ServerSelectionTimeoutError as ex:
            return {}
        except Exception as e:
            return {}

    '''
    批量查找
    '''

    def findMany(self, queue, start, count):
        # exclude 某个字段
        # db.Info.find_one({'$and': [{'$text': {'$search': "Hello"}, 'Username': 'John Doe'}]},
        #                  {"Expenses.description": 1, "fullName_normal": 1})
        cursor = self.collection.find().skip(start).limit(count)
        print(start)
        print(count)
        print(cursor)
        try:
            for data in cursor:
                queue.put(data)
            if queue.qsize() > 0:
                return True
            else:
                return False
        except Exception as ex:
            print("failed get data" + ex.message)

    '''
    统计字段信息
    '''

    def count(self):
        return self.collection.count()

    def close(self):
        self.client.close()

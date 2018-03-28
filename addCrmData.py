# coding=utf-8
import time

from mysqlManage import DB


class addCrmData():
    '''
    添加邮箱客户
    data 原来的 域名以及其他信息
    mx_info 变更之后的mx记录
    brand_info 品牌信息 可能为空
    coll 表示是哪个 mongodb 集合中的数据
    flag 表示是 添加还是 更新
    '''

    @classmethod
    def addMailCustomer(self, data, mx_info, brand_info, coll, flag):
        # 添加到 mysql 中
        db = DB()
        db.connect()
        brand_id = 0
        brand_name = ''
        title = ''
        if data['wwwtitle']:
            title = data['wwwtitle']
        if brand_info.has_key('brand_id'):
            brand_id = brand_info['brand_id']
            brand_name = brand_info['brand_name']
        nowtime = str(int(time.time()))
        insertSql = "INSERT INTO sm_bigdata_mail_customer" \
                    + "(`object_id`, `domain`,`title`,`flag`,`mx`,`brand_id`,`brand_name`,`coll`,`addtime`) VALUE ('" \
                    + str(data['_id']) + "','" + data['domain_name'] + "','" + title + "','" + flag + "','" + \
                    mx_info['mx'] + "','" + str(brand_id) + "','" + brand_name + "','" + coll + "','" + nowtime + "')"
        db.update(insertSql)
        db.close()

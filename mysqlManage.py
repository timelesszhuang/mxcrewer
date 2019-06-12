# coding=utf-8
import pymysql as mysql


class DB:
    conn = None
    flag = None
    ip = 'rdsfjnifbfjnifbo.mysql.rds.aliyuncs.com'

    def connect(self):
        self.conn = mysql.connect(host=self.ip, user='salesmen', passwd='', db='', port=3306,
                                  cursorclass=mysql.cursors.DictCursor, charset="utf8")

    def query(self, sql):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
        except mysql.Error:
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(sql)
        return cursor

    def update(self, sql):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            self.conn.commit()
        except mysql.Error:
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(sql)
            self.conn.commit()

    def close(self):
        self.conn.close()

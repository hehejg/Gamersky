# -*- coding: utf-8 -*-

import pymongo
from loguru import logger
# 数据库基本信息
db_configs = {
    'type': 'mongo',
    'host': '127.0.0.1',
    'port': '27017',
    "user": "",
    "password": "",
    'db_name': "gamersky_datas"
}

class Mongo():
    def __init__(self):
        self.db_name = db_configs.get("db_name")
        self.host = db_configs.get("host")
        self.port = db_configs.get("port")
        self.client = pymongo.MongoClient(f'mongodb://{self.host}:{self.port}')
        self.username = db_configs.get("user")
        self.password = db_configs.get("passwd")
        if self.username and self.password:
            self.db = self.client[self.db_name].authenticate(self.username, self.password)
        self.db = self.client[self.db_name]

    def find_data(self,col,where={}):
        # 获取状态为0的数据
        data = self.db[col].find(where)
        gen = (item for item in data)
        return gen

    def change_status(self, uuid, item, status_code=0):
        # status_code 0:初始,1:开始下载，2下载完了
        item["status"] = status_code
        self.db[self.col].update_one({'uuid': uuid}, {'$set': item})

    def save_data(self, items,col):
        if isinstance(items, list):
            for item in items:
                try:
                    self.db[col].update_one({
                        'id': item.get("id")},
                        {'$set': item},
                        upsert=True)
                except Exception as e:
                    logger.error(f"数据插入出错:{e.args},此时的item是:{item}")
        else:
            try:
                self.db[col].update_one({
                    'id': items.get("id")},
                    {'$set': items},
                    upsert=True)
            except Exception as e:
                logger.error(f"数据插入出错:{e.args},此时的item是:{item}")


if __name__ == '__main__':
    m = Mongo()
    m.find_data()

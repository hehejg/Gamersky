# -*- coding: utf-8 -*-
import asyncio
from loguru import logger
import datetime

from motor.motor_asyncio import AsyncIOMotorClient
from itertools import islice

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

db_configs = {
    'host': '127.0.0.1',
    'port': '27017',
    'db_name': 'gamersky_datas',
    'user': ''
}


class MotorOperation():
    def __init__(self):
        self.__dict__.update(**db_configs)
        if self.user:
            self.motor_uri = f"mongodb://{self.user}:{self.passwd}@{self.host}:{self.port}/{self.db_name}?authSource={self.db_name}"
        else:
            self.motor_uri = f"mongodb://{self.host}:{self.port}/{self.db_name}"
        self.client = AsyncIOMotorClient(self.motor_uri)
        self.mb = self.client[self.db_name]

    # async def get_use_list(self):
    #     fs = await aiofiles.open("namelist.txt", "r", encoding="utf-8")
    #     data = (i.replace("\n", "") async for i in fs)
    #     return data

    async def save_data_with_status(self, items, col="discogs_seed_data"):
        for i in range(0, len(items), 2000):
            tasks = []
            for item in islice(items, i, i + 2000):
                data = {}
                data["update_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                data["status"] = 0  # 0初始
                data["url"] = item
                tasks.append(data)
            logger.info("存新的url", tasks)
            await self.mb[col].insert_many(tasks)

    def add_index(self, key, col="discogs_seed_data"):
        # 添加索引
        self.mb[col].create_index(key)

    def save_data(self, items, col):
        if isinstance(items, list):
            for item in items:
                try:
                    self.mb[col].update_one({
                        'id': item.get("id")},
                        {'$set': item},
                        upsert=True)
                except Exception as e:
                    logger.error(f"数据插入出错:{e.args},此时的item是:{item}")
        else:
            try:
                self.mb[col].update_one({
                    'id': items.get("id")},
                    {'$set': items},
                    upsert=True)
            except Exception as e:
                logger.error(f"数据插入出错:{e.args},此时的item是:{item}")

    async def aio_save_data(self, items, col="discogs_index_data", key="obj_id"):
        # storage.info(f"此时的items:{items}")
        if isinstance(items, list):
            for item in items:
                try:
                    item[key] = item[key]
                    await self.mb[col].update_one({
                        key: item.get(key)},
                        {'$set': item},
                        upsert=True)
                except Exception as e:
                    logger.error(f"数据插入出错:{e.args}此时的item是:{item}")
        elif isinstance(items, dict):
            try:
                items[key] = items[key]
                await self.mb[col].update_one({
                    key: items.get(key)},
                    {'$set': items},
                    upsert=True)
            except Exception as e:
                logger.error(f"数据插入出错:{e.args}此时的item是:{items}")

    async def change_status(self, condition, col="discogs_seed_data", status_code=1):
        # status_code 0:初始,1:开始下载，2下载完了
        try:
            item = {}
            item["status"] = status_code
            # storage.info(f"修改状态,此时的数据是:{item}")
            await self.mb[col].update_one(condition, {'$set': item})
        except Exception as e:
            logger.error(f"修改状态出错:{e.args}此时的数据是:{item}")

    async def get_detail_datas(self):
        data = self.mb.discogs_index.find({'status': 0})
        async for item in data:
            print(item)
        return data

    async def reset_status(self, col="discogs_seed_data"):
        await self.mb[col].update_many({'status': 1}, {'$set': {"status": 0}})

    async def reset_all_status(self, col="discogs_seed_data"):
        await self.mb[col].update_many({}, {'$set': {"status": 0}})

    def find_data(self,col="discogs_seed_data",where={}):
        # 获取状态为0的数据
        data = self.mb[col].find(where)
        gen = (item for item in data)
        return gen

    async def aio_find_data(self, col="discogs_seed_data",where={}):
        '''
        获取状态为0的数据，作为爬取对象。
        :return:AsyncGeneratorType
        '''
        cursor = self.mb[col].find(where)
        async_gen = (item async for item in cursor)
        return async_gen

    async def do_delete_many(self):
        await self.mb.tiaopiao_data.delete_many({"flag": 0})


if __name__ == '__main__':
    m = MotorOperation()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(m.reset_all_status(col="discogs_index_data"))

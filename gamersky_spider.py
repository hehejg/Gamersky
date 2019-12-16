import asyncio
import aiohttp
from aiostream import stream
from async_retrying import retry
from loguru import logger
import requests
import re
import hashlib
from lxml import etree
from DB import mongohelper, mongo
from types import AsyncGeneratorType
import os

sem = asyncio.Semaphore(5)
USER_PROXY = False


class Base_Spider():
    def get_session(self, url, headers={}):
        session = requests.Session()
        session.headers.update(headers)
        response = session.get(url)
        response.encoding = "utf-8"
        return response.text

    async def init_session(self):
        '''
        创建Tcpconnector，包括ssl和连接数的限制
        创建一个全局session。
        :return:
        '''
        self.tc = aiohttp.connector.TCPConnector(limit=100, force_close=True,
                                                 enable_cleanup_closed=True,
                                                 ssl=False)
        self.session = aiohttp.ClientSession(connector=self.tc)

    def validateTitle(self, title):
        rstr = r"[\/\\\:\*\?\"\<\>\|]"  # '/ \ : * ? " < > |'
        new_title = re.sub(rstr, "_", title)  # 替换为下划线
        return new_title

    async def close(self):
        await self.tc.close()
        await self.session.close()

    async def branch(self, coros, limit=10):
        '''
        使用aiostream模块对异步生成器做一个切片操作。这里并发量为10.
        :param coros: 异步生成器
        :param limit: 并发次数
        :return:
        '''
        index = 0
        while True:
            xs = stream.preserve(coros)
            ys = xs[index:index + limit]
            t = await stream.list(ys)
            if not t:
                break
            await asyncio.ensure_future(asyncio.wait(t))
            index += limit + 1

    @retry(attempts=3)
    async def aio_get_session(self, url, source_type, status_code=200):
        if USER_PROXY:
            pass
        async with self.session.get(url) as response:
            status = response.status
            if status in [status_code, 201]:
                if source_type == "text":
                    source = await response.text()
                elif source_type == "buff":
                    source = await response.read()
        logger.info(f"get url:{url}")
        return source


class Seed_Spider(Base_Spider):
    def __init__(self):
        self.start_url = "https://www.gamersky.com/"
        self.picture_urls = []
    def get_details_url(self):
        urls = []
        datas = []
        for data in list(mongo.Mongo().find_data("details_url")):
            datas.append(data['id'])
        result = self.get_session(self.start_url)
        re_resulr = re.search('<ul class="Mid7img block">(.*?)<ul class="Mid7img none">', result, re.S).group(1)
        details_url = re.findall('href="(.*?)" title="(.*?)"', re_resulr, re.S)
        for details in details_url:
            dic = {}
            url = details[0]
            if not "https://www.gamersky.com/" in url: url = f'https://www.gamersky.com{url}'
            dic['url'] = url
            dic['title'] = details[1]
            dic['status'] = 0
            md5hash = hashlib.md5(url.encode("utf-8"))
            id = md5hash.hexdigest()
            dic['id'] = id
            if id in datas or not "动态图" in details[1]:
                logger.info('重复数据或者不是动态图 不插入')
            else:
                urls.append(dic)
        mongo.Mongo().save_data(urls, 'details_url')
        logger.info(f'保存成功 插入{len(urls)}条数据')
        urls = mongo.Mongo().find_data('details_url', where={'status': 0})
        [self.picture_urls.append(data['id']) for data in list(mongo.Mongo().find_data("picture_url"))]
        [self.get_pirture_url(url) for url in urls]

    def get_pirture_url(self, item):
        lists = []
        next_page = {}
        url = item['url']
        url='https://www.gamersky.com/ent/201912/1248353_8.shtml'
        print(url)
        response = self.get_session(url)
        print(response)
        html = etree.HTML(response)
        html_data = html.xpath('//div[@class="Mid2L_con"]//p[@align="center"]')
        for i in html_data:
            dic = {}
            picture_url = i.xpath('img[@class="picact"]/@src')
            print(picture_url)
            try:
                remark = i.xpath('text()')[0]
            except IndexError:
                remark = ''
            dic['picture_url'] = picture_url
            dic['remark'] = remark
            dic['status'] = 0
            md5hash = hashlib.md5(picture_url.encode("utf-8"))
            id = md5hash.hexdigest()
            dic['id'] = id

            if id in self.picture_urls:
                logger.info('重复数据 不插入')
            else:
                lists.append(dic)
        mongo.Mongo().save_data(lists, 'picture_url')
        logger.info(f'保存成功 插入{len(lists)}条数据')
        if '下一页' in response:
            next_page_url = re.search('.*href="(.*?)">下一页', response, re.S).group(1)
            next_page['url'] = next_page_url
            self.get_pirture_url(next_page)

    def start(self):
        self.get_details_url()


# class Details_Spder(Base_Spider):
#     def __init__(self):
#         self.path='E'
#     async def start(self):
#         # 获取mongo的数据,类型异步生成器。
#         data: AsyncGeneratorType = await mongohelper.MotorOperation().find_data(col="details_url")
#         await self.init_session()
#         # 分流
#         tasks = (asyncio.ensure_future(self.save_pircure(item)) async for item in data)
#         await self.branch(tasks)
#
#     async def save_pircure(self,item):
#         with open()
if __name__ == '__main__':
    s = Seed_Spider()
    s.start()

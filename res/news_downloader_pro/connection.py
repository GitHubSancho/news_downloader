import time
import pymongo
from motor.motor_asyncio import AsyncIOMotorClient


class Mongo:
    def __init__(self, conf) -> None:
        self.user = conf['user']
        self.password = conf['password']
        self.host = conf['host']
        self.port = conf['port']
        self.database = conf['database']
        self.collection = conf['collection']
        self._client()

    def __del__(self):
        self._close()

    async def __len__(self, _filter=None):
        if not _filter:
            _filter = {}
        return await self.coll.count_documents(_filter)

    @staticmethod
    def _build_uri(host, port, user=None, password=None):
        if user and password:
            return f'mongodb://{user}:{password}@{host}:{port}'
        return f'mongodb://{host}:{port}'

    @staticmethod
    def _build_defult_document(mode, url, host):
        return [{
            'url': url
        }, {
            '$set': {
                'url': url,
                'host': host,
                'mode': mode,
                'status': 'waiting',
                'pendedtime': 1,
                'failure': 0,
                'html': ''
            }
        }]

    @staticmethod
    def _build_defult_documents(mode, documents):
        return [
            Mongo._build_defult_document(mode, url, host)
            for url, host in documents
        ]

    @staticmethod
    def _build_update_document(mode,
                               url,
                               status=None,
                               failure=None,
                               html_zip=None):
        if mode == 'hub':
            return [[{'url': url}, {'$set': {'pendedtime': time.time()}}]]
        return [[{
            'url': url
        }, {
            '$set': {
                'status': status,
                'pendedtime': time.time(),
                'failure': failure,
                'html': html_zip
            }
        }]]

    @staticmethod
    def _build_update_documents(mode,
                                urls,
                                status=None,
                                failure=None,
                                html_zip=None):
        documents = []

        if html_zip:
            iter_zip = zip(urls, html_zip)
            [
                documents.extend(
                    Mongo._build_update_document(mode, url, status, failure,
                                                 html))
                for url, html in iter_zip
            ]
            return documents

        [
            documents.extend(
                Mongo._build_update_document(mode, url, status, failure,
                                             html_zip)) for url in urls
        ]
        return documents

    @staticmethod
    def _build_failure_documents(failures):
        if not failures:
            return []
        return [[{
            'url': url
        }, {
            '$set': {
                'failure': failure
            }
        }] for url, failure in failures.items()]

    def _client(self):
        self.client = AsyncIOMotorClient(self._build_uri(
            self.host, self.port, self.user, self.password),
                                         maxPoolSize=None)
        self._connect_db()
        return True

    def _connect_db(self):
        self.db = self.client[self.database]
        self.coll = self.db[self.collection]
        self.coll.create_index([('url', pymongo.ASCENDING)], unique=True)
        # 创建索引
        return

    def _close(self):
        if self.client:
            self.client.close()

    async def get(self, myfilter, projection=None, limit=0):
        if projection:
            documents = self.coll.find(myfilter, projection, limit=limit)
            return await documents.to_list(length=None)
        documents = self.coll.find(myfilter, limit=limit)
        return await documents.to_list(length=None)

    async def get_all_list(self):
        data = list(await self.get({}, {'url': 1, "_id": 0}))
        return [url['url'] for url in data]

    async def get_waiting_urls(self, pending_threshold, failure_threshold,
                               limit):
        refresh_time = time.time() - pending_threshold
        failure_threshold = failure_threshold
        return list(await self.get(
            {
                'pendedtime': {
                    '$lt': refresh_time
                },
                'status': 'waiting',
                'failure': {
                    '$lt': failure_threshold
                }
            },
            limit=limit))

    async def update_one(self, document):
        try:
            await self.coll.update_one(document[0], document[1], upsert=True)
        except Exception as e:
            print(e)

    async def update_many(self, documents):
        [await self.update_one(document) for document in documents]

    async def len_downloaded_documents(self):
        return await self.__len__({'html': {"$ne": ""}})

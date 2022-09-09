import os
import sys
import time
import lzma
import urllib.parse as urlparse
import asyncio
from concurrent.futures import ProcessPoolExecutor
import aiohttp
import psutil
from pymongo import MongoClient
from loader import Loader
from connection import Mongo
from downloader import Downloader
from html_parser import Parser

print("hello,world")

client = MongoClient("mongodb://dbadmin:12345678@mongodb:27017")
db = client["demo001"]
col = db["test"]
col.insert_one({'url': 'http://www.baidu.com'})
print([i['url'] for i in col.find()])

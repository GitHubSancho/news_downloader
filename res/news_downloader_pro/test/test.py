import sys
import time
import lzma
import urllib.parse as urlparse
import asyncio
from concurrent.futures import ProcessPoolExecutor
import aiohttp
import psutil
from news_downloader_pro.loader import Loader
from news_downloader_pro.connection import Mongo
from news_downloader_pro.downloader import Downloader
from news_downloader_pro.html_parser import Parser

print("hello")
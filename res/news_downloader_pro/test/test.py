import sys
import time
import lzma
import urllib.parse as urlparse
import asyncio
from concurrent.futures import ProcessPoolExecutor
import aiohttp
import psutil
from loader import Loader
from connection import Mongo
from downloader import Downloader
from html_parser import Parser

print("hello")
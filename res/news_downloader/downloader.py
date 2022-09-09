import random
import cchardet
import aiohttp
import asyncio


class Downloader:
    def __init__(self, ua_list=None) -> None:
        if ua_list:
            self.ua_list = ua_list

    async def fetch(self, session, url, headers=None, proxy=None, timeout=3):
        html = ''
        status = 0
        redirected_url = url
        if isinstance(url, dict):
            url = url['url']
        _headers = headers or random.choice(self.ua_list)
        try:
            async with session.get(url,
                                   headers=_headers,
                                   proxy=proxy,
                                   verify_ssl=False,
                                   timeout=timeout) as resp:
                status = resp.status
                html = await resp.read()
                encoding = cchardet.detect(html)['encoding']
                html = html.decode(encoding, errors='ignore')
                redirected_url = resp.url
        except aiohttp.ClientConnectorError as e:
            pass
        except OSError as e:
            pass
        except Exception as e:
            msg = f'Failed download: {url} | exception: {str(type(e))}, {str(e)}'
            # print(msg)
        return status, html, url, redirected_url, proxy

    async def crawler(self, session, task, proxy=None):
        task = [
            asyncio.create_task(self.fetch(session, url['url'], proxy=proxy))
            for url in task
        ]
        done, _ = await asyncio.wait(task)

        return [l._result for l in done]

    async def test_proxy(self, session, task):
        task = [
            asyncio.create_task(
                self.fetch(session, TEST_URL, proxy=proxy, timeout=10))
            for proxy in task
        ]
        done, _ = await asyncio.wait(task)

        return [l._result for l in done]

import random
import requests


class Proxy:
    TEST_URL = 'https://www.baidu.com'
    PROXYPOOL_URL = 'http://webapi.http.zhimacangku.com/getip'
    PROXYPOOL_API_KEY = '262150'
    PROXYPOOL_API_PARAMS = {
        'num': 100,
        'type': 3,
        'pro': 0,
        'city': 0,
        'yys': 0,
        'port': 1,
        'time': 1,
        # 'pack': PROXYPOOL_API_KEY,
        'ts': 0,
        'ys': 0,
        'cs': 0,
        'lb': 2,
        'sb': 0,
        'pb': 45,
        'mr': 1,
        'regions': ''
    }

    def __init__(self) -> None:
        pass

    def get_proxy_ip(self):
        resp = requests.get(PROXYPOOL_URL, params=PROXYPOOL_API_PARAMS)
        html = resp.text
        _ip = html.split('</br>')[:-1]
        return [f'http://{ip}' for ip in _ip]

    def build_proxy_pool(self, proxy):
        if not proxy:
            return []
        self.proxy_pool = {p: 50 for p in proxy}
        print(f'加载ip：{len(self.proxy_pool)}')
        return self.proxy_pool

    def sub_proxy_count(self, ip):
        count = self.proxy_pool.get(ip, None)
        if count is None:
            return None
        elif count <= 0:
            del self.proxy_pool[ip]
            return None
        self.proxy_pool[ip] -= 1
        return ip

    def choices(self, k):
        if not self.proxy_pool:
            return None
        keys = list(self.proxy_pool.keys())
        return random.choices(keys, k=k)

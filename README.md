# news_downloader_pro

## 项目简介
news_downloader_pro是一个新闻网页下载器，代码采用高并发设计，效率很高。

## 项目结构
```
news_downloader -- 父工程
├── newsdownloader_pro.yml -- 配置文件
├── newsdownloader_pro_hubs.yml -- 新闻主页配置文件
├── newsdownloader_pro_ua.yml -- User-Agent配置文件
└── res/newsdownloader_pro -- 主要模块
    ├─ test -- 测试代码
    ├── connection.py -- 数据库模块
    ├── downloader.py -- 下载器模块
    ├── html_parser.py -- 网页解析模块
    ├── loader.py -- 文件读取模块
    ├── main.py -- 程序入口
    └─ proxy.py -- ip代理模块
```

## 如何使用？
有两种方式：
- docker方式  
> 下载docker + docker-compose  
1. 克隆代码：`git clone https://github.com/GitHubSancho/news_downloader_pro.git`
2. 生成容器并运行：`docker-compose up`
3. 打开浏览器查看数据库：`127.0.0.1:1999`
- 普通方式  
1. 克隆代码
2. 安装 Mongodb 并启动服务
3. 下载依赖文件：`pip install -r requirements.txt`
4. 执行：`python ./res/news_downloader_pro/main.py`
5. 在 Mongodb 中查看数据
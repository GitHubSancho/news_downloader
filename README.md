# news_downloader

## 项目简介
news_downloader是一个新闻网页下载器，代码采用高并发设计，效率很高。

## 项目结构
```
news_downloader -- 父工程
├── newsdownloader.yml -- 配置文件
├── newsdownloader_hubs.yml -- 新闻主页配置文件
├── newsdownloader_ua.yml -- User-Agent配置文件
└── res/newsdownloader -- 主要模块
    ├── test -- 测试代码
    ├── connection.py -- 数据库模块
    ├── downloader.py -- 下载器模块
    ├── html_parser.py -- 网页解析模块
    ├── loader.py -- 文件读取模块
    ├── main.py -- 程序入口
    ├── proxy.py -- ip代理模块
    └── test.py -- 测试用例
```

## 程序流程
1. main.py 会每两分钟调用一次 loader.py 读取配置文件
2. main.py 将调用 connection.py 把去读到的新闻主页添加到mongodb
3. main.py 拉取mongodb中待下载链接推入到 downloader.py 进行下载
4. main.py 将下载后的网页及请求状态交给 html_parser.py 解析出网页内的链接
5. main.py 把请求的网页和解析出的链接打上标签后，调用 connection.py 传入mongodb


## 如何添加新闻网页？
在 news_downloader_hubs.yml 文件中以`- https://xxx`的格式添加


## 如何使用？
有两种方式：
- docker方式  
> 下载docker + docker-compose  
1. 克隆代码：`git clone https://github.com/GitHubSancho/news_downloader.git`
2. 生成容器并运行：`docker-compose up`
3. 打开浏览器查看数据库：`127.0.0.1:1999`
- 普通方式  
1. 克隆代码
2. 安装 Mongodb 并启动服务
3. 下载依赖文件：`pip install -r requirements.txt`
4. 执行：`python ./res/news_downloader/main.py`
5. 在 Mongodb 中查看数据


## 如何停止？
程序会每两分钟检查一次配置文件 news_downloader.yml ，当`exit`项为`True`时自动退出

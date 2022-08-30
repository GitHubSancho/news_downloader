FROM continuumio/miniconda3:4.9.2
LABEL author "Sancho"

ENV PATH /opt/conda/bin:$PATH
SHELL ["/bin/bash", "-c"]
COPY . /opt/news_downloader_pro
WORKDIR /opt/news_downloader_pro/res/news_downloader_pro


RUN conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/free/ \
    && conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/main \
    && conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/r \
    && conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/pro \
    && conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/msys2 \
    && conda config --set show_channel_urls yes \
    && source /opt/conda/etc/profile.d/conda.sh \
    && conda activate base \
    && conda update -n base -c defaults conda -y \
    && conda install pyyaml -y \
    && conda install pymongo -y \
    && conda install -c conda-forge motor -y \
    && conda install -c conda-forge cchardet -y\
    && conda install aiohttp -y \
    && conda install psutil -y 
# && conda install pyyaml pymongo aiohttp psutil -y \
# && conda install -c conda-forge motor cchardet -y \

# RUN yum install curl -y \
#     && curl -o /etc/yum.repos.d/CentOS-Base.repo https://mirrors.aliyun.com/repo/Centos-7.repo \
#     && sed -i -e '/mirrors.cloud.aliyuncs.com/d' -e '/mirrors.aliyuncs.com/d' /etc/yum.repos.d/CentOS-Base.repo \
#     && yum install mongodb-org -y\
#     && systemctl start mongod.service

# CMD ["python","main.py"]
CMD ["/bin/bash"]

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

RUN systemctl stop firewalld \
    && systemctl disable firewalld \
    && setenforce 0 \
    && sed -i 's#SELINUX=enforcing#SELINUX=disabled#g' /etc/sysconfig/selinux \
    && sed -i 's#SELINUX=enforcing#SELINUX=disabled#g' /etc/selinux/config \
    && sudo yum -y install mongodb-org \
    && sudo sed -i '/bindIp/{s/127.0.0.1/0.0.0.0/}' /etc/mongod.conf \
    && sudo sed -i '/^#security/a\security:\n  authorization: enabled' /etc/mongod.conf \
    && sudo systemctl start mongod \
    && sudo systemctl enable mongod

CMD ["python","main.py"]

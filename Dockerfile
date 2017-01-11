FROM ubuntu:latest
MAINTAINER Brian Abelson
RUN apt-get update -y --fix-missing
RUN apt-get install -y python-pip python-dev build-essential git vim ntp jq libtag1-dev
COPY bnpl/*.py /app/bnpl/
COPY bnpl/ext /app/bnpl/ext/
COPY bnpl/config /app/bnpl/config/
COPY tests/ /app/tests/
COPY setup.py requirements.txt README.md Manifest.in Makefile Dockerfile .gitignore /app/
WORKDIR /app
RUN chmod -R 777 /app/bnpl/ext
RUN mkdir -p /tmp
RUN chmod -R 777 /tmp 
RUN pip install --upgrade pip
RUN pip install gevent
RUN pip install -r requirements.txt
RUN python setup.py install
ENTRYPOINT ["bash"]


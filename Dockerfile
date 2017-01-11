FROM ubuntu:latest
MAINTAINER Brian Abelson
RUN apt-get update -y
RUN apt-get install -y python-pip python-dev build-essential git vim ntp
COPY . /bnpl
WORKDIR /bnpl
RUN pip install -r requirements.txt
RUN python setup.py install
ENTRYPOINT ["bash"]


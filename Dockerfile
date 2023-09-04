FROM rackattack-nas.dc1:5000/centos:7.6.1810

RUN mkdir /app

ADD . /app

WORKDIR /app

RUN pip3 install -r requirements.txt

CMD ["python", "main.py"]
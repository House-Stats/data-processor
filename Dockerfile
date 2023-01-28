FROM python:3.10.8

WORKDIR /app

COPY ./processor .
COPY ./requirements.txt .

# RUN wget https://github.com/edenhill/librdkafka/archive/v1.9.0.tar.gz && tar xvzf v1.9.0.tar.gz && cd librdkafka-1.9.0/ && ./configure && make && make install && ldconfig

RUN python3 -m pip install --upgrade pip setuptools wheel
RUN python3 -m pip install -r requirements.txt

CMD ["python3", "__init__.py"]
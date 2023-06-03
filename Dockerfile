FROM python:3.10.8

WORKDIR /app

COPY ./requirements.txt ./

RUN python3 -m pip install --upgrade pip setuptools wheel
RUN python3 -m pip install -r requirements.txt

COPY ./worker ./worker

CMD ["celery", "--app=worker", "worker", "--loglevel=info", "--concurrency=4", "--without-heartbeat", "--without-gossip", "--without-mingle", "-Ofair", "--task-events"]

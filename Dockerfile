FROM python:3.10.1-buster

ARG BRANCH

ENV PYTHONPATH=/app

WORKDIR /app/src

ADD ./requirements.txt /app/src/requirements.txt
RUN pip install --no-cache-dir -r /app/src/requirements.txt

ADD ./src /app/src/

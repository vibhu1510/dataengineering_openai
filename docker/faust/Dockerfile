FROM python:3.11-slim

WORKDIR /app

COPY docker/faust/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY src/ /app/src/

ENV PYTHONPATH=/app

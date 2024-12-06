FROM python:3.13.1


RUN apt update \
    && apt upgrade \
    && apt install sqlite3

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt


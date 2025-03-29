FROM python:3.13.2


RUN apt update \
    && apt upgrade -y \
    && apt install -y sqlite3

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt


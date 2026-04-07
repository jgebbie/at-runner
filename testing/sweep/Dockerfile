FROM python:3.12-slim

WORKDIR /repo

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir grpcio protobuf

COPY client/python/src /repo/client/python/src
COPY scripts /repo/scripts

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/repo/client/python/src

CMD ["/repo/scripts/test-sweep.sh"]

FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir grpcio protobuf

COPY --from=build-context /client/python/src /app/src
COPY run_tests.py /app/

ENV PYTHONPATH=/app/src
CMD ["python", "run_tests.py"]

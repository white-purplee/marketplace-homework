FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi==0.115.12 pydantic==2.11.4 uvicorn==0.34.0 \
    confluent-kafka==2.8.0 grpcio-tools==1.70.0 clickhouse-connect==0.8.15 \
    "psycopg[binary]==3.2.6" boto3==1.37.16
COPY movie.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. movie.proto
COPY producer.py analytics.py test_pipeline.py ./

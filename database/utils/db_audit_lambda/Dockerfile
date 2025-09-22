# Build for x86_64 platform
# docker build --platform linux/amd64 -t lambda-layer-builder .
# docker run --rm -v "$(pwd)":/output lambda-layer-builder

FROM python:3.10-slim

RUN apt-get update && apt-get install -y zip && rm -rf /var/lib/apt/lists/*

WORKDIR /layer

# Use compatible versions that work well in Lambda
RUN mkdir -p python && \
    pip install --no-cache-dir -t python/ \
    --only-binary=:all: \
    numpy==1.24.3 \
    pandas==2.0.3 \
    psycopg2-binary==2.9.9

RUN zip -r lambda-layer.zip python/

CMD ["cp", "lambda-layer.zip", "/output/"]
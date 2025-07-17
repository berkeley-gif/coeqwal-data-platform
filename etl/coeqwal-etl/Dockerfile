# ------------------------------------------------------------------
# COEQWAL DSS ➜ CSV Batch Image
#   - Builds pydsstools environment (heclib static lib injected)
#   - Includes AWS CLI + unzip for S3 + ZIP handling
#   - Copies project code into /app
#   - Wrapper entrypoint downloads ZIP, runs dss_to_csv.py, uploads outputs
# ------------------------------------------------------------------
# coeqwal-etl / Dockerfile
FROM --platform=linux/amd64 python:3.10-slim

# System deps (pydsstools build, unzip)
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    make \
    zlib1g-dev \
    gfortran \
    python3-dev \
    unzip \
 && rm -rf /var/lib/apt/lists/*

# pydsstools expected layout
RUN mkdir -p /pydsstools/src/external/dss/linux64 \
    && mkdir -p /pydsstools/src/external/dss/headers \
    && mkdir -p /pydsstools/src/external/gridv6/headers \
    && mkdir -p /pydsstools/src/external/zlib

# supply heclib.a
COPY heclib/heclib.a /pydsstools/src/external/dss/linux64/

# compiler & linker hints
ENV CFLAGS="-I/usr/include -I/pydsstools/src/external/dss/headers -I/pydsstools/src/external/gridv6/headers"
ENV LDFLAGS="-L/usr/lib/x86_64-linux-gnu -L/pydsstools/src/external/dss/linux64"

# Python deps + AWS CLI
RUN pip install --no-cache-dir \
    numpy==1.24.4 \
    pandas==2.0.3 \
    PyYAML==6.0.1 \
    psutil==5.9.8 \
    psycopg2-binary==2.9.9 \
    boto3==1.34.162 \
    awscli==1.34.162 \
    setuptools \
    wheel

# pydsstools after env/inputs ready
RUN pip install --no-cache-dir pydsstools

WORKDIR /app

# copy code
COPY python-code/ ./python-code/
COPY batch_entrypoint.sh ./batch_entrypoint.sh

# ensure executables
RUN chmod +x /app/batch_entrypoint.sh \
    && chmod +x /app/python-code/classify_dss.py || true

ENTRYPOINT ["/app/batch_entrypoint.sh"]
# etl/coeqwal-etl/Dockerfile

# ------------------------------------------------------------------

# COEQWAL DSS ➜ CSV batch image

#   - Builds pydsstools environment (via heclib static lib)

#   - Includes AWS CLI + unzip for S3 + ZIP handling

#   - Copies project code into /app

#   - Wrapper entrypoint downloads ZIP, runs dss_to_csv.py, uploads outputs

# ------------------------------------------------------------------


FROM --platform=linux/amd64 python:3.10-slim

# System deps (pydsstools build, unzip, curl for AWS CLI v2 installer)
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    make \
    zlib1g-dev \
    gfortran \
    python3-dev \
    unzip \
    curl \
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

# ---- Python dependencies ----
RUN pip install --no-cache-dir \
    numpy==1.24.4 \
    pandas==2.0.3 \
    PyYAML==6.0.1 \
    psutil==5.9.8 \
    psycopg2-binary==2.9.9 \
    boto3==1.34.162 \
    setuptools \
    wheel

# pydsstools after env/inputs ready
RUN pip install --no-cache-dir pydsstools

# ---- Install AWS CLI v2 (official installer) ----
RUN curl -L "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip \
 && unzip /tmp/awscliv2.zip -d /tmp \
 && /tmp/aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli --update \
 && rm -rf /tmp/aws /tmp/awscliv2.zip

# quick sanity check during build
RUN aws --version

WORKDIR /app

# copy code
COPY python-code/ ./python-code/
COPY batch_entrypoint.sh ./batch_entrypoint.sh

# ensure executables
RUN chmod +x /app/batch_entrypoint.sh \
    && chmod +x /app/python-code/classify_dss.py || true

ENTRYPOINT ["/app/batch_entrypoint.sh"]
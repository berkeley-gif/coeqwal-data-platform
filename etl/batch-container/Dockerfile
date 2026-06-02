# etl/batch-container/Dockerfile

# ------------------------------------------------------------------

# COEQWAL DSS -> CSV batch image

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

# compiler & linker hints (used by the pydsstools build below)
ENV CFLAGS="-I/usr/include -I/pydsstools/src/external/dss/headers -I/pydsstools/src/external/gridv6/headers"
ENV LDFLAGS="-L/usr/lib/x86_64-linux-gnu -L/pydsstools/src/external/dss/linux64"

# Python application deps. See requirements.txt for pin rationale.
# setuptools and wheel are build tools needed for pydsstools' from-source
# install; they are not application deps so they stay here.
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# pydsstools is built from source against the heclib.a above. Installed
# after numpy is on disk so the build can find numpy headers.
# Pinned to 2.4.0: the 3.0.x line ships manylinux wheels and pulls extra
# geospatial deps (rasterio) that this from-source/heclib build path does
# not satisfy, so an unpinned install breaks the image.
RUN pip install --no-cache-dir pydsstools==2.4.0

# Install AWS CLI v2 (official installer)
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
# ------------------------------------------------------------------
# COEQWAL DSS ➜ CSV Batch Image
#   - Builds pydsstools environment (heclib static lib injected)
#   - Includes AWS CLI + unzip for S3 + ZIP handling
#   - Copies project code into /app
#   - Wrapper entrypoint downloads ZIP, runs dss_to_csv.py, uploads outputs
# ------------------------------------------------------------------
    FROM --platform=linux/amd64 python:3.10-slim

    # System deps needed for pydsstools build + runtime tools
    RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        g++ \
        make \
        zlib1g-dev \
        gfortran \
        python3-dev \
        unzip \
        bash \
        ca-certificates \
     && rm -rf /var/lib/apt/lists/*
    
    # Create expected external source dirs (header dirs may be empty but required for build flags)
    RUN mkdir -p /pydsstools/src/external/dss/linux64 \
        && mkdir -p /pydsstools/src/external/dss/headers \
        && mkdir -p /pydsstools/src/external/gridv6/headers \
        && mkdir -p /pydsstools/src/external/zlib
    
    # Provide heclib static archive (required by some pydsstools builds)
    COPY heclib/heclib.a /pydsstools/src/external/dss/linux64/
    
    # Compiler / linker flags so pydsstools can find heclib (mirrors your working dev image)
    ENV CFLAGS="-I/usr/include -I/pydsstools/src/external/dss/headers -I/pydsstools/src/external/gridv6/headers"
    ENV LDFLAGS="-L/usr/lib/x86_64-linux-gnu -L/pydsstools/src/external/dss/linux64"
    
    # Python packages
    RUN pip install --no-cache-dir \
        awscli \
        boto3 \
        numpy==1.24.4 \
        pandas==2.0.3 \
        PyYAML==6.0.1 \
        psutil==5.9.8 \
        psycopg2-binary==2.9.9 \
        setuptools \
        wheel
    
    # pydsstools (installs hec-dss Python bindings; will use above flags if compiling)
    RUN pip install --no-cache-dir pydsstools
    
    # Copy application source into image
    WORKDIR /app
    COPY . /app
    
    # Wrapper entrypoint for AWS Batch
    COPY batch_entrypoint.sh /usr/local/bin/batch_entrypoint.sh
    RUN chmod +x /usr/local/bin/batch_entrypoint.sh
    
    ENV PYTHONUNBUFFERED=1
    
    ENTRYPOINT ["/usr/local/bin/batch_entrypoint.sh"]
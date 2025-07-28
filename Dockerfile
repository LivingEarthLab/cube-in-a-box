FROM --platform=linux/amd64 osgeo/gdal:ubuntu-small-3.6.3
# FROM osgeo/gdal:ubuntu-small-3.6.3

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    TINI_VERSION=v0.19.0 \
    SHELL=/bin/bash
# Detect system architecture and download the correct Tini binary
RUN ARCH=$(dpkg --print-architecture) && \
    case "$ARCH" in \
        amd64) TINI_ARCH="amd64";; \
        arm64) TINI_ARCH="arm64";; \
        *) echo "Unsupported architecture: $ARCH" && exit 1;; \
    esac && \
    curl -fsSL -o /tini https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${TINI_ARCH} && \
    chmod +x /tini

RUN apt-get update && \
    apt-get install -y \
      build-essential \
      git \
      # For Psycopg2
      libpq-dev python3-dev \
      python3-pip \
      python3-wheel \
      wget \
      # nodejs \
      # npm \
    && apt-get autoclean \
    && apt-get autoremove \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY requirements.txt /conf/
COPY ./products/* /conf/
# COPY products.csv /conf/
# COPY lsX_c2l2_sp.products.yaml /conf/
# COPY io_lulc_annual_v02.product.yaml /conf/
RUN pip3 install --no-cache-dir --requirement /conf/requirements.txt

WORKDIR /notebooks

ENTRYPOINT ["/tini", "--"]

CMD ["jupyter", "lab", "--allow-root", "--ip=0.0.0.0", "--NotebookApp.token=''"]
# CMD ["jupyter", "lab", "--allow-root", "--ip='0.0.0.0'", "--NotebookApp.token='secretpassword'"]

# OWS
# USER root

# RUN echo "[default]" > /tmp/datacube.conf

# COPY ./datacube-ows/ows_config /env/config/ows_config

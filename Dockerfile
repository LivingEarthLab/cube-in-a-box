FROM --platform=linux/amd64 osgeo/gdal:ubuntu-small-3.6.3

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    SHELL=/bin/bash

RUN apt-get update && \
    apt-get install -y \
      build-essential \
      git \
      # For Psycopg2
      libpq-dev python3-dev \
      python3-pip \
      python3-wheel \
      wget \
    && apt-get autoclean \
    && apt-get autoremove \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY requirements.txt /conf/
COPY ./products/* /conf/
RUN pip3 install --no-cache-dir --requirement /conf/requirements.txt

WORKDIR /notebooks

CMD ["jupyter", "lab", "--allow-root", "--ip=0.0.0.0", "--NotebookApp.token=''"]

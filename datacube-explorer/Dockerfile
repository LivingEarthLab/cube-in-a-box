FROM opendatacube/explorer:2.12.4

COPY ./cubedash /usr/local/lib/python3.12/dist-packages/cubedash

RUN apt-get update && \
    apt-get install -y \
        git \
        python3-pip \
    && apt-get autoclean \
    && apt-get autoremove \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

RUN pip3 install --break-system-packages \
        planetary-computer

FROM ghcr.io/osgeo/gdal:ubuntu-small-3.12.1

LABEL org.opencontainers.image.source="https://git.unepgrid.ch/NOSTRADAMUS/cube-in-a-box-jupyter" \
      org.opencontainers.image.description="The Cube in a Box is a simple way to run the Open Data Cube." \
      org.opencontainers.image.licenses="MIT"

# Environment setup
ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    SHELL=/bin/bash \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PATH="/opt/venv/bin:$PATH" \
    CXXFLAGS="-include cstdint"

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    libgomp1 \
    libpq-dev \
    python3-dev \
    python3-pip \
    python3-wheel \
    python3-venv \
    wget \
    sudo \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user (handle conflicts)
ARG UID=1000
ARG GID=1000
RUN if getent passwd ${UID}; then userdel -f $(getent passwd ${UID} | cut -d: -f1); fi && \
    if getent group ${GID}; then groupdel $(getent group ${GID} | cut -d: -f1); fi && \
    groupadd -g ${GID} jupyter && \
    useradd -m -u ${UID} -g jupyter -s /bin/bash jupyter && \
    mkdir -p /notebooks /opt/venv && \
    chown -R jupyter:jupyter /notebooks /opt/venv

# Create virtual environment
RUN python3 -m venv /opt/venv --system-site-packages

# Copy requirements first for better layer caching
COPY --chown=jupyter:jupyter requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel && \
    pip3 install --no-cache-dir --requirement /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt

# Copy start script
COPY scripts/start.sh /usr/local/bin/start.sh
RUN chmod +x /usr/local/bin/start.sh

# Entrypoint will handle user switching
ENTRYPOINT ["/usr/local/bin/start.sh"]
WORKDIR /notebooks

# Expose Jupyter port
EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser"]
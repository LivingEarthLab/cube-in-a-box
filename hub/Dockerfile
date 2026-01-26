FROM jupyterhub/jupyterhub:5.4.3

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    JUPYTERHUB_CONFIG=/srv/jupyterhub/jupyterhub_config.py

RUN python -m pip install --upgrade pip && \
    python -m pip install \
      dockerspawner \
      jupyterhub-nativeauthenticator

# Copy custom authenticator and templates
COPY custom_authenticator.py /srv/jupyterhub/custom_authenticator.py
COPY templates /srv/jupyterhub/templates

FROM jupyterhub/jupyterhub:5.4

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    JUPYTERHUB_CONFIG=/srv/jupyterhub/jupyterhub_config.py

RUN python -m pip install --upgrade pip && \
    python -m pip install \
    dockerspawner \
    jupyterhub-nativeauthenticator

# Copy custom authenticator, spawner hooks, and templates
# restricts signups to an allowed list of users
COPY custom_authenticator.py /srv/jupyterhub/custom_authenticator.py
# configures user environments (volumes, permissions, sudo)
COPY spawner_hooks.py /srv/jupyterhub/spawner_hooks.py
# custom templates for the jupyterhub ui (e.g. signup pages)
COPY templates /srv/jupyterhub/templates

FROM jupyterhub/jupyterhub:4.1.5

# Install dockerspawner and nativeauthenticator
RUN pip install --no-cache-dir \
    dockerspawner \
    jupyterhub-nativeauthenticator

# Copy config file, custom authenticator, and templates
COPY jupyterhub_config.py /srv/jupyterhub/jupyterhub_config.py
COPY custom_authenticator.py /srv/jupyterhub/custom_authenticator.py
COPY templates /srv/jupyterhub/templates

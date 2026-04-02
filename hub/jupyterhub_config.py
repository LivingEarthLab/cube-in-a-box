"""
JupyterHub Configuration File.

This configuration file sets up the JupyterHub environment, including:
- Authentication: Uses a custom authenticator to restrict signups.
- Spawning: Configures DockerSpawner to launch user containers.
- Persistence: Sets up database and cookie secret locations.
- User Environment: Configures volumes and environment variables via spawner hooks.
"""
import os

c = get_config()

# Data persistence
c.JupyterHub.db_url = "sqlite:////data/jupyterhub.sqlite"
c.JupyterHub.cookie_secret_file = "/data/jupyterhub_cookie_secret"

# Authenticator
import sys

sys.path.insert(0, "/srv/jupyterhub")
from custom_authenticator import CustomNativeAuthenticator

c.JupyterHub.authenticator_class = CustomNativeAuthenticator

# Admin users
admin_users = set(filter(None, os.environ.get("JUPYTERHUB_ADMINS", "").split(",")))
c.Authenticator.admin_users = admin_users

# Allowed users (JUPYTERHUB_USERS + JUPYTERHUB_ADMINS)
allowed_users = set(filter(None, os.environ.get("JUPYTERHUB_USERS", "").split(",")))
c.Authenticator.allowed_users = allowed_users.union(admin_users)

# Authenticator settings
# Enable signup and open_signup so authorized users can sign up immediately
# Our custom handler will block unauthorized users
c.NativeAuthenticator.enable_signup = True
c.NativeAuthenticator.open_signup = True

# Custom templates directory for signup page
c.JupyterHub.template_paths = ["/srv/jupyterhub/templates"]

# Spawner
from dockerspawner import DockerSpawner

c.JupyterHub.spawner_class = DockerSpawner
c.DockerSpawner.image = os.environ.get("DOCKER_JUPYTER_IMAGE")
c.DockerSpawner.network_name = os.environ.get("DOCKER_NETWORK_NAME")
c.DockerSpawner.pull_policy = "never"
c.DockerSpawner.cmd = ["jupyterhub-singleuser"]


# Import spawner hook from separate module for better readability
from spawner_hooks import setup_user_environment

c.DockerSpawner.pre_spawn_hook = setup_user_environment

# Persistence
# Mount user's notebook directory
notebook_dir = os.environ.get("DOCKER_NOTEBOOK_DIR", "/notebooks")
c.DockerSpawner.notebook_dir = notebook_dir

# User data persistence using environment variables for host paths
# Maps a volume "jupyterhub-user-<username>" to the user's notebook directory
c.DockerSpawner.volumes = {
    "jupyterhub-user-{username}": notebook_dir,
    os.environ.get("HOST_PRODUCTS_DIR"): {"bind": "/conf", "mode": "ro"},
    os.environ.get("HOST_DISTRIBUTED_CONFIG"): {
        "bind": "/etc/dask/distributed.yaml",
        "mode": "ro",
    },
}


# Always remove containers when stopped so they are recreated on next spawn.
# This ensures changes to admin status (and therefore permissions, credentials,
# and /local_data mount mode) are picked up on the next login.
c.DockerSpawner.remove = True

# For debugging arguments
c.DockerSpawner.debug = True

# Environment variables for the spawned container
c.DockerSpawner.environment = {
    "ODC_DEFAULT_DB_HOSTNAME": os.environ["POSTGRES_HOSTNAME"],
    "ODC_DEFAULT_DB_PORT": os.environ["POSTGRES_PORT"],
    "ODC_DEFAULT_DB_USERNAME": os.environ["POSTGRES_USER"],
    "ODC_DEFAULT_DB_PASSWORD": os.environ["POSTGRES_PASS"],
    "ODC_DEFAULT_DB_DATABASE": os.environ["POSTGRES_DBNAME"],
    "AWS_NO_SIGN_REQUEST": "true",
    "STAC_API_URL": "https://explorer.sandbox.dea.ga.gov.au/stac/",
    "BOKEH_RESOURCES": "inline",
    "DASK_DISTRIBUTED__DASHBOARD__LINK": "/jupyter/user/{JUPYTERHUB_USER}/proxy/{port}/status",
}

# Networking
c.JupyterHub.hub_ip = "0.0.0.0"
c.JupyterHub.hub_connect_ip = (
    "jupyterhub"  # The container name of the hub on the docker network
)
c.JupyterHub.base_url = "/jupyter"

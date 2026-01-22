import os

c = get_config()

# Data persistence
c.JupyterHub.db_url = 'sqlite:////data/jupyterhub.sqlite'
c.JupyterHub.cookie_secret_file = '/data/jupyterhub_cookie_secret'

# Authenticator
import sys
sys.path.insert(0, '/srv/jupyterhub')
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
c.JupyterHub.template_paths = ['/srv/jupyterhub/templates']

# Spawner
from dockerspawner import DockerSpawner
c.JupyterHub.spawner_class = DockerSpawner
c.DockerSpawner.image = os.environ.get("DOCKER_JUPYTER_IMAGE", "cube-in-a-box-jupyter:local")
c.DockerSpawner.network_name = os.environ.get("DOCKER_NETWORK_NAME", "cube-in-a-box-backend")
c.DockerSpawner.pull_policy = "never"
c.DockerSpawner.cmd = ["jupyterhub-singleuser"]

def pre_spawn_hook(spawner):
    """Grant root privileges to admin users."""
    if spawner.user.admin:
        spawner.extra_create_kwargs = {'user': '0'}
        spawner.environment.update({'GRANT_SUDO': 'yes'})
        spawner.args = ["--allow-root"]

c.DockerSpawner.pre_spawn_hook = pre_spawn_hook

# Persistence
# Mount user's notebook directory
notebook_dir = os.environ.get("DOCKER_NOTEBOOK_DIR", "/notebooks")
c.DockerSpawner.notebook_dir = notebook_dir

# User data persistence using environment variables for host paths
# Maps a volume "jupyterhub-user-<username>" to the user's notebook directory
c.DockerSpawner.volumes = {
    "jupyterhub-user-{username}": notebook_dir,
    os.environ.get("HOST_PRODUCTS_DIR"): {"bind": "/conf", "mode": "ro"},
    os.environ.get("HOST_DATA_DIR"): {"bind": "/local_data", "mode": "rw"},
    os.environ.get("HOST_DISTRIBUTED_CONFIG"): {"bind": "/etc/dask/distributed.yaml", "mode": "ro"},
}


# Remove containers once they are stopped
c.DockerSpawner.remove = True

# For debugging arguments
c.DockerSpawner.debug = True

# Environment variables for the spawned container
c.DockerSpawner.environment = {
    "ODC_DEFAULT_DB_HOSTNAME": "postgres",
    "ODC_DEFAULT_DB_PORT": "5432",
    "ODC_DEFAULT_DB_USERNAME": os.environ["POSTGRES_USER"],
    "ODC_DEFAULT_DB_PASSWORD": os.environ["POSTGRES_PASS"],
    "ODC_DEFAULT_DB_DATABASE": os.environ["POSTGRES_DBNAME"],
    "AWS_NO_SIGN_REQUEST": "true",
    "STAC_API_URL": "https://explorer.sandbox.dea.ga.gov.au/stac/",
    "BOKEH_RESOURCES": "inline",
    "DASK_DISTRIBUTED__DASHBOARD__LINK": "/user/{username}/proxy/{port}/status",
}

# Networking
c.JupyterHub.hub_ip = "0.0.0.0"
c.JupyterHub.hub_connect_ip = "jupyterhub" # The container name of the hub on the docker network
c.JupyterHub.base_url = "/jupyter"

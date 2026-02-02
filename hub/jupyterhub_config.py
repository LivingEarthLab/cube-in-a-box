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


def pre_spawn_hook(spawner):
    """Grant root privileges to admin users and setup shared folders."""
    username = spawner.user.name

    # Get environment variables for HOST paths (used for mounting in user containers)
    host_shared_static = os.environ.get("HOST_SHARED_STATIC", "")  # ./shared on host
    host_user_folders = os.environ.get("HOST_USER_FOLDERS", "")  # ./data/shared on host

    # Container paths (where these directories are mounted in THIS jupyterhub container)
    container_shared_static = "/shared_static"  # ./shared mounted here
    container_user_folders = "/shared_users"  # ./data/shared mounted here

    # Create user's shared folder in ./data/shared/{username} if it doesn't exist
    if os.path.exists(container_user_folders):
        user_folder_path = os.path.join(container_user_folders, username)
        os.makedirs(user_folder_path, exist_ok=True)

        # Set ownership to jupyter:jupyter (UID 1000:GID 100) so non-admin users can write
        try:
            os.chown(user_folder_path, 1000, 100)
            os.chmod(user_folder_path, 0o755)  # rwxr-xr-x
        except Exception as e:
            print(
                f"Warning: Could not set ownership/permissions on {user_folder_path}: {e}"
            )

    # Mount strategy to create the structure described in HOW_TO_USE_SHARED_FOLDERS.txt:
    # /notebooks/shared/
    #   ├── all_users/          (from ./data/shared)
    #   │   ├── user1/          (read-write for user1, read-only for others)
    #   │   └── user2/          (read-write for user2, read-only for others)
    #   ├── notebooks_demo/     (from ./shared/notebooks_demo - read-only)
    #   ├── HOW_TO_USE_SHARED_FOLDERS.txt (from ./shared - read-only)
    #   └── README.md           (from ./shared/README.md - read-only)

    # Mount static content from ./shared/ individually to avoid parent/child mount conflicts
    # Check existence using container paths, but mount using host paths
    if host_shared_static:
        # Mount notebooks_demo if it exists
        notebooks_demo_container = os.path.join(
            container_shared_static, "notebooks_demo"
        )
        if os.path.exists(notebooks_demo_container):
            notebooks_demo_host = os.path.join(host_shared_static, "notebooks_demo")
            spawner.volumes[notebooks_demo_host] = {
                "bind": "/notebooks/shared/notebooks_demo",
                "mode": "ro",
            }

        # Mount README.md if it exists
        readme_container = os.path.join(container_shared_static, "README.md")
        if os.path.exists(readme_container):
            readme_host = os.path.join(host_shared_static, "README.md")
            spawner.volumes[readme_host] = {
                "bind": "/notebooks/shared/README.md",
                "mode": "ro",
            }

        # Mount HOW_TO_USE_SHARED_FOLDERS.txt if it exists
        howto_container = os.path.join(
            container_shared_static, "HOW_TO_USE_SHARED_FOLDERS.txt"
        )
        if os.path.exists(howto_container):
            howto_host = os.path.join(
                host_shared_static, "HOW_TO_USE_SHARED_FOLDERS.txt"
            )
            spawner.volumes[howto_host] = {
                "bind": "/notebooks/shared/HOW_TO_USE_SHARED_FOLDERS.txt",
                "mode": "ro",
            }

    # Mount user folders from ./data/shared at /notebooks/shared/all_users
    if host_user_folders and os.path.exists(container_user_folders):
        # Mount all_users directory as read-only
        spawner.volumes[host_user_folders] = {
            "bind": "/notebooks/shared/all_users",
            "mode": "ro",
        }

        # Mount current user's own folder as read-write (overlays the read-only mount)
        user_folder_container = os.path.join(container_user_folders, username)
        if os.path.exists(user_folder_container):
            user_folder_host = os.path.join(host_user_folders, username)
            spawner.volumes[user_folder_host] = {
                "bind": f"/notebooks/shared/all_users/{username}",
                "mode": "rw",
            }

    # Grant sudo privileges to admin users
    # IMPORTANT: We do NOT run admins as root (user: '0') because that would allow them
    # to bypass read-only mount restrictions. Instead, we give them sudo access while
    # running as the jupyter user (UID 1000), which respects the ro mounts.
    if spawner.user.admin:
        spawner.environment.update(
            {"GRANT_SUDO": "yes", "NB_UID": "1000", "NB_GID": "100"}
        )
        # Note: We do NOT set spawner.extra_create_kwargs['user'] = '0'
        # This keeps them as jupyter user but with sudo access


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
    os.environ.get("HOST_DISTRIBUTED_CONFIG"): {
        "bind": "/etc/dask/distributed.yaml",
        "mode": "ro",
    },
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
    "DASK_DISTRIBUTED__DASHBOARD__LINK": "/jupyter/user/{JUPYTERHUB_USER}/proxy/{port}/status",
}

# Networking
c.JupyterHub.hub_ip = "0.0.0.0"
c.JupyterHub.hub_connect_ip = (
    "jupyterhub"  # The container name of the hub on the docker network
)
c.JupyterHub.base_url = "/jupyter"

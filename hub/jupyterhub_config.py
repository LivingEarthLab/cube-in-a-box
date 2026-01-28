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
    """Grant root privileges to admin users and setup shared folders."""
    # Create user's shared folder if it doesn't exist
    username = spawner.user.name
    host_shared_dir = os.environ.get("HOST_SHARED_DIR", "")
    host_shared_readme = os.environ.get("HOST_SHARED_README", "")
    container_shared_dir = os.environ.get("CONTAINER_SHARED_DIR", "")
    
    # Create user directory in the container's mounted shared folder
    if container_shared_dir:
        user_shared_path = os.path.join(container_shared_dir, username)
        os.makedirs(user_shared_path, exist_ok=True)
        # Set ownership to jupyter:jupyter (UID 1000:GID 100) so non-admin users can write
        try:
            os.chown(user_shared_path, 1000, 100)
        except Exception as e:
            print(f"Warning: Could not set ownership on {user_shared_path}: {e}")

    
    # Add shared folder volumes to spawner
    # Strategy: Separate mounts for RO pool and RW user folder to avoid overlay conflicts
    notebooks_dir = os.environ.get("HOST_NOTEBOOKS_DIR", "")
    if notebooks_dir:
        spawner.volumes[notebooks_dir] = {"bind": "/mnt/notebooks_demo", "mode": "ro"}
    
    if host_shared_dir:
        # 1. Mount entire shared directory as read-only base
        spawner.volumes[host_shared_dir] = {"bind": "/mnt/shared_data", "mode": "ro"}
        
        # 2. Mount user's own folder as Read-Write to a separate location
        user_host_shared_path = os.path.join(host_shared_dir, username)
        spawner.volumes[user_host_shared_path] = {"bind": "/mnt/my_shared_data", "mode": "rw"}

    if host_shared_readme:
        spawner.volumes[host_shared_readme] = {"bind": "/mnt/shared_README.md", "mode": "ro"}


    
    # Startup script with background loop to refresh symlinks
    # 1. Create structure
    # 2. Link my folder (RW) and notebooks (RO)
    # 3. Start background loop to link other users' folders (RO) from the shared pool
    #    This ensures new users become visible without restart
    script = f"""
    mkdir -p /notebooks/shared
    chown jupyter:jupyter /notebooks/shared
    
    # Static links
    ln -sf /mnt/notebooks_demo /notebooks/shared/notebooks_demo_ReadOnly
    ln -sf /mnt/my_shared_data /notebooks/shared/{username}
    ln -sf /mnt/shared_README.md /notebooks/shared/README.md
    
    # Function to update links (runs in background)
    update_links() {{
        while true; do
            for dir in /mnt/shared_data/*/; do
                if [ -d "$dir" ]; then
                    user_dir=$(basename "$dir")
                    if [ "$user_dir" != "{username}" ] && [ "$user_dir" != "README.md" ]; then
                        # Link if not exists
                        # Use bash bracing for variable expansion to safely append _ReadOnly    
                        # In Python f-string, we must double the braces {{ }} to get literal {{ }}
                        if [ ! -e "/notebooks/shared/${{user_dir}}_ReadOnly" ]; then
                            ln -sf "$dir" "/notebooks/shared/${{user_dir}}_ReadOnly"
                        fi
                    fi
                fi
            done
            sleep 30
        done
    }}
    
    export -f update_links
    bash -c update_links &
    
    exec jupyterhub-singleuser "$@"
    """
    
    # Simplify command to run the script
    # We use a trick to pass the script as an argument to bash -c
    spawner.cmd = ["bash", "-c", script, "--"]
    
    # Grant root privileges to admin users
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

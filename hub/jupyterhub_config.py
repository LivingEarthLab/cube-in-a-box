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
    # Strategy: Mount subdirectories with appropriate permissions
    
    notebooks_dir = os.environ.get("HOST_NOTEBOOKS_DIR", "")
    if notebooks_dir:
        spawner.volumes[notebooks_dir] = {"bind": "/notebooks/shared/notebooks_demo", "mode": "ro"}
    
    if host_shared_dir:
        # Mount entire shared directory as read-only to /notebooks/shared/all_users
        spawner.volumes[host_shared_dir] = {"bind": "/notebooks/shared/all_users", "mode": "ro"}
        
        # Mount user's own folder as Read-Write, overlaying their folder in the shared view
        user_host_shared_path = os.path.join(host_shared_dir, username)
        spawner.volumes[user_host_shared_path] = {"bind": f"/notebooks/shared/all_users/{username}", "mode": "rw"}
    
    # Startup script to create instruction file and set permissions
    # We create this BEFORE making the directory read-only
    script = f"""
    # Create the instruction file in /notebooks/shared/ before making it read-only
    cat > /notebooks/shared/HOW_TO_USE_SHARED_FOLDERS.txt << 'EOF'
SHARED FOLDER USAGE
===================

📁 Structure:
  /notebooks/shared/
    ├── all_users/          (Browse all users' shared folders)
    │   ├── user1/          (Read/Write for user1, Read-Only for others)
    │   ├── user2/          (Read/Write for user2, Read-Only for others)
    │   └── user3/          (Read/Write for user3, Read-Only for others)
    ├── notebooks_demo/     (Demo notebooks - Read Only)

✅ You CAN:
  - Read all folders in all_users/
  - Write ONLY in all_users/YOUR_USERNAME/

❌ You CANNOT:
  - Write in other users' folders (read-only mount)
  - Write in the notebooks_demo folder (read-only mount)
  - Create NEW files/folders directly in /notebooks/shared/ (read-only after startup)

💡 Tip: To share files with others, put them in all_users/YOUR_USERNAME/

📝 To edit a file or folder you need to copy it to the main /notebooks folder by either
  - manually copy/paste file(s) or folder
  - or use command: cp -R ./notebooks_demo/ ../notebooks`
EOF
    
    chmod 444 /notebooks/shared/HOW_TO_USE_SHARED_FOLDERS.txt
    
    # Make /notebooks/shared read-only (chmod won't stop root, but documents intent)
    chmod 555 /notebooks/shared
    
    exec jupyterhub-singleuser "$@"
    """
    
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

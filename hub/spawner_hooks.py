"""
JupyterHub DockerSpawner hooks for configuring user containers.

This module contains the pre-spawn hook logic that was extracted from
jupyterhub_config.py to improve readability and maintainability.
"""
import os


def setup_user_environment(spawner):
    """
    Configure volumes, permissions, and privileges for spawned user containers.
    
    This hook is called before each user container is spawned and handles:
    - Creating user-specific shared folders
    - Mounting static shared content (notebooks_demo, README.md, etc.)
    - Mounting user folders with proper read/write permissions
    - Granting sudo privileges to admin users
    
    Args:
        spawner: DockerSpawner instance for the user being spawned
    """
    username = spawner.user.name
    print(f"DEBUG: Provisioning environment for user: {username}")

    # Get environment variables for HOST paths (used for mounting in user containers)
    host_shared_static = os.environ.get("HOST_SHARED_STATIC", "")  # ./shared on host
    host_user_folders = os.environ.get("HOST_USER_FOLDERS", "")  # ./data/shared on host
    host_scripts_dir = os.environ.get("HOST_SCRIPTS_DIR", "")  # ./scripts on host

    # Container paths (where these directories are mounted in THIS jupyterhub container)
    container_shared_static = "/shared_static"  # ./shared mounted here
    container_user_folders = "/shared_users"  # ./data/shared mounted here

    # Create and configure user's personal folder
    _create_user_folder(username, container_user_folders)
    
    # Mount static shared content
    _mount_shared_static(spawner, host_shared_static, container_shared_static)
    
    # Mount user folders with proper permissions
    _mount_user_folders(spawner, username, host_user_folders, container_user_folders)
    
    # Grant sudo privileges to admin users
    _configure_admin_privileges(spawner)
    
    # Mount local data with dynamic permissions (admin only)
    _mount_local_data(spawner)
    
    # Mount the host script to override the one in the image (ensures fix is applied)
    if host_scripts_dir:
        spawner.volumes[f"{host_scripts_dir}/start.sh"] = {
            "bind": "/usr/local/bin/start.sh",
            "mode": "ro"
        }


def _mount_local_data(spawner):
    """
    Mount /local_data with rw or ro permissions based on admin status.
    
    Admins get 'rw' access and production database credentials.
    Regular users get 'ro' access and read-only database credentials.
    """
    host_data_dir = os.environ.get("HOST_DATA_DIR", "")
    if not host_data_dir:
        return

    # Check if user is admin
    is_admin = spawner.user.admin
    
    # Get database connection details from environment
    db_host = os.environ.get("POSTGRES_HOSTNAME", "postgres")
    db_port = os.environ.get("POSTGRES_PORT", "5432")
    db_name = os.environ.get("POSTGRES_DBNAME", "opendatacube")

    if is_admin:
        mode = "rw"
        restrict_datacube = "no"
        # Admin credentials (from environment)
        db_user = os.environ.get("POSTGRES_USER", "opendatacube")
        db_pass = os.environ.get("POSTGRES_PASS", "opendatacubepassword")
    else:
        mode = "ro"
        restrict_datacube = "yes"
        # Read-only credentials
        db_user = os.environ.get("POSTGRES_READONLY_USER", "odc_read_only")
        db_pass = os.environ.get("POSTGRES_READONLY_PASS", "odc_read_only_password")
    
    # Construct ODC_DEFAULT_DB_URL
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

    # Mount the volume
    spawner.volumes[host_data_dir] = {"bind": "/local_data", "mode": mode}
    
    # Set environment variables for the spawned container
    print(f"DEBUG: User {spawner.user.name} permissions - mode: {mode}, restrict_datacube: {restrict_datacube}")
    spawner.environment.update({
        "RESTRICT_DATACUBE": restrict_datacube,
        "ODC_DEFAULT_DB_USERNAME": db_user,
        "ODC_DEFAULT_DB_PASSWORD": db_pass,
        "ODC_DEFAULT_DB_URL": db_url,
    })


def _create_user_folder(username, container_user_folders):
    """
    Create user's shared folder in ./data/shared/{username} if it doesn't exist.
    
    Sets ownership to jupyter:jupyter (UID 1000:GID 100) so non-admin users can write.
    """
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


def _mount_shared_static(spawner, host_shared_static, container_shared_static):
    """
    Mount all static content from ./shared/ individually to avoid parent/child mount conflicts.
    
    Dynamically discovers and mounts all files and directories in the shared folder as read-only.
    
    This creates the structure:
    /notebooks/shared/
      ├── <item1>     (from ./shared/<item1> - read-only)
      ├── <item2>     (from ./shared/<item2> - read-only)
      └── ...
    
    Note: Mounts are configured when the container is spawned. To pick up new files or folders
    added to the root of./shared/, users must restart their server (File > Hub Control Panel
    > Stop My Server > Start My Server).
    """
    if not host_shared_static:
        return

    # Check if the container shared static path exists
    if not os.path.exists(container_shared_static):
        return

    # Iterate through all items in the shared static directory
    try:
        for item_name in os.listdir(container_shared_static):
            item_container_path = os.path.join(container_shared_static, item_name)
            item_host_path = os.path.join(host_shared_static, item_name)
            
            # Mount each item (file or directory) as read-only
            spawner.volumes[item_host_path] = {
                "bind": f"/notebooks/shared/{item_name}",
                "mode": "ro",
            }
    except Exception as e:
        print(f"Warning: Could not list contents of {container_shared_static}: {e}")


def _mount_user_folders(spawner, username, host_user_folders, container_user_folders):
    """
    Mount user folders from ./data/shared at /notebooks/shared/all_users.
    
    This creates a two-layer mount:
    1. Mount all_users directory as read-only (users can see all folders)
    2. Mount current user's own folder as read-write (overlays the read-only mount)
    
    Final structure:
    /notebooks/shared/
      └── all_users/          (from ./data/shared - read-only)
          ├── user1/          (read-write for user1, read-only for others)
          └── user2/          (read-write for user2, read-only for others)
    """
    if not host_user_folders or not os.path.exists(container_user_folders):
        return

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


def _configure_admin_privileges(spawner):
    """
    Grant sudo privileges to admin users while maintaining read-only mount restrictions.
    
    IMPORTANT: We do NOT run admins as root (user: '0') because that would allow them
    to bypass read-only mount restrictions. Instead, we give them sudo access while
    running as the jupyter user (UID 1000), which respects the ro mounts.
    """
    if spawner.user.admin:
        spawner.environment.update(
            {"GRANT_SUDO": "yes", "NB_UID": "1000", "NB_GID": "100"}
        )
        # Note: We do NOT set spawner.extra_create_kwargs['user'] = '0'
        # This keeps them as jupyter user but with sudo access

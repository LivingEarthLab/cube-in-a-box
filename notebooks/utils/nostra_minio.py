import fnmatch
import os
import socket
import json
from minio import Minio, S3Error
from minio.deleteobjects import DeleteObject
from concurrent.futures import ThreadPoolExecutor

def connect_and_check_endpoint(endpoint, login, password, secure=False):
    """
    Connects to a MinIO server endpoint and verifies the connection.
    
    Parameters:
    - endpoint (str): The MinIO server endpoint in the format 'host:port'.
    - login (str): The access key for authentication.
    - password (str): The secret key for authentication.
    - secure (bool, optional): Whether to use HTTPS for the connection. Defaults to False.
    
    Returns:
    - Minio: A Minio client instance if the connection is successful.
    - None: If the connection fails or authentication is incorrect.
    
    This function attempts to establish a connection to the specified MinIO endpoint
    and performs basic checks to ensure the connection is successful. It handles various
    error scenarios and provides informative messages about the nature of the failure.
    
    Error handling includes:
    - Host resolution errors
    - Connection refusal errors
    - Authentication errors (invalid access key or secret key)
    - SSL-related errors
    - Other unexpected errors
    """
    host, port = endpoint.split(':')

    try:
        with socket.create_connection((host, int(port)), timeout=5):
            pass
    except Exception as e:
        error_str = str(e)
        if 'Temporary failure in name resolution' in error_str:
            print(f"⚠️ Impossible to connect to host {host}")
        elif 'Connection refused' in error_str:
            print(f"⚠️ Impossible to connect to port {port}")
        else:
            print(f"⚠️ Failed to connect to {host}:{port} - {e}")
        return None

    client = Minio(endpoint, access_key=login, secret_key=password, secure=secure)

    try:
        client.list_buckets()
        print("✅ Connection successful")
        return client
    except Exception as e:
        error_str = str(e)
        if 'InvalidAccessKeyId' in error_str:
            print(f"⚠️ Invalid login")
        elif 'SignatureDoesNotMatch' in error_str:
            print(f"⚠️ Invalid password")
        elif 'SSLError' in error_str:
            print('⚠️ SSL issue, use secure=True option if using HTTPS')
        else:
            print(f"⚠️ Unknown error: {type(e).__name__}: {e}")
        return None


def create_bucket(minio_client, bucket_name):
    """
    Create bucket if it doesn't exists.
    
    Parameters:
    - minio_client (Minio): An existing Minio client instance.
    - bucket_name (str): The name of the bucket to check or create.
    
    Returns:
    - bool: True if the bucket exists or was created successfully, False otherwise.
    This function checks if the specified bucket exists in the MinIO server. If the bucket
    does not exist, it creates it. The function provides informative messages about the
    success or failure of the operation.
    
    Error handling includes:
    - Bucket existence checks
    - Bucket creation failures
    - Other unexpected errors
    """
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"✅ Bucket '{bucket_name}' created successfully.")
        else:
            print(f"✅ Bucket '{bucket_name}' already exists.")
        return True
    except Exception as e:
        print(f"⚠️ Failed to ensure bucket '{bucket_name}': {e}")
        return False


def set_anonymous_download_permissions(minio_client, bucket_name):
    """
    Sets anonymous download permissions for a specified bucket in the MinIO server.
    
    Parameters:
    - minio_client (Minio): An existing Minio client instance.
    - bucket_name (str): The name of the bucket to set permissions for.
    
    Returns:
    - bool: True if the permissions were set successfully, False otherwise.
    This function sets the bucket policy to allow anonymous downloads for the specified
    bucket. It provides informative messages about the success or failure of the operation.
    
    Error handling includes:
    - Policy setting failures
    - Other unexpected errors
    """
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket_name}/*"]
            }
        ]
    }

    try:
        minio_client.set_bucket_policy(bucket_name, json.dumps(policy))
        print("✅ Anonymous download permissions set successfully.")
        return True
    except S3Error as e:
        print(f"⚠️ Error setting bucket policy: {e}")
        return False

def upload_file(client, bucket_name, local_file_path, remote_file_path, overwrite=False, complete_missing=False, verbose=False):
    """ Uploads a single file to a specified path within a MinIO bucket. """
    should_upload = True
    try:
        client.stat_object(bucket_name, remote_file_path)
        if not overwrite and not complete_missing:
            print("❌ Some file/folder(s) already exist, set overwrite or complete-missing option to True")
            return False
        elif complete_missing:
            should_upload = False
    except S3Error as e:
        if e.code in ('NoSuchKey', 'NoSuchObject'):
            should_upload = True
        elif e.code == 'NoSuchBucket':
            raise
        else:
            print(f"⚠️ Error checking existence of '{remote_file_path}': {e}")
            return False

    if should_upload or overwrite:
        try:
            client.fput_object(bucket_name, remote_file_path, local_file_path)
            if verbose:
                print(f"✅ File '{local_file_path}' uploaded to '{remote_file_path}'.")
        except S3Error as e:
            print(f"⚠️ Error uploading file '{local_file_path}': {e}")
            return False

    return True

def upload_directory(client, bucket_name, local_path, remote_path, overwrite=False, complete_missing=False, verbose=False, max_workers=5):
    """ Recursively uploads a local directory and its contents to a specified path within a MinIO bucket in parallel.
    
    Parameters:
    - client (Minio): An initialized Minio client instance for interacting with the object storage.
    - bucket_name (str): The name of the destination bucket in the MinIO server.
    - local_path (str): The local filesystem path of the directory to be uploaded.
    - remote_path (str): The remote destination prefix (path) inside the bucket.
    - overwrite (bool, optional): If True, existing remote files will be overwritten. Defaults to False.
    - complete_missing (bool, optional): If True, only files that do not already exist on the server will be uploaded. Defaults to False.
    - verbose (bool, optional): If True, prints per-file upload messages. If False, suppresses them. Defaults to False.
    - max_workers (int, optional): Maximum number of worker threads. Defaults to 5.
    
    Returns:
    - bool: True if all files were uploaded successfully or skipped as intended, False if the upload failed or was aborted.
    """
    files_to_upload = []

    try:
        for root, _, files in os.walk(local_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_path)
                remote_file_path = os.path.join(remote_path, relative_path).replace("\\", "/")
                files_to_upload.append((local_file_path, remote_file_path))
    except Exception as e:
        print(f"⚠️ Error walking through directory: {e}")
        return False

    def upload_wrapper(file_tuple):
        local_file_path, remote_file_path = file_tuple
        return upload_file(client, bucket_name, local_file_path, remote_file_path, overwrite, complete_missing, verbose)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(upload_wrapper, files_to_upload))

    if all(results):
        print(f"✅ Directory '{local_path}' uploaded successfully.")
        return True
    else:
        print(f"⚠️ Some files failed to upload.")
        return False

# def upload_file(minio_client, bucket_name, file_path, object_name=None, overwrite=False):
#     """
#     Uploads a file to a specified bucket in the MinIO server.
#     Parameters:
#     - minio_client (Minio): An existing Minio client instance.
#     - bucket_name (str): The name of the bucket to upload the file to.
#     - file_path (str): The local path of the file to upload.
#     - object_name (str, optional): The name to give the object in the bucket. If not provided,
#       the file's basename will be used.
#     - overwrite (bool, optional): Whether to overwrite the file if it already exists. Defaults to False.
#     Returns:
#     - bool: True if the file was uploaded successfully, False otherwise.
#     This function uploads a file to the specified bucket in the MinIO server. It provides
#     informative messages about the success or failure of the operation.
#     Error handling includes:
#     - File upload failures
#     - Existing file conflicts (if overwrite is False)
#     - Other unexpected errors
#     """
#     object_name = object_name or os.path.basename(file_path)

#     try:
#         if not overwrite:
#             try:
#                 minio_client.stat_object(bucket_name, object_name)
#                 print(f"⚠️ File '{object_name}' already exists in bucket '{bucket_name}'. Use overwrite=True to replace it.")
#                 return False
#             except S3Error:
#                 pass

#         minio_client.fput_object(bucket_name, object_name, file_path)
#         print(f"✅ File '{file_path}' uploaded successfully as '{object_name}'.")
#         return True
#     except S3Error as e:
#         print(f"⚠️ Error uploading file: {e}")
#         return False


# def upload_directory(client, bucket_name, local_path, remote_path,
#                      overwrite=False, complete_missing=False, verbose=False):
#     """
#     Recursively uploads a local directory and its contents to a specified path within a MinIO bucket.
#     Parameters:
#     - client (Minio): An initialized Minio client instance for interacting with the object storage.
#     - bucket_name (str): The name of the destination bucket in the MinIO server.
#     - local_path (str): The local filesystem path of the directory to be uploaded.
#     - remote_path (str): The remote destination prefix (path) inside the bucket.
#     - overwrite (bool, optional): If True, existing remote files will be overwritten. Defaults to False.
#     - complete_missing (bool, optional): If True, only files that do not already exist on the server will be uploaded. Defaults to False.
#     - verbose (bool, optional): If True, prints per-file upload messages. If False, suppresses them. Defaults to False.
#     Returns:
#     - bool: True if all files were uploaded successfully or skipped as intended, False if the upload failed or was aborted.
#     This function walks through the given local directory and uploads each file to the specified MinIO bucket,
#     preserving the directory structure. It supports two optional modes:
#     - 'overwrite': replaces any existing files at the destination.
#     - 'complete_missing': only uploads files that are not already present.
#     If neither mode is enabled and an existing file is detected at the destination,
#     the function halts and informs the user, enforcing safe behavior by default.
#     Empty directories are skipped by default, as object storage systems like MinIO do not support folder concepts natively.
#     Error handling includes:
#     - File existence checks via object metadata.
#     - Upload errors due to connection issues, permissions, or invalid paths.
#     - Graceful recovery and logging of individual file failures.
#     """
#     try:
#         for root, _, files in os.walk(local_path):
#             for file in files:
#                 local_file_path = os.path.join(root, file)
#                 relative_path = os.path.relpath(local_file_path, local_path)
#                 remote_file_path = os.path.join(remote_path, relative_path).replace("\\", "/")

#                 should_upload = True
#                 try:
#                     client.stat_object(bucket_name, remote_file_path)
#                     if not overwrite and not complete_missing:
#                         print("❌ Some file/folder(s) already exist, set overwrite or complete-missing option to True")
#                         return False
#                     elif complete_missing:
#                         should_upload = False
#                 except S3Error as e:
#                     if e.code in ('NoSuchKey', 'NoSuchObject'):
#                         should_upload = True
#                     elif e.code == 'NoSuchBucket':
#                         raise
#                     else:
#                         print(f"⚠️ Error checking existence of '{remote_file_path}': {e}")
#                         return False

#                 if should_upload or overwrite:
#                     try:
#                         client.fput_object(bucket_name, remote_file_path, local_file_path)
#                         if verbose:
#                             print(f"✅ File '{local_file_path}' uploaded to '{remote_file_path}'.")
#                     except S3Error as e:
#                         print(f"⚠️ Error uploading file '{local_file_path}': {e}")
#                         return False

#         print(f"✅ Directory '{local_path}' uploaded successfully.")
#         return True
#     except S3Error as e:
#         print(f"⚠️ Error uploading directory: {e}")
#         return False

def format_size(size_bytes):
    """
    Convert bytes to human readable format.
    
    Parameters:
    - size_bytes (int): Size in bytes to convert.
    
    Returns:
    - str: Human readable size string (e.g., "1.5 GB", "256 MB").
    
    This function converts byte values into appropriate units using 1024 as the base.
    Supports conversion up to petabytes with appropriate decimal precision.
    """
    if size_bytes == 0:
        return "0 B"
    
    size_units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    size = float(size_bytes)
    unit_index = 0
    
    while size >= 1024.0 and unit_index < len(size_units) - 1:
        size /= 1024.0
        unit_index += 1
    
    if unit_index == 0:
        return f"{int(size)} {size_units[unit_index]}"
    else:
        return f"{size:.1f} {size_units[unit_index]}"

def list_bucket_tree(client, bucket_name, prefix='', max_recursion_level=None, show_sizes=False):
    """
    Lists the contents of a MinIO bucket in a tree-like format with optional size display.
    
    Parameters:
    - client (Minio): An initialized Minio client instance.
    - bucket_name (str): The name of the bucket to list.
    - prefix (str, optional): The prefix to filter objects by. Defaults to ''.
    - max_recursion_level (int, optional): Maximum recursion level. Defaults to None (no limit).
    - show_sizes (bool, optional): Whether to display file and folder sizes. Defaults to False.
    
    Returns:
    - None: The function prints the tree structure directly.
    
    This function retrieves all objects from the specified MinIO bucket and displays them
    in a hierarchical tree structure. When show_sizes is enabled, it calculates and displays
    human-readable file sizes and cumulative folder sizes.
    
    Features include:
    - Tree-like visual representation with Unicode box drawing characters
    - Optional prefix filtering for specific subdirectories
    - Configurable recursion depth limiting
    - File and folder size display with automatic unit conversion
    - Cumulative folder size calculation
    
    Error handling includes:
    - S3/MinIO connection and access errors
    - Bucket existence validation
    - Object listing failures
    """
    try:
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        
        directory_structure = {}
        file_sizes = {}
        
        for obj in objects:
            relative_path = obj.object_name[len(prefix):].strip('/')
            file_sizes[relative_path] = obj.size
            
            parts = relative_path.split('/')
            current_level = directory_structure
            for part in parts:
                if part not in current_level:
                    current_level[part] = {}
                current_level = current_level[part]
        
        def calculate_folder_sizes(node, current_path=''):
            """Recursively calculate cumulative folder sizes."""
            total_size = 0
            
            for key, value in node.items():
                full_path = os.path.join(current_path, key).replace('\\', '/') if current_path else key
                
                if isinstance(value, dict) and value:
                    folder_size = calculate_folder_sizes(value, full_path)
                    total_size += folder_size
                else:
                    if full_path in file_sizes:
                        total_size += file_sizes[full_path]
            
            return total_size
        
        folder_sizes = {}
        if show_sizes:
            def build_folder_sizes(node, current_path=''):
                for key, value in node.items():
                    full_path = os.path.join(current_path, key).replace('\\', '/') if current_path else key
                    
                    if isinstance(value, dict) and value:
                        folder_size = calculate_folder_sizes(value, full_path)
                        folder_sizes[full_path] = folder_size
                        build_folder_sizes(value, full_path)
            
            build_folder_sizes(directory_structure)
        
        def print_tree(node, current_level=0, is_last=False, indent='', current_path=''):
            if max_recursion_level is not None and current_level > max_recursion_level:
                print(indent + '... (truncated)')
                return
            
            for i, (key, value) in enumerate(node.items()):
                is_last_item = i == len(node) - 1
                full_path = os.path.join(current_path, key).replace('\\', '/') if current_path else key
                is_file = isinstance(value, dict) and not value
                
                size_display = ''
                if show_sizes:
                    if is_file and full_path in file_sizes:
                        size_display = f' ({format_size(file_sizes[full_path])})'
                    elif not is_file and full_path in folder_sizes:
                        size_display = f' ({format_size(folder_sizes[full_path])})'
                
                connector = '└── ' if is_last_item else '├── '
                print(indent + connector + key + size_display)
                
                if is_last_item:
                    new_indent = indent + '    '
                else:
                    new_indent = indent + '│   '
                
                if isinstance(value, dict) and value:
                    print_tree(value, current_level + 1, is_last_item, new_indent, full_path)
        
        print_tree(directory_structure)
        
    except S3Error as e:
        print(f"⚠️ Error listing bucket contents: {e}")

def empty_bucket(client, bucket_name, pattern="*", verbose=False):
    """
    Deletes objects from a specified MinIO bucket using a wildcard pattern.

    Parameters:
    - client (Minio): An initialized Minio client instance.
    - bucket_name (str): The name of the bucket to empty.
    - pattern (str, optional): A wildcard pattern (e.g., 'data/*.json', 'temp/*')
      to filter objects for deletion. Defaults to '*', which deletes all objects.
    - verbose (bool, optional): If True, prints status messages for each deleted object.
      Defaults to False.

    Returns:
    - bool: True if the operation was successful, False otherwise.

    This function uses a combination of server-side prefix filtering and client-side
    wildcard matching for efficiency and flexibility.
    """
    try:
        # Determine the initial prefix for server-side filtering from the pattern
        # This helps reduce the number of objects fetched from MinIO
        initial_prefix = pattern.split('*')[0]

        # List all objects that match the initial prefix
        objects_to_delete = []
        for obj in client.list_objects(bucket_name, recursive=True, prefix=initial_prefix):
            # Use fnmatch for more accurate wildcard filtering
            if fnmatch.fnmatch(obj.object_name, pattern):
                objects_to_delete.append(DeleteObject(obj.object_name))

        deleted_count = len(objects_to_delete)

        if deleted_count > 0:
            # Use client.remove_objects() for an efficient batch deletion
            errors = client.remove_objects(bucket_name, objects_to_delete)

            if verbose:
                for obj in objects_to_delete:
                    print(f"🗑️ Deleted object: {obj.name}")

            for error in errors:
                print(f"⚠️ Error deleting object: {error}")

            print(f"🗑️ Successfully deleted {deleted_count} object(s) from bucket '{bucket_name}'.")
        else:
            print(f"ℹ️ No objects found to delete in bucket '{bucket_name}' with the specified pattern.")

        return True

    except S3Error as e:
        print(f"⚠️ Error during deletion from bucket '{bucket_name}': {e}")
        return False

def delete_bucket(client, bucket_name):
    """
    Deletes a specified bucket from the MinIO server.
    
    Parameters:
    - client (Minio): An initialized Minio client instance.
    - bucket_name (str): The name of the bucket to be deleted.
    
    Returns:
    - bool: True if the bucket was deleted successfully, False if an error occurred.
    This function attempts to delete a bucket from the MinIO server. 
    The bucket must be empty before deletion, or the operation will fail.
    Error handling includes:
    - Attempting to delete non-empty buckets
    - Bucket not found
    - Permission errors
    - Network issues
    """
    try:
        client.remove_bucket(bucket_name)
        print(f"🗑️ Bucket '{bucket_name}' deleted successfully.")
        return True
    except S3Error as e:
        print(f"⚠️ Error deleting bucket '{bucket_name}': {e}")
        return False

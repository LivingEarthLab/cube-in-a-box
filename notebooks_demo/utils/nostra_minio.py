import time
from datetime import timedelta
import fnmatch
import os
import socket
import json
from concurrent.futures import ThreadPoolExecutor
from minio import Minio, S3Error
from minio.deleteobjects import DeleteObject
from rasterio.io import MemoryFile
from typing import Any, Dict, List, Optional

from .nostra_tools import human_readable_bytes

def connect_and_check_endpoint(
    endpoint: str, 
    login: str, 
    password: str, 
    secure: bool = False
) -> Optional[Minio]:
    """Connects to a Minio endpoint and verifies access.

    This function attempts to connect to a Minio server at the given endpoint, 
    using the provided login credentials. It checks for basic connectivity and 
    authentication errors.

    Args:
        endpoint (str): The Minio endpoint in the format 'host:port'. 
                         Example: 'localhost:9000'
        login (str): The access key for authentication. 
                         Example: 'YOUR_ACCESS_KEY'
        password (str): The secret key for authentication.
                         Example: 'YOUR_SECRET_KEY'
        secure (bool, optional): Whether to use secure (HTTPS) connection. 
                                 Defaults to False.

    Returns:
        Optional[Minio]: A Minio client object if the connection and authentication 
                         are successful, otherwise None.
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


def create_bucket(
    minio_client: Minio, 
    bucket_name: str
) -> bool:
    """Creates a Minio bucket if it doesn't already exist.

    This function checks if a bucket exists and creates it if it doesn't.
    It prints success or failure messages to the console.

    Args:
        minio_client (Minio): The Minio client object.
        bucket_name (str): The name of the bucket to create. 
                            Example: 'my-bucket'

    Returns:
        bool: True if the bucket was created or already exists, False otherwise.
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


def set_anonymous_download_permissions(
    minio_client: Minio, 
    bucket_name: str
) -> bool:
    """Sets anonymous download permissions for a Minio bucket.

    This function sets a bucket policy to allow anonymous read access to objects 
    within the specified bucket.

    Args:
        minio_client (Minio): The Minio client object.
        bucket_name (str): The name of the bucket to configure.
                            Example: 'public-bucket'

    Returns:
        bool: True if the policy was set successfully, False otherwise.
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

def upload_file(
    client: Minio, 
    bucket_name: str, 
    local_file_path: str, 
    remote_file_path: str, 
    overwrite: bool = False, 
    complete_missing: bool = False, 
    verbose: bool = False
) -> bool:
    """Uploads a file to a Minio bucket.

    This function uploads a local file to a specified bucket and remote path. 
    It handles existing files based on the 'overwrite' and 'complete_missing' flags.

    Args:
        client (Minio): The Minio client object.
        bucket_name (str): The name of the bucket to upload to. 
                            Example: 'my-bucket'
        local_file_path (str): The path to the local file to upload. 
                                Example: '/path/to/local/file.txt'
        remote_file_path (str): The desired path for the file in the bucket.
                                 Example: 'remote/file.txt'
        overwrite (bool, optional): Whether to overwrite existing files. 
                                     Defaults to False.
        complete_missing (bool, optional): Whether to upload only if the file is missing.
                                           Defaults to False.
        verbose (bool, optional): Whether to print verbose output. 
                                  Defaults to False.

    Returns:
        bool: True if the file was uploaded successfully or already existed 
              (and overwrite/complete_missing conditions were met), False otherwise.
    """
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

def _upload_wrapper(file_tuple):
        local_file_path, remote_file_path = file_tuple
        return upload_file(client, bucket_name, local_file_path, remote_file_path, overwrite, complete_missing, verbose)

def upload_directory(
    client: Minio, 
    bucket_name: str, 
    local_path: str, 
    remote_path: str, 
    overwrite: bool = False, 
    complete_missing: bool = False, 
    verbose: bool = False, 
    max_workers: int = 5
) -> bool:
    """Uploads a directory to a Minio bucket.

    This function recursively walks through a local directory and uploads all files 
    to a specified bucket and remote path. It utilizes a thread pool to upload files concurrently.

    Args:
        client (Minio): The Minio client object.
        bucket_name (str): The name of the bucket to upload to. 
                            Example: 'my-bucket'
        local_path (str): The path to the local directory to upload. 
                           Example: '/path/to/local/directory'
        remote_path (str): The desired path for the directory in the bucket.
                            Example: 'remote/directory'
        overwrite (bool, optional): Whether to overwrite existing files. 
                                     Defaults to False.
        complete_missing (bool, optional): Whether to upload only if the file is missing.
                                           Defaults to False.
        verbose (bool, optional): Whether to print verbose output. 
                                  Defaults to False.
        max_workers (int, optional): The maximum number of threads to use for uploading.
                                      Defaults to 5.

    Returns:
        bool: True if all files were uploaded successfully, False otherwise.
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

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(_upload_wrapper, files_to_upload))

    if all(results):
        print(f"✅ Directory '{local_path}' uploaded successfully.")
        return True
    else:
        print(f"⚠️ Some files failed to upload.")
        return False
        
def list_bucket_tree(
    client: Minio,
    bucket_name: str,
    prefix: str = '',
    max_recursion_level: Optional[int] = None,
    show_sizes: bool = False
) -> None:
    """Lists the contents of a Minio bucket as a tree structure.

    This function recursively lists the objects within a specified Minio bucket, 
    displaying them in a tree-like format. It can optionally show file sizes 
    and limit the recursion depth.

    Args:
        client (Minio): The Minio client object.
        bucket_name (str): The name of the bucket to list.
                            Example: 'my-bucket'
        prefix (str, optional): A prefix to filter objects by. Defaults to ''.
        max_recursion_level (int, optional): The maximum recursion depth to display. 
                                              Defaults to None (no limit).
        show_sizes (bool, optional): Whether to display file and folder sizes. 
                                     Defaults to False.
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
                        size_display = f' ({human_readable_bytes(file_sizes[full_path])})'
                    elif not is_file and full_path in folder_sizes:
                        size_display = f' ({human_readable_bytes(folder_sizes[full_path])})'
                
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

def _glob_to_regex(glob_pattern):
        pattern = glob_pattern.strip('/')
        pattern = re.escape(pattern)
        pattern = pattern.replace(r'\*\*', '.*')
        pattern = pattern.replace(r'\*', '[^/]+')
        return re.compile(pattern)
        
def list_last_level_folders(
    client: Minio,
    bucket_name: str,
    prefix: str = '',
    filter_pattern: Optional[str] = None
) -> list[str]:
    """Lists the last-level folders within a Minio bucket.

    This function retrieves a list of folders that are directly containing objects 
    within a specified Minio bucket and prefix. It can optionally filter the folders 
    based on a glob pattern.

    Args:
        client (Minio): The Minio client object.
        bucket_name (str): The name of the bucket to list.
                            Example: 'my-bucket'
        prefix (str, optional): A prefix to filter objects by. Defaults to ''.
        filter_pattern (str, optional): A glob pattern to filter folders. 
                                         Defaults to None.

    Returns:
        list[str]: A sorted list of last-level folder names.
    """
    try:
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        last_level_folders = set()

        for obj in objects:
            path_parts = obj.object_name.strip('/').split('/')
            if len(path_parts) > 1:
                parent_folder = '/'.join(path_parts[:-1])
                last_level_folders.add(parent_folder)

        if filter_pattern:
            regex = _glob_to_regex(filter_pattern)
            last_level_folders = {p for p in last_level_folders if regex.search(p)}

        return sorted(last_level_folders)

    except S3Error as e:
        print(f"⚠️ Error listing bucket contents: {e}")
        return []
        
def empty_bucket(
    client: Minio,
    bucket_name: str,
    pattern: str = "*",
    verbose: bool = False
) -> bool:
    """Empties a Minio bucket based on a wildcard pattern.

    This function deletes all objects within a specified Minio bucket that match 
    a given wildcard pattern. It uses server-side filtering to minimize the number 
    of objects fetched.

    Args:
        client (Minio): The Minio client object.
        bucket_name (str): The name of the bucket to empty.
                            Example: 'my-bucket'
        pattern (str, optional): A wildcard pattern to match objects for deletion. 
                                  Defaults to '*' (delete all objects).
        verbose (bool, optional): Whether to print verbose output about deleted objects.
                                  Defaults to False.

    Returns:
        bool: True if the operation completed successfully (or no objects were found), 
              False otherwise.
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

def delete_bucket(
    client: Minio,
    bucket_name: str
) -> bool:
    """Deletes a Minio bucket.

    This function deletes the specified Minio bucket.

    Args:
        client (Minio): The Minio client object.
        bucket_name (str): The name of the bucket to delete.
                            Example: 'my-bucket'

    Returns:
        bool: True if the bucket was deleted successfully, False otherwise.
    """
    try:
        client.remove_bucket(bucket_name)
        print(f"🗑️ Bucket '{bucket_name}' deleted successfully.")
        return True
    except S3Error as e:
        print(f"⚠️ Error deleting bucket '{bucket_name}': {e}")
        return False

def list_minio_files(
    minio_client: Minio,
    minio_bucket: str,
    folder: str,
    valid_exts: tuple = (".tif", ".tiff"),
    notvalid_exts: tuple = (".yaml"),
    recursive: bool = True
) -> tuple[List[str], List[str]]:
    """Lists files in a Minio bucket based on extensions.

    This function lists files within a specified Minio bucket and folder, 
    categorizing them based on their file extensions into 'images' and 'others'.

    Args:
        minio_client (Minio): The Minio client object.
        minio_bucket (str): The name of the bucket to list.
                             Example: 'my-bucket'
        folder (str): The prefix (folder) within the bucket to list files from.
                      Example: 'images/'
        valid_exts (tuple, optional): A tuple of valid file extensions for images. 
                                      Defaults to (".tif", ".tiff").
        notvalid_exts (tuple, optional): A tuple of extensions to exclude. 
                                         Defaults to (".yaml").
        recursive (bool, optional): Whether to list files recursively. 
                                     Defaults to True.

    Returns:
        tuple[List[str], List[str]]: A tuple containing two lists: 
                                      the first list contains paths to image files, 
                                      and the second list contains paths to other files.
    """
    images = []
    others = []
    for obj in minio_client.list_objects(minio_bucket, prefix=folder, recursive=recursive):
        if obj.object_name.lower().endswith(valid_exts):
            images.append(obj.object_name)
        elif not obj.object_name.lower().endswith(notvalid_exts):
            others.append(obj.object_name)
            
    return images, others
            
def describe_minio_image(
    minio_client: Minio,
    minio_bucket: str,
    image_path: str
) -> Dict[str, Any]:
    """Describes the geospatial properties of an image in Minio.

    This function retrieves an image from a Minio bucket and extracts its geospatial 
    properties, such as bounds, CRS, polygon coordinates, transform, and shape.

    Args:
        minio_client (Minio): The Minio client object.
        minio_bucket (str): The name of the bucket containing the image.
                             Example: 'my-bucket'
        image_path (str): The path to the image within the bucket.
                           Example: 'images/my_image.tif'

    Returns:
        Dict[str, Any]: A dictionary containing the extracted geospatial properties:
                          'epsg_code' (int or None), 'polygon' (list of coordinates), 
                          'transform' (transform object), 'shape' (tuple of width and height).
    """
    try:
        response = minio_client.get_object(minio_bucket, image_path)
        with MemoryFile(response.read()) as memfile:
            with memfile.open() as img:
                # Get bounds and CRS
                left, bottom, right, top = img.bounds
                crs = img.crs

                # # Spatial reference (WKT)
                # spatial_reference = str(getattr(img, 'crs_wkt', None) or crs.wkt)

                # EPSG code (if available)
                epsg_code = None
                if crs and crs.is_epsg_code:
                    epsg_code = int(crs.to_epsg())

                # Polygon corners (closed loop: ul -> ur -> lr -> ll -> ul)
                polygon = [
                    [left, top],     # ul
                    [right, top],    # ur
                    [right, bottom], # lr
                    [left, bottom],  # ll
                    [left, top]      # ul (close the polygon)
                ]

                # Transform object
                transform = img.transform

                # Shape (width, height)
                shape = (img.width, img.height)

                return {
                    # 'spatial_reference': spatial_reference,
                    'epsg_code': epsg_code,
                    'polygon': polygon,
                    'transform': transform,
                    'shape': shape
                }
    finally:
        response.close()
        response.release_conn()  
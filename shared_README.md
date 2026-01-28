# Shared Directory

This directory is shared among users of the JupyterHub instance.

## Purpose
The primary purpose of this folder is to facilitate file sharing and collaboration between users. Any file or directory placed in this folder is accessible to other users who have access to the shared volume in Read Only mode (to make it clear "_ReadOnly" is appended to the directory name). In other words .ipynb can be edited executed by other users, but not overwritten.

## Editing and saving shared folders and files
Files and folders can by copy/pasted manually to a different directory. Or with commands such as:
- `cp -rL notebooks_demo_ReadOnly ../notebooks_demo`
- `cp -L ./anyuser/Anyname.ipynb ../AnyName.ipynb`.

In case user is admin, it might have to change userand group using `chmod` command:
- `chown -R jupyter:jupyter ../notebooks_demo`
- `chown jupyter:jupyter AnyName.ipynb`

## Important Notes
- **Visibility**: Content in this folder is visible to all users with access to the shared mount.
- **Data Safety**: Do not place sensitive credentials or private data in this directory.

# Cube in a Box

[![License: EUPL v1.2](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

The Cube in a Box is a simple way to run the [Open Data Cube](https://www.opendatacube.org). The current repository is based on [https://github.com/opendatacube/cube-in-a-box](https://github.com/opendatacube/cube-in-a-box) with several modifications (in red in next figure):

![cube-in-a-box_new_architecture.excalidraw.png](./figures/cube-in-a-box_new_architecture.excalidraw.png)

- Default Jupyter notebook replaced by Jupyterlab
- `sign_url` function added to access data in [Planetary Computer](https://planetarycomputer.microsoft.com/catalog)
- Default source for `Sentinel-2` ([https://earth-search.aws.element84.com/v0/](https://earth-search.aws.element84.com/v0/), slow and unstable) replaced by [Planetary Computer](https://planetarycomputer.microsoft.com/catalog)
- Default ESRI Land Cover source ([io-lulc]([Planetary Computer](https://planetarycomputer.microsoft.com/dataset/io-lulc)), deprecated) replaced by [io-lulc-annual-v02](https://planetarycomputer.microsoft.com/dataset/io-lulc-annual-v02)
- `Landsat Collection 2 Level 2 Science Products` added
- Jupyter notebook modified or created for each available product (they will run only if you run `make setup` without customization)
- [datacube-explorer](https://github.com/opendatacube/datacube-explorer) added and modified to access [Planetary Computer](https://planetarycomputer.microsoft.com/catalog) data (using `sign_url` function)
- `DATETIME` added as `make` argument

## How to use:

### 1. Local environment setup (Linux, macOS, Windows)

This project is run via `docker compose` and a `Makefile`. Before running `docker compose` commands through `make`, ensure you have:

- **Docker** with **Docker Compose** support
- **GNU Make**

Below are platform-specific setup instructions.

#### Linux

1. Install Docker Engine

   - Install Docker Engine for your distribution (Ubuntu/Debian/Fedora, etc.) using the [official Docker instructions](https://docs.docker.com/engine/install/).
   - Add your `user` to the `docker` group so you can run Docker without `sudo`, then log out/in.

2. Install Docker Compose

   - Recent Docker Engine installations include the Compose plugin and expose it as `docker compose ...`.
   - Verify:
     - `docker --version`
     - `docker compose version`

3. Install Make

   - Install GNU Make using your package manager.
   - Verify:
     - `make --version`

#### macOS

1. Install Docker Desktop

   - Install [Docker Desktop for Mac](https://docs.docker.com/desktop/setup/install/mac-install/) and ensure it is running.
   - Verify:
     - `docker --version`
     - `docker compose version`

2. Install Make

   - macOS typically has `make` available via Xcode Command Line Tools.
   - Install if needed: `xcode-select --install`
   - Verify:
     - `make --version`

#### Windows (recommended: WSL2 + Docker Desktop)

The simplest way to use `make` on Windows is to run the project inside **WSL2** (Windows Subsystem for Linux) while using **Docker Desktop** as the Docker backend.

1. Install WSL2

   - Install [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) and a Linux distribution (Ubuntu is a common choice).
   - Open your WSL terminal (e.g., Ubuntu).

2. Install Docker Desktop

   - Install [Docker Desktop](https://docs.docker.com/desktop/setup/install/windows-install/) for Windows and enable:
     - **Use WSL 2 based engine**
     - **WSL Integration** for your chosen Linux distribution (Settings → Resources → WSL Integration)

3. Install Make inside WSL

   - In your WSL terminal, install GNU Make:
     - Debian/Ubuntu: `sudo apt update && sudo apt install -y make`
   - Verify:
     - `make --version`

4. Verify Docker access from WSL

   In your WSL terminal, run:
     - `docker --version`
     - `docker compose version`
   
   If these work, WSL is correctly talking to Docker Desktop.

> Notes:
>
> - Run all `make ...` commands **from the WSL terminal** (not PowerShell) to ensure a consistent Linux-like environment.
> - Store the repository inside the WSL filesystem (e.g., `~/projects/...`) for better performance than `/mnt/c/...`.

#### Quick verification

Once installed, you should be able to run:

- `make --version`
- `docker --version`
- `docker compose version`

### 2. Usage

#### Environment variables

This repository uses environment variables to configure the local domain, database credentials, and the Jupyter password.

1. Create a `.env` file (Docker Compose reads `.env` by default):

   ```bash
   cp .env.default .env
   ````

2. Edit `.env` to match your setup:
   - Set strong passwords for `POSTGRES_PASS`
   - Configure `JUPYTERHUB_ADMINS` with admin usernames
   - Optionally add regular users to `JUPYTERHUB_USERS`

##### Required variables

| Variable              | Required | Default (as provided)  | Example                    | Description                                                   |
| --------------------- | -------: | ---------------------- | -------------------------- | ------------------------------------------------------------- |
| `DOMAIN`              |      Yes | `localhost`            | `localhost`                | Hostname used to access the web endpoints (Jupyter/Explorer). |
| `POSTGRES_DBNAME`     |      Yes | `opendatacube`         | `opendatacube`             | PostgreSQL database name used by Open Data Cube.              |
| `POSTGRES_USER`       |      Yes | `opendatacube`         | `opendatacube`             | PostgreSQL user for the Open Data Cube database.              |
| `POSTGRES_PASS`       |      Yes | `opendatacubepassword` | `a-strong-password`        | PostgreSQL password for the Open Data Cube database.          |
| `JUPYTERHUB_ADMINS`   |      Yes | (none)                 | `admin,bruno`              | Comma-separated list of JupyterHub admin usernames.           |
| `JUPYTERHUB_USERS`    |       No | (none)                 | `guest,alice,bob`          | Comma-separated list of authorized non-admin usernames.       |

#### User Management

JupyterHub uses NativeAuthenticator with a custom signup handler that restricts access to pre-authorized users only.

##### How User Authorization Works

1. **Authorized Users**: Only users listed in `JUPYTERHUB_ADMINS` or `JUPYTERHUB_USERS` in the `.env` file can successfully sign up
2. **Unauthorized Users**: Users not in these lists will see an error message directing them to contact the administrator
3. **Self-Service Signup**: Authorized users can create their own accounts via the signup page
4. **Admin Creation**: Administrators can also create user accounts through the JupyterHub admin panel

##### Adding Authorized Users

**Method 1: Via `.env` file (Recommended for initial setup)**

1. Edit the `.env` file:
   ```bash
   # Admin users (have full control over JupyterHub)
   JUPYTERHUB_ADMINS=admin,bruno
   
   # Regular users (can only access their own notebooks)
   JUPYTERHUB_USERS=guest,alice,bob
   ```

2. Restart JupyterHub to apply changes:
   ```bash
   docker-compose restart jupyterhub
   ```

3. Users can now visit `http://<DOMAIN>/jupyter/hub/signup` to create their accounts

**Method 2: Via JupyterHub Admin Panel (For ad-hoc user additions)**

1. Log in as an admin user
2. Navigate to `http://<DOMAIN>/jupyter/hub/admin`
3. Click "Add Users"
4. Enter the username and click "Add Users"
5. The user is created immediately and can log in with their password

> **Note**: Users created via the admin panel are automatically authorized and can sign up. However, they won't have admin privileges unless also added to `JUPYTERHUB_ADMINS` in `.env`.

##### User Signup Flow

**For Authorized Users:**
1. Visit `http://<DOMAIN>/jupyter/hub/signup`
2. Fill in username (must match one in `.env`), password, and optional email
3. Submit the form
4. See success message: "The signup was successful! You can now go to the home page and log in to the system."
5. Log in at `http://<DOMAIN>/jupyter/hub/login`

**For Unauthorized Users:**
1. Contact the administrator to be added to `JUPYTERHUB_USERS` in `.env`

##### Managing Existing Users

**View all users:**
- Log in as admin → Navigate to `http://<DOMAIN>/jupyter/hub/admin`
- You'll see a list of all users with their status and last activity

**Edit user:**
- Click "Edit User" next to any user
- You can make them admin, delete them, or manage their servers

**Delete user:**
- Click "Edit User" → "Delete User"
- This removes the user account but doesn't delete their notebook files (stored in `jupyterhub-user-<username>` volume)

##### User Data and Notebooks

Each user's notebooks are stored in a Docker volume named `jupyterhub-user-<username>`. These volumes persist even if the user account is deleted.

**Backup user notebooks:**
```bash
docker run --rm -v jupyterhub-user-<username>:/source -v $(pwd)/backups:/backup alpine tar czf /backup/user-<username>-notebooks.tar.gz -C /source .
```

**Restore user notebooks:**
```bash
docker run --rm -v jupyterhub-user-<username>:/target -v $(pwd)/backups:/backup alpine tar xzf /backup/user-<username>-notebooks.tar.gz -C /target
```

**Remove all user volumes:**
```bash
make purge-all-users CONFIRM=1
```

##### Security Best Practices

1. **Use strong passwords** for admin accounts
2. **Regularly review** the user list in the admin panel
3. **Remove unused accounts** to minimize security risks
4. **Backup user data** regularly (see Backup and Restore section)
5. **Keep `JUPYTERHUB_ADMINS` minimal** - only trusted users should have admin access

#### Using the Open Data Cube via `make`

All interaction with the stack is wrapped behind `make` targets. To see the authoritative list on your machine:

```bash
make help
```

##### Command reference (from `make help`)

| Command                | Description                                                                                |
| ---------------------- | ------------------------------------------------------------------------------------------ |
| `make backup`          | Create a backup of the PostgreSQL database                                                 |
| `make build`           | Build the images locally                                                                   |
| `make build-nocache`   | Build the images locally from scratch                                                      |
| `make clean`           | Stop everything and remove containers, volumes, and built images                           |
| `make down`            | Stop the running services (keeps your data and images)                                     |
| `make help`            | Show available commands                                                                    |
| `make index`           | Index example data for the selected area/time (uses BBOX and DATETIME)                     |
| `make index-parallel`  | Index data using the automated script (recommended)                                        |
| `make index-serie`     | Index data step-by-step (older method; slower)                                             |
| `make init`            | Initialize the Open Data Cube database (run once after setup)                              |
| `make logs`            | Show live logs from all services (useful for troubleshooting)                              |
| `make product`         | Load product definitions into the database (describes available datasets)                  |
| `make pull`            | Download all service images (recommended before first run in prod mode)                    |
| `purge-user`           | Remove a specific user container and volume. Irreversible; requires HUB_USER and CONFIRM=1 | 
| `purge-users`          | Remove all spawned JupyterHub user containers. Irreversible; requires CONFIRM=1            |
| `make purge-data`      | Delete local data in ./data (pg and local_data). Irreversible; requires CONFIRM=1          |
| `make release-push`    | Build and push multi-architecture production images to the configured container registry   |
| `make restore`         | Restore PostgreSQL database from a backup file (requires BACKUP_FILE and CONFIRM=1)        |
| `make setup`           | First-time setup (mode-dependent: uses pull in prod, build in dev)                         |
| `make shell`           | Open a terminal inside the Jupyter container (requires HUB_USER)                           |
| `make status`          | Show what is running (containers and their status)                                         |
| `make up`              | Start the environment in the background (then open Jupyter in your browser)                |
| `make update-explorer` | Rebuild the Explorer index so datasets appear in the web UI                                |


##### Common usage patterns

- First-time setup (default parameters):

  ```bash
  make setup
  ```

- Setup with a specific area/time (BBOX, DATETIME):
 
  ```bash
  # Switzerland 1 year
  make setup BBOX=5.95,45.81,10.50,47.81 DATETIME=2024-01-01/2024-12-31
  
  # Switzerland all years (till end 2025, might take a while)
  make setup BBOX=5.95,45.81,10.50,47.81 DATETIME=1984-01-01/2025-12-31
  ```

- Start/stop and troubleshoot:

  ```bash
  make up
  make status
  make logs
  make down
  ```

- Reset options (use with care):

  ```bash
  # Stop everything and remove containers/volumes/images
  make clean
  
  # Irreversible: delete local data in ./data (requires confirmation)
  make purge-data CONFIRM=1
  ```

- Dev mode (local builds):

  ```bash
  # One-off dev invocation
  make up MODE=dev
  
  # Or set dev mode for the entire session
  export MODE=dev
  
  # Build images in dev mode
  make build MODE=dev
  make build-nocache MODE=dev
  ```

#### Access to applications

- JupyterHub is available on: `http://<DOMAIN>/jupyter/` (Use NativeAuthenticator for login - admin users defined in `JUPYTERHUB_ADMINS`)
- Explorer is available on: `http://<DOMAIN>/explorer`

#### Backup and Restore

##### Creating a backup

To create a backup of your PostgreSQL database:

```bash
make backup
```

This will create a timestamped SQL dump file in the `./backups` directory (e.g., `./backups/opendatacube_20260121_141530.sql`).

##### Restoring from a backup

To restore a database from a backup file:

```bash
make restore BACKUP_FILE=./backups/opendatacube_20260121_141530.sql CONFIRM=1
```

> **⚠️ WARNING**: Restoring will overwrite your current database. Make sure you have a recent backup before proceeding.

##### Volume backup procedures

The following directories contain persistent data and should be backed up regularly:

- `./data/pg/` - PostgreSQL database files
- `./data/local_data/` - Local data cache
- `./data/jupyterhub_data/` - JupyterHub configuration and user data
- User notebooks are stored in Docker volumes named `jupyterhub-user-<username>`

**Manual volume backup:**

```bash
# Backup user notebooks
docker run --rm -v jupyterhub-user-<username>:/source -v $(pwd)/backups:/backup alpine tar czf /backup/user-<username>-notebooks.tar.gz -C /source .

# Backup all data directories
tar czf backups/data-backup-$(date +%Y%m%d).tar.gz ./data/
```

**Restore user notebooks:**

```bash
# Restore user notebooks
docker run --rm -v jupyterhub-user-<username>:/target -v $(pwd)/backups:/backup alpine tar xzf /backup/user-<username>-notebooks.tar.gz -C /target
```


## Specificities

- Sentinel 2 indexation requires `archive-less-mature` option in [Makefile](./Makefile) to keep only the most recent version of a given scene, but will trigger an ERROR message (which should be a WARNING as non-blocking).

## Contributing

Contributions are welcome! Feel free to submit PRs or open issues for feature requests.

## License

This project is licensed under the **MIT License**.

**Copyright © 2025 UNIGE/GRID**

You are free to use, modify, and distribute this software under the terms of the MIT License.
For more details, see the full license text: [MIT](https://opensource.org/license/mit).

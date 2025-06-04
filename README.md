# Cube in a Box

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

### 1. Setup:

**First time users of Docker should run (on Linux):**

* `bash setup.sh` - This will get your system running and install everything you need.
* Note that after this step you will either need to logout/login, or run the next step with `sudo`

**If you already have cloned the repo and have `make` , `docker` and `docker-compose` installed.**

* Default :`make setup`
* Switzerland 1 year: `make setup BBOX=5.95,45.81,10.50,47.81 DATETIME=2024-01-01/2024-12-31`
* Switzerland all years (till end 2025, might take a while (~15' in my case): `make setup BBOX=5.95,45.81,10.50,47.81 DATETIME=1984-01-01/2025-12-31`

### 2. Usage:

- Jupyterlab available on [http://localhost/lab](http://localhost/lab) using the password `secretpassword`

- Default Jupyter notebook still available on [http://localhost/tree](http://localhost/tree) using the password `secretpassword`

- Explorer available on [http://localhost:81](http://localhost:81) 

# Specificities

- Sentinel 2 indexation requires `archive-less-mature` option in [Makefile](./Makefile) to keep only the most recent version of a given scene, but will trigger an ERROR message (which should be a WARNING as non-blocking).

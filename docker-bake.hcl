variable "PLATFORMS" {
  default = "linux/amd64,linux/arm64"
}

variable "REGISTRY" {
  default = "git.unepgrid.ch/nostradamus"
}

variable "TAG" {
  default = "prod"
}

group "dev" {
  pull    = true
  targets = ["jupyter_dev", "explorer_dev"]
}

group "release" {
  pull    = true
  targets = ["jupyter", "explorer"]
}

# Common settings for multi-platform builds
target "_common_release" {
  platforms = split(",", PLATFORMS)
}

# --- Dev (local) targets: build for the current machine and load into Docker Engine ---
target "jupyter_dev" {
  context    = "."
  dockerfile = "Dockerfile"
  tags       = ["cube-in-a-box-jupyter:dev"]
  output     = ["type=docker"]
}

target "explorer_dev" {
  context    = "datacube-explorer"
  dockerfile = "Dockerfile"
  tags       = ["cube-in-a-box-explorer:dev"]
  output     = ["type=docker"]
}

# --- Release (multi-arch) targets: intended for pushing to a registry ---
target "jupyter" {
  inherits   = ["_common_release"]
  context    = "."
  dockerfile = "Dockerfile"
  tags       = ["${REGISTRY}/cube-in-a-box-jupyter-test:${TAG}"]
}

target "explorer" {
  inherits   = ["_common_release"]
  context    = "datacube-explorer"
  dockerfile = "Dockerfile"
  tags       = ["${REGISTRY}/cube-in-a-box-explorer-test:${TAG}"]
}

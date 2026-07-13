#!/bin/bash
set -e

info() {
    if [ "${START_QUIET:-}" != "1" ]; then
        echo "$@"
    fi
}

# Fix ownership of /local_data if it exists and is writable
if [ -d "/local_data" ]; then
    if [ -w "/local_data" ]; then
        info "Fixing /local_data ownership..."
        chown -R 1000:100 /local_data
    else
        info "Skipping /local_data ownership fix (/local_data is read-only)"
    fi
fi

# Handle sudo permissions if requested
if [ "$GRANT_SUDO" == "yes" ]; then
    info "Granting sudo access to jupyter user..."
    echo "jupyter ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/jupyter
    chmod 0440 /etc/sudoers.d/jupyter
else
    info "Restricting sudo access for non-admin user..."
    # Only root can execute sudo; typical users will get "Permission denied"
    chmod 0700 $(command -v sudo)
fi

# Handle datacube CLI restriction if requested
if [ "$RESTRICT_DATACUBE" == "yes" ]; then
    info "Using read-only database credentials for datacube..."
    # Note: We no longer chmod 0700 the binary because it broke the Python library.
    # Security is now enforced at the database level via the read-only user.
fi

# Export PYTHONPATH to include shared packages (e.g. installed via make install-le)
export PYTHONPATH="/local_data/site-packages:${PYTHONPATH:-}"

# Switch to jupyter user for execution if currently root
if [ "$(id -u)" == "0" ]; then
    info "Switching to jupyter user..."
    # Resolve the command path to ensure sudo finds it (e.g. in /opt/venv/bin)
    CMD="$1"
    if [[ "$CMD" != /* ]] && command -v "$CMD" >/dev/null; then
        # Check if the command is executable
        RESOLVED_CMD=$(command -v "$CMD")
        shift
        set -- "$RESOLVED_CMD" "$@"
    fi

    # We use sudo -E to preserve environment variables
    # We use exec to replace the shell process
    exec sudo -E -u jupyter "PATH=$PATH" "HOME=/home/jupyter" "PYTHONPATH=$PYTHONPATH" "$@"
else
    exec "$@"
fi

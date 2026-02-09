#!/bin/bash
set -e

# Fix ownership of /local_data if it exists
if [ -d "/local_data" ]; then
    echo "Fixing /local_data ownership..."
    chown -R 1000:100 /local_data
fi

# Handle sudo permissions if requested
if [ "$GRANT_SUDO" == "yes" ]; then
    echo "Granting sudo access to jupyter user..."
    echo "jupyter ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/jupyter
    chmod 0440 /etc/sudoers.d/jupyter
else
    echo "Restricting sudo access for non-admin user..."
    # Only root can execute sudo; typical users will get "Permission denied"
    chmod 0700 $(command -v sudo)
fi

# Switch to jupyter user for execution if currently root
if [ "$(id -u)" == "0" ]; then
    echo "Switching to jupyter user..."
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
    exec sudo -E -u jupyter "PATH=$PATH" "HOME=/home/jupyter" "$@"
else
    exec "$@"
fi

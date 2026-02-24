#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Run migrations
echo "Running migrations..."
python3.10 ./deIdentification/manage.py migrate

# Setup portal
echo "Setting up portal..."
python3.10 ./deIdentification/manage.py setup_portal

# Start the server
echo "Starting the server..."
cd /nd_deployment/deIdentification
exec gunicorn deIdentification.wsgi:application --bind 0.0.0.0:8000


sqlcmd -S ECWDBSRV2 -U neurodisc1 -P "M@k3.0459!" -Q "BACKUP DATABASE mobiledoc TO DISK = 'C:\ROHITCHOUHAN\Files\Backup\backupfiles\mobiledoc.bak'"

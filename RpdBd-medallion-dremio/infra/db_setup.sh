#!/usr/bin/env bash
# Instalacja i konfiguracja PostgreSQL na n5 (wymaga sudo):  infra/db_setup.sh [haslo]
set -euo pipefail
PASS="${1:-rpdbd_$(head -c6 /dev/urandom | od -An -tx1 | tr -d ' \n')}"
sudo apt-get install -y postgresql
sudo sed -i "s/^#\?listen_addresses.*/listen_addresses = '*'/" /etc/postgresql/*/main/postgresql.conf
echo "host rpdbd rpdbd 10.0.0.0/8 scram-sha-256" | sudo tee -a /etc/postgresql/*/main/pg_hba.conf >/dev/null
sudo systemctl restart postgresql
sudo -u postgres psql -c "CREATE USER rpdbd WITH PASSWORD '$PASS'" || sudo -u postgres psql -c "ALTER USER rpdbd PASSWORD '$PASS'"
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='rpdbd'" | grep -q 1 || sudo -u postgres createdb -O rpdbd rpdbd
echo "Dodaj do .env na n4 i n5:"
echo "PG_DSN=postgresql://rpdbd:$PASS@n5:5432/rpdbd"

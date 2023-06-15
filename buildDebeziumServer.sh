#!/usr/bin/env bash
set -e

# prereqs
make debezium
make debezium-server

# rebuiling because we make changes to these components, and it may be cached. TODO: find better alternative for this.
make debezium-core
make debezium-server-core
make postgres-connector
#!/usr/bin/env bash
set -e

# prereqs
make debezium
# rebuiling because we make changes to these components, and it may be cached. TODO: find better alternative for this.
make debezium-core
make debezium-server-core
make postgres-connector

make debezium-server
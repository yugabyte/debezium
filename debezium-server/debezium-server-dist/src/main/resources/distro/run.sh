#!/bin/bash
#
# Copyright Debezium Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ -z "$JAVA_HOME" ]; then
  JAVA_BINARY="java"
else
  JAVA_BINARY="$JAVA_HOME/bin/java"
fi

if [ "$OSTYPE" = "msys" ] || [ "$OSTYPE" = "cygwin" ]; then
  PATH_SEP=";"
else
  PATH_SEP=":"
fi

RUNNER=$(ls "$SCRIPT_DIR"/debezium-server-*runner.jar)

ENABLE_DEBEZIUM_SCRIPTING=${ENABLE_DEBEZIUM_SCRIPTING:-false}
LIB_PATH="$SCRIPT_DIR/lib/*"
if [[ "${ENABLE_DEBEZIUM_SCRIPTING}" == "true" ]]; then
    LIB_PATH=$LIB_PATH$PATH_SEP"$SCRIPT_DIR/lib_opt/*"
fi

DEBUGGER="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
exec "$JAVA_BINARY" $DEBEZIUM_OPTS $JAVA_OPTS -cp "$RUNNER"$PATH_SEP"conf"$PATH_SEP$LIB_PATH "$DEBUGGER" io.debezium.server.Main
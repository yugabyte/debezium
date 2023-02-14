#!/bin/bash
#
# Copyright Debezium Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

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

RUNNER=$(ls debezium-server-*runner.jar)

ENABLE_DEBEZIUM_SCRIPTING=${ENABLE_DEBEZIUM_SCRIPTING:-false}
LIB_PATH="lib/*"
if [[ "${ENABLE_DEBEZIUM_SCRIPTING}" == "true" ]]; then
    LIB_PATH=$LIB_PATH$PATH_SEP"lib_opt/*"
fi

JAVA_OPTS="-Xmx3G"
ASYNC_PROFILER_PATH="/path/to/libasyncProfiler.so"
PROFILER="-agentpath:${ASYNC_PROFILER_PATH}=start,event=cpu,file=profile.html"
PROFILER="-XX:StartFlightRecording=disk=true,dumponexit=true,filename=flight.jfr"
DEBUGGER="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
exec "$JAVA_BINARY" $DEBEZIUM_OPTS $JAVA_OPTS "$PROFILER" -cp "$RUNNER"$PATH_SEP"conf"$PATH_SEP$LIB_PATH "$DEBUGGER" io.debezium.server.Main
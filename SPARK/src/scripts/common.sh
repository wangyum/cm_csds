#!/bin/bash
##
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##

#
# Set of utility functions shared across different Spark CSDs.
#

set -ex

function log {
  timestamp=$(date)
  echo "$timestamp: $1"       #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

# Time marker for both stderr and stdout
log "Running Spark CSD control script..."
log "Detected CDH_VERSION of [$CDH_VERSION]"

# Set this to not source defaults
export BIGTOP_DEFAULTS_DIR=""

export HADOOP_HOME=${HADOOP_HOME:-$CDH_HADOOP_HOME}
export HDFS_BIN=$HADOOP_HOME/../../bin/hdfs

export HADOOP_CONF_DIR="$CONF_DIR/yarn-conf"
if [ ! -d "$HADOOP_CONF_DIR" ]; then
  HADOOP_CONF_DIR="$CONF_DIR/hadoop-conf"
  if [ ! -d "$HADOOP_CONF_DIR" ]; then
    log "No Hadoop configuration found."
    exit 1
  fi
fi

# If SPARK_HOME is not set, make it the default
DEFAULT_SPARK_HOME=/opt/cloudera/parcels/YSPARK
SPARK_HOME=${SPARK_HOME:-$CDH_SPARK_HOME}
export SPARK_HOME=${SPARK_HOME:-$DEFAULT_SPARK_HOME}

# We want to use a local conf dir
export SPARK_CONF_DIR="$CONF_DIR/spark-conf"
if [ ! -d "$SPARK_CONF_DIR" ]; then
  mkdir "$SPARK_CONF_DIR"
fi

# Variables used when generating configs.
export SPARK_ENV="$SPARK_CONF_DIR/spark-env.sh"
export SPARK_DEFAULTS="$SPARK_CONF_DIR/spark-defaults.conf"

# Copy the log4j directory to the config directory if it exists.
if [ -f $CONF_DIR/log4j.properties ]; then
  cp "$CONF_DIR/log4j.properties" "$SPARK_CONF_DIR"
fi

# Set JAVA_OPTS for the daemons
# sets preference to IPV4
export SPARK_DAEMON_JAVA_OPTS="$SPARK_DAEMON_JAVA_OPTS -Djava.net.preferIPv4Stack=true"

# Reads a line in the format "$host:$key=$value", setting those variables.
function readconf {
  local conf
  IFS=':' read host conf <<< "$1"
  IFS='=' read key value <<< "$conf"
}

function get_default_fs {
  "$HDFS_BIN" --config $1 getconf -confKey fs.defaultFS
}

# replace $1 with $2 in file $3
function replace {
  perl -pi -e "s#${1}#${2}#g" $3
}

function add_to_classpath {
  local CLASSPATH_FILE="$1"
  local CLASSPATH="$2"

  # Break the classpath into individual entries
  IFS=: read -a CLASSPATH_ENTRIES <<< "$CLASSPATH"

  # Expand each component of the classpath, resolve symlinks, and add
  # entries to the classpath file, ignoring duplicates.
  for pattern in "${CLASSPATH_ENTRIES[@]}"; do
    for entry in $pattern; do
      entry=$(readlink -m "$entry")
      name=$(basename $entry)
      if [ -f "$entry" ] && ! grep -q "/$name\$" "$CLASSPATH_FILE"; then
        echo "$entry" >> "$CLASSPATH_FILE"
      fi
    done
  done
}

# prepare the spark-env.sh file specified in $1 for use.
# $2 should contain the path to the Spark jar in HDFS. This is for backwards compatibility
# so that users of CDH 5.1 and earlier have a way to reference it.
function prepare_spark_env {
  replace "{{HADOOP_HOME}}" "$HADOOP_HOME" $SPARK_ENV
  replace "{{SPARK_HOME}}" "$SPARK_HOME" $SPARK_ENV
  replace "{{SPARK_EXTRA_LIB_PATH}}" "$SPARK_LIBRARY_PATH" $SPARK_ENV
  replace "{{SPARK_JAR_HDFS_PATH}}" "$SPARK_JAR" $SPARK_ENV
  replace "{{MASTER_PORT}}" "$MASTER_PORT" $SPARK_ENV
  replace "{{PYTHON_PATH}}" "$PYTHON_PATH" ""$SPARK_ENV""

  # Create a classpath.txt file with all the entries that should be in Spark's classpath.
  # The classpath is expanded so that we can de-duplicate entries, to avoid having the JVM
  # opening the same jar file multiple times.
  local CLASSPATH_FILE="$(dirname $SPARK_ENV)/classpath.txt"
  local CLASSPATH_FILE_TMP="${CLASSPATH_FILE}.tmp"

  touch "$CLASSPATH_FILE_TMP"
  add_to_classpath "$CLASSPATH_FILE_TMP" "$HADOOP_HOME/client/*.jar"

  local HADOOP_CP="$($HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR classpath)"
  add_to_classpath "$CLASSPATH_FILE_TMP" "$HADOOP_CP"

  # CDH-29066. Some versions of CDH don't define CDH_AVRO_HOME nor CDH_PARQUET_HOME. But the CM
  # agent does define a default value for CDH_PARQUET_HOME which does not work with parcels. So
  # detect those cases here and do the right thing.
  if [ -z "$CDH_AVRO_HOME" ]; then
    CDH_AVRO_HOME="$CDH_HADOOP_HOME/../avro"
  fi
  if [ -n "$PARCELS_ROOT" ]; then
    if ! [[ $CDH_PARQUET_HOME == $PARCELS_ROOT* ]]; then
      CDH_PARQUET_HOME="$CDH_HADOOP_HOME/../parquet"
    fi
  fi

  add_to_classpath "$CLASSPATH_FILE_TMP" "$CDH_HIVE_HOME/lib/*.jar"
  add_to_classpath "$CLASSPATH_FILE_TMP" "$CDH_FLUME_HOME/lib/*.jar"
  add_to_classpath "$CLASSPATH_FILE_TMP" "$CDH_PARQUET_HOME/lib/*.jar"
  add_to_classpath "$CLASSPATH_FILE_TMP" "$CDH_AVRO_HOME/*.jar"
  if [ -n "HADOOP_CLASSPATH" ]; then
    add_to_classpath "$CLASSPATH_FILE_TMP" "$HADOOP_CLASSPATH"
  fi

  if [ -n "CDH_SPARK_CLASSPATH" ]; then
    add_to_classpath "$CLASSPATH_FILE_TMP" "$CDH_SPARK_CLASSPATH"
  fi

  cat "$CLASSPATH_FILE_TMP" | sort | uniq > "$CLASSPATH_FILE"
  rm -f "$CLASSPATH_FILE_TMP"
}

function find_local_spark_jar {
  # CDH-28715: use the version-less symlink if available to work around a bug in CM's
  # stale configuration detection. This should allow a newer Spark installation to run
  # with a config that has not been updated, although any other updated configs will
  # be missed.
  local SPARK_JAR_LOCAL_PATH=
  if [ -f "$SPARK_HOME/jars/*.jar" ]; then
    echo "$SPARK_HOME/jars/*.jar"
  elif [ -f "$SPARK_HOME/assembly/jars/*.jar" ]; then
    echo "$SPARK_HOME/assembly/jars/*.jar"
  else
    echo "$SPARK_JAR_LOCAL_PATH"
  fi
}

function run_spark_class {
  local ARGS=($@)
  ARGS+=($ADDITIONAL_ARGS)
  prepare_spark_env
  cmd="$SPARK_HOME/bin/spark-class ${ARGS[@]}"
  echo "Running [$cmd]"
  exec $cmd
}

function start_history_server {
  log "Starting Spark History Server"
  local CONF_FILE="$CONF_DIR/spark-history-server.conf"
  local LOG_DIR="$(get_default_fs $HADOOP_CONF_DIR)$HISTORY_LOG_DIR"
  if [ -f "$CONF_FILE" ]; then
    echo "spark.history.fs.logDirectory=$LOG_DIR" >> "$CONF_FILE"

    ARGS=(
      "org.apache.spark.deploy.history.HistoryServer"
      "--properties-file"
      "$CONF_FILE"
    )
  else
    ARGS=(
      "org.apache.spark.deploy.history.HistoryServer"
      -d
      "$LOG_DIR"
    )
  fi

  if [ "$SPARK_PRINCIPAL" != "" ]; then
    KRB_OPTS="-Dspark.history.kerberos.enabled=true"
    KRB_OPTS="$KRB_OPTS -Dspark.history.kerberos.principal=$SPARK_PRINCIPAL"
    KRB_OPTS="$KRB_OPTS -Dspark.history.kerberos.keytab=spark_on_yarn.keytab"
    export SPARK_DAEMON_JAVA_OPTS="$KRB_OPTS $SPARK_DAEMON_JAVA_OPTS"
  fi
  run_spark_class "${ARGS[@]}"
}

function deploy_client_config {
  log "Deploying client configuration"

  prepare_spark_env
  if [ -n "$PYTHON_PATH" ]; then
    echo "spark.executorEnv.PYTHONPATH=$PYTHON_PATH" >> $SPARK_DEFAULTS
  fi

  # Move the Yarn configuration under the Spark config. Do not overwrite Spark's log4j config.
  HADOOP_CLIENT_CONF_DIR="$SPARK_CONF_DIR/$(basename $HADOOP_CONF_DIR)"
  mkdir "$HADOOP_CLIENT_CONF_DIR"
  for i in "$HADOOP_CONF_DIR"/*; do
    if [ $(basename "$i") != log4j.properties ]; then
      mv $i "$HADOOP_CLIENT_CONF_DIR"

      # CDH-28425. Because of OPSAPS-25695, we need to fix the YARN config ourselves.
      target="$HADOOP_CLIENT_CONF_DIR/$(basename $i)"
      replace "{{CDH_MR2_HOME}}" "$CDH_MR2_HOME" "$target"
      replace "{{HADOOP_CLASSPATH}}" "$HADOOP_CLASSPATH" "$target"
      replace "{{JAVA_LIBRARY_PATH}}" "" "$target"
    fi
  done

  # SPARK 1.1 makes "file:" the default protocol for the location of event logs. So we need
  # to fix the configuration file to add the protocol.
  DEFAULT_FS=$(get_default_fs "$HADOOP_CLIENT_CONF_DIR")
  if grep -q 'spark.eventLog.dir' "$SPARK_DEFAULTS"; then
    replace "(spark\\.eventLog\\.dir)=(.*)" "\\1=$DEFAULT_FS\\2" "$SPARK_DEFAULTS"
  fi

  # If a history server is configured, set its address in the default config file so that
  # the Yarn RM web ui links to the history server for Spark apps.
  HISTORY_PROPS="$CONF_DIR/history.properties"
  HISTORY_HOST=
  if [ -f "$HISTORY_PROPS" ]; then
    for line in $(cat "$HISTORY_PROPS")
    do
      readconf "$line"
      case $key in
       (spark.history.ui.port)
         HISTORY_HOST="$host"
         HISTORY_PORT="$value"
       ;;
      esac
    done
    if [ -n "$HISTORY_HOST" ]; then
      echo "spark.yarn.historyServer.address=http://$HISTORY_HOST:$HISTORY_PORT" >> \
        "$SPARK_DEFAULTS"
    fi
  fi

  if [ $CDH_VERSION -ge 5 ]; then
    # If no Spark jar is defined, look for the location of the jar on the local filesystem,
    # which we assume will be the same across the cluster.
    if ! grep -q 'spark.yarn.jar' "$SPARK_DEFAULTS"; then
      if [ -n "$SPARK_JAR" ]; then
        SPARK_JAR="$DEFAULT_FS$SPARK_JAR"
      else
        SPARK_JAR="local:$(find_local_spark_jar)"
      fi
      echo "spark.yarn.jar=$SPARK_JAR" >> "$SPARK_DEFAULTS"
    fi
  fi

  # Set the default library paths for drivers and executors.
  EXTRA_LIB_PATH="$HADOOP_HOME/lib/native"
  if [ -n "$SPARK_LIBRARY_PATH" ]; then
    EXTRA_LIB_PATH="$EXTRA_LIB_PATH:$SPARK_LIBRARY_PATH"
  fi
  for i in driver executor yarn.am; do
    if ! grep -q "^spark\\.${i}\\.extraLibraryPath" "$SPARK_DEFAULTS"; then
      echo "spark.${i}.extraLibraryPath=$EXTRA_LIB_PATH" >> "$SPARK_DEFAULTS"
    fi
  done

  # If using parcels, write extra configuration that tells Spark to replace the parcel
  # path with references to the NM's environment instead, so that users can have different
  # paths on each node.
  if [ -n "$PARCELS_ROOT" ]; then
    echo "spark.yarn.config.gatewayPath=$PARCELS_ROOT" >> "$SPARK_DEFAULTS"
    echo "spark.yarn.config.replacementPath={{HADOOP_COMMON_HOME}}/../../.." >> "$SPARK_DEFAULTS"
  fi
}

function upload_jar {
  # The assembly jar does not exist in Spark for CDH4.
  if [ $CDH_VERSION -lt 5 ]; then
    log "Detected CDH [$CDH_VERSION]. Uploading Spark assembly jar skipped."
    exit 0
  fi

  if [ -z "$SPARK_JAR" ]; then
    log "Spark jar configuration is empty, skipping upload."
    exit 0
  fi

  log "Uploading Spark assembly jar to '$SPARK_JAR' on CDH $CDH_VERSION cluster"

  if [ -n "$SPARK_PRINCIPAL" ]; then
    # Source the common script to use acquire_kerberos_tgt
    . $COMMON_SCRIPT
    export SCM_KERBEROS_PRINCIPAL="$SPARK_PRINCIPAL"
    acquire_kerberos_tgt spark_on_yarn.keytab
  fi

  SPARK_JAR_LOCAL_PATH=$(find_local_spark_jar)

  # Does it already exist on HDFS?
  if $HDFS_BIN dfs -test -f "$SPARK_JAR" ; then
    BAK="$SPARK_JAR.$(date +%s)"
    log "Backing up existing Spark jar as $BAK"
    "$HDFS_BIN" dfs -mv "$SPARK_JAR" "$BAK"
  else
    # Create HDFS hierarchy
    "$HDFS_BIN" dfs -mkdir -p $(dirname "$SPARK_JAR")
  fi

  "$HDFS_BIN" dfs -put "$SPARK_JAR_LOCAL_PATH" "$SPARK_JAR"
  exit $?
}

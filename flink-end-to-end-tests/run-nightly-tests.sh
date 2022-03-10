#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

END_TO_END_DIR="`dirname \"$0\"`" # relative
END_TO_END_DIR="`( cd \"$END_TO_END_DIR\" && pwd -P)`" # absolutized and normalized
if [ -z "$END_TO_END_DIR" ] ; then
    # error; for some reason, the path is not accessible
    # to the script (e.g. permissions re-evaled after suid)
    exit 1  # fail
fi

export END_TO_END_DIR

if [ -z "${FLINK_DIR:-}" ] ; then
    echo "You have to export the Flink distribution directory as FLINK_DIR"
    exit 1
fi

if [ -z "$FLINK_LOG_DIR" ] ; then
    export FLINK_LOG_DIR="$FLINK_DIR/log"
fi

# On Azure CI, use artifacts dir
if [ -z "$DEBUG_FILES_OUTPUT_DIR" ] ; then
    export DEBUG_FILES_OUTPUT_DIR="$FLINK_LOG_DIR"
fi

source "${END_TO_END_DIR}/../tools/ci/maven-utils.sh"
source "${END_TO_END_DIR}/test-scripts/test-runner-common.sh"

function run_on_exit {
    collect_coredumps $(pwd) $DEBUG_FILES_OUTPUT_DIR
    collect_dmesg $DEBUG_FILES_OUTPUT_DIR
}

on_exit run_on_exit

if [[ ${PROFILE} == *"enable-adaptive-scheduler"* ]]; then
	echo "Enabling adaptive scheduler properties"
	export JVM_ARGS="-Dflink.tests.enable-adaptive-scheduler=true"
fi

FLINK_DIR="`( cd \"$FLINK_DIR\" && pwd -P)`" # absolutized and normalized

echo "flink-end-to-end-test directory: $END_TO_END_DIR"
echo "Flink distribution directory: $FLINK_DIR"

echo "Java and Maven version"
java -version
run_mvn -version

echo "Free disk space"
df -h

echo "Running with profile '$PROFILE'"

# Template for adding a test:

# run_test "<description>" "$END_TO_END_DIR/test-scripts/<script_name>" ["skip_check_exceptions"]

# IMPORTANT:
# With the "skip_check_exceptions" flag one can disable default exceptions and errors checking in log files. This should be done
# carefully though. A valid reasons for doing so could be e.g killing TMs randomly as we cannot predict what exception could be thrown. Whenever
# those checks are disabled, one should take care that a proper checks are performed in the tests itself that ensure that the test finished
# in an expected state.

printf "\n\n==============================================================================\n"
printf "Running bash end-to-end tests\n"
printf "==============================================================================\n"

function run_group_1 {
    ################################################################################
    # Checkpointing tests
    ################################################################################

    run_test "Resuming Savepoint (hashmap, async, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 hashmap true"
    run_test "Resuming Savepoint (hashmap, sync, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 hashmap false"
    run_test "Resuming Savepoint (hashmap, async, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 hashmap true"
    run_test "Resuming Savepoint (hashmap, sync, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 hashmap false"
    run_test "Resuming Savepoint (hashmap, async, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 hashmap true"
    run_test "Resuming Savepoint (hashmap, sync, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 hashmap false"
    run_test "Resuming Savepoint (rocks, no parallelism change, heap timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 rocks false heap"
    run_test "Resuming Savepoint (rocks, scale up, heap timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 rocks false heap"
    run_test "Resuming Savepoint (rocks, scale down, heap timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 rocks false heap"
    run_test "Resuming Savepoint (rocks, no parallelism change, rocks timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 rocks false rocks"
    run_test "Resuming Savepoint (rocks, scale up, rocks timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 rocks false rocks"
    run_test "Resuming Savepoint (rocks, scale down, rocks timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 rocks false rocks"

    run_test "Resuming Externalized Checkpoint (hashmap, async, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 hashmap true true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (hashmap, sync, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 hashmap false true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (hashmap, async, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 5 hashmap true true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (hashmap, sync, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 4 hashmap false true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (hashmap, async, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 5 2 hashmap true true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (hashmap, sync, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 4 2 hashmap false true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (rocks, non-incremental, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true false" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (rocks, incremental, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (rocks, non-incremental, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 3 4 rocks true false" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (rocks, incremental, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 4 rocks true true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (rocks, non-incremental, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 5 3 rocks true false" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (rocks, incremental, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 4 2 rocks true true" "skip_check_exceptions"

    run_test "Resuming Externalized Checkpoint after terminal failure (hashmap, async) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 hashmap true false true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint after terminal failure (hashmap, sync) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 hashmap false false true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint after terminal failure (rocks, non-incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true false true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint after terminal failure (rocks, incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true true true" "skip_check_exceptions"

    run_test "RocksDB Memory Management end-to-end test" "$END_TO_END_DIR/test-scripts/test_rocksdb_state_memory_control.sh"

    ################################################################################
    # Docker / Container / Kubernetes tests
    ################################################################################

    if [[ ${PROFILE} != *"enable-adaptive-scheduler"* ]]; then
        run_test "Wordcount on Docker test (custom fs plugin)" "$END_TO_END_DIR/test-scripts/test_docker_embedded_job.sh dummy-fs"
    fi
}

function run_group_2 {
    ################################################################################
    # Miscellaneous
    ################################################################################

    run_test "Elasticsearch (v6.8.20) sink end-to-end test" "$END_TO_END_DIR/test-scripts/test_streaming_elasticsearch.sh 6 https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.8.20.tar.gz"
    run_test "Elasticsearch (v7.15.2) sink end-to-end test" "$END_TO_END_DIR/test-scripts/test_streaming_elasticsearch.sh 7 https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.15.2-linux-x86_64.tar.gz"

    run_test "Quickstarts Java nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_quickstarts.sh java"
    run_test "Quickstarts Scala nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_quickstarts.sh scala"

    run_test "Walkthrough DataStream Java nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_datastream_walkthroughs.sh java"
    run_test "Walkthrough DataStream Scala nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_datastream_walkthroughs.sh scala"

    run_test "Avro Confluent Schema Registry nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_confluent_schema_registry.sh"

    run_test "Dependency shading of table modules test" "$END_TO_END_DIR/test-scripts/test_table_shaded_dependencies.sh"

    LOG4J_PROPERTIES=${END_TO_END_DIR}/../tools/ci/log4j.properties

    MVN_LOGGING_OPTIONS="-Dlog.dir=${DEBUG_FILES_OUTPUT_DIR} -DlogBackupDir=${DEBUG_FILES_OUTPUT_DIR} -Dlog4j.configurationFile=file://$LOG4J_PROPERTIES"
    MVN_COMMON_OPTIONS="-Dflink.forkCount=2 -Dflink.forkCountTestPackage=2 -Dfast -Pskip-webui-build"
    e2e_modules=$(find flink-end-to-end-tests -mindepth 2 -maxdepth 5 -name 'pom.xml' -printf '%h\n' | sort -u | tr '\n' ',')
    e2e_modules="${e2e_modules},$(find flink-walkthroughs -mindepth 2 -maxdepth 2 -name 'pom.xml' -printf '%h\n' | sort -u | tr '\n' ',')"

    PROFILE="$PROFILE -Prun-end-to-end-tests"
    run_mvn ${MVN_COMMON_OPTIONS} ${MVN_LOGGING_OPTIONS} ${PROFILE} verify -pl ${e2e_modules} -DdistDir=$(readlink -e build-target) -Dcache-dir=$E2E_CACHE_FOLDER -Dcache-download-attempt-timeout=4min -Dcache-download-global-timeout=10min

    EXIT_CODE=$?
}

if [ "$1" == "1" ]; then
    run_group_1
elif [ "$1" == "2" ]; then
    run_group_2
else
    run_group_1
    run_group_2
fi



exit $EXIT_CODE

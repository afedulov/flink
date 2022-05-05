#!/bin/bash -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

. "${SCRIPT_DIR}/publish_lib.sh"

# fail immediately
# set -o errexit
# set -o nounset
# print command before executing
# set -o xtrace

./add-custom.sh -u "https://s3.amazonaws.com/flink-nightly/flink-1.16-SNAPSHOT-bin-scala_2.12.tgz" -j 8 -n 1.16-SNAPSHOT-scala_2.12-java8

# test Flink with Java11 image as well
./add-custom.sh -u "https://s3.amazonaws.com/flink-nightly/flink-1.16-SNAPSHOT-bin-scala_2.12.tgz" -j 11 -n 1.16-SNAPSHOT-scala_2.12-java11

publish_snapshots

# vim: et ts=2 sw=2

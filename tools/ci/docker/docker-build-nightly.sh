#!/usr/bin/env bash

RELEASE_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
export RELEASE_VERSION
echo "Determined RELEASE_VERSION as '$RELEASE_VERSION' "

# fail immediately
set -o errexit
set -o nounset
# print command before executing
set -o xtrace
FLINK_DIR=$(pwd)

#RELEASE_DIR=${FLINK_DIR}/tools/ci/docker/release
#mkdir -p ${RELEASE_DIR}
#dir_name="flink-$RELEASE_VERSION-bin"
#cd flink-dist/target/flink-${RELEASE_VERSION}-bin
#tar czf "${dir_name}.tgz" flink-*
#cp flink-*.tgz ${RELEASE_DIR}
#cp -r flink-dist/target/flink-${RELEASE_VERSION}-bin tools/docker/release
#cd ${RELEASE_DIR}

#Reduce size of the context dir
DOCKER_DIR=${FLINK_DIR}/tools/ci/docker
DIST_DIR=${DOCKER_DIR}/dist
mkdir -p ${DIST_DIR}
cp -r flink-dist/target/flink-${RELEASE_VERSION}-bin/flink-${RELEASE_VERSION}/* ${DIST_DIR}/

cd "${DOCKER_DIR}"
docker build -t flink:1.16-SNAPSHOT -f scala_2.12-java8-debian/Dockerfile .


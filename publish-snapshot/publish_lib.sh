#!/bin/bash -e

# This test script discovers all image variants, builds them, then runs a
# simple smoke test for each:
# - start up a jobmanager
# - wait for the /overview web UI endpoint to return successfully
# - start up a taskmanager
# - wait for the /overview web UI endpoint to report 1 connected taskmanager

IMAGE_REPO=flink

function image_tag() {
    local dockerfile
    dockerfile="$1"

    local variant minor_version
    variant="$(basename "$(dirname "$dockerfile")")"

    echo "${variant}"
}

function image_name() {
    local image_tag
    image_tag="$1"

    echo "${IMAGE_REPO}:${image_tag}"
}

function build_image() {
    local dockerfile
    dockerfile="$1"

    local image_tag image_name dockerfile_dir
    image_tag="$(image_tag "$dockerfile")"
    image_name="$(image_name "$image_tag")"
    dockerfile_dir="$(dirname "$dockerfile")"

    echo >&2 "===> Building ${image_tag} image..."
    docker build -t "$image_name" "$dockerfile_dir"
}

function publish_snapshots() {
    echo >&2 "==> Publish image"
    publish "$(ls ./*/*/Dockerfile)" ""
}


function publish(){
    local dockerfiles="$1"
    local docker_run_command_args="$2"

    for dockerfile in $dockerfiles; do
        build_image "$dockerfile"
    done
}


# vim: ts=4 sw=4 et

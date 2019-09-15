#!/usr/bin/env bash


source "$(dirname "$0")"/env.sh

PROJECT_DIR=`pwd`
$FLINK_DIR/bin/flink run -d -p 4 -c sql_case.SqlSubmit target/FLINK_CASE-1.0-SNAPSHOT.jar -w "${PROJECT_DIR}"/src/main/resources/ -f "$1".sql
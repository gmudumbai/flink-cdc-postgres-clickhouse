#!/usr/bin/env bash
# Builds the fat jar inside a Maven container (no local Java/Maven needed).
set -euo pipefail
cd "$(dirname "$0")/../flink-job"
docker run --rm -v "$PWD":/build -v cdc-m2:/root/.m2 -w /build \
  maven:3.9-eclipse-temurin-17 mvn -q -B package
ls -lh target/cdc-job.jar

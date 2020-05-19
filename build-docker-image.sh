#!/usr/bin/env bash
name="oco2peak"
version="0.0.1"
TAG="$name:$version"
echo "Build with tag $TAG"
docker build --tag $TAG .
echo "Done"
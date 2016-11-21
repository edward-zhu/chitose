#!/bin/bash

echo "packing..."
# update package
sbt package

echo "uploading..."
# upload jar file
rsync -r target/ jz2653@dumbo:~/project/target/

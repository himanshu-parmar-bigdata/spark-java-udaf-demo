#!/bin/sh
mvn clean install

aws s3 cp  ./target/spark-*.jar s3://dp-repository-dev/cdp-stream/

# upload jar file to master to run on cluster

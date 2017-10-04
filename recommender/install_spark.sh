#!/bin/bash
set -e

SPARK_VERSION='2.2.0'
HADOOP_VERSION='2.7'

curl https://d3kbcqa49mib13.cloudfront.net/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -o spark.tgz

tar -xzf spark.tgz

mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /tmp/spark


#!/bin/sh

spark-submit --master yarn --deploy-mode cluster hdfs_read.py

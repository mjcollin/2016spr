#!/bin/bash

cores=1
ex=2

export TOMCAT_OPTS="-Dlog4j.warn"
time spark-submit --executor-cores ${cores} --num-executors ${ex} --executor-memory 2G --driver-memory 2G parqueter.py

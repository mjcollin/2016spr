#!/bin/bash

cores=5
ex=18

export TOMCAT_OPTS="-Dlog4j.warn"
time spark-submit --executor-cores ${cores} --num-executors ${ex} --executor-memory 5G --driver-memory 8G parqueter.py

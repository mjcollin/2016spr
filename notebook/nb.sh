#!/bin/bash

# http://ramhiser.com/2015/02/01/configuring-ipython-notebook-support-for-pyspark/
export SPARK_HOME="/opt/spark"
#export PYSPARK_SUBMIT_ARGS="--master cloudera0.acis.ufl.edu"
#export PYSPARK_SUBMIT_ARGS="--executor-memory 3G --num-executors 3 --executor-cores 5 pyspark-shell "
ipython notebook --profile=pyspark --ip='*' --no-browser

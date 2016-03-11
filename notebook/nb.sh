#!/bin/bash

# http://ramhiser.com/2015/02/01/configuring-ipython-notebook-support-for-pyspark/
export SPARK_HOME="/opt/spark"
#export PYSPARK_SUBMIT_ARGS="--master cloudera0.acis.ufl.edu"
ipython notebook --profile=pyspark --ip='*' --no-browser

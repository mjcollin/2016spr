from __future__ import print_function                                                                                           
import os                                                                                                                       
import sys                                                                                                                      
import re                                                                                                                       
import unicodecsv
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from operator import add


import csv
import StringIO

# Parse function originally from idb-spark repo
def parse(str, headers=None):
    if headers is not None:
        retval = {}
    else:
        retval = []

    try:

        b = StringIO.StringIO(str)
        r = csv.reader(b)

        for line in r:
            i = 0
            for value in line:
                if headers:
                    retval[ headers[i] ] = value
                else:
                    retval.append(value)
                i += 1
    except Exception as e:
        with open("/tmp/rejects", "a") as f:
            f.write("PROBLEM WITH: {0}\n".format(str))

        # assume we're parsing a line and not the headers
        for h in headers:
            retval[h] = ""
        #raise e

    return retval


if __name__ == "__main__":

    base_path = "hdfs://cloudera0.acis.ufl.edu:8020/user/mcollins/test_data"
    recordset = "edbd"

    fn = "{0}/occurrence_raw_{1}.csv".format(base_path, recordset)
    out_dir = "{0}/test_parquet_{1}".format(base_path, recordset)

    sc = SparkContext(appName="Parqueter")

    records = sc.textFile(fn)
    first_line = records.take(1)[0]
    headers = parse(first_line)

    # filter removes header line which is going to be unique
    records = records.filter(lambda line: line != first_line)
    parsed = records.map(lambda x: parse(x.encode("utf8"), headers) )
    parsed.cache()

    sqlContext = SQLContext(sc)
    df = parsed.map(lambda l: Row(**dict(l))).toDF()

    df.write.parquet(out_dir)

    indf = sqlContext.read.parquet(out_dir)

    print(indf.head())

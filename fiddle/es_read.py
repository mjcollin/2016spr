from __future__ import print_function
from pyspark.sql import SQLContext, Row
import json

conf = {"es.resource":"idigbio-3.0.0/records", "es.nodes":"c17node52.acis.ufl.edu", "es.query":'''
          {
            "query":{
              "bool":{
                "must":[
                  {
                  "term":{
                    "genus":"acer"
                  }
                  }
                ]
              }
            }
          }
'''}

es_rrd = sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat", keyClass="org.apache.hadoop.io.NullWritable", valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

#don't use, dict isn't flat enough 
#df = es_rrd.map(lambda l: Row(**dict(l["data"]))).toDF()

#df = es_rrd.map(lambda l: Row({"uuid":l[0]})).toDF()
#print(df.count())
#df.write.parquet("acer_uuids.parquet")


#print(es_rrd.first())
#es_rrd.cache()
#print(es_rrd.first())

for r in es_rrd.take(10):
    print(json.dumps(r))

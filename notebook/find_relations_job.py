from __future__ import print_function
from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as sql
import pyspark.sql.types as types
import nltk

# Load Processed Parquet
sc = SparkContext(appName="Relations")
sqlContext = SQLContext(sc)
notes = sqlContext.read.parquet("../data/idigbio_notes.parquet")

notes = notes.sample(withReplacement=False, fraction=0.01)

# Let's go pipline our whole analysis
from lib.tokens import Tokens
from lib.pos_tags import PosTags
from lib.relations import Relations

t = Tokens()
p = PosTags()
r = Relations()

def pipeline(s):
    '''
    Given a string, return a list of relations
    '''
    return r.find(p.tag(t.tokenize(s)))

pipeline_udf = sql.udf(pipeline, types.ArrayType(
                                       types.MapType(
                                               types.StringType(), 
                                               types.MapType(
                                                       types.StringType(),
                                                       types.StringType()
                                                       )
                                               )
                                       )
                    )


relations = notes.withColumn("rels", pipeline_udf(notes["document"]))

relations.write.parquet('../data/idigbio_relations.parquet')

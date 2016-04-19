from __future__ import print_function
from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as sql
import pyspark.sql.types as types
import nltk

# Load Processed Parquet
sc = SparkContext(appName="Phrases")
sqlContext = SQLContext(sc)
notes = sqlContext.read.parquet("../data/idigbio_notes.parquet")

notes = notes.sample(withReplacement=False, fraction=0.001)

# Let's go pipline our whole analysis
from lib.tokens import Tokens
from lib.pos_tags import PosTags
from lib.chunks import Chunks

t = Tokens()
p = PosTags()
c = Chunks()

c.train(c.load_training_data("../data/chunker_training_50_fixed.json"))

def pipeline(s):
    '''
    Given a string, return a list of relations
    '''
    return c.assemble(c.tag(p.tag(t.tokenize(s))))

pipeline_udf = sql.udf(pipeline, types.ArrayType(
                                       types.MapType(
                                               types.StringType(), 
                                               types.StringType()
                                               )
                                       )
                    )


phrases = notes\
    .withColumn("phrases", pipeline_udf(notes["document"]))\
    .select(sql.explode(sql.col("phrases")).alias("text"))\
    .filter(sql.col("text")["tag"] == "NP")\
    .select(sql.lower(sql.col("text")["phrase"]).alias("phrase"))\
    .groupBy(sql.col("phrase"))\
    .count()

phrases.write.parquet('../data/idigbio_phrases.parquet')

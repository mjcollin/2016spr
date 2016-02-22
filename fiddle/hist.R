#install.packages("C:/cygwin64/home/mcollins/2016spr/fiddle/R/lib/sparkr.zip",
#                 repos = NULL, type = "win.binary")

Sys.setenv(JAVA_HOME="C:/Program Files/Java/jdk1.8.0_73")
Sys.setenv(SPARK_HOME="c:/cygwin64/home/mcollins/spark-1.6.0-bin-hadoop2.6")
library(SparkR)
sc <- sparkR.init(master="spark://apihack-c18.idigbio.org:7077")

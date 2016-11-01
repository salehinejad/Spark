
from pyspark import SparkContext, SparkConf

conf=SparkConf()
conf.setMaster("local[4]")
conf.setAppName("Simple Khan")
conf.set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)


# map(func) returns a new distributed dataset by
# passing each element of source through a functions
rdd = sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
rdd.repartition(1).saveAsTextFile("pat")

# Write the elements of the dataset as a Hadoop Sequence
# File in a given path in the local filesystem, HDFS or
# any other Hadoop-supported file system.
rdd.saveAsSequenceFile("pat2")
print sorted(sc.sequenceFile("pat2").collect())


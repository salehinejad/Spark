
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
print rdd.collect()


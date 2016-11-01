
from pyspark import SparkContext, SparkConf
import time
logFile = "loggy.md"  # Should be some file on your system
conf=SparkConf()
conf.setMaster("local[4]")
conf.setAppName("Simple Khan")
conf.set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)
a=time.time()
logData = sc.textFile(logFile).cache()

def summer(a,b):
    return a+b
lineLengths = logData.map(lambda s: len(s)) # map performs the task
lineLengths.persist() #to cache lineLenghts for later use - RDD persistence; to cancel: lineLengths.unpersist()
# severl levels of persistence is possible - refer to manual

#Aggregate the elements of the dataset using a function func
totalLength1 = lineLengths.reduce(summer) # reduce collects the results; takes two returns one
# methos 2
totalLength2 = lineLengths.reduce(lambda a,b: a+b) # reduce collects the results; takes two returns one

print lineLengths.collect()
print totalLength1
print totalLength2




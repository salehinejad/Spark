
from pyspark import SparkContext, SparkConf

logFile = "loggy.md"  # Should be some file on your system
conf=SparkConf()
conf.setMaster("local[4]")
conf.setAppName("Simple Khan")
conf.set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)

logData = sc.textFile(logFile).cache()

def summer(a,b):
    return a+b
lineLengths = logData.map(lambda s: len(s)) # map performs the task
#Aggregate the elements of the dataset using a function func
totalLength1 = lineLengths.reduce(summer) # reduce collects the results; takes two returns one
# methos 2
totalLength2 = lineLengths.reduce(lambda a,b: a+b) # reduce collects the results; takes two returns one

print lineLengths.collect()
print totalLength1
print totalLength2




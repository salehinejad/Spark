from pyspark import SparkContext, SparkConf

logFile = "README.md"  # Should be some file on your system
conf=SparkConf()
conf.setMaster("local[4]")
conf.setAppName("Simple Khan")
conf.set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)

logData = sc.textFile(logFile).cache()

def counter(chri):
    return 'a' in chri

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print "Lines with a: %i, lines with b: %i" % (numAs, numBs)

from pyspark import SparkContext, SparkConf

logFile = "loggy.md"  # Should be some file on your system
conf=SparkConf()
conf.setMaster("local[4]")
conf.setAppName("Simple Khan")
conf.set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)

logData = sc.textFile(logFile).cache()

def counter(line):
    return 'a' in line

numAs = logData.filter(counter).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print "Lines with a: %i, lines with b: %i" % (numAs, numBs)

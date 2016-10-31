from pyspark import SparkContext, SparkConf

logFile = "loggy.md"  # Should be some file on your system
conf=SparkConf()
conf.setMaster("local[4]")
conf.setAppName("Simple Khan")
conf.set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)

logData = sc.textFile(logFile).cache()
keywords=['Scala','Python']

def counter(line):
    return any(k in line for k in keywords)

numAs = logData.filter(counter).count()

print "Lines with keywords: %i " % (numAs)

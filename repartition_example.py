from pyspark import SparkContext, SparkConf

logFile = "loggy.md"  # Should be some file on your system
conf=SparkConf()
conf.setMaster("local[4]")
conf.setAppName("Simple Khan")
conf.set("spark.executor.memory", "2g")
sc = SparkContext(conf=conf)

logData = sc.textFile(logFile).cache()

def counter(line):
    return 'a' in line

numAs = logData.filter(counter).count()
numBs = logData.filter(lambda s: 'b' in s).count()
any_res = logData.filter(lambda s: 'Scala' in s)
any_res.repartition(3).saveAsTextFile('thispath')# dumpu any_res in three files
print any_res.count()
print any_res.collect()
print "Lines with a: %i, lines with b: %i" % (numAs, numBs)


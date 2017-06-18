from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
import csv

### path to SPARK_HOME
KEVIN_HOME = ""
LUNA_HOME = "~/Desktop/Spark/spark-2.1.1-bin-hadoop2.7/"
WILL_HOME = "~/spark-2.1.1-bin-hadoop2.7/"
###
SPARK_HOME = WILL_HOME

### the URL given in the spark UI
KEVIN_URL = ""
LUNA_URL = "spark://ubuntu:7077"
WILL_URL = "spark://losangeles.linux.ucla.edu:7077"
###
MASTER_URL = WILL_URL

conf = SparkConf()
conf.setMaster(MASTER_URL)
conf.setAppName("cs249")
conf.set("spark.executor.memory", "1g")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


### some directory you specify to hold data/results
KEVIN_PREFIX = ""
LUNA_PREFIX = "/home/guest/Desktop/Spark/sparkScripts"
WILL_PREFIX = "/home/wlai/accel_world"
###
path_prefix = WILL_PREFIX

logFile = path_prefix + "/log.log"
resultFile = path_prefix + "/results.txt"

trainingData = []
testData = []

train = sc.textFile(path_prefix + '/training.csv')
header = train.first()
train = train.filter(lambda row: row != header)
trainingData = train.map(lambda line: line.split(',')).map(lambda line: (float(line[39]), line[0], line[1], Vectors.dense(line[2::])))

test = sc.textFile(path_prefix + '/test.csv')
header = test.first()
test = test.filter(lambda row: row != header)
testData = test.map(lambda line: line.split(',')).map(lambda line: (float(line[39]), line[0], line[1], Vectors.dense(line[2::])))


training = sqlContext.createDataFrame(trainingData, ["label", "name1", "name2", "features"])

with open(logFile, "w") as log:
    lr = LogisticRegression(maxIter=10, regParam=0.01)
    log.write("LogisticRegression params:\n" + str(lr.explainParams()) + "\n")
    model = lr.fit(training)
    log.write("Model was fit using params: ")
    log.write(str(model.extractParamMap())) # this may not be working, not sure if str cast works haha

    test = sqlContext.createDataFrame(testData, ["label", "name1", "name2", "features"])

    prediction = model.transform(test)
    with open(resultFile, "w") as results:
        res = prediction.select("features", "label", "probability", "prediction").rdd
        acc = res.filter(lambda (f, label, prob, pred): label == pred).count() / float(testData.count())
        results.write('Accuracy: ' + str(acc) + '\n\n')
        r = res.map(lambda row: "features=%s, label=%s -> prob=%s, prediction=%s\n" % (row.features, row.label, row.probability, row.prediction))
        for line in r.collect():
            results.write(line)


print "\n<----FINISHED---->\n"



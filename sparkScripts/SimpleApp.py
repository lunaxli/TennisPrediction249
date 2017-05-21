"""SimpleApp.py"""
#Reads in the READme file in the spark home dir and counts the number of a's and b's

from pyspark import SparkContext, SparkConf

SPARK_HOME = "/Users/kevinnguyen/Documents/ucla/cs249-Spring/project/spark/spark-2.1.1-bin-hadoop2.7/"

conf = SparkConf()
conf.setMaster("local")
conf.setAppName("cs249")
conf.set("spark.executor.memory", "1g")
sc = SparkContext(conf = conf)

logFile = SPARK_HOME + "README.md"  # Should be some file on your system
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print "Lines with a: %i, lines with b: %i" % (numAs, numBs)



# conf.setMaster("spark://Kevins-MacBook-Pro.local:7077")
# from pyspark.mllib.classification import SVMWithSGD, SVMModel
# from pyspark.mllib.regression import LabeledPoint

# Load and parse the data
# def parsePoint(line):
#     values = [float(x) for x in line.split(' ')]
#     return LabeledPoint(values[0], values[1:])

# data = sc.textFile(SPARK_HOME + "data/mllib/sample_svm_data.txt")
# parsedData = data.map(parsePoint)

# # Build the model
# model = SVMWithSGD.train(parsedData, iterations=100)

# # Evaluating the model on training data
# labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
# trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
# print("-------------")
# print("Training Error = " + str(trainErr))
# print("-------------")

# Save and load model
# model.save(sc, "target/tmp/pythonSVMWithSGDModel")
# sameModel = SVMModel.load(sc, "target/tmp/pythonSVMWithSGDModel")








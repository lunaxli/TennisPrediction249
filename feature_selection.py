from pyspark.ml.feature import ChiSqSelector
from pyspark.ml.linalg import Vectors
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
import glob
import csv

# set up connection to spark master
SPARK_HOME = "/Users/kevinnguyen/Documents/ucla/cs249-Spring/project/spark/spark-2.1.1-bin-hadoop2.7/"

spark = SparkSession.builder.master("spark://Kevins-MacBook-Pro.local:7077").config("spark.driver.cores", 2).config("spark.driver.memory", "3g").config("spark.executor.memory", "2g").appName("cs249").getOrCreate()
sc = spark.sparkContext
sc.textFile(SPARK_HOME + "README.md")

# create dataframe for feature selection
cwd = os.getcwd()
path = cwd + "/atp_matches_features_half.csv"
dfFeaturesArray = []
players = {}
idCount = 0

for filename in glob.glob(path):
	with open(filename, 'r') as f_in:
		# read input as csv dictionary
		reader = csv.DictReader(f_in)
		for row in reader:
			# create features vector, and dataframe row
			dfRowFeatures = Vectors.dense(float(row["surface"]), float(row["seed"]), float(row["o_seed"]), float(row["ht"]), float(row["o_ht"]), float(row["age"]), float(row["o_age"]), float(row["rank"]), float(row["o_rank"]), float(row["rank_pts"]), float(row["o_rank_pts"]), float(row["ace"]), float(row["o_ace"]), float(row["df"]), float(row["o_df"]), float(row["svpt"]), float(row["o_svpt"]), float(row["1stIn"]), float(row["o_1stIn"]), float(row["1stWon"]), float(row["o_1stWon"]), float(row["2ndWon"]), float(row["o_2ndWon"]), float(row["SvGms"]), float(row["o_SvGms"]), float(row["bpSaved"]), float(row["o_bpSaved"]), float(row["bpFaced"]), float(row["o_bpFaced"]))
			dfRow = (idCount, dfRowFeatures, float(row["result"]))
			idCount += 1
			dfFeaturesArray.append(dfRow)

# create dataframe 	
df = spark.createDataFrame(dfFeaturesArray, ["id", "features", "result"])

# specify selector method
# selector = ChiSqSelector(numTopFeatures=10, featuresCol="features", outputCol="selectedFeatures", labelCol="result")
selector = ChiSqSelector(featuresCol="features", outputCol="selectedFeatures", labelCol="result")
selector.setSelectorType("fpr").setFpr(0.9)

# generate results
result = selector.fit(df).transform(df)

print("ChiSqSelector output with top %d features selected" % selector.getNumTopFeatures())
result.show(40, False)

# Features considered from atp_matches_features.csv
# surface, seed, o_seed, ht, o_ht, age, o_age, rank, o_rank, rank_pts, o_rank_pts, ace, o_ace, df, o_df, svpt, o_svpt, 1stIn, o_1stIn, 1stWon, o_1stWon, 2ndWon, o_2ndWon, SvGms, o_SvGms, bpSaved, o_bpSaved, bpFaced, o_bpFaced, result

# Same features as considered above but named differently in test data
# "surface", "avg_seed1", "avg_seed2", "avg_ht1", "avg_ht2", "avg_age1", "avg_age2", "avg_rank1", "avg_rank2", "avg_rank_pts1", "avg_rank_pts2", "avg_aces1", "avg_aces2", "avg_dfs1", "avg_dfs2", "avg_svpts1", "avg_svpts2", "avg_1stIn1", "avg_1stIn2", "avg_1stWon1", "avg_1stWon2", "avg_2ndWon1", "avg_2ndWon2", "avg_SvGms1", "avg_SvGms2", "avg_bpSaved1", "avg_bpSaved2", "avg_bpFaced1", "avg_bpFaced2", "result"













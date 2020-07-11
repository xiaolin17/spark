from __future__ import print_function

from pyspark.sql.functions import col
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml import Pipeline


spark = SparkSession\
        .builder\
        .appName("logistic regression Example")\
        .getOrCreate()

#sc = spark.SparkContext

train_file = "/home/zkpk/data/train.csv"
data = spark.read.csv(path=train_file, header='true', inferSchema='true')

drop_list = ['Dates', 'DayOfWeek', 'PdDistrict', 'Resolution', 'Address', 'X', 'Y']
data = data.select([column for column in data.columns if column not in drop_list])
data.show(5)

data.printSchema()


data.groupBy("Category") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

data.groupBy("Descript") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()


labelIndexer = StringIndexer().setInputCol("Category").setOutputCol("label")
data2 = labelIndexer.fit(data).transform(data)
data2.select('*').show()


tokenizer = Tokenizer(inputCol="Descript", outputCol="words")
wordsData = tokenizer.transform(data2)

# stop words
add_stopwords = ["http","https","amp","rt","t","c","the"] # standard stop words
stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(add_stopwords)
refined_wordsData = stopwordsRemover.transform(wordsData)


#label_stringIdx = StringIndexer(inputCol = "Category", outputCol = "label")

hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=2000)
featurizedData = hashingTF.transform(refined_wordsData)
idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

# set seed for reproducibility
(trainingData, testData) = rescaledData.randomSplit([0.7, 0.3], seed = 100)
print("Training Dataset Count: " + str(trainingData.count()))
print("Test Dataset Count: " + str(testData.count()))


rf = RandomForestClassifier(labelCol="label", \
                            featuresCol="features", \
                            numTrees = 100, \
                            maxDepth = 4, \
                            maxBins = 32)

# Train model with Training Data
rfModel = rf.fit(trainingData)
predictions = rfModel.transform(testData)
predictions.filter(predictions['prediction'] == 0) \
    .select("Descript","Category","probability","label","prediction") \
    .orderBy("probability", ascending=False) \
    .show(n = 10, truncate = 30)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
print(evaluator.evaluate(predictions))


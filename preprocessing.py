import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import Tokenizer,StopWordsRemover, CountVectorizer,IDF,StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vector
from pyspark.sql.functions import length
from pyspark.ml import Pipeline

from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
#from sklearn import linear_model
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
import numpy as np
import pyspark.sql.functions as F

sc=SparkContext.getOrCreate()
sc.setLogLevel('OFF')
ssc = StreamingContext(sc,1)
spark=SparkSession(sc)

def tokenizer_func(df):
	tokenizer = Tokenizer(inputCol="feature1", outputCol="message_token")
	countTokens = udf(lambda message_token: len(message_token), IntegerType())
	tokenized = tokenizer.transform(df)
	tokenized.select("feature1", "message_token")\
	.withColumn("tokens", countTokens(col("message_token"))).show(truncate=False)


def dataclean(df):
	df = df.withColumn('length',length(df['feature1']))
	tokenizer = tokenizer(inputCol="feature1", outputCol="token_text")
	stopremove = StopWordsRemover(inputCol='token_text',outputCol='stop_tokens')
	count_vec = CountVectorizer(inputCol='stop_tokens',outputCol='c_vec')
	idf = IDF(inputCol="c_vec", outputCol="tf_idf")
	ham_spam_to_num = StringIndexer(inputCol='feature2',outputCol='label')
	clean_up = VectorAssembler(inputCols=['tf_idf','length'],outputCol='features')
	data_prep_pipe = Pipeline(stages=[ham_spam_to_num,tokenizer,stopremove,count_vec,idf,clean_up])
	cleaner = data_prep_pipe.fit(df)
	clean_data = cleaner.transform(df)
	return clean_data
	


rec = ssc.socketTextStream('localhost',6100)

        
def readstream(rdd):
        if(len(rdd.collect())>0):
          df=spark.read.json(rdd)
          tokenizer_func(df)
          cleaningdata=dataclean(df)
          df.show()
               
rec.foreachRDD(lambda x:readstream(x))

ssc.start()
ssc.awaitTermination()

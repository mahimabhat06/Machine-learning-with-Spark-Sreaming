from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col

sc=SparkContext.getOrCreate()
sc.setLogLevel('OFF')
ssc=StreamingContext(sc,1)
spark=SparkSession(sc)

try:
	rec=ssc.socketTextStream('localhost',6100)
except Exception as e:
	print(e)
	
def readstream(rdd):
	if(len(rdd.collect())>0):
		df=spark.read.json(rdd)
		df.show()

rec.foreachRDD(lambda x:readstream(x))

ssc.start()
ssc.awaitTermination()

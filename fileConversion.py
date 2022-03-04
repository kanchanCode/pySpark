import findspark
findspark.init()
import pyspark
# Create SparkSession and sparkcontext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark=SparkSession.builder\
      .master("local")\
      .appName('json_txt')\
      .getOrCreate()
input_df=spark.read.json('abc.json',multiLine=True)
input_df.printSchema()
input_df.show()
input_df.write.option("header","true").csv("ConvertedCSV")
input_df.write.parquet("ConvertedPARQUET")
input_df.select("fruit").write.format("text").mode("overwrite").save("ConvertedTEXT.txt")
#input_df.write.partitionBy("name","Education").format("avro").save("ConvertedAVRO)

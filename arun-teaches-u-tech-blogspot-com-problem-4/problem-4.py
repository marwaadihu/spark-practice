# pyspark --master yarn --conf spark.ui.port=12990 --packages com.databricks:spark-avro_2.10:2.0.1

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName('problem-4').setMaster('yarn-client')

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

orders = sqlContext.read.load('/user/saurinchauhan/anilagrawal/cloudera/problem5/avro/orders','com.databricks.spark.avro')

# save the data to hdfs using snappy compression as parquet file at /user/cloudera/problem5/parquet-snappy-compress
sqlContext.setConf('spark.sql.parquet.compression.codec','snappy')
orders.write.save('/user/saurinchauhan/anilagrawal/cloudera/problem5/parquet-snappy-compress','parquet')

# save the data to hdfs using gzip compression as text file at /user/cloudera/problem5/text-gzip-compress
ordersRDD = orders.rdd
ordersMap = ordersRDD.map(lambda line: str(line[0]) + "," + str(line[1]) + "," + str(line[2])+ "," + line[3])
ordersMap.saveAsTextFile('/user/saurinchauhan/anilagrawal/cloudera/problem5/text-gzip-compress','org.apache.hadoop.io.compress.GzipCodec')

# save the data to hdfs using no compression as sequence file at /user/cloudera/problem5/sequence
ordersMap.map(lambda line: (None, line)).saveAsSequenceFile('/user/saurinchauhan/anilagrawal/cloudera/problem5/sequence')

# save the data to hdfs using snappy compression as text file at /user/cloudera/problem5/text-snappy-compress
ordersMap.saveAsTextFile('/user/saurinchauhan/anilagrawal/cloudera/problem5/text-snappy-compress','org.apache.hadoop.io.compress.SnappyCodec')

sqlContext.setConf('spark.sql.parquet.compression.codec','snappy')
orders = sqlContext.read.load('/user/saurinchauhan/anilagrawal/cloudera/problem5/parquet-snappy-compress','parquet')

# save the data to hdfs using no compression as parquet file at /user/cloudera/problem5/parquet-no-compress
sqlContext.setConf('spark.sql.parquet.compression.codec','uncompressed')
orders.write.save('/user/saurinchauhan/anilagrawal/cloudera/problem5/parquet-no-compress','parquet')

# save the data to hdfs using snappy compression as avro file at /user/cloudera/problem5/avro-snappy
sqlContext.setConf('spark.sql.avro.compression.codec','snappy')
orders.write.save('/user/saurinchauhan/anilagrawal/cloudera/problem5/avro-snappy','com.databricks.spark.avro')

orders = sqlContext.read.load('/user/saurinchauhan/anilagrawal/cloudera/problem5/avro-snappy','com.databricks.spark.avro')

# save the data to hdfs using no compression as json file at /user/cloudera/problem5/json-no-compress
orders.toJSON().saveAsTextFile('/user/saurinchauhan/anilagrawal/cloudera/problem5/json-no-compress')

# save the data to hdfs using gzip compression as json file at /user/cloudera/problem5/json-gzip
orders.toJSON().saveAsTextFile('/user/saurinchauhan/anilagrawal/cloudera/problem5/json-gzip','org.apache.hadoop.io.compress.GzipCodec')

orders = sqlContext.read.load('/user/saurinchauhan/anilagrawal/cloudera/problem5/json-gzip','json')

# save the data to as comma separated text using gzip compression at   /user/cloudera/problem5/csv-gzip
orders.rdd.map(lambda line: (str(line[0])+","+str(line[1])+","+str(line[2])+","+line[3])).saveAsTextFile('/user/saurinchauhan/anilagrawal/cloudera/problem5/csv-gzip','org.apache.hadoop.io.compress.GzipCodec')

orders = sc.sequenceFile('/user/saurinchauhan/anilagrawal/cloudera/problem5/sequence','org.apache.hadoop.io.Text','org.apache.hadoop.io.Text')

ordersDF = orders.map(lambda line: tuple(line[1].split(','))).toDF()

sqlContext.setConf('spark.sql.parquet.compression.codec','uncompressed')
ordersDF.write.save('/user/saurinchauhan/anilagrawal/cloudera/problem5/orc','orc')
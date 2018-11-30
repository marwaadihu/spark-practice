# pyspark --master yarn --conf spark.ui.port=12990 --packages com.databricks:spark-avro_2.10:2.0.1

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName('problem1').setMaster('yarn-client')

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

sqlContext.setConf('spark.sql.avro.compression.codec','snappy')

ordersDF = sqlContext.read.load('/user/riddhiparkhiya/anilagrawal/cloudera/problem1/orders', 'com.databricks.spark.avro')

orderItemsDF = sqlContext.read.load('/user/riddhiparkhiya/anilagrawal/cloudera/problem1/order-items','com.databricks.spark.avro')

ordersDF.registerTempTable('orders')

orderItemsDF.registerTempTable('order_items')

resultDF = sqlContext.sql('\
    select to_date(from_unixtime(o.order_date/1000)) as order_date, o.order_status, sum(oi.order_item_subtotal) as total_amount, count(distinct o.order_id) as total_orders \
    from orders o \
    join order_items oi \
    on o.order_id == oi.order_item_order_id \
    group by o.order_date, o.order_status \
    order by o.order_date desc, o.order_status, total_amount desc,total_orders')

sqlContext.setConf('spark.sql.parquet.compression.codec','gzip')

resultDF.write.mode('overwrite').save('/user/riddhiparkhiya/anilagrawal/cloudera/problem1/result4a-gzip', "parquet")

sqlContext.setConf('spark.sql.parquet.compression.codec','snappy')

resultDF.write.mode('overwrite').save('/user/riddhiparkhiya/anilagrawal/cloudera/problem1/result4a-snappy','parquet')

sqlContext.setConf('spark.sql.csv.compression.codec','uncompressed')

resultDF.map(lambda line: str(line[0]) + "," + line[1] + "," + str(line[2]) +"," + str(line[3])).coalesce(1).saveAsTextFile('/user/riddhiparkhiya/anilagrawal/cloudera/problem1/result4a-csv')
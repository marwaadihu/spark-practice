# pyspark --master yarn --conf spark.ui.port=12990 --packages com.databricks:spark-avro_2.10:2.0.1

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('problem-2').setMaster('yarn-client')

sc = SparkContext(conf=conf)

products = sc.textFile('/user/riddhiparkhiya/anilagrawal/cloudera/problem2/products')

productsTuple = products.map(lambda line: tuple(line.split('|')))

productsDF = productsTuple.map(lambda line: (int(line[0]), int(line[1]), line[2], line[3], float(line[4]), line[5])).toDF(schema=['product_id', 'product_category_id', 'product_name', 'product_description', 'product_price', 'product_image'])

productsDF.registerTempTable('products')

resultDF = sqlContext.sql('\
select product_category_id, \
max(product_price) as max_price, \
min(product_price) as min_price, \
count(product_id) as total_products, \
avg(product_price) as average_price_per_product \
from products \
where product_price < 100 \
group by product_category_id \
order by product_category_id')

sqlContext.setConf('spark.sql.avro.compression.codec','snappy')

resultDF.write.mode('overwrite').save('/user/riddhiparkhiya/anilagrawal/cloudera/problem2/products/result-df','com.databricks.spark.avro')
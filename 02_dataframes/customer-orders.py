from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
import time

spark = SparkSession.builder.appName('customerOrders').getOrCreate()

start_time = time.time()
schema = (StructType([
            StructField('customerID', IntegerType(), True),
            StructField('itemID', IntegerType(), True),
            StructField('amount', FloatType(), True)
            ])
          )

sales_df = (spark.read.schema(schema).csv('../data/customer-orders.csv')
            .filter(F.col('amount').isNotNull())
            .groupBy('customerID')
            .agg(
                F.round(F.sum(F.col('amount')),2).alias('totalAmount')
                , F.count('*').alias('orderCount')
                )
            .withColumn('avgOrderAmount', F.round(F.col('totalAmount')/F.col('orderCount'), 2))
            .orderBy(F.col('totalAmount'), ascending=False)
            )

sales_df.show(5)
print(f'Total {time.time()-start_time:.2f} sec')

spark.stop()

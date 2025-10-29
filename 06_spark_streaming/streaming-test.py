from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkStreaming').getOrCreate()

streamDF = spark.readStream.text('logs')
countLines = streamDF.count()
query = countLines.writeStream.outputMode('complete').format('console').queryName('countLines').start()

query.awaitTermination()
spark.stop()

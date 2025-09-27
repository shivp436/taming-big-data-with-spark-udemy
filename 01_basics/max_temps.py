from pyspark import SparkConf, SparkContext
from datetime import datetime

# Optimized Spark Configuration
#conf = (SparkConf()
#        .setMaster('local[*]')  # Use all available cores
#       .setAppName('MinTemperature')
#       .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
#       .set("spark.sql.adaptive.enabled", "true")
#       .set("spark.sql.adaptive.coalescePartitions.enabled", "true"))

conf = SparkConf().setMaster('local').setAppName('MinTemperature')
sc = SparkContext(conf=conf)

# Read & Optimize Partitioning
rdd = sc.textFile('1800.csv', minPartitions=sc.defaultParallelism * 2)

def parse_lines(line):
    try:
        values = line.split(',')
        if len(values) < 4 or values[2] != 'TMAX':
            return None
        
        station_id = values[0]
        date_str = values[1]  
        temp = int(values[3])
        
        return (station_id, (date_str, temp))
    except (ValueError, IndexError):
        return None

def format_date(date_str):
    """Convert date only when needed for output"""
    return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')

result = (rdd
          .map(parse_lines)
          .filter(lambda x: x is not None)
          .reduceByKey(lambda x,y: x if x[1]>y[1] else y))

for station_id, (date, temp) in result.collect():
    date_formatted = format_date(date)
    print(f"Station: {station_id} => Max Temp: {temp}C on {date_formatted}")

sc.stop()

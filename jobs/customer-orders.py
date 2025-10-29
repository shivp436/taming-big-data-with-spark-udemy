from pyspark import SparkConf, SparkContext
import os

# Configure Spark to connect to the cluster
conf = SparkConf().setAppName('CustomerOrders')
conf.set("spark.master", "spark://spark-master:7077")
sc = SparkContext(conf=conf)

# Use the mounted volume path inside the container
file_path = "/opt/spark/work-dir/data/customer-orders.csv"

# Check if file exists
if not os.path.exists(file_path):
    print(f"ERROR: File not found at {file_path}")
    print("Available files in data directory:")
    data_dir = "/opt/spark/work-dir/data"
    if os.path.exists(data_dir):
        for f in os.listdir(data_dir):
            print(f"  - {f}")
    exit(1)

file = sc.textFile(file_path)

def parse_orders(line):
    content = line.split(',')
    customer_id = content[0]
    value = float(content[2])
    return (customer_id, value)

rdd = (file
       .map(parse_orders)
       .reduceByKey(lambda a,b: a+b)
       .sortBy(lambda x: x[1], ascending=False)
       )

t10_cust = rdd.take(10)

print(f"\n" + '+'*50)
print("Top 10 Customers by Total Amount Spent")
print('-'*50)

for cust in t10_cust:
    cust_id, amount = cust
    print(f"Customer-{cust_id:0>2} with ${round(amount, 2):.2f}")
print('+'*50 + f"\n")

sc.stop()

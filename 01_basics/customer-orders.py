from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('CustomerOrders')
sc = SparkContext(conf=conf)

file = sc.textFile('../data/customer-orders.csv')

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

print(f"\n", '+'*50)
print("Top 10 Customers by Total Amount Spent")
print('-'*50)

for cust in t10_cust:
    cust_id, amount = cust
    print(f"Customer-{cust_id:0>2} with ${round(amount, 2)}")
print('+'*50, f"\n")

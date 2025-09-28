import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

from pyspark.sql import SparkSession, functions as F
import pyspark.pandas as ps
import pandas as pd

# Initialize a Spark Session
spark = (SparkSession.builder
         .appName('PandasOnSpark')
         .config('spark.sql.ansi.enabled', 'false')
         .config('spark.executorEnv.PYARROW_IGNORE_TIMEZONE', '1')
         .config('spark.driverEnv.PYARROW_IGNORE_TIMEZONE', '1')
         .getOrCreate()
         )

data = {
        'id': [1, 2, 3, 4],
        'name': ['P1','P2','P3','P4'],
        'age': [10,20,30,40]
        }

psdf2 = ps.DataFrame(data)
# print(psdf2)

def sum_row(row) -> str:
    # _id, name, age = row
    return (f"hello world {row['name']} of age {row['age']}")

psdf2['summ'] = psdf2.apply(sum_row, axis='columns')
print(psdf2)

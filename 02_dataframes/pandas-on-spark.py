# Keep this at the top
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

# Dummy Data
data = {
        'id': [1, 2, 3, 4],
        'name': ['P1','P2','P3','P4'],
        'age': [10,20,30,40]
        }

# Create a Pandas DataFrame
pandas_df = pd.DataFrame(data)

print(f'\n=========== Pandas Dataframe: ===========')
print(pandas_df)


# Convert Pandas DF to Spark DF
spark_df = spark.createDataFrame(pandas_df).cache()

print(f'\n=========== Spark Dataframe: ===========')
spark_df.show()

print(f'\n=========== Spark Dataframe Schema: ===========')
spark_df.printSchema()

print(f'\n=========== Spark Dataframe Filter Function: ===========')
filtered_spark_df = spark_df.filter(spark_df.age > 20).cache()
filtered_spark_df.show()

print(f'\n===== Convert Filtered Spark DF Back To Pandas DF: =======')
filtered_pandas_df = filtered_spark_df.toPandas()
print(filtered_pandas_df)

print(f'\n=========== Pandas DF to PandasOnSpark DF: ===========')
ps_df = ps.DataFrame(pandas_df)
print(ps_df)

print(f'\n=========== Spark DF to PandasOnSpark DF: ===========')
ps2_df = ps.DataFrame(spark_df)
print(ps2_df)

print(f'\n======= Panda Like Operations on PandasOnSpark DF: ======')
ps_df['new_age'] = ps_df['age'] + 10
print(ps_df)

print(f"""\n
      KEEP THE ROW-WISE APPLY FIRST IN ORDER:
      - .APPLY WITH AXIS='COLUMNS' IS INEFFICIENT
      - IT WILL BREAK THE COMPUTE IF COMES AFTER COLUMN-WISE APPLY
      """)

print(f'\n======= Row Level Apply Operation on PandasOnSpark DF: ======')
def row_desc(row) -> str:
    return (f"{row['name']} is of age {row['age']}")

ps_df['desc'] = ps_df.apply(row_desc, axis='columns')
# axis='columns' takes a whole row as input
print(ps_df)

print(f"""\n
      THIS WILL COME AFTER ROW-WISE
      """)

print(f'\n======= Apply Operation on PandasOnSpark DF: ======')
def categorize_age(age) -> str:
    if age >= 50:
        return 'Fifties'
    elif age >= 40:
        return 'Forties'
    elif age >= 30:
        return 'Thirties'
    else:
        return 'Twenties'

ps_df['age_group'] = ps_df['age'].apply(categorize_age)
print(ps_df)

print(f'\n=========== PandasOnSpark DF to Spark DF: ===========')
new_spark_df = ps_df.to_spark(index_col="__index_level_0__")
#new_spark_df = ps_df.to_spark(index_col=None)
# or ps_sd.reset_index()
new_spark_df.show()

# Stop the Spark Engine
spark.stop()

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

url = "jdbc:postgresql://<...>.ap-southeast-1.rds.amazonaws.com:<PORT>/<DB_NAME>"
properties = {
    "user" : "...",
    "password" : "..."
}

df = spark.read.jdbc(url=url, table="stock_data_fortest", properties=properties)

def add_sma_to_dataframe(df, column_name, window_size):
    # Define window specification for SMA
    window_spec_sma = Window.partitionBy("stockname").orderBy("datetime")  # replace with the name of your date or sequence column
    window_spec_sma = window_spec_sma.rowsBetween(-(window_size - 1), 0)
    
    # Window specification to get row number
    window_spec_rownum = Window.partitionBy("stockname").orderBy("datetime")

    # Calculate row number
    df = df.withColumn("row_number", F.row_number().over(window_spec_rownum))

    # Calculate SMA based on condition
    sma_column_name = f"sma_{column_name}"
    df = df.withColumn(sma_column_name, F.when(F.col("row_number") >= window_size, F.avg(column_name).over(window_spec_sma)).otherwise(None))
    
    # Drop rows without SMA values
    df = df.filter(F.col(sma_column_name).isNotNull())
    
    # Drop row number column
    df = df.drop("row_number")
    
    return df

# Add SMA for high_price attribute
window_size = 5  # Adjust this to your desired SMA window size
df = add_sma_to_dataframe(df, "high_price", window_size)

# df.printSchema()
# df.show(5)

output_path = "s3://<bucket>/testf"
df.write.csv(output_path, mode="overwrite", header=True)

job.commit()
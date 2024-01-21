from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap
from pyspark.sql.types import *
from pyspark.sql.functions import col, round
from google.cloud import storage
from datetime import datetime

def run_pyspark():
    bucket_name = 'data-bucket-crypto'
    directory_path = 'raw-data/'

    ### Get the CSV Name
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=directory_path))
    csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
    full_paths = [f'gs://{bucket_name}/{file}' for file in csv_files]
    latest_file = max(full_paths, key=lambda file: bucket.get_blob(file[len(f'gs://{bucket_name}/'):]).time_created)
    latest_file_name = latest_file.split("/")[-1]


    current_date = datetime.now().strftime('%Y-%m-%d')

    file_path = f'gs://data-bucket-crypto/raw-data/{latest_file_name}'
    output_path = f'gs://data-bucket-crypto/transformed-data/'

    ## Create Session:
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.appName('Transform').getOrCreate()

    # Import DF
    no_schema_df = spark.read.csv(file_path, header=True)
    base_columns = no_schema_df.columns

    # Create Schema
    base_fields = StructType([
            StructField("Asset", StringType(), True)])

    date_fields = [StructField(date, FloatType(), True) for date in base_columns[1:]]

    for field in date_fields:
        base_fields.add(field)

    schema = base_fields

    # Import DF with schema
    df = spark.read.csv(file_path, header=True, schema=schema)

    # Transform DF
    Value = base_columns[-1]
    Yesterday_value = base_columns[-2]
    Week_ago = base_columns[-7]
    Month_ago = base_columns[-29]

    df = df.withColumn("Day", round(((col(Value) - col(Yesterday_value)) / col(Yesterday_value)) * 100, 2)
                ).withColumn('Value', col(Value)
                ).withColumn('Week', round(((col(Value) - col(Week_ago)) / col(Week_ago)) * 100, 2)
                ).withColumn('Month', round(((col(Value) - col(Month_ago)) / col(Month_ago)) * 100, 2))

    df = df.select('Asset', 'Value', 'Day', 'Week', 'Month')

    df = df.withColumn("Day", df["Day"].cast(FloatType())
        ).withColumn("Week", df["Week"].cast(FloatType())
        ).withColumn("Month", df["Month"].cast(FloatType())
        ).withColumn("Asset", initcap("Asset"))

    df.write.mode('overwrite').parquet(output_path)

if __name__ == "__main__":
    run_pyspark()
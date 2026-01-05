import os
from pyspark.sql import SparkSession

# MinIO configuration from environment variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "miniopassword123")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")

# Iceberg REST catalog configuration
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://lakehouse/warehouse")

def create_spark_session():
    """Create Spark session with Iceberg support."""
    return SparkSession.builder \
        .appName("Load Bronze to Iceberg") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", ICEBERG_REST_URI) \
        .config("spark.sql.catalog.iceberg.warehouse", ICEBERG_WAREHOUSE) \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.io.ResolvingFileIO") \
        .config("spark.sql.catalog.iceberg.s3.endpoint", MINIO_ENDPOINT) \
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
        .config("spark.sql.catalog.iceberg.s3.access-key-id", MINIO_ACCESS_KEY) \
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", MINIO_SECRET_KEY) \
        .config("spark.sql.catalog.iceberg.s3.region", MINIO_REGION) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def main():
    
    spark = create_spark_session()
    
    # Read from MinIO 
    base_path = "s3a://lol-bronze"
    
    tables = {
        'ladder': f"{base_path}/ladder/",
        'match_ids': f"{base_path}/match_ids/",
        'matches_participants': f"{base_path}/matches_participants/",
        'matches_teams': f"{base_path}/matches_teams/"
    }
    
    for table_name, path in tables.items():
        try:
            # Read parquet
            df = spark.read.parquet(path)
            print(f"Read {df.count()} rows from {path}")
            
            # Write to Iceberg
            iceberg_table = f"iceberg.bronze.{table_name}"
            df.writeTo(iceberg_table).createOrReplace()
            print(f"Created {iceberg_table}")
            
        except Exception as e:
            print(f"Error with {table_name}: {e}")
    
    spark.sql("SHOW TABLES IN iceberg.bronze").show()
    
    spark.stop()
    print("\nDone!")

if __name__ == '__main__':
    main()

from pyspark.sql import SparkSession
import os

def create_spark_session(app_name="LakeHouse"):
    """Create Spark session with Iceberg REST catalog and S3A filesystem"""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", 
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
                "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", 
                os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181"))
        .config("spark.sql.catalog.iceberg.warehouse", "s3://lakehouse/warehouse")
        .config("spark.sql.defaultCatalog", "iceberg")
        .config("fs.s3a.endpoint", os.getenv("AWS_S3_ENDPOINT", "http://minio:9000"))
        .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "miniopassword123"))
        .config("fs.s3a.path.style.access", "true")
        .config("fs.s3a.attempts.maximum", "1")
        .config("fs.s3a.connection.establish.timeout", "5000")
        .config("fs.s3a.connection.timeout", "10000")
        .getOrCreate()
    )

if __name__ == "__main__":
    spark = create_spark_session()
    print(f"Spark session created: {spark.version}")

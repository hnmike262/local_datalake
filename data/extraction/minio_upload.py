from pathlib import Path
from minio import Minio
from minio.error import S3Error
from config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, 
    MINIO_BUCKET, BRONZE_DIR, PLATFORM
)

def create_minio_client() -> Minio:
    """Create MinIO client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False 
    )

def create_bucket(client: Minio, bucket_name: str):
    """Create bucket if not exists."""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f" Created bucket: {bucket_name}")
    else:
        print(f" Bucket exists: {bucket_name}")

def upload_bronze_files(client: Minio):
    """Upload all parquet files from bronze/ to MinIO."""
    
    # Files to upload 
    files = [
        'ladder.parquet',
        'match_ids.parquet', 
        'matches_participants.parquet',
        'matches_teams.parquet'
    ]
    
    uploaded = 0
    for filename in files:
        local_path = BRONZE_DIR / filename
        if not local_path.exists():
            print(f" {filename} (not found)")
            continue
        
        # MinIO object path 
        table_name = filename.replace('.parquet', '')
        object_name = f"{table_name}/data.parquet"
        
        try:
            client.fput_object(
                bucket_name=MINIO_BUCKET,
                object_name=object_name,
                file_path=str(local_path),
                content_type='application/octet-stream'
            )
            print(f" Uploaded: {object_name}")
            uploaded += 1
        except S3Error as e:
            print(f" Failed {filename}: {e}")
    
    return uploaded

def main():    
    try:
        client = create_minio_client()
        
        # Test connection
        client.list_buckets()
        print("Connected to MinIO")
        
        # Ensure bucket exists
        create_bucket(client, MINIO_BUCKET)
        
        # Upload files
        uploaded = upload_bronze_files(client)
        
        print (f"\n {uploaded} files uploaded")
        
        
    except Exception as e:
        print(" MinIO is running...")


if __name__ == '__main__':
    main()

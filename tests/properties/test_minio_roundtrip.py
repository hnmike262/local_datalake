import boto3
import pytest
from hypothesis import given, strategies as st

@pytest.fixture
def minio_client():
    return boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='miniopassword123',
        region_name='us-east-1'
    )

@given(data=st.text(min_size=1, max_size=1000))
def test_minio_roundtrip(minio_client, data):
    """Property 1: MinIO Data Round-Trip Persistence"""
    bucket = 'test-roundtrip'
    key = 'test-file.txt'
    
    # Ensure bucket exists
    try:
        minio_client.create_bucket(Bucket=bucket)
    except:
        pass
    
    # Write
    minio_client.put_object(Bucket=bucket, Key=key, Body=data)
    
    # Read
    response = minio_client.get_object(Bucket=bucket, Key=key)
    retrieved_data = response['Body'].read().decode('utf-8')
    
    # Verify
    assert retrieved_data == data, f"Data mismatch: {retrieved_data} != {data}"

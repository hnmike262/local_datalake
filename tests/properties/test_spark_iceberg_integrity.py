import pytest
from hypothesis import given, strategies as st

def test_spark_available():
    """Property 2: Spark-Iceberg Availability Check"""
    # Check if Spark is accessible on port 8084
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('localhost', 8084))
    sock.close()
    
    # Result of 0 means connection successful
    assert result == 0, "Spark not available on port 8084"

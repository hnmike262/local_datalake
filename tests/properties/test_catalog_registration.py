import requests
import pytest
from hypothesis import given, strategies as st

ICEBERG_REST_URL = 'http://localhost:8181'

def test_catalog_connectivity():
    """Property 3: Catalog Registration Connectivity"""
    response = requests.get(f'{ICEBERG_REST_URL}/v1/config')
    assert response.status_code == 200
    assert 'version' in response.json()

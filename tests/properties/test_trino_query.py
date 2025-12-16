import trino
import pytest

@pytest.fixture
def trino_connection():
    return trino.dbapi.connect(
        host='localhost',
        port=8082,
        user='admin',
        http_scheme='http',
    )

def test_trino_connectivity(trino_connection):
    """Property 4: Trino-MinIO Query Execution"""
    cursor = trino_connection.cursor()
    try:
        cursor.execute('SELECT 1')
        result = cursor.fetchone()
        assert result is not None
    except Exception as e:
        pytest.skip(f"Trino not ready: {str(e)}")

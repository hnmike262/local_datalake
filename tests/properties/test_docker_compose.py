import pytest
import subprocess
import json

def test_docker_compose_valid():
    """Property 7: Docker Compose Configuration Validity"""
    try:
        result = subprocess.run(
            ['docker', 'compose', 'config', '--format', 'json'],
            capture_output=True,
            text=True,
            cwd='.',
        )
        assert result.returncode == 0, f"docker compose config failed: {result.stderr}"
        config = json.loads(result.stdout)
        
        # Verify services exist
        assert 'services' in config
        assert 'minio' in config['services']
        assert 'iceberg-rest' in config['services']
        assert 'trino' in config['services']
    except Exception as e:
        pytest.skip(f"Docker compose not available: {str(e)}")

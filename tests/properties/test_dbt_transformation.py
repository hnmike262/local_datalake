import pytest

def test_dbt_project_structure():
    """Property 5: dbt Transformation Structure"""
    import os
    
    dbt_project = 'dbt/dbt_project.yml'
    assert os.path.exists(dbt_project), f"dbt_project.yml not found at {dbt_project}"
    
    # Verify key files exist
    assert os.path.exists('dbt/profiles.yml'), "profiles.yml missing"
    assert os.path.exists('dbt/models/bronze'), "Bronze models dir missing"
    assert os.path.exists('dbt/models/silver'), "Silver models dir missing"
    assert os.path.exists('dbt/models/gold'), "Gold models dir missing"

"""
Test script to verify pipeline_dag_generator works correctly.

This script tests:
1. Config loading and validation
2. DAG generation from config
3. Pipeline structure verification
"""
import sys
import json
from pathlib import Path

# Add dags to Python path
sys.path.insert(0, str(Path(__file__).parent / 'dags'))

from pipeline_dag_generator import (
    load_pipeline_config,
    parse_date,
    parse_retry_delay,
)


def test_config_loading():
    """Test that we can load the example config."""
    print("=" * 60)
    print("TEST 1: Config Loading")
    print("=" * 60)
    
    config_path = Path(__file__).parent / 'dags' / 'configs' / 'example_gsheet_to_postgres.json'
    
    if not config_path.exists():
        print(f"✗ Config file not found: {config_path}")
        return False
    
    try:
        config = load_pipeline_config(str(config_path))
        print(f"✓ Config loaded successfully")
        print(f"  - DAG ID: {config['dag_id']}")
        print(f"  - Description: {config['description']}")
        print(f"  - Schedule: {config.get('schedule_interval')}")
        print(f"  - Source type: {config['source']['type']}")
        print(f"  - Destination type: {config['destination']['type']}")
        print(f"  - Transforms: {len(config.get('transforms', []))}")
        return True
    except Exception as e:
        print(f"✗ Error loading config: {e}")
        return False


def test_date_parsing():
    """Test date parsing utility."""
    print("\n" + "=" * 60)
    print("TEST 2: Date Parsing")
    print("=" * 60)
    
    test_cases = [
        "2025-01-01",
        "2025-01-01 10:30:00",
    ]
    
    all_passed = True
    for date_str in test_cases:
        try:
            dt = parse_date(date_str)
            print(f"✓ Parsed '{date_str}' -> {dt}")
        except Exception as e:
            print(f"✗ Failed to parse '{date_str}': {e}")
            all_passed = False
    
    return all_passed


def test_retry_delay_parsing():
    """Test retry delay parsing."""
    print("\n" + "=" * 60)
    print("TEST 3: Retry Delay Parsing")
    print("=" * 60)
    
    test_cases = [5, 10, 60]
    
    all_passed = True
    for minutes in test_cases:
        try:
            td = parse_retry_delay(minutes)
            print(f"✓ Parsed {minutes} minutes -> {td}")
        except Exception as e:
            print(f"✗ Failed to parse {minutes} minutes: {e}")
            all_passed = False
    
    return all_passed


def test_config_validation():
    """Test that config validation catches missing fields."""
    print("\n" + "=" * 60)
    print("TEST 4: Config Validation")
    print("=" * 60)
    
    import tempfile
    
    # Test with invalid config (missing required fields)
    invalid_config = {
        "dag_id": "test_dag",
        # Missing 'source' and 'destination'
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(invalid_config, f)
        temp_path = f.name
    
    try:
        config = load_pipeline_config(temp_path)
        print(f"✗ Validation should have failed for incomplete config")
        return False
    except ValueError as e:
        print(f"✓ Validation correctly rejected incomplete config")
        print(f"  - Error: {e}")
        return True
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False
    finally:
        Path(temp_path).unlink()


def test_dag_structure():
    """Test that DAG structure components are present."""
    print("\n" + "=" * 60)
    print("TEST 5: DAG Structure Verification")
    print("=" * 60)
    
    try:
        # Check that factories exist
        from factories.source_factory import SourceFactory
        from factories.transform_factory import TransformFactory
        from factories.destination_factory import DestinationFactory
        from dag_builder import DAGBuilder
        
        print("✓ All factory imports successful")
        print(f"  - SourceFactory: {len(SourceFactory._registry)} types registered")
        print(f"  - TransformFactory: {len(TransformFactory._registry)} types registered")
        print(f"  - DestinationFactory: {len(DestinationFactory._registry)} types registered")
        
        # Check DAGBuilder methods
        assert hasattr(DAGBuilder, 'extract_data'), "Missing extract_data method"
        assert hasattr(DAGBuilder, 'transform_data'), "Missing transform_data method"
        assert hasattr(DAGBuilder, 'load_data'), "Missing load_data method"
        assert hasattr(DAGBuilder, 'build_etl_pipeline'), "Missing build_etl_pipeline method"
        
        print("✓ DAGBuilder has all required methods")
        return True
        
    except Exception as e:
        print(f"✗ Error verifying DAG structure: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("PIPELINE GENERATOR TEST SUITE")
    print("=" * 60 + "\n")
    
    results = {
        "Config Loading": test_config_loading(),
        "Date Parsing": test_date_parsing(),
        "Retry Delay Parsing": test_retry_delay_parsing(),
        "Config Validation": test_config_validation(),
        "DAG Structure": test_dag_structure(),
    }
    
    print("\n" + "=" * 60)
    print("TEST RESULTS SUMMARY")
    print("=" * 60)
    
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    total = len(results)
    passed = sum(results.values())
    
    print("\n" + "-" * 60)
    print(f"Total: {passed}/{total} tests passed")
    print("-" * 60 + "\n")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print(f"⚠️  {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())

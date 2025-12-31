#!/usr/bin/env python3
"""Test script for validation transforms - no Airflow dependencies."""

import sys
import os

# Add dags directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dags'))

from tasks.transforms.validation_transforms import ValidateDuplicateIdsTransform


def test_no_duplicates():
    """Test validation with no duplicates - should pass."""
    print("\n" + "=" * 80)
    print("TEST 1: No duplicates (should pass)")
    print("=" * 80)
    
    config = {
        "id_columns": ["eeid"],
        "action": "fail",
        "log_duplicates": True
    }
    
    data = [
        {"eeid": "E01001", "name": "Alice", "dept": "HR"},
        {"eeid": "E01002", "name": "Bob", "dept": "IT"},
        {"eeid": "E01003", "name": "Charlie", "dept": "Sales"},
    ]
    
    transform = ValidateDuplicateIdsTransform(config)
    result = transform.transform(data)
    
    print(f"✓ Test passed: {len(result)} records validated")
    return True


def test_with_duplicates_fail():
    """Test validation with duplicates - should fail."""
    print("\n" + "=" * 80)
    print("TEST 2: Duplicates with action='fail' (should raise error)")
    print("=" * 80)
    
    config = {
        "id_columns": ["eeid"],
        "action": "fail",
        "log_duplicates": True
    }
    
    data = [
        {"eeid": "E01001", "name": "Alice", "dept": "HR"},
        {"eeid": "E01002", "name": "Bob", "dept": "IT"},
        {"eeid": "E01002", "name": "Bob Duplicate", "dept": "Sales"},  # Duplicate!
        {"eeid": "E01003", "name": "Charlie", "dept": "Sales"},
        {"eeid": "E01003", "name": "Charlie Duplicate", "dept": "HR"},  # Duplicate!
    ]
    
    transform = ValidateDuplicateIdsTransform(config)
    
    try:
        result = transform.transform(data)
        print("✗ Test failed: Expected ValueError but none was raised")
        return False
    except ValueError as e:
        print(f"✓ Test passed: Validation correctly caught duplicates")
        print(f"  Error message:\n{str(e)}")
        return True


def test_with_duplicates_warn():
    """Test validation with duplicates - should warn and continue."""
    print("\n" + "=" * 80)
    print("TEST 3: Duplicates with action='warn' (should warn and continue)")
    print("=" * 80)
    
    config = {
        "id_columns": ["eeid"],
        "action": "warn",
        "log_duplicates": True
    }
    
    data = [
        {"eeid": "E01001", "name": "Alice", "dept": "HR"},
        {"eeid": "E01002", "name": "Bob", "dept": "IT"},
        {"eeid": "E01002", "name": "Bob Duplicate", "dept": "Sales"},  # Duplicate!
    ]
    
    transform = ValidateDuplicateIdsTransform(config)
    result = transform.transform(data)
    
    if len(result) == len(data):
        print(f"✓ Test passed: All {len(result)} records returned with warning")
        return True
    else:
        print(f"✗ Test failed: Expected {len(data)} records, got {len(result)}")
        return False


def test_with_duplicates_deduplicate_first():
    """Test deduplication keeping first occurrence."""
    print("\n" + "=" * 80)
    print("TEST 4: Duplicates with action='deduplicate', keep='first'")
    print("=" * 80)
    
    config = {
        "id_columns": ["eeid"],
        "action": "deduplicate",
        "keep": "first",
        "log_duplicates": True
    }
    
    data = [
        {"eeid": "E01001", "name": "Alice", "dept": "HR"},
        {"eeid": "E01002", "name": "Bob FIRST", "dept": "IT"},
        {"eeid": "E01002", "name": "Bob SECOND", "dept": "Sales"},  # Duplicate!
        {"eeid": "E01003", "name": "Charlie", "dept": "Sales"},
    ]
    
    transform = ValidateDuplicateIdsTransform(config)
    result = transform.transform(data)
    
    if len(result) == 3:
        # Check that we kept the first Bob
        bob_record = [r for r in result if r["eeid"] == "E01002"][0]
        if bob_record["name"] == "Bob FIRST":
            print(f"✓ Test passed: Kept first occurrence (3 unique records)")
            return True
        else:
            print(f"✗ Test failed: Expected 'Bob FIRST', got '{bob_record['name']}'")
            return False
    else:
        print(f"✗ Test failed: Expected 3 records, got {len(result)}")
        return False


def test_with_duplicates_deduplicate_last():
    """Test deduplication keeping last occurrence."""
    print("\n" + "=" * 80)
    print("TEST 5: Duplicates with action='deduplicate', keep='last'")
    print("=" * 80)
    
    config = {
        "id_columns": ["eeid"],
        "action": "deduplicate",
        "keep": "last",
        "log_duplicates": True
    }
    
    data = [
        {"eeid": "E01001", "name": "Alice", "dept": "HR"},
        {"eeid": "E01002", "name": "Bob FIRST", "dept": "IT"},
        {"eeid": "E01002", "name": "Bob LAST", "dept": "Sales"},  # Duplicate!
        {"eeid": "E01003", "name": "Charlie", "dept": "Sales"},
    ]
    
    transform = ValidateDuplicateIdsTransform(config)
    result = transform.transform(data)
    
    if len(result) == 3:
        # Check that we kept the last Bob
        bob_record = [r for r in result if r["eeid"] == "E01002"][0]
        if bob_record["name"] == "Bob LAST":
            print(f"✓ Test passed: Kept last occurrence (3 unique records)")
            return True
        else:
            print(f"✗ Test failed: Expected 'Bob LAST', got '{bob_record['name']}'")
            return False
    else:
        print(f"✗ Test failed: Expected 3 records, got {len(result)}")
        return False


def test_multi_column_ids():
    """Test validation with composite ID columns."""
    print("\n" + "=" * 80)
    print("TEST 6: Multi-column IDs (composite key)")
    print("=" * 80)
    
    config = {
        "id_columns": ["employee_id", "department_id"],
        "action": "fail",
        "log_duplicates": True
    }
    
    data = [
        {"employee_id": "E001", "department_id": "D001", "name": "Alice"},
        {"employee_id": "E001", "department_id": "D002", "name": "Alice in Dept 2"},  # Different dept, OK
        {"employee_id": "E002", "department_id": "D001", "name": "Bob"},
    ]
    
    transform = ValidateDuplicateIdsTransform(config)
    result = transform.transform(data)
    
    print(f"✓ Test passed: {len(result)} records with composite keys validated")
    return True


def main():
    """Run all tests."""
    print("\n" + "=" * 80)
    print("VALIDATION TRANSFORM TEST SUITE")
    print("=" * 80)
    
    tests = [
        test_no_duplicates,
        test_with_duplicates_fail,
        test_with_duplicates_warn,
        test_with_duplicates_deduplicate_first,
        test_with_duplicates_deduplicate_last,
        test_multi_column_ids,
    ]
    
    results = []
    for test_func in tests:
        try:
            passed = test_func()
            results.append((test_func.__name__, passed))
        except Exception as e:
            print(f"\n✗ Test {test_func.__name__} raised unexpected exception:")
            print(f"  {type(e).__name__}: {e}")
            results.append((test_func.__name__, False))
    
    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    
    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)
    
    for test_name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"  {status}: {test_name}")
    
    print(f"\nTotal: {passed_count}/{total_count} tests passed")
    
    if passed_count == total_count:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n❌ {total_count - passed_count} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())

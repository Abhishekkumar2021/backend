"""
Manual Test Script for PostgreSQL Connectors
Run this to verify connectors work before integrating with API
"""
import sys
import json
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from app.connectors.postgresql import PostgreSQLSource, PostgreSQLDestination


def test_source_connection():
    """Test PostgreSQL source connection"""
    print("\n" + "="*60)
    print("TEST 1: PostgreSQL Source Connection")
    print("="*60)
    
    config = {
        "host": "localhost",
        "port": 5432,
        "database": "data_agent_system",  # Change to your database
        "user": "postgres",      # Change to your username
        "password": "postgres",  # Change to your password
        "schema": "public"
    }
    
    source = PostgreSQLSource(config)
    
    # Test connection
    print("\n📡 Testing connection...")
    result = source.test_connection()
    print(f"   Success: {result.success}")
    print(f"   Message: {result.message}")
    print(f"   Metadata: {result.metadata}")
    
    if not result.success:
        print("\n❌ Connection test failed. Check your config.")
        return False
    
    return True


def test_schema_discovery():
    """Test schema discovery"""
    print("\n" + "="*60)
    print("TEST 2: Schema Discovery")
    print("="*60)
    
    config = {
        "host": "localhost",
        "port": 5432,
        "database": "data_agent_system",
        "user": "postgres",
        "password": "postgres",
        "schema": "public"
    }
    
    source = PostgreSQLSource(config)
    
    print("\n🔍 Discovering schema...")
    schema = source.discover_schema()
    
    print(f"\n✅ Found {len(schema.tables)} tables")
    print(f"   PostgreSQL Version: {schema.version}")
    
    for table in schema.tables[:5]:  # Show first 5 tables
        print(f"\n   📊 Table: {table.name}")
        print(f"      Rows: {table.row_count:,}")
        print(f"      Columns: {len(table.columns)}")
        
        for col in table.columns[:3]:  # Show first 3 columns
            pk = "🔑 " if col.primary_key else "   "
            fk = f" → {col.foreign_key}" if col.foreign_key else ""
            print(f"         {pk}{col.name} ({col.data_type.value}){fk}")
    
    return True


def test_data_extraction():
    """Test data extraction from a table"""
    print("\n" + "="*60)
    print("TEST 3: Data Extraction")
    print("="*60)
    
    config = {
        "host": "localhost",
        "port": 5432,
        "database": "data_agent_system",
        "user": "postgres",
        "password": "postgres",
        "schema": "public"
    }
    
    # First, discover tables
    source = PostgreSQLSource(config)
    schema = source.discover_schema()
    
    if not schema.tables:
        print("\n⚠️  No tables found in database")
        return False
    
    # Pick first table
    test_table = schema.tables[0].name
    print(f"\n📤 Extracting data from table: {test_table}")
    
    # Get record count
    count = source.get_record_count(test_table)
    print(f"   Total records: {count:,}")
    
    # Read first 5 records
    print("\n   Sample records:")
    records = []
    for i, record in enumerate(source.read(test_table)):
        if i >= 5:
            break
        records.append(record)
        print(f"      {i+1}. {json.dumps(record.data, default=str, indent=8)}")
    
    print(f"\n✅ Successfully read {len(records)} records")
    
    return True


def test_destination_connection():
    """Test PostgreSQL destination connection"""
    print("\n" + "="*60)
    print("TEST 4: PostgreSQL Destination Connection")
    print("="*60)
    
    config = {
        "host": "localhost",
        "port": 5432,
        "database": "data_agent_system",
        "user": "postgres",
        "password": "postgres",
        "schema": "public",
        "write_mode": "insert"
    }
    
    destination = PostgreSQLDestination(config)
    
    # Test connection
    print("\n📡 Testing connection...")
    result = destination.test_connection()
    print(f"   Success: {result.success}")
    print(f"   Message: {result.message}")
    
    if not result.success:
        print("\n❌ Connection test failed. Check your config.")
        return False
    
    return True


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("🧪 PostgreSQL Connector Test Suite")
    print("="*60)
    print("\n⚠️  Make sure you have:")
    print("   1. PostgreSQL running locally")
    print("   2. Updated connection config in this script")
    print("   3. At least one table in your database")
    
    input("\nPress Enter to continue...")
    
    try:
        # Run tests
        tests = [
            test_source_connection,
            test_schema_discovery,
            test_data_extraction,
            test_destination_connection
        ]
        
        results = []
        for test in tests:
            try:
                result = test()
                results.append(result)
            except Exception as e:
                print(f"\n❌ Test failed with error: {str(e)}")
                import traceback
                traceback.print_exc()
                results.append(False)
        
        # Summary
        print("\n" + "="*60)
        print("📊 TEST SUMMARY")
        print("="*60)
        passed = sum(results)
        total = len(results)
        print(f"   Passed: {passed}/{total}")
        
        if passed == total:
            print("\n✅ All tests passed! PostgreSQL connector is ready.")
        else:
            print("\n⚠️  Some tests failed. Check errors above.")
    
    except KeyboardInterrupt:
        print("\n\n⏹️  Tests interrupted by user")


if __name__ == "__main__":
    main()
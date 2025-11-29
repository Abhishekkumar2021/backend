"""
Test Script for File-Based Connectors
Tests SQLite, File System, and S3 connectors
"""
import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from app.connectors.sqlite import SQLiteSource
from app.connectors.filesystem import FileSystemDestination, FileSystemSource
from app.connectors.base import Record


def setup_test_sqlite_db():
    """Create a test SQLite database"""
    db_path = Path("test_data.db")
    
    # Remove existing
    if db_path.exists():
        db_path.unlink()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create test table
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            created_at TEXT
        )
    """)
    
    # Insert test data
    test_data = [
        (1, "Alice Johnson", "alice@example.com", 30, "2024-01-15"),
        (2, "Bob Smith", "bob@example.com", 25, "2024-02-20"),
        (3, "Charlie Brown", "charlie@example.com", 35, "2024-03-10"),
        (4, "Diana Prince", "diana@example.com", 28, "2024-04-05"),
        (5, "Eve Adams", "eve@example.com", 32, "2024-05-12")
    ]
    
    cursor.executemany(
        "INSERT INTO users VALUES (?, ?, ?, ?, ?)",
        test_data
    )
    
    # Create another table with foreign key
    cursor.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product TEXT,
            amount REAL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    
    orders_data = [
        (1, 1, "Laptop", 999.99),
        (2, 1, "Mouse", 29.99),
        (3, 2, "Keyboard", 79.99),
        (4, 3, "Monitor", 299.99),
        (5, 4, "Headphones", 149.99)
    ]
    
    cursor.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?)",
        orders_data
    )
    
    conn.commit()
    conn.close()
    
    print(f"✅ Created test database: {db_path}")
    return db_path


def test_sqlite_source():
    """Test SQLite source connector"""
    print("\n" + "="*60)
    print("TEST 1: SQLite Source Connector")
    print("="*60)
    
    db_path = setup_test_sqlite_db()
    
    config = {
        "database_path": str(db_path),
        "batch_size": 100
    }
    
    source = SQLiteSource(config)
    
    # Test connection
    print("\n📡 Testing connection...")
    result = source.test_connection()
    print(f"   Success: {result.success}")
    print(f"   Message: {result.message}")
    print(f"   Metadata: {result.metadata}")
    
    if not result.success:
        return False
    
    # Discover schema
    print("\n🔍 Discovering schema...")
    schema = source.discover_schema()
    print(f"   Found {len(schema.tables)} tables")
    print(f"   Version: {schema.version}")
    
    for table in schema.tables:
        print(f"\n   📊 Table: {table.name}")
        print(f"      Rows: {table.row_count}")
        print(f"      Columns: {len(table.columns)}")
        
        for col in table.columns[:3]:
            pk = "🔑 " if col.primary_key else "   "
            fk = f" → {col.foreign_key}" if col.foreign_key else ""
            print(f"         {pk}{col.name} ({col.data_type.value}){fk}")
    
    # Read data
    print("\n📤 Reading data from 'users' table...")
    records = list(source.read("users"))
    print(f"   Read {len(records)} records")
    
    for i, record in enumerate(records[:3], 1):
        print(f"      {i}. {record.data}")
    
    # Cleanup
    db_path.unlink()
    
    return True


def test_filesystem_destination():
    """Test file system destination connector"""
    print("\n" + "="*60)
    print("TEST 2: File System Destination")
    print("="*60)
    
    output_dir = Path("test_output")
    output_dir.mkdir(exist_ok=True)
    
    # Test data
    test_records = [
        Record(stream="customers", data={
            "id": 1,
            "name": "John Doe",
            "email": "john@example.com",
            "balance": 1500.50
        }),
        Record(stream="customers", data={
            "id": 2,
            "name": "Jane Smith",
            "email": "jane@example.com",
            "balance": 2300.75
        }),
        Record(stream="customers", data={
            "id": 3,
            "name": "Bob Wilson",
            "email": "bob@example.com",
            "balance": 890.25
        })
    ]
    
    formats = ['parquet', 'json', 'jsonl', 'csv', 'avro']
    
    for fmt in formats:
        print(f"\n📝 Testing {fmt.upper()} format...")
        
        config = {
            "base_path": str(output_dir),
            "format": fmt,
            "compression": "snappy" if fmt == "parquet" else "none"
        }
        
        destination = FileSystemDestination(config)
        
        # Test connection
        result = destination.test_connection()
        if not result.success:
            print(f"   ❌ Test failed: {result.message}")
            continue
        
        # Write data
        try:
            count = destination.write(iter(test_records))
            print(f"   ✅ Wrote {count} records")
            
            # Check file exists
            files = list((output_dir / "customers").glob(f"*.{fmt}"))
            if files:
                file_size = files[0].stat().st_size
                print(f"   📁 File created: {files[0].name} ({file_size} bytes)")
        except Exception as e:
            print(f"   ❌ Write failed: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print(f"\n✅ Output files saved to: {output_dir}")
    
    return True


def test_filesystem_source():
    """Test file system source connector"""
    print("\n" + "="*60)
    print("TEST 3: File System Source")
    print("="*60)
    
    output_dir = Path("test_output")
    
    if not output_dir.exists():
        print("   ⚠️  No test data found. Run destination test first.")
        return False
    
    formats = ['parquet', 'json', 'csv']
    
    for fmt in formats:
        print(f"\n📖 Reading {fmt.upper()} files...")
        
        # Find test file
        test_files = list((output_dir / "customers").glob(f"*.{fmt}"))
        if not test_files:
            print(f"   ⚠️  No {fmt} files found")
            continue
        
        test_file = test_files[0]
        
        config = {
            "file_path": str(test_file),
            "format": fmt
        }
        
        source = FileSystemSource(config)
        
        # Test connection
        result = source.test_connection()
        if not result.success:
            print(f"   ❌ Test failed: {result.message}")
            continue
        
        # Read data
        try:
            records = list(source.read("customers"))
            print(f"   ✅ Read {len(records)} records")
            
            if records:
                print(f"   Sample: {records[0].data}")
        except Exception as e:
            print(f"   ❌ Read failed: {str(e)}")
    
    return True


def test_round_trip():
    """Test complete pipeline: SQLite → File System"""
    print("\n" + "="*60)
    print("TEST 4: Round Trip (SQLite → File System)")
    print("="*60)
    
    # Setup SQLite source
    db_path = setup_test_sqlite_db()
    
    source_config = {
        "database_path": str(db_path)
    }
    
    source = SQLiteSource(source_config)
    
    # Setup File System destination
    output_dir = Path("test_output_roundtrip")
    output_dir.mkdir(exist_ok=True)
    
    dest_config = {
        "base_path": str(output_dir),
        "format": "parquet",
        "compression": "snappy"
    }
    
    destination = FileSystemDestination(dest_config)
    
    # Extract from SQLite
    print("\n📤 Extracting from SQLite...")
    records = source.read("users")
    
    # Load to File System
    print("📥 Loading to File System (Parquet)...")
    count = destination.write(records)
    print(f"✅ Wrote {count} records")
    
    # Verify
    output_file = list((output_dir / "users").glob("*.parquet"))[0]
    print(f"📁 Output file: {output_file}")
    print(f"   Size: {output_file.stat().st_size} bytes")
    
    # Read back to verify
    verify_config = {
        "file_path": str(output_file),
        "format": "parquet"
    }
    
    verify_source = FileSystemSource(verify_config)
    verify_records = list(verify_source.read("users"))
    
    print(f"✅ Verified: Read back {len(verify_records)} records")
    
    # Cleanup
    db_path.unlink()
    
    return True


def test_format_comparison():
    """Compare file sizes across formats"""
    print("\n" + "="*60)
    print("TEST 5: Format Size Comparison")
    print("="*60)
    
    output_dir = Path("test_output")
    
    if not output_dir.exists():
        print("   ⚠️  No test data found.")
        return False
    
    print("\n📊 File Size Comparison:")
    print(f"{'Format':<10} {'Size (bytes)':<15} {'Compression'}")
    print("-" * 50)
    
    formats = {
        'parquet': 'snappy',
        'json': 'none',
        'jsonl': 'none',
        'csv': 'none',
        'avro': 'none'
    }
    
    sizes = []
    
    for fmt, compression in formats.items():
        files = list((output_dir / "customers").glob(f"*.{fmt}"))
        if files:
            size = files[0].stat().st_size
            sizes.append((fmt, size, compression))
            print(f"{fmt:<10} {size:<15} {compression}")
    
    if sizes:
        smallest = min(sizes, key=lambda x: x[1])
        print(f"\n🏆 Most efficient: {smallest[0].upper()} ({smallest[1]} bytes)")
    
    return True


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("🧪 File-Based Connectors Test Suite")
    print("="*60)
    print("\n📋 Testing:")
    print("   1. SQLite Source")
    print("   2. File System Destination (Parquet, JSON, CSV, JSONL, Avro)")
    print("   3. File System Source")
    print("   4. Round Trip (SQLite → File System)")
    print("   5. Format Comparison")
    
    input("\nPress Enter to continue...")
    
    try:
        tests = [
            test_sqlite_source,
            test_filesystem_destination,
            test_filesystem_source,
            test_round_trip,
            test_format_comparison
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
            print("\n✅ All tests passed! File connectors are ready.")
        else:
            print("\n⚠️  Some tests failed. Check errors above.")
    
    except KeyboardInterrupt:
        print("\n\n⏹️  Tests interrupted by user")


if __name__ == "__main__":
    main()
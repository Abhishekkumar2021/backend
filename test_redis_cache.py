"""
Redis Cache Test Script
Verify cache is working correctly
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from app.services.cache import get_cache


def test_cache_connection():
    """Test Redis connection"""
    print("\n" + "="*60)
    print("TEST 1: Redis Connection")
    print("="*60)
    
    cache = get_cache()
    
    if cache.is_available():
        print("✅ Redis is connected and responding")
        return True
    else:
        print("❌ Redis is not available")
        print("   Make sure Redis is running: redis-server")
        return False


def test_basic_operations():
    """Test basic cache operations"""
    print("\n" + "="*60)
    print("TEST 2: Basic Cache Operations")
    print("="*60)
    
    cache = get_cache()
    
    # Test SET
    print("\n📝 Testing SET operation...")
    success = cache.set("test_key", {"foo": "bar", "number": 42})
    print(f"   Set result: {'✅ Success' if success else '❌ Failed'}")
    
    # Test GET
    print("\n📖 Testing GET operation...")
    value = cache.get("test_key")
    print(f"   Get result: {value}")
    
    if value == {"foo": "bar", "number": 42}:
        print("   ✅ Data matches")
    else:
        print("   ❌ Data mismatch")
        return False
    
    # Test DELETE
    print("\n🗑️  Testing DELETE operation...")
    success = cache.delete("test_key")
    print(f"   Delete result: {'✅ Success' if success else '❌ Failed'}")
    
    # Verify deletion
    value = cache.get("test_key")
    if value is None:
        print("   ✅ Key deleted successfully")
    else:
        print("   ❌ Key still exists")
        return False
    
    return True


def test_connection_caching():
    """Test connection-specific caching"""
    print("\n" + "="*60)
    print("TEST 3: Connection Caching")
    print("="*60)
    
    cache = get_cache()
    
    test_connection_id = 999
    
    # Test schema caching
    print("\n📊 Testing schema cache...")
    test_schema = {
        "tables": [
            {
                "name": "users",
                "columns": [
                    {"name": "id", "type": "integer"},
                    {"name": "email", "type": "string"}
                ]
            }
        ],
        "version": "PostgreSQL 15.0"
    }
    
    cache.set_schema(test_connection_id, test_schema)
    retrieved = cache.get_schema(test_connection_id)
    
    if retrieved == test_schema:
        print("   ✅ Schema caching works")
    else:
        print("   ❌ Schema caching failed")
        return False
    
    # Test config caching
    print("\n🔐 Testing config cache...")
    test_config = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db"
    }
    
    cache.set_config(test_connection_id, test_config)
    retrieved = cache.get_config(test_connection_id)
    
    if retrieved == test_config:
        print("   ✅ Config caching works")
    else:
        print("   ❌ Config caching failed")
        return False
    
    # Test test result caching
    print("\n🧪 Testing test result cache...")
    test_result = {
        "success": True,
        "message": "Connection successful",
        "tested_at": "2024-01-01T00:00:00"
    }
    
    cache.set_test_result(test_connection_id, test_result)
    retrieved = cache.get_test_result(test_connection_id)
    
    if retrieved == test_result:
        print("   ✅ Test result caching works")
    else:
        print("   ❌ Test result caching failed")
        return False
    
    # Test invalidation
    print("\n🗑️  Testing cache invalidation...")
    cache.invalidate_connection(test_connection_id)
    
    schema = cache.get_schema(test_connection_id)
    config = cache.get_config(test_connection_id)
    test_res = cache.get_test_result(test_connection_id)
    
    if schema is None and config is None and test_res is None:
        print("   ✅ Cache invalidation works")
    else:
        print("   ❌ Cache invalidation failed")
        return False
    
    return True


def test_cache_stats():
    """Test cache statistics"""
    print("\n" + "="*60)
    print("TEST 4: Cache Statistics")
    print("="*60)
    
    cache = get_cache()
    
    # Add some test data
    for i in range(5):
        cache.set(f"test_{i}", {"value": i})
    
    stats = cache.get_stats()
    
    print(f"\n📊 Cache Stats:")
    print(f"   Available: {stats.get('available')}")
    print(f"   Connected Clients: {stats.get('connected_clients')}")
    print(f"   Memory Used: {stats.get('used_memory_human')}")
    print(f"   Total Keys: {stats.get('total_keys')}")
    print(f"   Keys by Type: {stats.get('keys_by_type')}")
    
    # Cleanup
    cache.clear_all()
    
    return stats.get('available', False)


def test_ttl():
    """Test TTL expiration"""
    print("\n" + "="*60)
    print("TEST 5: TTL (Time To Live)")
    print("="*60)
    
    cache = get_cache()
    
    print("\n⏱️  Setting key with 2 second TTL...")
    cache.set("ttl_test", {"expires": "soon"}, ttl=2)
    
    print("   Checking immediately...")
    value = cache.get("ttl_test")
    if value:
        print("   ✅ Key exists")
    else:
        print("   ❌ Key not found")
        return False
    
    print("   Waiting 3 seconds...")
    import time
    time.sleep(3)
    
    print("   Checking after expiration...")
    value = cache.get("ttl_test")
    if value is None:
        print("   ✅ Key expired as expected")
        return True
    else:
        print("   ❌ Key still exists (TTL failed)")
        return False


def main():
    """Run all cache tests"""
    print("\n" + "="*60)
    print("🧪 Redis Cache Test Suite")
    print("="*60)
    print("\n⚠️  Make sure Redis is running:")
    print("   brew services start redis  (macOS)")
    print("   sudo systemctl start redis (Linux)")
    print("   redis-server               (Manual)")
    
    input("\nPress Enter to continue...")
    
    try:
        tests = [
            test_cache_connection,
            test_basic_operations,
            test_connection_caching,
            test_cache_stats,
            test_ttl
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
            print("\n✅ All tests passed! Redis cache is working perfectly.")
        else:
            print("\n⚠️  Some tests failed. Check errors above.")
    
    except KeyboardInterrupt:
        print("\n\n⏹️  Tests interrupted by user")


if __name__ == "__main__":
    main()
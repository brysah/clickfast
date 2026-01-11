"""
Test script to verify the implementation
"""
import sys
sys.path.insert(0, '.')

try:
    print("Testing imports...")
    from csv_handler import CSVHandler
    print("✅ csv_handler imported successfully")
    
    from models import ConversionStats, CleanupResponse
    print("✅ models imported successfully")
    
    # Test CSVHandler methods exist
    handler = CSVHandler()
    assert hasattr(handler, 'cleanup_old_conversions'), "cleanup_old_conversions method missing"
    assert hasattr(handler, 'cleanup_all_sources'), "cleanup_all_sources method missing"
    assert hasattr(handler, '_append_to_history'), "_append_to_history method missing"
    print("✅ All CSVHandler methods exist")
    
    print("\n🎉 All tests passed!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

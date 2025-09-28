#!/usr/bin/env python3
"""
Test script to verify GitHub Actions setup and environment variables.
Run this locally to ensure your configuration will work in GitHub Actions.
"""

import os
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import R2, API, Environment

def test_environment_variables():
    """Test that all required environment variables are set."""
    print("🔍 Testing Environment Variables...")

    required_secrets = [
        'R2_ACCESS_KEY_ID',
        'R2_SECRET_ACCESS_KEY',
        'R2_ACCOUNT_ID'
    ]

    optional_vars = [
        'AMFI_NAV_TIMEOUT',
        'AMFI_SCHEME_TIMEOUT',
        'MAX_RETRIES',
        'RETRY_DELAY',
        'HISTORICAL_FETCH_DAYS',
        'CHUNK_SIZE',
        'LOG_LEVEL',
        'ENVIRONMENT'
    ]

    # Check required secrets
    missing_secrets = []
    for var in required_secrets:
        if not os.getenv(var):
            missing_secrets.append(var)
        else:
            print(f"✅ {var}: Set (value hidden)")

    if missing_secrets:
        print(f"❌ Missing required secrets: {', '.join(missing_secrets)}")
        return False

    # Check optional variables (show current values)
    print("\n📋 Optional Configuration Variables:")
    for var in optional_vars:
        value = os.getenv(var, 'Not set (using default)')
        print(f"   {var}: {value}")

    return True

def test_r2_connection():
    """Test R2 connection setup."""
    print("\n🔗 Testing R2 Connection...")

    try:
        r2 = R2()

        # Check if credentials are available
        if not r2.ACCESS_KEY_ID:
            print("❌ R2_ACCESS_KEY_ID not set")
            return False
        if not r2.SECRET_ACCESS_KEY:
            print("❌ R2_SECRET_ACCESS_KEY not set")
            return False
        if not r2.ACCOUNT_ID:
            print("❌ R2_ACCOUNT_ID not set")
            return False

        # Test connection setup (without actually connecting)
        print(f"✅ R2 bucket: {r2.bucket_name}")
        print(f"✅ Asset class: {r2.asset_class}")
        print("✅ R2 credentials configured")

        # Test path generation
        test_path = r2.get_full_path('raw', 'test_file')
        print(f"✅ Path generation works: {test_path}")

        return True

    except Exception as e:
        print(f"❌ R2 connection test failed: {e}")
        return False

def test_api_configuration():
    """Test API configuration."""
    print("\n🌐 Testing API Configuration...")

    try:
        print(f"✅ AMFI NAV timeout: {API.AMFI_NAV_TIMEOUT}s")
        print(f"✅ AMFI scheme timeout: {API.AMFI_SCHEME_TIMEOUT}s")
        print(f"✅ Max retries: {API.MAX_RETRIES}")
        print(f"✅ Retry delay: {API.RETRY_DELAY}s")
        return True

    except Exception as e:
        print(f"❌ API configuration test failed: {e}")
        return False

def test_dependencies():
    """Test that required Python packages are available."""
    print("\n📦 Testing Dependencies...")

    required_packages = [
        'pandas',
        'requests',
        'duckdb',
        'dotenv'
    ]

    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package}: Available")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package}: Missing")

    if missing_packages:
        print(f"\n❌ Missing packages: {', '.join(missing_packages)}")
        print("Install with: pip install " + " ".join(missing_packages))
        return False

    return True

def test_script_availability():
    """Test that the target scripts exist and are executable."""
    print("\n📝 Testing Script Availability...")

    scripts_to_check = [
        'scripts/03_daily_nav_transform.py',
        'scripts/daily_nav_clean.py'
    ]

    all_scripts_ok = True
    for script_path in scripts_to_check:
        full_path = project_root / script_path
        if full_path.exists():
            print(f"✅ {script_path}: Found")
        else:
            print(f"❌ {script_path}: Not found")
            all_scripts_ok = False

    return all_scripts_ok

def main():
    """Run all tests."""
    print("🚀 GitHub Actions Setup Test\n")
    print("=" * 50)

    tests = [
        ("Environment Variables", test_environment_variables),
        ("R2 Connection", test_r2_connection),
        ("API Configuration", test_api_configuration),
        ("Dependencies", test_dependencies),
        ("Script Availability", test_script_availability)
    ]

    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results.append((test_name, False))
        print()

    # Summary
    print("=" * 50)
    print("📊 Test Summary:")

    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {test_name}: {status}")
        if result:
            passed += 1

    print(f"\n🎯 Overall: {passed}/{len(results)} tests passed")

    if passed == len(results):
        print("\n🎉 All tests passed! Your GitHub Actions setup should work correctly.")
        return True
    else:
        print("\n⚠️  Some tests failed. Please fix the issues before using GitHub Actions.")
        print("\n💡 Quick fixes:")
        print("   - Set missing environment variables")
        print("   - Install missing dependencies: pip install -r requirements.txt")
        print("   - Ensure scripts exist in the correct locations")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
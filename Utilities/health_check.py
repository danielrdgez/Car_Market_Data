"""
System Health Check - Car Data Pipeline
========================================
Verifies all components are working correctly before production use.

Run this script to ensure:
- Database schema is correct
- Python dependencies are installed
- Configuration is valid
- Files are in place
"""

import sys
import os
from pathlib import Path

def colored(text, color):
    """Add color to terminal output"""
    colors = {
        'green': '\033[92m',
        'yellow': '\033[93m',
        'red': '\033[91m',
        'reset': '\033[0m'
    }
    return f"{colors.get(color, '')}{text}{colors['reset']}"

def check_dependencies():
    """Check if required Python packages are installed"""
    print("\n" + "="*60)
    print("CHECKING DEPENDENCIES")
    print("="*60)

    required = [
        'selenium',
        'selenium_stealth',
        'pandas',
        'urllib3'
    ]

    missing = []
    for package in required:
        try:
            __import__(package)
            print(f"  ✓ {package:20s} {colored('INSTALLED', 'green')}")
        except ImportError:
            print(f"  ✗ {package:20s} {colored('MISSING', 'red')}")
            missing.append(package)

    if missing:
        print(f"\n{colored('⚠️  Missing packages:', 'yellow')} {', '.join(missing)}")
        print(f"   Run: pip install {' '.join(missing)}")
        return False
    else:
        print(f"\n{colored('✓ All dependencies installed!', 'green')}")
        return True

def check_database_schema():
    """Verify database schema is correct"""
    print("\n" + "="*60)
    print("CHECKING DATABASE SCHEMA")
    print("="*60)

    db_path = Path(__file__).parent.parent / "CAR_DATA_OUTPUT" / "CAR_DATA.db"

    if not db_path.exists():
        print(f"  {colored('ℹ️  Database does not exist yet', 'yellow')}")
        print(f"     It will be created on first run")
        return True

    try:
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(listings)")
        columns = {col[1] for col in cursor.fetchall()}

        required = {'vin', 'loaddate', 'year', 'title', 'details', 'price', 'mileage'}
        missing = required - columns

        if missing:
            print(f"  {colored('✗ Missing columns:', 'red')} {', '.join(missing)}")
            print(f"  {colored('   Run:', 'yellow')} python Utilities/fix_database_schema.py")
            conn.close()
            return False
        else:
            cursor.execute("SELECT COUNT(*) FROM listings")
            count = cursor.fetchone()[0]
            print(f"  ✓ Schema valid - all required columns present")
            print(f"  ✓ Database contains {count:,} records")
            conn.close()
            return True

    except Exception as e:
        print(f"  {colored('✗ Error checking schema:', 'red')} {e}")
        return False

def check_files():
    """Check if all required files exist"""
    print("\n" + "="*60)
    print("CHECKING PROJECT FILES")
    print("="*60)

    base_path = Path(__file__).parent.parent

    required_files = [
        "DataPipeline/DataAquisition.py",
        "DataPipeline/database.py",
        "DataPipeline/NHTSA_enrichment.py",
        "Utilities/verify_schema.py",
        "Utilities/fix_database_schema.py",
        "requirements.txt",
        "README.md"
    ]

    all_exist = True
    for file in required_files:
        file_path = base_path / file
        if file_path.exists():
            print(f"  ✓ {file}")
        else:
            print(f"  {colored('✗ ' + file, 'red')} (MISSING)")
            all_exist = False

    if all_exist:
        print(f"\n{colored('✓ All required files present!', 'green')}")
        return True
    else:
        print(f"\n{colored('⚠️  Some files are missing', 'yellow')}")
        return False

def check_configuration():
    """Check if configuration is valid"""
    print("\n" + "="*60)
    print("CHECKING CONFIGURATION")
    print("="*60)

    try:
        pipeline_path = Path(__file__).parent.parent / "DataPipeline"
        sys.path.insert(0, str(pipeline_path))
        from DataAquisition import Config

        print(f"  ✓ Config loaded successfully")
        print(f"     ZIP Code: {Config.INPUT_ZIP}")
        print(f"     Radius: {Config.INPUT_RADIUS} miles")
        print(f"     Headless: {Config.HEADLESS}")
        print(f"     Driver restart: Every {Config.DRIVER_RESTART_INTERVAL} iterations")
        print(f"     Log cleanup: Every {Config.LOG_CLEANUP_INTERVAL} iteration(s)")
        return True

    except Exception as e:
        print(f"  {colored('✗ Error loading config:', 'red')} {e}")
        return False

def check_output_directory():
    """Check if output directory exists and is writable"""
    print("\n" + "="*60)
    print("CHECKING OUTPUT DIRECTORY")
    print("="*60)

    output_dir = Path(__file__).parent.parent / "CAR_DATA_OUTPUT"

    if not output_dir.exists():
        print(f"  ℹ️  Creating output directory...")
        output_dir.mkdir(exist_ok=True)

    # Test write permission
    test_file = output_dir / ".health_check_test"
    try:
        test_file.write_text("test")
        test_file.unlink()
        print(f"  ✓ Output directory: {output_dir}")
        print(f"  ✓ Write permission: OK")
        return True
    except Exception as e:
        print(f"  {colored('✗ Cannot write to output directory:', 'red')} {e}")
        return False

def main():
    """Run all health checks"""
    print("\n" + "="*60)
    print("CAR DATA PIPELINE - SYSTEM HEALTH CHECK")
    print("="*60)

    checks = [
        ("Dependencies", check_dependencies),
        ("Project Files", check_files),
        ("Configuration", check_configuration),
        ("Output Directory", check_output_directory),
        ("Database Schema", check_database_schema),
    ]

    results = {}
    for name, check_func in checks:
        try:
            results[name] = check_func()
        except Exception as e:
            print(f"\n{colored('✗ Unexpected error in ' + name + ':', 'red')} {e}")
            results[name] = False

    # Summary
    print("\n" + "="*60)
    print("HEALTH CHECK SUMMARY")
    print("="*60)

    for name, passed in results.items():
        status = colored("✓ PASS", "green") if passed else colored("✗ FAIL", "red")
        print(f"  {name:25s} {status}")

    all_passed = all(results.values())

    print("\n" + "="*60)
    if all_passed:
        print(colored("✓✓✓ ALL CHECKS PASSED - SYSTEM READY ✓✓✓", "green"))
        print("\nYou can now run:")
        print("  python DataPipeline/DataAquisition.py")
    else:
        print(colored("⚠️  SOME CHECKS FAILED - PLEASE FIX BEFORE RUNNING", "yellow"))
        print("\nRecommended actions:")
        if not results.get("Dependencies"):
            print("  1. pip install -r requirements.txt")
        if not results.get("Database Schema"):
            print("  2. python Utilities/fix_database_schema.py")
        print("  3. Run this health check again")
    print("="*60)

    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())

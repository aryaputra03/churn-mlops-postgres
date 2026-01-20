"""
Test script to verify password hashing works correctly

This ensures bcrypt is working properly without passlib interference.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.crud import get_password_hash, verify_password


def test_password_hashing():
    """Test password hashing and verification"""
    print("=" * 60)
    print("PASSWORD HASHING TEST")
    print("=" * 60)
    
    # Test passwords
    test_cases = [
        ("Test123!", "Short secure password"),
        ("Admin123!", "Admin password"),
        ("Pass123!", "User password"),
        ("VeryLongPasswordThatIsStillUnder72Bytes123!", "Long password"),
    ]
    
    for password, description in test_cases:
        print(f"\n{description}:")
        print(f"  Original: {password}")
        print(f"  Length: {len(password)} chars, {len(password.encode('utf-8'))} bytes")
        
        try:
            # Hash the password
            hashed = get_password_hash(password)
            print(f"  Hashed: {hashed[:50]}...")
            print(f"  Hash length: {len(hashed)} chars")
            
            # Verify correct password
            is_valid = verify_password(password, hashed)
            print(f"  Verification (correct): {is_valid}")
            assert is_valid, "Correct password should verify"
            
            # Verify wrong password
            is_invalid = verify_password("WrongPassword123!", hashed)
            print(f"  Verification (wrong): {is_invalid}")
            assert not is_invalid, "Wrong password should not verify"
            
            print(f"  ✓ PASSED")
            
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    print("\n" + "=" * 60)
    print("ALL PASSWORD TESTS PASSED!")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = test_password_hashing()
    sys.exit(0 if success else 1)
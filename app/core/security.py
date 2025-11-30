"""Security utilities and helpers."""

import secrets
from typing import Optional


def generate_token(length: int = 32) -> str:
    """Generate a secure random token.
    
    Args:
        length: Token length in bytes
        
    Returns:
        Hex-encoded token string
    """
    return secrets.token_hex(length)


def constant_time_compare(a: str, b: str) -> bool:
    """Compare two strings in constant time.
    
    Args:
        a: First string
        b: Second string
        
    Returns:
        True if strings are equal
    """
    return secrets.compare_digest(a.encode(), b.encode())
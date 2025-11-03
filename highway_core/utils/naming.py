import hashlib
import re


def generate_unique_identifier(base_name: str, workflow_id: str) -> str:
    """Generate unique identifier for container resources"""
    # Create a hash of the workflow_id to ensure uniqueness
    hash_obj = hashlib.md5(workflow_id.encode("utf-8"))
    short_hash = hash_obj.hexdigest()[:8]  # Use first 8 chars of hash
    return f"{base_name}_{short_hash}"


def generate_safe_container_name(name: str, workflow_id: str) -> str:
    """Generate RFC 1123 compliant container name"""
    base_name = re.sub(r"[^a-zA-Z0-9_-]", "_", name.lower())
    unique_name = generate_unique_identifier(base_name, workflow_id)
    # Ensure name length compliance (max 63 chars)
    return unique_name[:63]

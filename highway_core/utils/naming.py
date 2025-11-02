import re

def generate_unique_identifier(base_name: str, workflow_id: str) -> str:
    """Generate unique identifier for container resources"""
    # Use first 8 chars of workflow_id for uniqueness
    short_id = workflow_id[:8] if len(workflow_id) > 8 else workflow_id
    return f"{base_name}_{short_id}"

def generate_safe_container_name(name: str, workflow_id: str) -> str:
    """Generate RFC 1123 compliant container name"""
    base_name = re.sub(r'[^a-zA-Z0-9_-]', '_', name.lower())
    unique_name = generate_unique_identifier(base_name, workflow_id)
    # Ensure name length compliance (max 63 chars)
    return unique_name[:63]

# --- tools/ansible.py ---
# Stubs for 'deploy.ansible.*' functions.


def run_playbook(playbook_path: str) -> dict:
    print(f"  [Tool.Ansible] STUB: Running playbook {playbook_path}")
    return {"status": "ok", "changed": 5, "failed": 0}

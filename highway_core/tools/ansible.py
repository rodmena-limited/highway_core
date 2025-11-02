# --- tools/ansible.py ---
# Stubs for 'deploy.ansible.*' functions.


import logging

logger = logging.getLogger(__name__)


def run_playbook(playbook_path: str) -> dict:
    logger.info("  [Tool.Ansible] STUB: Running playbook %s", playbook_path)
    return {"status": "ok", "changed": 5, "failed": 0}

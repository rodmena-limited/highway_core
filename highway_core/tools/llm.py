# --- tools/llm.py ---
# Stubs for 'llm.*' functions.


import logging

logger = logging.getLogger(__name__)


def ingest_specification(url: str) -> dict:
    logger.info("  [Tool.LLM] STUB: Analyzing spec from %s", url)
    return {"repo_url": "git@github.com:ai/project.git", "microservices": []}


def write_implementation(workspace: str, target: str, prompt: str) -> str:
    logger.info("  [Tool.LLM] STUB: Writing code for %s", target)
    return "stubbed code diff..."

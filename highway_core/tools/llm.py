# --- tools/llm.py ---
# Stubs for 'llm.*' functions.


def ingest_specification(url: str) -> dict:
    print(f"  [Tool.LLM] STUB: Analyzing spec from {url}")
    return {"repo_url": "git@github.com:ai/project.git", "microservices": []}


def write_implementation(workspace: str, target: str, prompt: str) -> str:
    print(f"  [Tool.LLM] STUB: Writing code for {target}")
    return "stubbed code diff..."

# --- tools/filesystem.py ---
# Implements 'fs.write' and 'fs.read'.


import logging

logger = logging.getLogger(__name__)


def write(file_path: str, content: str) -> dict[str, str]:
    logger.info(
        "  [Tool.FS.Write] STUB: Writing %s chars to %s", len(content), file_path
    )
    # with open(file_path, 'w') as f:
    #     f.write(content)
    return {"status": "ok", "path": file_path}


def read(file_path: str) -> str:
    logger.info("  [Tool.FS.Read] STUB: Reading from %s", file_path)
    # with open(file_path, 'r') as f:
    #     return f.read()
    return "stubbed file content"

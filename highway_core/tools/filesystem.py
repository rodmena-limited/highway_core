# --- tools/filesystem.py ---
# Implements 'fs.write' and 'fs.read'.


def write(file_path: str, content: str) -> dict:
    print(f"  [Tool.FS.Write] STUB: Writing {len(content)} chars to {file_path}")
    # with open(file_path, 'w') as f:
    #     f.write(content)
    return {"status": "ok", "path": file_path}


def read(file_path: str) -> str:
    print(f"  [Tool.FS.Read] STUB: Reading from {file_path}")
    # with open(file_path, 'r') as f:
    #     return f.read()
    return "stubbed file content"

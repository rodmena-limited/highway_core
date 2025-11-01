# --- tools/registry.py ---
# Purpose: Loads all tools and provides a single 'get' interface.
# Responsibilities:
# - Dynamically imports all tool modules.
# - Stores a map of string names (e.g., 'tools.fetch.get') to functions.

from . import log, fetch, command, filesystem, llm, vcs, ansible, ci, memory


class ToolRegistry:
    def __init__(self):
        self.functions = {
            # Log
            "tools.log.info": log.info,
            "tools.log.error": log.error,
            # Fetch
            "tools.fetch.get": fetch.get,
            "tools.fetch.post": fetch.post,
            # Command
            "tools.shell.run": command.run,
            # Filesystem
            "fs.write": filesystem.write,
            "fs.read": filesystem.read,
            # Memory (part of state, but exposed as a tool)
            "tools.memory.set": memory.set_memory,
            # Agentic Tools (stubs)
            "llm.analyzer.ingest_specification": llm.ingest_specification,
            "llm.coder.write_implementation": llm.write_implementation,
            "vcs.git.clone_and_setup_repo": vcs.clone_and_setup_repo,
            "vcs.git.commit_files": vcs.commit_files,
            "deploy.ansible.run_playbook": ansible.run_playbook,
            "ci.run_all_tests": ci.run_all_tests,
        }
        print(f"ToolRegistry loaded with {len(self.functions)} functions.")

    def get_function(self, name: str):
        func = self.functions.get(name)
        if func is None:
            raise KeyError(f"Tool '{name}' not found in registry.")
        return func

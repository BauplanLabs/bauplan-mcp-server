#!/usr/bin/env python3

import json
import os
import sys
from pathlib import Path


def generate_config():
    project_root = Path.cwd().resolve()

    venv_python = project_root / ".venv" / "bin" / "python3"
    if not venv_python.exists():
        # Try Windows path
        venv_python = project_root / ".venv" / "Scripts" / "python.exe"
        if not venv_python.exists():
            print(
                "Error: Virtual environment not found. Please create it first with 'uv venv'",
                file=sys.stderr,
            )
            sys.exit(1)

    config = {
        "mcpServers": {
            "mcp-bauplan": {
                "command": str(venv_python),
                "args": [str(project_root / "main.py"), "--transport", "stdio"],
                "workingDirectory": str(project_root),
            }
        }
    }

    print(json.dumps(config, indent=4))

    print("\To use this configuration:", file=sys.stderr)
    print(" 1. Copy the JSON output above", file=sys.stderr)
    print(" 2. Add it to your Claude Desktop configuration file:", file=sys.stderr)
    print(
        "  - macOS: ~/Library/Application Support/Claude/claude_desktop_config.json",
        file=sys.stderr,
    )
    print(
        "  - Windows: %APPDATA%\\Claude\\claude_desktop_config.json", file=sys.stderr
    )
    print("  - Linux: ~/.config/Claude/claude_desktop_config.json", file=sys.stderr)


if __name__ == "__main__":
    generate_config()

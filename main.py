#!/usr/bin/env python3
"""
MCP-Bauplan Server

A Model Context Protocol server for Bauplan integration.
"""

from mcp_bauplan.app import main

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--transport", default="stdio", choices=["stdio", "sse", "streamable-http"]
    )
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--profile", type=str, default=None, help="Bauplan profile to use")
    args = ap.parse_args()
    main(transport=args.transport, host=args.host, port=args.port, profile=args.profile)

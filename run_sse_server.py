#!/usr/bin/env python3
"""
Startup script for the NiFi MCP SSE Server.
This script provides an easy way to start the SSE-enabled server with proper configuration.
"""

import os
import sys
import uvicorn
from pathlib import Path

def main():
    """Start the SSE server with proper configuration."""
    
    # Add the project root to Python path
    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))
    
    # Set up environment
    os.environ.setdefault("PYTHONPATH", str(project_root))
    
    # Output startup messages to stderr to avoid interfering with MCP JSON communication
    print("Starting NiFi MCP SSE Server...", file=sys.stderr)
    print("=" * 50, file=sys.stderr)
    print("Server: fastmcp_sse_server", file=sys.stderr)
    print("Host: 0.0.0.0", file=sys.stderr)
    print("Port: 8000", file=sys.stderr)
    print("Mode: reload (development)", file=sys.stderr)
    print("=" * 50, file=sys.stderr)
    
    # Start the server
    uvicorn.run(
        "nifi_mcp_server.fastmcp_sse_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
        access_log=False
    )

if __name__ == "__main__":
    main() 
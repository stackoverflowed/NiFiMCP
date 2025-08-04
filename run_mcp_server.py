#!/usr/bin/env python3
"""
Startup script for the NiFi MCP Server with SSE support.
This script starts the MCP server using fastmcp with SSE transport.
"""

import os
import sys
import asyncio
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def main():
    """Start the MCP server."""
    
    # Output startup messages to stderr to avoid interfering with MCP JSON communication
    print("Starting NiFi MCP Server with SSE support...", file=sys.stderr)
    print("=" * 50, file=sys.stderr)
    print("Server: nifi_mcp_server.mcp_wrapper", file=sys.stderr)
    print("Transport: SSE", file=sys.stderr)
    print("Host: 0.0.0.0", file=sys.stderr)
    print("Port: 8001", file=sys.stderr)
    print("Server running on: http://localhost:8001", file=sys.stderr)
    print("=" * 50, file=sys.stderr)
    
    # Import and run the MCP server
    from nifi_mcp_server.mcp_wrapper import mcp_server
    
    # List available tools to stderr
    print("\nAvailable tools:", file=sys.stderr)
    
    # Get tools asynchronously
    async def list_tools():
        tools = await mcp_server._list_tools()
        for tool in tools:
            print(f"  - {tool.name}: {tool.description}", file=sys.stderr)
    
    # Run the async function
    asyncio.run(list_tools())
    
    print("\nStarting server...", file=sys.stderr)
    mcp_server.run(
        transport="sse",
        host="0.0.0.0",
        port=8001
    )

if __name__ == "__main__":
    main() 
#!/usr/bin/env python3
"""
Startup script for the NiFi MCP Server with stdio transport.
This script starts the MCP server using fastmcp with stdio transport for Claude Desktop.
"""

import os
import sys
import asyncio
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def main():
    """Start the MCP server with stdio transport."""
    
    # Output startup messages to stderr to avoid interfering with MCP JSON communication
    print("Starting NiFi MCP Server with stdio transport...", file=sys.stderr)
    print("=" * 50, file=sys.stderr)
    print("Server: nifi_mcp_server.mcp_wrapper", file=sys.stderr)
    print("Transport: stdio", file=sys.stderr)
    print("Compatible with: Claude Desktop", file=sys.stderr)
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
    
    print("\nStarting server with stdio transport...", file=sys.stderr)
    mcp_server.run(transport="stdio")

if __name__ == "__main__":
    main() 
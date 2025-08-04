#!/usr/bin/env python3
"""
NiFi MCP Server Wrapper
This module provides a proper MCP server wrapper around the NiFi server functionality
using fastmcp with SSE transport.
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List, Union

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from fastmcp import FastMCP
except ImportError:
    print("fastmcp not found. Installing...", file=sys.stderr)
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "fastmcp"])
    from fastmcp import FastMCP

# Import NiFi server components
from .core import get_nifi_client
from .nifi_client import NiFiAuthenticationError
from config.settings import get_nifi_servers, get_nifi_server_config
from .api_tools.utils import _tool_phase_registry

# Create MCP server with SSE support
mcp_server = FastMCP(
    name="nifi-controller",
    instructions="NiFi MCP Server - Provides tools for managing Apache NiFi flows, processors, and process groups"
)

# Helper function to get NiFi client
async def _get_nifi_client_for_tool(server_id: str = "nifi-local-example"):
    """Get a NiFi client for tool execution."""
    try:
        client = await get_nifi_client(server_id)
        return client
    except Exception as e:
        raise Exception(f"Failed to get NiFi client for server {server_id}: {e}")

# Import and register all NiFi tools
from .api_tools import creation, modification, operation, helpers, review

# Process Group Tools
@mcp_server.tool
async def list_process_groups(
    process_group_id: str = "root",
    nifi_server_id: str = "nifi-local-example"
):
    """List all process groups in the specified process group."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await operation.list_process_groups(client, process_group_id)
        return result
    finally:
        await client.close()

@mcp_server.tool
async def create_process_group(
    name: str,
    parent_group_id: str = "root",
    position_x: float = 0.0,
    position_y: float = 0.0,
    nifi_server_id: str = "nifi-local-example"
):
    """Create a new process group in NiFi."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await creation.create_process_group(
            client, name, parent_group_id, position_x, position_y
        )
        return result
    finally:
        await client.close()

@mcp_server.tool
async def delete_process_group(
    process_group_id: str,
    nifi_server_id: str = "nifi-local-example"
):
    """Delete a process group from NiFi."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await modification.delete_nifi_object(client, "process_group", process_group_id)
        return result
    finally:
        await client.close()

@mcp_server.tool
async def update_process_group_state(
    process_group_id: str,
    state: str,  # "RUNNING", "STOPPED", "ENABLED", "DISABLED"
    nifi_server_id: str = "nifi-local-example"
):
    """Update the state of a process group."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await modification.update_process_group_state(client, process_group_id, state)
        return result
    finally:
        await client.close()

# Processor Tools
@mcp_server.tool
async def list_processors(
    process_group_id: str = "root",
    nifi_server_id: str = "nifi-local-example"
):
    """List all processors in the specified process group."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await operation.list_processors(client, process_group_id)
        return result
    finally:
        await client.close()

@mcp_server.tool
async def create_processor(
    processor_type: str,
    name: str,
    parent_group_id: str = "root",
    position_x: float = 0.0,
    position_y: float = 0.0,
    nifi_server_id: str = "nifi-local-example"
):
    """Create a new processor in NiFi."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await creation.create_processor(
            client, processor_type, name, parent_group_id, position_x, position_y
        )
        return result
    finally:
        await client.close()

@mcp_server.tool
async def update_processor(
    processor_id: str,
    properties: Dict[str, Any],
    nifi_server_id: str = "nifi-local-example"
):
    """Update processor properties."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await modification.update_processor(client, processor_id, properties)
        return result
    finally:
        await client.close()

@mcp_server.tool
async def delete_processor(
    processor_id: str,
    nifi_server_id: str = "nifi-local-example"
):
    """Delete a processor from NiFi."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await modification.delete_nifi_object(client, "processor", processor_id)
        return result
    finally:
        await client.close()

# Connection Tools
@mcp_server.tool
async def list_connections(
    process_group_id: str = "root",
    nifi_server_id: str = "nifi-local-example"
):
    """List all connections in the specified process group."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await operation.list_connections(client, process_group_id)
        return result
    finally:
        await client.close()

@mcp_server.tool
async def create_connection(
    source_id: str,
    target_id: str,
    source_type: str,
    target_type: str,
    name: str,
    parent_group_id: str = "root",
    nifi_server_id: str = "nifi-local-example"
):
    """Create a new connection between components."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await creation.create_connection(
            client, source_id, target_id, source_type, target_type, name, parent_group_id
        )
        return result
    finally:
        await client.close()

@mcp_server.tool
async def delete_connection(
    connection_id: str,
    nifi_server_id: str = "nifi-local-example"
):
    """Delete a connection from NiFi."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await modification.delete_nifi_object(client, "connection", connection_id)
        return result
    finally:
        await client.close()

# Port Tools
@mcp_server.tool
async def list_input_ports(
    process_group_id: str = "root",
    nifi_server_id: str = "nifi-local-example"
):
    """List all input ports in the specified process group."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await operation.list_input_ports(client, process_group_id)
        return result
    finally:
        await client.close()

@mcp_server.tool
async def list_output_ports(
    process_group_id: str = "root",
    nifi_server_id: str = "nifi-local-example"
):
    """List all output ports in the specified process group."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await operation.list_output_ports(client, process_group_id)
        return result
    finally:
        await client.close()

@mcp_server.tool
async def create_input_port(
    name: str,
    parent_group_id: str = "root",
    position_x: float = 0.0,
    position_y: float = 0.0,
    nifi_server_id: str = "nifi-local-example"
):
    """Create a new input port in NiFi."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await creation.create_input_port(
            client, name, parent_group_id, position_x, position_y
        )
        return result
    finally:
        await client.close()

@mcp_server.tool
async def create_output_port(
    name: str,
    parent_group_id: str = "root",
    position_x: float = 0.0,
    position_y: float = 0.0,
    nifi_server_id: str = "nifi-local-example"
):
    """Create a new output port in NiFi."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await creation.create_output_port(
            client, name, parent_group_id, position_x, position_y
        )
        return result
    finally:
        await client.close()

# Flow Management Tools
@mcp_server.tool
async def get_flow_summary(
    process_group_id: str = "root",
    nifi_server_id: str = "nifi-local-example"
):
    """Get a summary of the flow in the specified process group."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await helpers.get_flow_summary(client, process_group_id)
        return result
    finally:
        await client.close()

@mcp_server.tool
async def document_flow(
    process_group_id: str = "root",
    nifi_server_id: str = "nifi-local-example"
):
    """Generate documentation for a NiFi flow."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await review.document_flow(client, process_group_id)
        return result
    finally:
        await client.close()

# Configuration Tools
@mcp_server.tool
async def list_nifi_servers():
    """List all configured NiFi servers."""
    servers = get_nifi_servers()
    return [{"id": server.get("id"), "name": server.get("name")} for server in servers]

@mcp_server.tool
async def get_server_info(
    nifi_server_id: str = "nifi-local-example"
):
    """Get information about a specific NiFi server."""
    server_config = get_nifi_server_config(nifi_server_id)
    if not server_config:
        raise Exception(f"NiFi server '{nifi_server_id}' not found")
    
    return {
        "id": nifi_server_id,
        "name": server_config.get("name"),
        "url": server_config.get("url"),
        "username": server_config.get("username")
    }

# FlowFile Management Tools
@mcp_server.tool
async def list_flowfiles(
    connection_id: str,
    nifi_server_id: str = "nifi-local-example"
):
    """List FlowFiles in a connection."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await operation.list_flowfiles(client, connection_id)
        return result
    finally:
        await client.close()

@mcp_server.tool
async def purge_connection(
    connection_id: str,
    nifi_server_id: str = "nifi-local-example"
):
    """Purge all FlowFiles from a connection."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await operation.purge_connection(client, connection_id)
        return result
    finally:
        await client.close()

# Controller Service Tools
@mcp_server.tool
async def list_controller_services(
    process_group_id: str = "root",
    nifi_server_id: str = "nifi-local-example"
):
    """List controller services in the specified process group."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await operation.list_controller_services(client, process_group_id)
        return result
    finally:
        await client.close()

@mcp_server.tool
async def create_controller_service(
    service_type: str,
    name: str,
    parent_group_id: str = "root",
    nifi_server_id: str = "nifi-local-example"
):
    """Create a new controller service in NiFi."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await creation.create_controller_service(
            client, service_type, name, parent_group_id
        )
        return result
    finally:
        await client.close()

# Additional Operation Tools
@mcp_server.tool
async def operate_nifi_objects(
    operations: List[Dict[str, Any]],
    nifi_server_id: str = "nifi-local-example"
):
    """Performs start, stop, enable, or disable operations on multiple NiFi objects in batch."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await operation.operate_nifi_objects(operations)
        return result
    finally:
        await client.close()

@mcp_server.tool
async def invoke_nifi_http_endpoint(
    url: str,
    process_group_id: str,
    method: str = "POST",
    payload: Optional[Union[str, Dict[str, Any]]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: int = 10,
    nifi_server_id: str = "nifi-local-example"
):
    """Sends an HTTP request to a specified URL, typically to test a NiFi flow endpoint."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await operation.invoke_nifi_http_endpoint(
            url, process_group_id, method, payload, headers, timeout_seconds
        )
        return result
    finally:
        await client.close()

@mcp_server.tool
async def purge_flowfiles(
    target_id: str,
    target_type: str,
    timeout_seconds: Optional[int] = 30,
    nifi_server_id: str = "nifi-local-example"
):
    """Purges all FlowFiles from a connection or all connections in a process group."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await operation.purge_flowfiles(target_id, target_type, timeout_seconds)
        return result
    finally:
        await client.close()

@mcp_server.tool
async def analyze_nifi_processor_errors(
    processor_id: str,
    include_suggestions: bool = True,
    nifi_server_id: str = "nifi-local-example"
):
    """Analyze processor errors and provide debugging suggestions for faster resolution."""
    client = await _get_nifi_client_for_tool(nifi_server_id)
    try:
        result = await operation.analyze_nifi_processor_errors(processor_id, include_suggestions)
        return result
    finally:
        await client.close()

if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="NiFi MCP Server")
    parser.add_argument("--transport", choices=["stdio", "sse"], default="stdio", 
                       help="Transport type (default: stdio)")
    parser.add_argument("--host", default="0.0.0.0", help="Host for SSE transport (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8001, help="Port for SSE transport (default: 8001)")
    
    args = parser.parse_args()
    
    # Output startup messages to stderr to avoid interfering with MCP JSON communication
    print(f"Starting NiFi MCP Server with {args.transport.upper()} transport...", file=sys.stderr)
    
    if args.transport == "sse":
        print(f"Server running on: http://{args.host}:{args.port}", file=sys.stderr)
    
    print("Available tools:", file=sys.stderr)
    
    # Get tools asynchronously
    async def list_tools():
        tools = await mcp_server._list_tools()
        for tool in tools:
            print(f"  - {tool.name}: {tool.description}", file=sys.stderr)
    
    # Run the async function
    asyncio.run(list_tools())
    
    # Run the server with the specified transport
    if args.transport == "stdio":
        mcp_server.run(transport="stdio")
    else:
        mcp_server.run(
            transport="sse",
            host=args.host,
            port=args.port
        ) 
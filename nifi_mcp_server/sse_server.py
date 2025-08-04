"""
NiFi MCP Server with Server-Sent Events (SSE) using FastMCP.

This server provides the same functionality as the HTTP-based server but uses
SSE for real-time communication with MCP clients like Claude Desktop.
"""

import asyncio
import signal
from typing import List, Dict, Optional, Any, Union, Literal
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Body, Request, Query, Header
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import sys
from loguru import logger 
from docstring_parser import parse 
from contextlib import asynccontextmanager
from textwrap import dedent
from mcp.server import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.shared.exceptions import McpError

# --- Setup Logging --- 
try:
    from config.logging_setup import setup_logging, request_context
    setup_logging(context='server')
except ImportError as e:
    logger.warning(f"Logging setup failed: {e}. Check config/logging_setup.py and Python path. Using basic stderr logger.")
    logger.add(sys.stderr, level="INFO")

# Import our NiFi API client and exception
from nifi_mcp_server.nifi_client import NiFiAuthenticationError

# Import core components
from .core import mcp, get_nifi_client

# Import the context var from logging_setup
from config.logging_setup import request_context

# Import ContextVars
from .request_context import current_nifi_client, current_request_logger, current_user_request_id, current_action_id

# Import Utilities
from .api_tools.utils import (
    tool_phases,
    _format_processor_summary,
    _format_connection_summary,
    _format_port_summary,
    filter_processor_data,
    filter_created_processor_data,
    filter_connection_data,
    filter_port_data,
    filter_process_group_data,
    _tool_phase_registry
)

# Import Tool Modules
from .api_tools import review
from .api_tools import creation
from .api_tools import modification
from .api_tools import operation
from .api_tools import helpers

# Import Config Settings
from config.settings import get_nifi_servers

# === FastAPI Application Setup === #

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    logger.info("FastAPI SSE server starting up...")
    
    # Configure logging for server context
    try:
        from config.logging_setup import setup_logging
        setup_logging('server')
        logger.info("Server logging configured")
    except Exception as e:
        logger.error(f"Failed to configure server logging: {e}", exc_info=True)
    
    # Configure LLM clients for workflow execution
    try:
        from nifi_chat_ui.chat_manager_compat import configure_llms
        configure_llms()
        logger.info("LLM clients configured for workflow execution")
    except Exception as e:
        logger.error(f"Failed to configure LLM clients: {e}", exc_info=True)
    
    # Check NiFi server configurations
    try:
        nifi_servers = get_nifi_servers()
        logger.info(f"Found {len(nifi_servers)} NiFi server configurations.")
    except Exception as e:
        logger.error(f"Failed to read NiFi server configurations: {e}", exc_info=True)
    
    yield
    
    # Shutdown logic
    logger.info("FastAPI SSE server shutting down...")
    logger.info("Cleanup finished.")

# Create FastAPI app with SSE support
app = FastAPI(
    title="NiFi MCP SSE Server",
    description="NiFi MCP Server with Server-Sent Events support",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === SSE Endpoints === #

@app.get("/")
async def root():
    """Root endpoint for health check."""
    return {"message": "NiFi MCP SSE Server", "status": "running"}

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "server": "NiFi MCP SSE"}

@app.get("/mcp/sse")
async def mcp_sse_endpoint():
    """SSE endpoint for MCP communication."""
    async def event_stream():
        try:
            # Send initial connection event
            yield f"data: {json.dumps({'type': 'connection', 'status': 'connected'})}\n\n"
            
            # Keep connection alive
            while True:
                await asyncio.sleep(30)  # Send heartbeat every 30 seconds
                yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': asyncio.get_event_loop().time()})}\n\n"
                
        except asyncio.CancelledError:
            logger.info("SSE connection cancelled")
        except Exception as e:
            logger.error(f"SSE stream error: {e}")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Cache-Control"
        }
    )

# === Tool Registration with FastMCP === #

# Register all tools from the existing modules
def register_tools():
    """Register all NiFi tools with the FastMCP server."""
    
    # Import all tool functions from the existing modules
    from .api_tools.review import (
        list_nifi_objects, list_nifi_objects_with_streaming, get_nifi_object_details,
        document_nifi_flow, search_nifi_flow, get_process_group_status,
        list_flowfiles, get_flowfile_event_details
    )
    
    from .api_tools.operation import (
        operate_nifi_objects, invoke_nifi_http_endpoint, purge_flowfiles,
        analyze_nifi_processor_errors
    )
    
    from .api_tools.modification import (
        delete_nifi_processor_properties, update_nifi_processor_relationships,
        update_nifi_connection, delete_nifi_objects, update_nifi_processors_properties
    )
    
    from .api_tools.creation import (
        create_nifi_processors, create_nifi_ports, create_controller_services,
        create_nifi_process_group, create_complete_nifi_flow, create_nifi_connections,
        lookup_nifi_processor_types, get_controller_service_types
    )
    
    from .api_tools.helpers import get_expert_help
    
    # Register all tools
    tools_to_register = [
        # Review tools
        list_nifi_objects,
        list_nifi_objects_with_streaming,
        get_nifi_object_details,
        document_nifi_flow,
        search_nifi_flow,
        get_process_group_status,
        list_flowfiles,
        get_flowfile_event_details,
        
        # Operation tools
        operate_nifi_objects,
        invoke_nifi_http_endpoint,
        purge_flowfiles,
        analyze_nifi_processor_errors,
        
        # Modification tools
        delete_nifi_processor_properties,
        update_nifi_processor_relationships,
        update_nifi_connection,
        delete_nifi_objects,
        update_nifi_processors_properties,
        
        # Creation tools
        create_nifi_processors,
        create_nifi_ports,
        create_controller_services,
        create_nifi_process_group,
        create_complete_nifi_flow,
        create_nifi_connections,
        lookup_nifi_processor_types,
        get_controller_service_types,
        
        # Helper tools
        get_expert_help
    ]
    
    for tool_func in tools_to_register:
        try:
            # Register the tool with FastMCP
            mcp.tool()(tool_func)
            logger.debug(f"Registered tool: {tool_func.__name__}")
        except Exception as e:
            logger.error(f"Failed to register tool {tool_func.__name__}: {e}")

# Register tools on startup
register_tools()

# === HTTP Endpoints for Compatibility === #

@app.get("/config/nifi-servers", response_model=List[Dict[str, str]], tags=["Configuration"])
async def list_nifi_servers(request: Request):
    """List configured NiFi servers."""
    logger.info("Request received for /config/nifi-servers")
    try:
        servers = get_nifi_servers()
        logger.info(f"Returning {len(servers)} NiFi server configurations to the client.")
        return servers
    except Exception as e:
        logger.error(f"Error listing NiFi servers: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tools", response_model=List[Dict[str, Any]], tags=["Tools"])
async def get_tools(
    request: Request, 
    phase: str | None = Query(None)
):
    """Get available tools, optionally filtered by phase."""
    logger.info("Request received for /tools")
    
    try:
        # Get all registered tools from FastMCP
        tools = []
        for tool_name, tool_info in mcp._tools.items():
            tool_dict = {
                "name": tool_name,
                "description": tool_info.description or "",
                "parameters": tool_info.parameters or {},
                "phases": tool_phases.get(tool_name, ["Review", "Build", "Modify", "Operate"])
            }
            
            # Filter by phase if specified
            if phase is None or phase in tool_dict["phases"]:
                tools.append(tool_dict)
        
        logger.info(f"Returning {len(tools)} tool definitions (Phase: {phase or 'All'}).")
        return tools
        
    except Exception as e:
        logger.error(f"Error getting tools: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# === Tool Execution Endpoint === #

from pydantic import BaseModel

class ContextModel(BaseModel):
    user_request_id: Optional[str] = "-"
    action_id: Optional[str] = "-"

class ToolExecutionPayload(BaseModel):
    arguments: Dict[str, Any]
    context: Optional[ContextModel] = None

@app.post("/tools/{tool_name}", tags=["Tools"])
async def execute_tool(
    tool_name: str,
    payload: ToolExecutionPayload,
    request: Request,
    nifi_server_id: Optional[str] = Header(None, alias="X-Nifi-Server-Id")
) -> Any:
    """Execute a specific tool."""
    logger.info(f"Tool execution request: {tool_name}")
    
    try:
        # Set up context
        context = payload.context or ContextModel()
        current_user_request_id.set(context.user_request_id)
        current_action_id.set(context.action_id)
        
        # Get NiFi client if needed
        nifi_client = None
        if nifi_server_id:
            nifi_client = await get_nifi_client(nifi_server_id)
            current_nifi_client.set(nifi_client)
        
        # Execute the tool using FastMCP
        if tool_name not in mcp._tools:
            raise HTTPException(status_code=404, detail=f"Tool '{tool_name}' not found")
        
        tool_func = mcp._tools[tool_name].func
        result = await tool_func(**payload.arguments)
        
        return result
        
    except ToolError as e:
        logger.error(f"Tool execution error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in tool execution: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Clean up context
        if nifi_client:
            await nifi_client.close()

# === Main Entry Point === #

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "nifi_mcp_server.sse_server:app",
        host="0.0.0.0",
        port=8002,  # Different port from HTTP server
        reload=True,
        log_level="info"
    ) 
import asyncio
import signal
from typing import List, Dict, Optional, Any, Union, Literal
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Body, Request, Query 
from fastapi.responses import JSONResponse 
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
import os
import sys
from loguru import logger 
from docstring_parser import parse 

# --- Setup Logging --- 
try:
    # Adjust import path based on project structure if necessary
    # If server.py is run directly from project root, this might need adjustment
    # Assuming server is run from project root or config is in PYTHONPATH
    from config.logging_setup import setup_logging
    setup_logging()
except ImportError as e:
    logger.warning(f"Logging setup failed: {e}. Check config/logging_setup.py and Python path. Using basic stderr logger.")
    # Minimal fallback if setup fails
    logger.add(sys.stderr, level="INFO")
# ---------------------

# Import our NiFi API client and exception (Absolute Import)
from nifi_mcp_server.nifi_client import NiFiAuthenticationError # Keep Error import
# REMOVED from nifi_mcp_server.nifi_client import NiFiClient

# Import MCP server components (Corrected for v1.6.0)
# REMOVED from mcp.server import FastMCP

# Import core components AFTER logging is setup, but BEFORE tools
from .core import mcp, nifi_api_client

from mcp.shared.exceptions import McpError # Base error
from mcp.server.fastmcp.exceptions import ToolError # Tool-specific errors

# REMOVED MCP Instantiation Block

# REMOVED NiFiClient Instantiation Block

# --- Import Utilities AFTER core components are imported ---
from .api_tools.utils import (
    tool_phases,
    ensure_authenticated,
    _format_processor_summary,
    _format_connection_summary,
    _format_port_summary,
    filter_processor_data,
    filter_created_processor_data,
    filter_connection_data,
    filter_port_data,
    filter_process_group_data,
    _tool_phase_registry # Import the registry itself
)
# ---------------------------------------------------------------------

# --- Import Tool Modules AFTER mcp is defined to allow registration ---
from .api_tools import review # Imports list_*, get_*, document_*
from .api_tools import creation
from .api_tools import modification
from .api_tools import operation
from .api_tools import lookup
# Add other tool module imports here as they are created
# from .api_tools import lookup
# ---------------------------------------------------------------------



# === FastAPI Application Setup === #
app = FastAPI(
    title="NiFi MCP REST Bridge", 
    description="Exposes NiFi MCP tools via a REST API."
)

# --- CORS Middleware --- #
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permissive for now
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- FastAPI Event Handlers --- #
@app.on_event("startup")
async def startup_event():
    logger.info("FastAPI server starting up...")
    # Perform initial authentication check
    try:
        # Pass logger and client to ensure_authenticated
        await ensure_authenticated(nifi_api_client, logger) 
        logger.info("Initial NiFi authentication successful.")
    except Exception as e:
        logger.error(f"Initial NiFi authentication failed on startup: {e}", exc_info=True)
        # Depending on requirements, you might want to prevent startup
        # raise RuntimeError("NiFi authentication failed, cannot start server.") from e

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("FastAPI server shutting down...")
    await cleanup()

# --- REST API Endpoints --- #

@app.get("/tools", response_model=List[Dict[str, Any]])
async def get_tools(
    request: Request, 
    phase: str | None = Query(None) # Add phase query parameter
):
    """Retrieve the list of available MCP tools, optionally filtered by phase."""
    user_request_id = request.state.user_request_id
    action_id = request.state.action_id
    bound_logger = logger.bind(user_request_id=user_request_id, action_id=action_id, requested_phase=phase)
    
    bound_logger.debug(f"/tools endpoint received phase parameter: {phase!r}") # Log the raw value
    
    try:
        bound_logger.debug(f"Fetching tools, requested phase: '{phase}'")
        formatted_tools = []
        tool_manager = getattr(mcp, '_tool_manager', None)
        if tool_manager:
            tools_info = tool_manager.list_tools()
            
            for tool_info in tools_info: 
                tool_name = getattr(tool_info, 'name', 'unknown')
                tool_phases_list = _tool_phase_registry.get(tool_name, [])
                if not tool_phases_list:
                     bound_logger.warning(f"Could not find phase tags in registry for tool '{tool_name}'. Assuming it belongs to all phases for safety.")

                requested_phase_lower = phase.lower() if phase else None
                tool_phases_lower = [p.lower() for p in tool_phases_list]
                
                if requested_phase_lower and requested_phase_lower != "all" and requested_phase_lower not in tool_phases_lower:
                    bound_logger.trace(f"Skipping tool '{tool_name}' due to phase mismatch (requested: {phase}, tool phases: {tool_phases_list})")
                    continue # Skip this tool if phase doesn't match

                raw_docstring = getattr(tool_info, 'description', '')
                parsed_docstring = parse(raw_docstring)
                returns_description = ""
                if parsed_docstring.returns and parsed_docstring.returns.description:
                    returns_description = f"\n\n**Returns:**\n{parsed_docstring.returns.description}"

                base_description_parts = []
                if parsed_docstring.short_description:
                    base_description_parts.append(parsed_docstring.short_description)
                
                if parsed_docstring.long_description:
                    base_description_parts.append("\n\n" + parsed_docstring.long_description) # Add separation

                if base_description_parts:
                    base_description = "".join(base_description_parts)
                    bound_logger.trace(f"Using parsed short/long description for tool '{tool_name}'")
                else:
                    base_description = raw_docstring.split('\n\n')[0] # Original fallback
                    bound_logger.trace(f"Falling back to basic description for tool '{tool_name}' (parsing empty?)")

                tool_description = f"{base_description}{returns_description}"

                param_descriptions = {p.arg_name: p.description for p in parsed_docstring.params}
                raw_params_schema = getattr(tool_info, 'parameters', {})
                parameters_schema = {"type": "object", "properties": {}}
                raw_properties = raw_params_schema.get('properties', {})
                cleaned_properties = {}
                if isinstance(raw_properties, dict):
                    for prop_name, prop_schema in raw_properties.items():
                        if isinstance(prop_schema, dict):
                            cleaned_schema = prop_schema.copy()
                            cleaned_schema.pop('anyOf', None)
                            cleaned_schema.pop('title', None)
                            cleaned_schema.pop('default', None)
                            cleaned_schema['description'] = param_descriptions.get(prop_name, '')
                            cleaned_properties[prop_name] = cleaned_schema
                        else:
                            logger.warning(f"Property '{prop_name}' in tool '{tool_name}' has non-dict schema: {prop_schema}. Skipping property.")
                parameters_schema["properties"] = cleaned_properties
                required_list = raw_params_schema.get('required', [])
                if required_list and cleaned_properties:
                     parameters_schema["required"] = required_list
                elif "required" in parameters_schema:
                     del parameters_schema["required"]
                if not parameters_schema["properties"]:
                     del parameters_schema["properties"]
                     if "required" in parameters_schema: del parameters_schema["required"]
                if 'required' in raw_params_schema:
                    parameters_schema["required"] = list(raw_params_schema['required'])
                if 'properties' in parameters_schema:
                    for prop_name, prop_data in parameters_schema['properties'].items():
                        if isinstance(prop_data, dict) and 'enum' in prop_data:
                            prop_data['enum'] = [str(val) for val in prop_data['enum']]
                
                formatted_tools.append({
                    "type": "function",
                    "function": {
                        "name": tool_name,
                        "description": tool_description,
                        "parameters": parameters_schema
                    },
                    "phases": tool_phases_list # Include phases in the response
                })
            bound_logger.info(f"Returning {len(formatted_tools)} tool definitions (Phase: {phase or 'All'}).")
            return formatted_tools
        else:
            bound_logger.warning("Could not find ToolManager (_tool_manager) on MCP instance.")
            return []
    except Exception as e:
        bound_logger.error(f"Error retrieving tool definitions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error retrieving tools.")

# Define a Pydantic model for the request body with context support
from pydantic import BaseModel

# Define a model for context data
class ContextModel(BaseModel):
    user_request_id: Optional[str] = "-"
    action_id: Optional[str] = "-"

class ToolExecutionPayload(BaseModel):
    arguments: Dict[str, Any]
    context: Optional[ContextModel] = None

# Middleware for binding context IDs to logger
@app.middleware("http")
async def add_context_to_logger(request, call_next):
    user_request_id = request.headers.get("X-Request-ID", "-")
    action_id = request.headers.get("X-Action-ID", "-")
    
    if user_request_id != "-" or action_id != "-":
        logger.debug(f"Received request with context IDs: user_request_id={user_request_id}, action_id={action_id}")
    
    request.state.user_request_id = user_request_id
    request.state.action_id = action_id
    
    response = await call_next(request)
    return response

@app.post("/tools/{tool_name}")
async def execute_tool(tool_name: str, payload: ToolExecutionPayload, request: Request) -> Dict[str, Any]:
    """Execute a specified MCP tool with the given arguments via ToolManager."""
    user_request_id = request.state.user_request_id
    action_id = request.state.action_id
    
    if payload.context and user_request_id == "-":
        user_request_id = payload.context.user_request_id
    if payload.context and action_id == "-":
        action_id = payload.context.action_id
    
    bound_logger = logger.bind(user_request_id=user_request_id, action_id=action_id)
    
    bound_logger.info(f"Received request to execute tool '{tool_name}' via ToolManager with arguments: {payload.arguments}")
    
    tool_manager = getattr(mcp, '_tool_manager', None)
    if not tool_manager:
        bound_logger.error("Could not find ToolManager on MCP instance during execution.")
        raise HTTPException(status_code=500, detail="Internal server configuration error: ToolManager not found.")

    try:
        await ensure_authenticated(nifi_api_client, bound_logger) 
        
        tool_context = {"user_request_id": user_request_id, "action_id": action_id}
        result = await tool_manager.call_tool(tool_name, payload.arguments, context=tool_context)
            
        bound_logger.info(f"Execution of tool '{tool_name}' via ToolManager successful.")
        
        return {"result": result}
        
    except ToolError as e:
        bound_logger.error(
            "ToolError executing tool '{tool_name}' via ToolManager: {error_details}", 
            tool_name=tool_name, 
            error_details=str(e), 
            exc_info=True
        )
        return JSONResponse(
            status_code=422,
            content={"detail": f"Tool execution failed: {str(e)}"}
        )
    except NiFiAuthenticationError as e:
         bound_logger.error(f"NiFi Authentication Error during tool '{tool_name}' execution: {e}", exc_info=True)
         return JSONResponse(
             status_code=403, 
             content={"detail": f"NiFi authentication failed: {str(e)}. Check server credentials."}
        ) 
    except Exception as e:
        if isinstance(e, TypeError) and (
            "required positional argument" in str(e) or 
            "missing" in str(e) and "required argument" in str(e) or
            "unexpected keyword argument" in str(e)
        ):
             bound_logger.warning(f"Invalid arguments provided for tool '{tool_name}': {e}")
             return JSONResponse(status_code=422, detail=f"Invalid or missing arguments for tool '{tool_name}': {e}")

        bound_logger.error(f"Unexpected error executing tool '{tool_name}' via ToolManager: {e}", exc_info=True)
        if "Context is not available outside of a request" in str(e):
             bound_logger.error(f"Tool '{tool_name}' likely requires context, which is unavailable in this REST setup.")
             return JSONResponse(status_code=501, detail=f"Tool '{tool_name}' cannot be executed via REST API as it requires MCP context.")
        return JSONResponse(status_code=500, detail=f"Internal server error executing tool '{tool_name}'.")


# Keep cleanup function
async def cleanup():
    pass # Add pass to fix empty function body

# Run with uvicorn if this module is run directly
if __name__ == "__main__":
    import uvicorn
    # Disable default access logs to potentially reduce noise/interleaving
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000, 
        log_level="info", # Keep uvicorn's own level if desired
        access_log=False # Disable standard access log lines
    )

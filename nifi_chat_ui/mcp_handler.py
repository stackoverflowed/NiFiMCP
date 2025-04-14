# Placeholder for MCP server interaction logic
# This will handle starting the server subprocess and communicating with it. 

import streamlit as st
import requests # Use requests for HTTP calls
import json
from typing import List, Dict, Any, Optional
import os
# Remove standard logging import
# import logging 
from loguru import logger # Import Loguru logger
from google.protobuf.internal.containers import MessageMap # Import the type if possible

# --- Configuration --- #
# URL for the FastAPI server
API_BASE_URL = "http://localhost:8000"

# --- Remove All MCP Client, Threading, Asyncio imports and helpers --- #
# (Imports like ClientSession, stdio_client, websocket_client, McpError, ToolError removed)
# (Helpers like get_server_params, run_async_in_thread removed)

# --- Tool Execution (Synchronous HTTP) --- #
def execute_mcp_tool(
    tool_name: str, 
    params: dict,
    user_request_id: str | None = None, # Added context ID
    action_id: str | None = None # Added context ID
) -> dict | str:
    """Executes a tool call via the REST API."""
    # Bind context IDs for logging within this function call
    bound_logger = logger.bind(user_request_id=user_request_id, action_id=action_id)
    
    url = f"{API_BASE_URL}/tools/{tool_name}"

    # Log context IDs explicitly for debugging
    bound_logger.debug(f"Tool execution context: user_request_id={user_request_id}, action_id={action_id}")

    # Convert MapComposite to dict before creating payload
    processed_params = {}
    for key, value in params.items():
        # Check if the object is an instance of MapComposite
        # Using type name check as a fallback if direct import is tricky
        if isinstance(value, MessageMap) or type(value).__name__ == 'MapComposite': 
            processed_params[key] = dict(value)
        else:
            processed_params[key] = value

    # Add context IDs to the payload
    payload = {
        "arguments": processed_params,
        "context": {
            "user_request_id": user_request_id or "-",
            "action_id": action_id or "-"
        }
    }

    # Create headers with context IDs
    headers = {
        "X-Request-ID": user_request_id or "-",
        "X-Action-ID": action_id or "-",
        "Content-Type": "application/json"
    }
    
    bound_logger.info(f"Executing tool '{tool_name}' via API: {url}")
    bound_logger.debug(f"Payload for tool '{tool_name}': {payload}")
    bound_logger.debug(f"Headers for tool '{tool_name}': {headers}")
    
    # --- Log MCP Request ---
    bound_logger.bind(
        interface="mcp", 
        direction="request", 
        data={"url": url, "payload": payload, "headers": headers}
    ).debug("Sending request to MCP API")
    # -----------------------
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=60) # Add timeout
        response.raise_for_status() # Raise exception for bad status codes (4xx or 5xx)
        
        result_data = response.json()
        bound_logger.info(f"Received successful response from API for tool '{tool_name}'.")
        bound_logger.debug(f"API Response data: {result_data}")
        
        # --- Log MCP Response (Success) ---
        bound_logger.bind(
            interface="mcp", 
            direction="response", 
            data={"status_code": response.status_code, "body": result_data}
        ).debug("Received successful response from MCP API")
        # --------------------------------
        
        return result_data.get("result", result_data) 

    except requests.exceptions.HTTPError as e:
        error_detail = "Unknown API error"
        error_body = None
        try:
            error_body = e.response.json()
            error_detail = error_body.get("detail", e.response.text)
        except json.JSONDecodeError:
            error_detail = e.response.text 
            error_body = {"raw_text": error_detail} # Store raw text if not JSON
            
        error_message = f"API Error executing tool '{tool_name}': {e.response.status_code} - {error_detail}"
        bound_logger.error(error_message)
        
        # --- Log MCP Response (Error) ---
        bound_logger.bind(
            interface="mcp", 
            direction="response", 
            data={"status_code": e.response.status_code, "body": error_body}
        ).debug("Received error response from MCP API")
        # -------------------------------
        
        st.error(error_message) # Keep UI error
        return error_message # Return the error string
        
    except requests.exceptions.ConnectionError as e:
        error_message = f"Connection Error: Could not connect to the MCP API server at {API_BASE_URL}. Is it running?"
        # Replace logging with logger
        bound_logger.error(f"{error_message} ({e})")
        # logging.error(f"{error_message} ({e})") # Use logging
        st.error(error_message) # Keep UI error
        return error_message
        
    except requests.exceptions.Timeout:
        error_message = f"Timeout connecting to MCP API server for tool '{tool_name}'."
        # Replace logging with logger
        bound_logger.error(error_message)
        # logging.error(error_message) # Use logging
        st.error(error_message) # Keep UI error
        return error_message
        
    except Exception as e:
        # Catch other unexpected errors (e.g., JSON decoding of success response)
        error_message = f"Unexpected error during tool execution API call for '{tool_name}'"
        # Replace logging.exception with logger.exception
        bound_logger.exception(error_message) # Includes traceback
        # logging.exception(error_message) # Use logging.exception to include traceback
        st.error(f"{error_message}: {e}") # Also show brief error in UI
        return f"{error_message}: {e}" # Return error string

# --- Tool Definitions (Synchronous HTTP) --- #
# @st.cache_data # Consider caching this
def get_available_tools(
    user_request_id: str | None = None,
    action_id: str | None = None,
    phase: str | None = None # Add phase parameter
) -> list[dict]:
    """Fetches tool definitions from the REST API, optionally filtered by phase."""
    # Construct URL with optional phase parameter
    if phase and phase.lower() != "all":
        url = f"{API_BASE_URL}/tools?phase={phase}" # Pass phase if specified
    else:
        url = f"{API_BASE_URL}/tools"
    
    # Bind context IDs for logging within this function call
    bound_logger = logger.bind(user_request_id=user_request_id, action_id=action_id)
    
    # Use logger instead of print
    bound_logger.info(f"Fetching available tools from API: {url}")
    
    # --- ADD LOGGING HERE ---
    bound_logger.debug(f"Constructed request URL for tools: {url}")
    # ------------------------
    
    try:
        # Create headers with context IDs
        headers = {
            "X-Request-ID": user_request_id or "-",
            "X-Action-ID": action_id or "-",
            "Content-Type": "application/json"
        }
        
        # Log headers for debugging
        bound_logger.debug(f"Headers for tools request: {headers}")
        
        response = requests.get(url, headers=headers, timeout=30) # Add timeout and headers
        response.raise_for_status() # Raise exception for bad status codes
        
        tools = response.json() # Expecting a list of tool dicts
        if isinstance(tools, list):
            # Use logger instead of print
            bound_logger.info(f"Successfully retrieved {len(tools)} tool definitions from API.")
            return tools
        else:
            error_message = f"API Error: Unexpected format received for tools list (expected list, got {type(tools)})."
            # Use logger instead of print
            bound_logger.error(error_message)
            st.error(error_message) # Keep UI error
            return []
            
    except requests.exceptions.HTTPError as e:
        error_detail = "Unknown API error"
        try:
            error_detail = e.response.json().get("detail", e.response.text)
        except json.JSONDecodeError:
            error_detail = e.response.text
            
        error_message = f"API Error fetching tools: {e.response.status_code} - {error_detail}"
        # Replace logging with logger
        bound_logger.error(error_message)
        # logging.error(error_message) # Use logging
        st.error(error_message) # Keep UI error
        return []
        
    except requests.exceptions.ConnectionError as e:
        error_message = f"Connection Error: Could not connect to the MCP API server at {API_BASE_URL} to get tools. Is it running?"
        # Replace logging with logger
        bound_logger.error(f"{error_message} ({e})")
        # logging.error(f"{error_message} ({e})") # Use logging
        st.error(error_message) # Keep UI error
        return []
        
    except requests.exceptions.Timeout:
        error_message = f"Timeout connecting to MCP API server to get tools."
        # Replace logging with logger
        bound_logger.error(error_message)
        # logging.error(error_message) # Use logging
        st.error(error_message) # Keep UI error
        return []
        
    except Exception as e:
        error_message = f"Unexpected error during get_tools API call: {e}"
        # Replace logging.exception with logger.exception
        bound_logger.exception(error_message)
        # logging.exception(error_message) # Use logging.exception
        st.error(error_message) # Keep UI error
        return []

# Ensure streamlit UI code calls these synchronous functions directly.

# Remove old/unused functions and state
# def stop_mcp_server(): ... (no longer needed, session handles process)
# mcp_server_process = None
# mcp_lock = threading.Lock()
# stderr_thread = None
# stderr_queue = queue.Queue()
# def read_stderr(): ...
# def check_server_stderr(): ...
# def start_mcp_server(): ...
# def get_mcp_client(): ...

# --- Tool Definitions --- #

# @st.cache_data(ttl=3600)  # Cache for 1 hour, adjust as needed
# def get_tool_definitions() -> list[dict] | None:
#     """Fetches tool definitions from the MCP server.
#     Returns a list of tool definitions in OpenAI function-calling format,
#     or None if the server doesn't support tool definition queries.
#     """
#     # Use our refactored execute function
#     result_or_error = execute_mcp_tool("mcp.get_tool_definitions", {})
#
#     # If execute_mcp_tool returned an error string
#     if isinstance(result_or_error, str):
#         # Check if the error indicates the method wasn't found
#         # This requires inspecting the error string or potentially modifying
#         # execute_mcp_tool to return error codes.
#         # Let's check common substrings for now.
#         if "method not found" in result_or_error.lower() or f"(code: {types.METHOD_NOT_FOUND})" in result_or_error.lower():
#             print("Warning: MCP server likely doesn't support mcp.get_tool_definitions")
#             return None # Indicate not supported
#         else:
#             # Log other errors but still return None as definitions aren't available
#             print(f"Error getting tool definitions: {result_or_error}")
#             return None
#
#     # If we got a dictionary back, it should be the successful result
#     elif isinstance(result_or_error, dict):
#         try:
#             # According to our server implementation, the result should be: {'tools': [...]}
#             if "tools" in result_or_error:
#                 tools = result_or_error["tools"]
#                 if isinstance(tools, list):
#                     print(f"Successfully retrieved {len(tools)} tool definitions from server")
#                     return tools
#                 else:
#                     print(f"Unexpected format for 'tools' key in get_tool_definitions result: {type(tools)}")
#                     return None
#             else:
#                 print(f"Missing 'tools' key in successful get_tool_definitions result: {result_or_error}")
#                 return None
#
#         except Exception as e:
#             # Catch errors during result parsing
#             print(f"Error parsing successful tool definitions result: {e}")
#             return None
#     else:
#         # Should not happen if execute_mcp_tool works correctly
#         print(f"Unexpected return type from execute_mcp_tool: {type(result_or_error)}")
#         return None 
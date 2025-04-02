# Placeholder for MCP server interaction logic
# This will handle starting the server subprocess and communicating with it. 

import subprocess
import sys
import os
import asyncio # Import asyncio
import json
from typing import List, Dict, Any, Optional

# Imports based on the provided example and types
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
import mcp.types as types
from mcp.shared.exceptions import McpError
from mcp.server.fastmcp.exceptions import ToolError # Tool-specific errors
import streamlit as st

# --- Configuration --- #

# Calculate the presumed path to the MCP server script
# Assumes nifi_chat_ui and nifi_mcp_server are siblings
current_dir = os.path.dirname(__file__)
DEFAULT_MCP_SERVER_SCRIPT_PATH = os.path.abspath(os.path.join(current_dir, "..", "nifi_mcp_server", "server.py")) # Corrected to server.py based on earlier listing

# Allow overriding via environment variable
MCP_SERVER_SCRIPT_PATH = os.getenv("NIFI_MCP_SERVER_PATH", DEFAULT_MCP_SERVER_SCRIPT_PATH)

# --- Helper Function to Create Server Params --- #

def get_server_params() -> Optional[StdioServerParameters]:
    """Creates StdioServerParameters, checking if the script exists."""
    if not os.path.exists(MCP_SERVER_SCRIPT_PATH):
        st.error(f"MCP Server script not found at: {MCP_SERVER_SCRIPT_PATH}\nPlease ensure it exists or set the NIFI_MCP_SERVER_PATH environment variable.")
        return None
    
    python_executable = sys.executable
    return StdioServerParameters(
        command=python_executable,
        args=[MCP_SERVER_SCRIPT_PATH],
        # env=None, # Add environment vars if needed
        # cwd=os.path.dirname(MCP_SERVER_SCRIPT_PATH) # Set cwd if needed
    )

# --- Tool Execution (Async) --- #

async def execute_mcp_tool_async(tool_name: str, params: dict) -> dict | str:
    """(Async) Executes a tool call via the MCP ClientSession.
    Returns the tool result as a dict or an error message string.
    """
    server_params = get_server_params()
    if not server_params:
        return "Error: MCP Server script path not configured correctly."

    try:
        print(f"Attempting MCP tool call: {tool_name} with params: {params}")
        async with stdio_client(server_params) as (reader, writer):
            async with ClientSession(reader, writer) as session:
                # Initialize connection (might be needed per session)
                await session.initialize()
                print(f"MCP Session initialized for {tool_name}.")

                # Call the tool
                # The session.call_tool method likely returns a CallToolResult object
                # or raises an McpError/ToolError on failure.
                call_tool_result: types.CallToolResult = await session.call_tool(
                    name=tool_name,
                    arguments=params
                )
                print(f"MCP Success Result ({tool_name}): {call_tool_result}")

                # Extract the content part, which is a list of content items
                # We need to decide how to format this for the LLM.
                # Let's return the raw list of content dicts for now.
                # Or maybe serialize the whole CallToolResult?
                # For now, let's try returning the model_dump for simplicity
                # if hasattr(call_tool_result, 'model_dump'):
                #     return call_tool_result.model_dump() 
                # else:
                #     return vars(call_tool_result) # Fallback
                
                # Let's try returning the content directly, assuming it's what the LLM needs
                content_list = []
                for content_item in call_tool_result.content:
                    if hasattr(content_item, 'model_dump'):
                         content_list.append(content_item.model_dump())
                    else:
                         # Fallback for older pydantic or unexpected types
                         content_list.append(vars(content_item)) 
                
                # The LLM likely expects a single dictionary result, not a list.
                # How should we combine multiple content items?
                # Option 1: Return the first text item?
                # Option 2: Concatenate text items?
                # Option 3: Return a dict indicating multiple parts?
                # Let's return a structured dict indicating the content list for now.
                return {"tool_output_content": content_list} 


    except ToolError as e:
        error_message = f"MCP Tool Error ({tool_name}): {e.message} (Code: {e.code})"
        if e.data:
            error_message += f" Data: {json.dumps(e.data)}"
        st.error(error_message)
        print(f"MCP ToolError: {error_message}")
        return error_message # Return error string
    except McpError as e:
        error_message = f"MCP Communication Error ({tool_name}): {e.message} (Code: {e.code})"
        st.error(error_message)
        print(f"MCP McpError: {error_message}")
        return error_message
    except Exception as e:
        # Catch other potential exceptions (e.g., server process crashing, init failure)
        st.error(f"Error during MCP tool execution for {tool_name}: {e}")
        print(f"Exception during MCP execute async for {tool_name}: {e}", exc_info=True) # Log traceback
        return f"Unexpected error during MCP execution: {e}"

# --- Tool Definitions (Async) --- #

async def get_tool_definitions_async() -> Optional[List[Dict[str, Any]]]:
    """(Async) Fetches tool definitions from the MCP server using ClientSession.
    Uses the standard session.list_tools() method.
    Returns a list of tool definitions in OpenAI function-calling format, or None.
    """
    server_params = get_server_params()
    if not server_params:
        print("MCP Server script path not configured correctly for getting definitions.")
        return None

    try:
        print("Attempting to get MCP tool definitions via session.list_tools()...")
        async with stdio_client(server_params) as (reader, writer):
            async with ClientSession(reader, writer) as session:
                await session.initialize()
                print("MCP Session initialized for list_tools.")

                # Use session.list_tools()
                tools_list: List[types.Tool] = await session.list_tools()
                print(f"Successfully retrieved {len(tools_list)} tool definitions from server via list_tools")

                # Convert mcp.types.Tool objects to the dict format expected by LLMs/UI
                formatted_tools = []
                for tool in tools_list:
                    # Ensure inputSchema exists and is a dictionary, default to empty if not
                    input_schema = tool.inputSchema if isinstance(tool.inputSchema, dict) else {}
                    formatted_tools.append({
                        "name": tool.name,
                        "description": tool.description or "",
                        # Use the validated input_schema
                        "parameters": input_schema 
                    })
                return formatted_tools

    except McpError as e:
        # Handle cases where the server might not support list_tools
        # or other communication errors occur during the attempt.
        # We need to check if list_tools itself is the special tool or if it's implicitly supported
        # The example implies list_tools is a standard session method.
        # If the server doesn't implement tools capability, initialize might fail,
        # or list_tools might raise an error.
        print(f"MCP McpError getting tool definitions: {e.message} (Code: {e.code})")
        # Let's assume an error here means definitions aren't available
        st.warning(f"Could not retrieve tool definitions from MCP server: {e.message}")
        return None
    except Exception as e:
        print(f"Unexpected error getting tool definitions: {e}", exc_info=True)
        st.error(f"An unexpected error occurred while fetching tool definitions: {e}")
        return None

# --- Synchronous Wrappers --- #

def execute_mcp_tool(tool_name: str, params: dict) -> dict | str:
    """Synchronous wrapper for execute_mcp_tool_async."""
    try:
        # Check if an event loop is running (e.g., in testing or specific environments)
        loop = asyncio.get_running_loop()
        # If yes, schedule the coroutine
        # This might be needed if Streamlit starts running an event loop internally
        # future = asyncio.run_coroutine_threadsafe(execute_mcp_tool_async(tool_name, params), loop)
        # return future.result() 
        # For standard Streamlit, run should be sufficient
        return asyncio.run(execute_mcp_tool_async(tool_name, params))
    except RuntimeError as e:
        # If no event loop is running, asyncio.run() is appropriate
        if "cannot run nested" in str(e).lower():
             # Handle nested loop issue if it occurs, though less likely now
             print("Warning: Nested asyncio loop detected. Consider restructuring.")
             # Potentially use nest_asyncio or rethink the structure
             # For now, let's just raise to see where it happens
             raise e
        return asyncio.run(execute_mcp_tool_async(tool_name, params))
    except Exception as e:
        print(f"Error in sync wrapper for execute_mcp_tool: {e}", exc_info=True)
        st.error(f"Failed to run MCP tool asynchronously: {e}")
        return f"Internal error running async tool: {e}"

# Restore the @st.cache_data decorator
@st.cache_data(ttl=3600) 
def get_available_tools() -> list[dict]:
    """Synchronous wrapper for get_tool_definitions_async."""
    try:
        # Correctly call asyncio.run() on the async function
        tools = asyncio.run(get_tool_definitions_async())
        return tools if tools is not None else []
    except RuntimeError as e:
        if "cannot run nested" in str(e).lower():
             print("Warning: Nested asyncio loop detected for get_available_tools.")
             # If a loop is already running, we might need a different approach,
             # but let's stick with asyncio.run for now as it's the standard
             # way to call async from sync in simple cases.
             # Alternatively, could try creating a new loop if needed:
             # loop = asyncio.new_event_loop()
             # asyncio.set_event_loop(loop)
             # tools = loop.run_until_complete(get_tool_definitions_async())
             # loop.close()
             # return tools if tools is not None else []
             raise e # Re-raise for now to see if this happens
        # If no loop was running, run should create one
        tools = asyncio.run(get_tool_definitions_async())
        return tools if tools is not None else []
    except Exception as e:
        print(f"Error in sync wrapper for get_available_tools: {e}", exc_info=True)
        st.error(f"Failed to get available tools asynchronously: {e}")
        return []

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
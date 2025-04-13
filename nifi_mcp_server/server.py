import asyncio
# Remove standard logging import
# import logging 
import signal # Add signal import for cleanup
from typing import List, Dict, Optional, Any, Union, Literal
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Body, Request
from fastapi.responses import JSONResponse # Import JSONResponse
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
import os
import sys
from loguru import logger # Import Loguru logger
from docstring_parser import parse # <<< ADD IMPORT HERE

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
from nifi_mcp_server.nifi_client import NiFiClient, NiFiAuthenticationError
# Import flow documentation tools
from nifi_mcp_server.flow_documenter import (
    extract_important_properties,
    analyze_expressions,
    build_graph_structure,
    format_connection,
    find_source_to_sink_paths,
    find_decision_branches
)

# Import MCP server components (Corrected for v1.6.0)
from mcp.server import FastMCP
# Remove non-existent imports
# from mcp.context import ToolContext 
# from mcp.shared.types import ToolExecutionResult
# Corrected error import path based on file inspection for v1.6.0
from mcp.shared.exceptions import McpError # Base error
from mcp.server.fastmcp.exceptions import ToolError # Tool-specific errors

# Configure logging for the server - Set level to DEBUG
# --- REMOVED OLD LOGGING SETUP ---
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger("nifi_mcp_server")
# logger.debug("nifi_mcp_server logger initialized with DEBUG level.")
# --- Using Loguru logger now ---
logger.info("Loguru logger initialized for nifi_mcp_server.") # Example Loguru usage

# Load .env file at module level for potential Uvicorn execution
load_dotenv()

# --- Server Setup ---

# Initialize FastMCP server - name should be descriptive
# Apply version-specific workarounds for MCP 1.6.0 based on Perplexity analysis
mcp = FastMCP(
    "nifi_controller",
    description="An MCP server to interact with Apache NiFi.",
    protocol_version="2024-09-01",  # Explicitly set protocol version
    type_validation_mode="compat",  # Use compatibility mode for type validation
    # json_serializer=lambda x: x     # REMOVED: Let MCP handle default serialization
)

# Instantiate our NiFi API client (uses environment variables for config)
# Consider a more robust way to handle client lifecycle if needed
try:
    nifi_api_client = NiFiClient()
    logger.info("NiFi API Client instantiated.")
except ValueError as e:
    logger.error(f"Failed to instantiate NiFiClient: {e}. Ensure NIFI_API_URL is set.")
    # Decide how to handle this - maybe exit or have tools return errors
    nifi_api_client = None # Mark as unavailable

# --- Helper Function for Authentication (Keep for potential future use, but commented tools won't call it) ---

async def ensure_authenticated():
    """Helper to ensure the NiFi client is authenticated before tool use."""
    if nifi_api_client is None:
        raise ToolError("NiFi Client is not configured properly (check NIFI_API_URL).")
    if not nifi_api_client.is_authenticated:
        logger.info("NiFi client not authenticated. Attempting authentication...")
        try:
            await nifi_api_client.authenticate()
            logger.info("Authentication successful via MCP tool request.")
        except NiFiAuthenticationError as e:
            logger.error(f"Authentication failed during tool execution: {e}")
            # Raise ToolError, but indicate user action needed in the message
            raise ToolError(
                f"NiFi authentication failed ({e}). Please ensure NIFI_USERNAME and NIFI_PASSWORD "
                "are correctly set in the server's environment/.env file."
            ) from e
        except Exception as e:
            logger.error(f"Unexpected error during authentication: {e}", exc_info=True)
            raise ToolError(f"An unexpected error occurred during NiFi authentication: {e}")
    pass # Add pass to avoid syntax error if body is empty


# --- NiFi Tools ---

# --- Helper Functions for list_nifi_objects ---

def _format_processor_summary(processors_data):
    """Formats basic processor data from NiFi API list response."""
    formatted = []
    if processors_data: # Already a list from list_processors
        for proc in processors_data:
            component = proc.get('component', {})
            status = proc.get('status', {})
            # Use the existing filter function for consistency, can enhance later
            basic_info = filter_processor_data(proc) 
            formatted.append(basic_info)
    return formatted

def _format_connection_summary(connections_data):
    """Formats basic connection data from NiFi API list response."""
    formatted = []
    if connections_data: # Already a list from list_connections
        for conn in connections_data:
             # Use existing filter function
            basic_info = filter_connection_data(conn)
            formatted.append(basic_info)
    return formatted

def _format_port_summary(input_ports_data, output_ports_data):
    """Formats and combines input and output port data for summary list."""
    formatted = []
    # Process Input Ports
    if input_ports_data: # Already a list
        for port in input_ports_data:
            component = port.get('component', {})
            status = port.get('status', {})
            formatted.append({
                "id": port.get('id'),
                "name": component.get('name'),
                "type": "INPUT_PORT",
                "state": component.get('state'),
                "comments": component.get('comments'),
                "concurrent_tasks": component.get('concurrentlySchedulableTaskCount'),
                "validation_errors": component.get('validationErrors'),
                "active_thread_count": status.get('aggregateSnapshot', {}).get('activeThreadCount'), # Check nesting
                "queued_count": status.get('aggregateSnapshot', {}).get('flowFilesQueued'),
                "queued_size": status.get('aggregateSnapshot', {}).get('bytesQueued'),
            })
    # Process Output Ports
    if output_ports_data: # Already a list
        for port in output_ports_data:
            component = port.get('component', {})
            status = port.get('status', {})
            formatted.append({
                "id": port.get('id'),
                "name": component.get('name'),
                "type": "OUTPUT_PORT",
                "state": component.get('state'),
                "comments": component.get('comments'),
                "concurrent_tasks": component.get('concurrentlySchedulableTaskCount'),
                "validation_errors": component.get('validationErrors'),
                "active_thread_count": status.get('aggregateSnapshot', {}).get('activeThreadCount'), # Check nesting
                "queued_count": status.get('aggregateSnapshot', {}).get('flowFilesQueued'),
                "queued_size": status.get('aggregateSnapshot', {}).get('bytesQueued'),
            })
    return formatted

async def _get_process_group_contents_counts(pg_id: str, nifi_client: NiFiClient, local_logger) -> Dict[str, int]:
    """Fetches counts of components within a specific process group."""
    counts = {"processors": 0, "connections": 0, "ports": 0, "process_groups": 0}
    try:
        # Attempt to use the more efficient /flow endpoint first
        # --- Log NiFi Request (Get Flow) ---
        nifi_req = {"operation": "get_process_group_flow", "process_group_id": pg_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API (for counts)")
        # ----------------------------------
        pg_flow_details = await nifi_client.get_process_group_flow(pg_id)
        # --- Log NiFi Response (Get Flow) ---
        # Log only presence of flow data
        nifi_resp = {"has_flow_details": bool(pg_flow_details and 'processGroupFlow' in pg_flow_details)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp).debug("Received from NiFi API (for counts)")
        # -----------------------------------
        if pg_flow_details and 'processGroupFlow' in pg_flow_details:
            flow_content = pg_flow_details['processGroupFlow'].get('flow', {})
            counts["processors"] = len(flow_content.get('processors', []))
            counts["connections"] = len(flow_content.get('connections', []))
            counts["ports"] = len(flow_content.get('inputPorts', [])) + len(flow_content.get('outputPorts', []))
            counts["process_groups"] = len(flow_content.get('processGroups', []))
            local_logger.debug(f"Got counts for PG {pg_id} via /flow endpoint: {counts}")
            return counts
        else:
             local_logger.warning(f"Could not get counts via /flow for PG {pg_id}, falling back to individual calls.")
             # Fallback: Make individual calls (less efficient)
             processors = await nifi_client.list_processors(pg_id)
             connections = await nifi_client.list_connections(pg_id)
             input_ports = await nifi_client.get_input_ports(pg_id)
             output_ports = await nifi_client.get_output_ports(pg_id)
             process_groups = await nifi_client.get_process_groups(pg_id)
             counts["processors"] = len(processors) if processors else 0
             counts["connections"] = len(connections) if connections else 0
             counts["ports"] = (len(input_ports) if input_ports else 0) + (len(output_ports) if output_ports else 0)
             counts["process_groups"] = len(process_groups) if process_groups else 0
             local_logger.debug(f"Got counts for PG {pg_id} via individual calls: {counts}")
             return counts
             
    except (ConnectionError, ValueError, NiFiAuthenticationError) as e:
        # Log specific API errors during count fetching but don't fail the whole hierarchy
        local_logger.error(f"Error fetching counts for PG {pg_id}: {e}")
        # Log NiFi error response
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (for counts)")
        return counts # Return zero counts on error
    except Exception as e:
         local_logger.error(f"Unexpected error fetching counts for PG {pg_id}: {e}", exc_info=True)
         # Log NiFi error response
         local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API (for counts)")
         return counts # Return zero counts


async def _get_process_group_name(pg_id: str, nifi_client: NiFiClient, local_logger) -> str:
    """Helper to safely get a process group's name."""
    if pg_id == "root":
        return "Root"
    try:
        details = await nifi_client.get_process_group_details(pg_id)
        return details.get("component", {}).get("name", f"Unnamed PG ({pg_id})")
    except (ConnectionError, ValueError, NiFiAuthenticationError) as e:
        local_logger.warning(f"Could not fetch details for PG {pg_id} to get name: {e}")
        return f"Unknown PG ({pg_id})"
    except Exception as e:
        local_logger.error(f"Unexpected error fetching name for PG {pg_id}: {e}", exc_info=True)
        return f"Error PG ({pg_id})"


async def _list_components_recursively(
    object_type: Literal["processors", "connections", "ports"],
    pg_id: str,
    nifi_client: NiFiClient,
    local_logger
) -> List[Dict]:
    """Recursively lists processors, connections, or ports within a process group hierarchy."""
    all_results = [] # List to store results from all levels
    
    # Get the name of the current process group
    current_pg_name = await _get_process_group_name(pg_id, nifi_client, local_logger)
    
    # Fetch components for the current level
    current_level_objects = []
    try:
        if object_type == "processors":
            raw_objects = await nifi_client.list_processors(pg_id)
            current_level_objects = _format_processor_summary(raw_objects)
        elif object_type == "connections":
            raw_objects = await nifi_client.list_connections(pg_id)
            current_level_objects = _format_connection_summary(raw_objects)
        elif object_type == "ports":
            input_ports = await nifi_client.get_input_ports(pg_id)
            output_ports = await nifi_client.get_output_ports(pg_id)
            current_level_objects = _format_port_summary(input_ports, output_ports)
            
        if current_level_objects:
            all_results.append({
                "process_group_id": pg_id,
                "process_group_name": current_pg_name,
                "objects": current_level_objects
            })
            
    except (ConnectionError, ValueError, NiFiAuthenticationError) as e:
        local_logger.error(f"Error fetching {object_type} for PG {pg_id} during recursion: {e}")
        # Append error info for this PG level instead of objects
        all_results.append({
             "process_group_id": pg_id,
             "process_group_name": current_pg_name,
             "error": f"Failed to retrieve {object_type}: {e}"
        })
    except Exception as e:
        local_logger.error(f"Unexpected error fetching {object_type} for PG {pg_id} during recursion: {e}", exc_info=True)
        all_results.append({
             "process_group_id": pg_id,
             "process_group_name": current_pg_name,
             "error": f"Unexpected error retrieving {object_type}: {e}"
        })

    # Recurse into child process groups
    try:
        child_groups = await nifi_client.get_process_groups(pg_id)
        if child_groups:
            for child_group_entity in child_groups:
                child_id = child_group_entity.get('id')
                if child_id:
                    # Make the recursive call for the child
                    recursive_results = await _list_components_recursively(
                        object_type=object_type,
                        pg_id=child_id,
                        nifi_client=nifi_client,
                        local_logger=local_logger # Pass logger down
                    )
                    # Extend the main list with results from the child hierarchy
                    all_results.extend(recursive_results)
                    
    except (ConnectionError, ValueError, NiFiAuthenticationError) as e:
        local_logger.error(f"Error fetching child groups for PG {pg_id} during recursion: {e}")
        # Add error marker for this level's children fetching
        all_results.append({
             "process_group_id": pg_id,
             "process_group_name": current_pg_name,
             "error_fetching_children": f"Failed to retrieve child groups: {e}"
        })
    except Exception as e:
        local_logger.error(f"Unexpected error fetching child groups for PG {pg_id}: {e}", exc_info=True)
        all_results.append({
             "process_group_id": pg_id,
             "process_group_name": current_pg_name,
             "error_fetching_children": f"Unexpected error retrieving child groups: {e}"
        })
        
    return all_results



async def _get_process_group_hierarchy(
    pg_id: str, 
    nifi_client: NiFiClient, 
    local_logger,
    recursive_search: bool # Add recursive flag
) -> Dict[str, Any]:
    """Fetches the hierarchy starting from pg_id, optionally recursively."""
    hierarchy_data = { "id": pg_id, "name": "Unknown", "child_process_groups": [] }
    try:
        # Get parent group details for name
        parent_name = await _get_process_group_name(pg_id, nifi_client, local_logger)
        hierarchy_data["name"] = parent_name

        # Get immediate child groups
        # --- Log NiFi Request (Get Child Groups) ---
        nifi_req_children = {"operation": "get_process_groups", "process_group_id": pg_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req_children).debug("Calling NiFi API")
        # ------------------------------------------
        child_groups_response = await nifi_client.get_process_groups(pg_id)
        # --- Log NiFi Response (Get Child Groups) ---
        child_count = len(child_groups_response) if child_groups_response else 0
        nifi_resp_children = {"child_group_count": child_count}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp_children).debug("Received from NiFi API")
        # -------------------------------------------
        child_groups_list = child_groups_response # Client returns list directly

        if child_groups_list:
            for child_group_entity in child_groups_list:
                child_id = child_group_entity.get('id')
                child_component = child_group_entity.get('component', {})
                child_name = child_component.get('name', f"Unnamed PG ({child_id})")

                if child_id:
                    # Fetch counts for this child group
                    counts = await _get_process_group_contents_counts(child_id, nifi_client, local_logger)

                    # --- Recursive Call (Conditional) --- 
                    child_data = {
                        "id": child_id,
                        "name": child_name,
                        "counts": counts
                    }
                    
                    # Fetch the hierarchy for the child group itself ONLY if recursive_search is True
                    if recursive_search:
                        local_logger.debug(f"Recursively fetching hierarchy for child PG: {child_id}")
                        child_hierarchy = await _get_process_group_hierarchy(
                            pg_id=child_id, 
                            nifi_client=nifi_client, 
                            local_logger=local_logger, 
                            recursive_search=True # Propagate recursion
                        )
                        # Add the recursively fetched children of this child group
                        child_data["children"] = child_hierarchy.get("child_process_groups", [])
                    # ------------------------------------
                    
                    hierarchy_data["child_process_groups"].append(child_data)

        return hierarchy_data

    except (ConnectionError, ValueError, NiFiAuthenticationError) as e:
        # Handle potential errors during API calls (e.g., invalid pg_id, network issues)
        local_logger.error(f"Error fetching process group hierarchy for {pg_id}: {e}")
        # Log NiFi error response
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        # Return partial data or a specific error structure
        hierarchy_data["error"] = f"Failed to retrieve full hierarchy for process group {pg_id}: {e}"
        return hierarchy_data # Return what we have, with error marker
    except Exception as e:
         local_logger.error(f"Unexpected error fetching hierarchy for {pg_id}: {e}", exc_info=True)
         local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
         hierarchy_data["error"] = f"Unexpected error retrieving hierarchy for {pg_id}: {e}"
         return hierarchy_data


@mcp.tool()
async def list_nifi_objects(
    object_type: Literal["processors", "connections", "ports", "process_groups"],
    process_group_id: str | None = None,
    search_scope: Literal["current_group", "recursive"] = "current_group" # Updated parameter
) -> Union[List[Dict], Dict]:
    """
    Lists NiFi objects or provides a hierarchy view for process groups within a specified scope.

    Parameters
    ----------
    object_type : Literal["processors", "connections", "ports", "process_groups"]
        The type of NiFi objects to list.
        - 'processors': Lists processors with basic details and status.
        - 'connections': Lists connections with basic details and status.
        - 'ports': Lists input and output ports with basic details and status.
        - 'process_groups': Lists child process groups under the target group (see search_scope).
    process_group_id : str | None, optional
        The UUID of the process group to inspect. If None or omitted, defaults to the root process group. (default is None)
    search_scope : Literal["current_group", "recursive"], optional
        Determines the scope of the listing. Defaults to 'current_group'.
        - 'current_group': Lists objects only within the specified `process_group_id`. For 'process_groups', shows only immediate children with counts.
        - 'recursive': For 'processors', 'connections', or 'ports', lists objects in the specified group and all nested subgroups. For 'process_groups', provides the full nested hierarchy including children of children, with counts at each level. (default is "current_group")

    Returns
    -------
    Union[List[Dict], Dict]
        A list or dictionary depending on the object_type and search_scope. See Args descriptions for specifics. Raises ToolError if an API error occurs.
    """
    local_logger = logger.bind(tool_name="list_nifi_objects", object_type=object_type, search_scope=search_scope)
    await ensure_authenticated()

    target_pg_id = process_group_id
    if target_pg_id is None:
        local_logger.info("No process_group_id provided, fetching root process group ID.")
        try:
            # --- Log NiFi Request (Get Root PG ID) ---
            nifi_get_req = {"operation": "get_root_process_group_id"}
            local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
            # ----------------------------------------
            target_pg_id = await nifi_api_client.get_root_process_group_id()
            # --- Log NiFi Response (Get Root PG ID) ---
            nifi_get_resp = {"root_pg_id": target_pg_id}
            local_logger.bind(interface="nifi", direction="response", data=nifi_get_resp).debug("Received from NiFi API")
            # -----------------------------------------
            local_logger.info(f"Using root process group ID: {target_pg_id}")
        except (ConnectionError, ValueError, NiFiAuthenticationError) as e:
            local_logger.error(f"Failed to get root process group ID: {e}", exc_info=True)
            local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
            raise ToolError(f"Failed to determine root process group ID: {e}")
        except Exception as e:
             local_logger.error(f"Unexpected error getting root process group ID: {e}", exc_info=True)
             local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
             raise ToolError(f"An unexpected error occurred determining root process group ID: {e}")

    local_logger = local_logger.bind(process_group_id=target_pg_id) # Bind the final PG ID
    local_logger.info(f"Executing list_nifi_objects for type '{object_type}' in group '{target_pg_id}'")

    try:
        if object_type == "processors":
            if search_scope == "recursive": # Check new parameter value
                local_logger.info(f"Performing recursive search for {object_type} starting from {target_pg_id}")
                # Call the recursive helper
                return await _list_components_recursively(object_type, target_pg_id, nifi_api_client, local_logger)
            else:
                # Original non-recursive logic
                # --- Log NiFi Request --- 
                nifi_req = {"operation": "list_processors", "process_group_id": target_pg_id}
                local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
                # -----------------------
                processors_list = await nifi_api_client.list_processors(target_pg_id)
                # --- Log NiFi Response --- 
                nifi_resp = {"processor_count": len(processors_list)}
                local_logger.bind(interface="nifi", direction="response", data=nifi_resp).debug("Received from NiFi API")
                # -----------------------
                return _format_processor_summary(processors_list)

        elif object_type == "connections":
            if search_scope == "recursive": # Check new parameter value
                local_logger.info(f"Performing recursive search for {object_type} starting from {target_pg_id}")
                # Call the recursive helper
                return await _list_components_recursively(object_type, target_pg_id, nifi_api_client, local_logger)
            else:
                # Original non-recursive logic
                # --- Log NiFi Request ---
                nifi_req = {"operation": "list_connections", "process_group_id": target_pg_id}
                local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
                # -----------------------
                connections_list = await nifi_api_client.list_connections(target_pg_id)
                # --- Log NiFi Response ---
                nifi_resp = {"connection_count": len(connections_list)}
                local_logger.bind(interface="nifi", direction="response", data=nifi_resp).debug("Received from NiFi API")
                # -----------------------
                return _format_connection_summary(connections_list)

        elif object_type == "ports":
            if search_scope == "recursive": # Check new parameter value
                local_logger.info(f"Performing recursive search for {object_type} starting from {target_pg_id}")
                # Call the recursive helper
                return await _list_components_recursively(object_type, target_pg_id, nifi_api_client, local_logger)
            else:
                 # Original non-recursive logic
                # --- Log NiFi Request (Input Ports) ---
                nifi_req_in = {"operation": "get_input_ports", "process_group_id": target_pg_id}
                local_logger.bind(interface="nifi", direction="request", data=nifi_req_in).debug("Calling NiFi API")
                # ------------------------------------
                input_ports_list = await nifi_api_client.get_input_ports(target_pg_id)
                # --- Log NiFi Response (Input Ports) ---
                nifi_resp_in = {"input_port_count": len(input_ports_list)}
                local_logger.bind(interface="nifi", direction="response", data=nifi_resp_in).debug("Received from NiFi API")
                # --------------------------------------

                # --- Log NiFi Request (Output Ports) ---
                nifi_req_out = {"operation": "get_output_ports", "process_group_id": target_pg_id}
                local_logger.bind(interface="nifi", direction="request", data=nifi_req_out).debug("Calling NiFi API")
                # -------------------------------------
                output_ports_list = await nifi_api_client.get_output_ports(target_pg_id)
                # --- Log NiFi Response (Output Ports) ---
                nifi_resp_out = {"output_port_count": len(output_ports_list)}
                local_logger.bind(interface="nifi", direction="response", data=nifi_resp_out).debug("Received from NiFi API")
                # ---------------------------------------
                return _format_port_summary(input_ports_list, output_ports_list)

        elif object_type == "process_groups":
            # Pass the boolean result of the scope check to the hierarchy helper
            is_recursive = (search_scope == "recursive")
            local_logger.info(f"Building process group hierarchy for {target_pg_id} (Recursive: {is_recursive})")
            hierarchy_data = await _get_process_group_hierarchy(target_pg_id, nifi_api_client, local_logger, is_recursive)
            # Log completion of hierarchy build
            local_logger.info(f"Successfully built process group hierarchy for {target_pg_id}")
            return hierarchy_data
        else:
            # Should be caught by Literal, but belt-and-suspenders
            local_logger.error(f"Invalid object_type provided: {object_type}")
            raise ToolError(f"Invalid object_type specified: {object_type}. Must be one of 'processors', 'connections', 'ports', 'process_groups'.")

    except (NiFiAuthenticationError, ConnectionError, ValueError) as e:
        local_logger.error(f"API error listing {object_type}: {e}", exc_info=True)
        # --- Log NiFi Response (Error) --- 
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        # --------------------------------
        raise ToolError(f"Failed to list NiFi {object_type}: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error listing {object_type}: {e}", exc_info=True)
        # --- Log NiFi Response (Error) --- 
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
        # --------------------------------
        raise ToolError(f"An unexpected error occurred listing {object_type}: {e}")


def filter_processor_data(processor):
    """Extract only the essential fields from a processor object"""
    component = processor.get("component", {}) # Added safety get
    status = processor.get("status", {}) # Added safety get
    config = component.get("config", {}) # Get config dict
    properties = config.get("properties", {}) # Get properties from config

    return {
        "id": processor.get("id"),
        "name": component.get("name"),
        "type": component.get("type"),
        "state": component.get("state"),
        "position": processor.get("position"),
        "runStatus": status.get("runStatus"), # Use the safer status get
        "validationStatus": component.get("validationStatus"),
        "validationErrors": component.get("validationErrors", []),
        "relationships": component.get("relationships", []), # Use safer component get
        "properties": properties, # Add properties here

    }

def filter_created_processor_data(processor_entity):
    """Extract only the essential fields from a newly created processor entity"""
    component = processor_entity.get("component", {})
    revision = processor_entity.get("revision", {})
    return {
        "id": processor_entity.get("id"),
        "name": component.get("name"),
        "type": component.get("type"),
        "position": processor_entity.get("position"), # Position is top-level in creation response
        "validationStatus": component.get("validationStatus"),
        "validationErrors": component.get("validationErrors"),
        "version": revision.get("version"), # Get version from revision dict
    }

def filter_connection_data(connection_entity):
    """Extract only the essential identification fields from a connection entity."""
    # Access nested component data safely
    component = connection_entity.get("component", {})
    source = component.get("source", {})
    destination = component.get("destination", {})

    return {
        "id": connection_entity.get("id"),
        "uri": connection_entity.get("uri"),
        "sourceId": source.get("id"),
        "sourceGroupId": source.get("groupId"),
        "sourceType": source.get("type"),
        "sourceName": source.get("name"),
        "destinationId": destination.get("id"),
        "destinationGroupId": destination.get("groupId"),
        "destinationType": destination.get("type"),
        "destinationName": destination.get("name"),
        "name": component.get("name"), # Get connection name safely
        "selectedRelationships": component.get("selectedRelationships"),
        "availableRelationships": component.get("availableRelationships"),
    }

def filter_port_data(port_entity):
    """Extract essential fields from a port entity (input or output)."""
    component = port_entity.get("component", {})
    revision = port_entity.get("revision", {})
    return {
        "id": port_entity.get("id"),
        "name": component.get("name"),
        "type": component.get("type"), # Will be INPUT_PORT or OUTPUT_PORT
        "state": component.get("state"),
        "position": port_entity.get("position"),
        "comments": component.get("comments"),
        "allowRemoteAccess": component.get("allowRemoteAccess"),
        "concurrentlySchedulableTaskCount": component.get("concurrentlySchedulableTaskCount"),
        "validationStatus": component.get("validationStatus"),
        "validationErrors": component.get("validationErrors"),
        "version": revision.get("version"),
    }

def filter_process_group_data(pg_entity):
    """Extract essential fields from a process group entity."""
    component = pg_entity.get("component", {})
    revision = pg_entity.get("revision", {})
    status = pg_entity.get("status", {}).get("aggregateSnapshot", {}) # Status is nested
    return {
        "id": pg_entity.get("id"),
        "name": component.get("name"),
        "position": pg_entity.get("position"),
        "comments": component.get("comments"),
        "parameterContext": component.get("parameterContext", {}).get("id"), # Just the ID
        "flowfileConcurrency": component.get("flowfileConcurrency"),
        "flowfileOutboundPolicy": component.get("flowfileOutboundPolicy"),
        # Basic status counts if available from creation response
        "runningCount": status.get("runningCount"), 
        "stoppedCount": status.get("stoppedCount"),
        "invalidCount": status.get("invalidCount"),
        "disabledCount": status.get("disabledCount"),
        "activeRemotePortCount": status.get("activeRemotePortCount"),
        "inactiveRemotePortCount": status.get("inactiveRemotePortCount"),
        "version": revision.get("version"),
    }

@mcp.tool()
async def create_nifi_processor(
    processor_type: str,
    name: str,
    position_x: int,
    position_y: int,
    process_group_id: str | None = None, # Use pipe syntax
    # Add config later if needed
    # config: Optional[Dict[str, Any]] = None
) -> Dict:
    """
    Creates a new processor within a specified NiFi process group.

    If process_group_id is not provided, it will attempt to create the processor
    in the root process group.

    Args:
        processor_type: The fully qualified Java class name of the processor type (e.g., "org.apache.nifi.processors.standard.GenerateFlowFile").
        name: The desired name for the new processor instance.
        position_x: The desired X coordinate for the processor on the canvas.
        position_y: The desired Y coordinate for the processor on the canvas.
        process_group_id: The UUID of the process group where the processor should be created. Defaults to the root group if None.
        # config: An optional dictionary representing the processor's configuration properties.

    Returns:
        A dictionary representing the result, including status and the created entity.
    """
    local_logger = logger.bind(tool_name="create_nifi_processor")
    await ensure_authenticated() # Ensure we are logged in

    target_pg_id = process_group_id
    if target_pg_id is None:
        local_logger.info("No process_group_id provided for creation, fetching root process group ID.")
        try:
            # --- Log NiFi Request --- 
            nifi_request_data = {"operation": "get_root_process_group_id"}
            local_logger.bind(interface="nifi", direction="request", data=nifi_request_data).debug("Calling NiFi API")
            # -----------------------
            target_pg_id = await nifi_api_client.get_root_process_group_id()
            # --- Log NiFi Response --- 
            nifi_response_data = {"root_pg_id": target_pg_id}
            local_logger.bind(interface="nifi", direction="response", data=nifi_response_data).debug("Received from NiFi API")
            # -----------------------
        except Exception as e:
            local_logger.error(f"Failed to get root process group ID: {e}", exc_info=True)
            # --- Log NiFi Response (Error) --- 
            local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
            # --------------------------------
            raise ToolError(f"Failed to determine root process group ID for creation: {e}")

    position = {"x": position_x, "y": position_y}
    local_logger.info(f"Executing create_nifi_processor: Type='{processor_type}', Name='{name}', Position={position} in group: {target_pg_id}")

    try:
        # --- Log NiFi Request --- 
        nifi_request_data = {
            "operation": "create_processor", 
            "process_group_id": target_pg_id,
            "processor_type": processor_type,
            "name": name,
            "position": position,
            "config": None # Log config when implemented
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_request_data).debug("Calling NiFi API")
        # -----------------------
        processor_entity = await nifi_api_client.create_processor(
            process_group_id=target_pg_id,
            processor_type=processor_type,
            name=name,
            position=position,
            config=None # Add config dict here when implemented
        )
        # --- Log NiFi Response --- 
        # Log filtered data to keep it concise
        nifi_response_data = filter_created_processor_data(processor_entity)
        local_logger.bind(interface="nifi", direction="response", data=nifi_response_data).debug("Received from NiFi API")
        # -----------------------
        
        local_logger.info(f"Successfully created processor '{name}' with ID: {processor_entity.get('id', 'N/A')}")
        
        # Check validation status from the response
        component = processor_entity.get("component", {})
        validation_status = component.get("validationStatus", "UNKNOWN")
        validation_errors = component.get("validationErrors", [])
        
        if validation_status == "VALID":
            return {
                "status": "success",
                "message": f"Processor '{name}' created successfully.",
                "entity": nifi_response_data # Return already filtered data
            }
        else:
            error_msg_snippet = f" ({validation_errors[0]})" if validation_errors else ""
            local_logger.warning(f"Processor '{name}' created but is {validation_status}{error_msg_snippet}. Requires configuration or connections.")
            return {
                "status": "warning",
                "message": f"Processor '{name}' created but is currently {validation_status}{error_msg_snippet}. Further configuration or connections likely required.",
                "entity": nifi_response_data # Return already filtered data
            }
            
    except (NiFiAuthenticationError, ConnectionError, ValueError) as e: # Include ValueError for potential client-side validation issues
        local_logger.error(f"API error creating processor: {e}", exc_info=True)
        # --- Log NiFi Response (Error) --- 
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        # --------------------------------
        return {"status": "error", "message": f"Failed to create NiFi processor: {e}", "entity": None}
    except Exception as e:
        local_logger.error(f"Unexpected error creating processor: {e}", exc_info=True)
        # --- Log NiFi Response (Error) --- 
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        # --------------------------------
        return {"status": "error", "message": f"An unexpected error occurred during processor creation: {e}", "entity": None}


@mcp.tool()
async def create_nifi_connection(
    source_id: str,
    relationships: List[str], # ADDED: List of relationship names
    target_id: str,
    # process_group_id: str | None = None, # REMOVED: Automatically determined
    # Add more options like selected_relationships if needed
) -> Dict:
    """
    Creates a connection between two components (processors or ports) within the same NiFi process group,
    selecting one or more relationships from the source component.
    The process group is automatically determined based on the parent group of the source and target components.

    Args:
        source_id: The UUID of the source component (processor, input port, or output port).
        relationships: A non-empty list of relationship names originating from the source component that should be selected for this connection.
        target_id: The UUID of the target component (processor, input port, or output port).
        # process_group_id: REMOVED - The UUID of the process group containing the components.

    Returns:
        A dictionary representing the created connection entity. Raises ToolError if components are not found,
        are in different process groups, or if the API call fails.
    """
    # Update logger binding
    local_logger = logger.bind(tool_name="create_nifi_connection", source_id=source_id, target_id=target_id, relationships=relationships)
    await ensure_authenticated()

    # --- Input Validation ---
    if not relationships:
        raise ToolError("The 'relationships' list cannot be empty. At least one relationship must be provided.")
    if not isinstance(relationships, list) or not all(isinstance(item, str) for item in relationships):
        raise ToolError("Invalid 'relationships' elements. Expected a non-empty list of strings (relationship names).")
    # ------------------------

    source_entity = None
    source_type = None
    target_entity = None
    target_type = None

    # --- 1. Fetch Source Component Details & Determine Type/Parent PG ---
    local_logger.info(f"Fetching details for source component {source_id}...")
    try:
        # Try Processor first
        try:
            source_entity = await nifi_api_client.get_processor_details(source_id)
            source_type = "PROCESSOR"
            local_logger.info(f"Source component {source_id} identified as a PROCESSOR.")
        except ValueError: # Not a processor, try Input Port
            try:
                source_entity = await nifi_api_client.get_input_port_details(source_id)
                source_type = "INPUT_PORT"
                local_logger.info(f"Source component {source_id} identified as an INPUT_PORT.")
            except ValueError: # Not an Input Port, try Output Port
                try:
                    source_entity = await nifi_api_client.get_output_port_details(source_id)
                    source_type = "OUTPUT_PORT"
                    local_logger.info(f"Source component {source_id} identified as an OUTPUT_PORT.")
                except ValueError: # Not found as any type
                    raise ToolError(f"Source component with ID {source_id} not found or is not a connectable type (Processor, Input Port, Output Port).")

        # --- 2. Fetch Target Component Details & Determine Type/Parent PG ---
        local_logger.info(f"Fetching details for target component {target_id}...")
        # Try Processor first
        try:
            target_entity = await nifi_api_client.get_processor_details(target_id)
            target_type = "PROCESSOR"
            local_logger.info(f"Target component {target_id} identified as a PROCESSOR.")
        except ValueError: # Not a processor, try Input Port
            try:
                target_entity = await nifi_api_client.get_input_port_details(target_id)
                target_type = "INPUT_PORT"
                local_logger.info(f"Target component {target_id} identified as an INPUT_PORT.")
            except ValueError: # Not an Input Port, try Output Port
                try:
                    target_entity = await nifi_api_client.get_output_port_details(target_id)
                    target_type = "OUTPUT_PORT"
                    local_logger.info(f"Target component {target_id} identified as an OUTPUT_PORT.")
                except ValueError: # Not found as any type
                     raise ToolError(f"Target component with ID {target_id} not found or is not a connectable type (Processor, Input Port, Output Port).")

        # --- 3. Extract and Validate Parent Process Group IDs --- # REMOVED: Moved inside try block below

        source_parent_pg_id = source_entity.get("component", {}).get("parentGroupId")
        target_parent_pg_id = target_entity.get("component", {}).get("parentGroupId")

        if not source_parent_pg_id or not target_parent_pg_id:
            missing_component = "source" if not source_parent_pg_id else "target"
            local_logger.error(f"Could not determine parent process group ID for {missing_component} component.")
            raise ToolError(f"Could not determine parent process group ID for {missing_component} component '{source_id if missing_component == 'source' else target_id}'.")

        if source_parent_pg_id != target_parent_pg_id:
            local_logger.error(f"Source ({source_parent_pg_id}) and target ({target_parent_pg_id}) components are in different process groups.")
            raise ToolError(f"Source component '{source_id}' (in group {source_parent_pg_id}) and target component '{target_id}' (in group {target_parent_pg_id}) must be in the same process group to connect.")

        common_parent_pg_id = source_parent_pg_id
        local_logger = local_logger.bind(process_group_id=common_parent_pg_id) # Bind the derived PG ID
        local_logger.info(f"Validated that source and target are in the same process group: {common_parent_pg_id}")

    except (ValueError, NiFiAuthenticationError, ConnectionError) as e:
         # Catch errors during detail fetching
         local_logger.error(f"API error occurred while fetching component details: {e}", exc_info=True)
         raise ToolError(f"Failed to fetch details for source/target components: {e}")
    except Exception as e:
         local_logger.error(f"Unexpected error occurred while fetching component details: {e}", exc_info=True)
         raise ToolError(f"An unexpected error occurred while fetching component details: {e}")


    # --- 4. Check for Existing Connections (Duplicate Prevention) ---
    local_logger.info(f"Checking for existing connections between {source_id} and {target_id} in group {common_parent_pg_id}...")
    try:
        # --- Log NiFi Request (List Connections) ---
        nifi_list_req = {"operation": "list_connections", "process_group_id": common_parent_pg_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_list_req).debug("Calling NiFi API (for duplicate check)")
        # ------------------------------------------
        existing_connections = await nifi_api_client.list_connections(common_parent_pg_id)
        # --- Log NiFi Response (List Connections) ---
        nifi_list_resp = {"connection_count": len(existing_connections)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_list_resp).debug("Received from NiFi API (for duplicate check)")
        # -------------------------------------------
        
        for existing_conn_entity in existing_connections:
            existing_comp = existing_conn_entity.get("component", {})
            existing_source = existing_comp.get("source", {})
            existing_dest = existing_comp.get("destination", {})
            
            if existing_source.get("id") == source_id and existing_dest.get("id") == target_id:
                # Duplicate found!
                existing_conn_id = existing_conn_entity.get("id")
                local_logger.warning(f"Duplicate connection detected. Existing connection ID: {existing_conn_id}")
                
                # Get details of the existing connection to report relationships
                try:
                    existing_conn_details = await nifi_api_client.get_connection(existing_conn_id)
                    existing_relationships = existing_conn_details.get("component", {}).get("selectedRelationships", [])
                except Exception as detail_err:
                    local_logger.error(f"Could not fetch details for existing connection {existing_conn_id} during duplicate check: {detail_err}")
                    existing_relationships = ["<error retrieving>"] # Placeholder
                
                error_msg = (
                    f"A connection already exists between source '{source_id}' and target '{target_id}'. "
                    f"Existing connection ID: {existing_conn_id}. "
                    f"Currently selected relationships: {existing_relationships}. "
                    f"Use the 'update_nifi_connection' tool to modify the relationships if needed."
                )
                return {"status": "error", "message": error_msg, "entity": None} # Return error, do not create
                
        local_logger.info("No duplicate connection found. Proceeding with creation.")
        
    except (NiFiAuthenticationError, ConnectionError, ValueError) as e:
        local_logger.error(f"API error listing connections during duplicate check: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (for duplicate check)")
        # Raise ToolError to signal failure during the check phase
        raise ToolError(f"Failed to check for existing connections before creation: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error listing connections during duplicate check: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (for duplicate check)")
        raise ToolError(f"An unexpected error occurred while checking for existing connections: {e}")

    # --- 5. Call NiFi Client to Create Connection --- (Renumbered step)
    # relationships is already a list from the input parameter
    local_logger.info(f"Executing create_nifi_connection: From {source_id} ({source_type}, {relationships}) To {target_id} ({target_type}) in derived group {common_parent_pg_id}")

    try:
        # --- Log NiFi Request (Create Connection) ---
        nifi_create_req = {
            "operation": "create_connection",
            "process_group_id": common_parent_pg_id,
            "source_id": source_id,
            "target_id": target_id,
            "relationships": relationships, # Pass the list directly
            "source_type": source_type,
            "target_type": target_type
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_create_req).debug("Calling NiFi API")
        # ------------------------------------------
        connection_entity = await nifi_api_client.create_connection(
            process_group_id=common_parent_pg_id,
            source_id=source_id,
            target_id=target_id,
            relationships=relationships, # Pass the list directly
            source_type=source_type,
            target_type=target_type
        )
        # --- Log NiFi Response (Create Connection) ---
        # Log the full entity, might be large but useful for debug
        local_logger.bind(interface="nifi", direction="response", data=connection_entity).debug("Received from NiFi API (full details)")
        # -------------------------------------------

        # Filter the result before returning
        filtered_connection = filter_connection_data(connection_entity)

        local_logger.info(f"Successfully created connection with ID: {filtered_connection.get('id', 'N/A')}. Returning filtered details.")
        return filtered_connection # Return the filtered connection details

    except (NiFiAuthenticationError, ConnectionError, ValueError) as e:
        local_logger.error(f"API error creating connection: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"Failed to create NiFi connection: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error creating connection: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"An unexpected error occurred during connection creation: {e}")


@mcp.tool()
async def get_nifi_object_details(
    object_type: Literal["processor", "connection", "port", "process_group"],
    object_id: str
) -> Dict:
    """
    Retrieves the full details and configuration of a specific NiFi object.

    Args:
        object_type: The type of the object ('processor', 'connection', 'port', 'process_group').
        object_id: The UUID of the object to retrieve.

    Returns:
        A dictionary containing the object's full entity representation from the NiFi API.
        Raises ToolError if the object is not found or an API error occurs.
    """
    local_logger = logger.bind(tool_name="get_nifi_object_details", object_type=object_type, object_id=object_id)
    await ensure_authenticated()

    local_logger.info(f"Executing get_nifi_object_details for {object_type} ID: {object_id}")
    try:
        details = None
        operation = f"get_{object_type}_details"
        if object_type == "processor":
            # --- Log NiFi Request --- 
            nifi_req = {"operation": "get_processor_details", "processor_id": object_id}
            local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
            # -----------------------
            details = await nifi_api_client.get_processor_details(object_id)
        
        elif object_type == "connection":
            # --- Log NiFi Request ---
            nifi_req = {"operation": "get_connection", "connection_id": object_id}
            local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
            # -----------------------
            details = await nifi_api_client.get_connection(object_id) # Method name differs slightly
        
        elif object_type == "port":
            # Try input port first
            try:
                 # --- Log NiFi Request (Input Port) ---
                nifi_req_in = {"operation": "get_input_port_details", "port_id": object_id}
                local_logger.bind(interface="nifi", direction="request", data=nifi_req_in).debug("Calling NiFi API (trying input port)")
                # ------------------------------------
                details = await nifi_api_client.get_input_port_details(object_id)
                operation = "get_input_port_details" # Update operation for logging
            except ValueError: # Raised by client on 404
                 local_logger.warning(f"Input port {object_id} not found, trying output port.")
                 # --- Log NiFi Response (Input Port Not Found) ---
                 local_logger.bind(interface="nifi", direction="response", data={"error": "Input port not found"}).debug("Received error from NiFi API")
                 # -------------------------------------------------
                 # Try output port
                 try:
                     # --- Log NiFi Request (Output Port) ---
                     nifi_req_out = {"operation": "get_output_port_details", "port_id": object_id}
                     local_logger.bind(interface="nifi", direction="request", data=nifi_req_out).debug("Calling NiFi API (trying output port)")
                     # -------------------------------------
                     details = await nifi_api_client.get_output_port_details(object_id)
                     operation = "get_output_port_details" # Update operation for logging
                 except ValueError as e_out: # Raised by client on 404 for output port
                     local_logger.warning(f"Output port {object_id} also not found.")
                     # --- Log NiFi Response (Output Port Not Found) ---
                     local_logger.bind(interface="nifi", direction="response", data={"error": "Output port not found"}).debug("Received error from NiFi API")
                     # --------------------------------------------------
                     raise ToolError(f"Port with ID {object_id} not found (checked both input and output).") from e_out
        
        elif object_type == "process_group":
            # --- Log NiFi Request ---
            nifi_req = {"operation": "get_process_group_details", "process_group_id": object_id}
            local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
            # -----------------------
            details = await nifi_api_client.get_process_group_details(object_id)
        
        else:
            # Should be caught by Literal type hint
            local_logger.error(f"Invalid object_type specified: {object_type}")
            raise ToolError(f"Invalid object_type specified: {object_type}")

        # --- Log NiFi Response (Success) ---
        # Log full entity as details are the purpose here
        local_logger.bind(interface="nifi", direction="response", data=details).debug(f"Received {object_type} details from NiFi API")
        # -----------------------------------
        local_logger.info(f"Successfully retrieved details for {object_type} {object_id}")
        return details

    except ValueError as e: # Specific catch for 'not found' from client methods (except port handled above)
        local_logger.warning(f"{object_type.capitalize()} with ID {object_id} not found: {e}")
        # --- Log NiFi Response (Error) ---
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        # --------------------------------
        raise ToolError(f"{object_type.capitalize()} with ID {object_id} not found.") from e
    except (NiFiAuthenticationError, ConnectionError) as e:
        local_logger.error(f"API error getting {object_type} details: {e}", exc_info=True)
        # --- Log NiFi Response (Error) ---
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        # --------------------------------
        raise ToolError(f"Failed to get NiFi {object_type} details: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error getting {object_type} details: {e}", exc_info=True)
        # --- Log NiFi Response (Error) ---
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
        # --------------------------------
        raise ToolError(f"An unexpected error occurred getting {object_type} details: {e}")


@mcp.tool()
async def delete_nifi_object(
    object_type: Literal["processor", "connection", "port", "process_group"],
    object_id: str
) -> Dict:
    """
    Deletes a specific NiFi object after fetching its current revision.

    IMPORTANT: Deletion preconditions apply (e.g., process groups must be empty, components stopped/disconnected).

    Args:
        object_type: The type of object to delete ('processor', 'connection', 'port', 'process_group'). Ports are handled automatically (input/output). Process groups must be empty and stopped. Processors/ports should ideally be stopped and disconnected.
        object_id: The UUID of the object to delete.

    Returns:
        A dictionary indicating success or failure status and a message.
    """
    local_logger = logger.bind(tool_name="delete_nifi_object", object_type=object_type, object_id=object_id)
    await ensure_authenticated()

    local_logger.info(f"Executing delete_nifi_object for {object_type} ID: {object_id}")

    # --- 1. Get current revision --- 
    current_entity = None
    revision = None
    version = None
    port_type_found = None # To track if we found an input or output port

    try:
        local_logger.info("Fetching current details to get revision...")
        if object_type == "processor":
            current_entity = await nifi_api_client.get_processor_details(object_id)
        elif object_type == "connection":
            current_entity = await nifi_api_client.get_connection(object_id)
        elif object_type == "process_group":
             current_entity = await nifi_api_client.get_process_group_details(object_id)
        elif object_type == "port":
            # Try input first
            try:
                current_entity = await nifi_api_client.get_input_port_details(object_id)
                port_type_found = "input"
                local_logger.info("Found object as an input port.")
            except ValueError: # 404 Not Found
                local_logger.warning("Object not found as input port, trying output port...")
                try:
                    current_entity = await nifi_api_client.get_output_port_details(object_id)
                    port_type_found = "output"
                    local_logger.info("Found object as an output port.")
                except ValueError: # 404 Not Found
                    raise ValueError(f"Port with ID {object_id} not found (checked input and output).")
        else:
            # Should not happen with Literal
             raise ToolError(f"Invalid object_type '{object_type}' for deletion.")

        # Extract revision and version
        if current_entity:
            revision = current_entity.get('revision')
            if not revision or not isinstance(revision, dict) or 'version' not in revision:
                 raise ToolError(f"Could not determine valid revision for {object_type} {object_id}. Cannot delete.")
            version = revision.get('version')
            if not isinstance(version, int):
                 raise ToolError(f"Revision version for {object_type} {object_id} is not a valid integer: {version}. Cannot delete.")
        else:
            # This case should be covered by specific ValueErrors below, but as a fallback
            raise ToolError(f"Could not fetch details for {object_type} {object_id}. Cannot delete.")
            
    except ValueError as e: # Catches 404s from the get calls
        local_logger.warning(f"{object_type.capitalize()} {object_id} not found: {e}")
        return {"status": "error", "message": f"{object_type.capitalize()} {object_id} not found."} 
    except (NiFiAuthenticationError, ConnectionError) as e:
        local_logger.error(f"API error getting details for {object_type} {object_id} before delete: {e}", exc_info=True)
        return {"status": "error", "message": f"Failed to get current details for {object_type} {object_id} before deletion: {e}"}
    except Exception as e:
        local_logger.error(f"Unexpected error getting details for {object_type} {object_id} before delete: {e}", exc_info=True)
        return {"status": "error", "message": f"Unexpected error getting details for {object_type} {object_id} before deletion: {e}"}

    # --- 2. Attempt Deletion --- 
    try:
        local_logger.info(f"Attempting deletion with version {version}...")
        delete_successful = False
        operation = f"delete_{object_type}"
        if object_type == "processor":
        # --- Log NiFi Request (Delete) --- 
            nifi_del_req = {"operation": "delete_processor", "processor_id": object_id, "version": version, "clientId": nifi_api_client._client_id}
            local_logger.bind(interface="nifi", direction="request", data=nifi_del_req).debug("Calling NiFi API")
        # ---------------------------------
            delete_successful = await nifi_api_client.delete_processor(object_id, version)
        
        elif object_type == "connection":
            # --- Log NiFi Request (Delete) ---
            nifi_del_req = {"operation": "delete_connection", "connection_id": object_id, "version": version, "clientId": nifi_api_client._client_id}
            local_logger.bind(interface="nifi", direction="request", data=nifi_del_req).debug("Calling NiFi API")
            # ---------------------------------
            delete_successful = await nifi_api_client.delete_connection(object_id, version)
        
        elif object_type == "port":
            if port_type_found == "input":
                operation = "delete_input_port"
                # --- Log NiFi Request (Delete) ---
                nifi_del_req = {"operation": operation, "port_id": object_id, "version": version, "clientId": nifi_api_client._client_id}
                local_logger.bind(interface="nifi", direction="request", data=nifi_del_req).debug("Calling NiFi API")
                # ---------------------------------
                delete_successful = await nifi_api_client.delete_input_port(object_id, version)
            elif port_type_found == "output":
                 operation = "delete_output_port"
                 # --- Log NiFi Request (Delete) ---
                 nifi_del_req = {"operation": operation, "port_id": object_id, "version": version, "clientId": nifi_api_client._client_id}
                 local_logger.bind(interface="nifi", direction="request", data=nifi_del_req).debug("Calling NiFi API")
                 # ---------------------------------
                 delete_successful = await nifi_api_client.delete_output_port(object_id, version)
            # No else needed, already validated port_type_found

        elif object_type == "process_group":
             # --- Log NiFi Request (Delete) ---
             nifi_del_req = {"operation": "delete_process_group", "pg_id": object_id, "version": version, "clientId": nifi_api_client._client_id}
             local_logger.bind(interface="nifi", direction="request", data=nifi_del_req).debug("Calling NiFi API")
             # ---------------------------------
             delete_successful = await nifi_api_client.delete_process_group(object_id, version)

        # --- Log NiFi Response (Delete Result) --- 
        nifi_del_resp = {"deleted_id": object_id, "status": "success" if delete_successful else "failure"}
        local_logger.bind(interface="nifi", direction="response", data=nifi_del_resp).debug(f"Received result from NiFi API ({operation})")
        # -----------------------------------------

        if delete_successful:
            local_logger.info(f"Successfully deleted {object_type} {object_id}")
            return {"status": "success", "message": f"{object_type.capitalize()} {object_id} deleted successfully."}
        else:
            # This might occur if client returns False on 404 during delete (already gone)
            local_logger.warning(f"Deletion call for {object_type} {object_id} returned unsuccessful (may already be deleted).")
            return {"status": "error", "message": f"{object_type.capitalize()} {object_id} could not be deleted (it might have been deleted already or failed silently)."}

    except ValueError as e: # Catches 409 Conflicts from delete calls
        local_logger.error(f"Conflict error deleting {object_type} {object_id}: {e}", exc_info=True)
        # --- Log NiFi Response (Error) ---
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e), "status_code": 409}).debug(f"Received error from NiFi API ({operation})")
        # --------------------------------
        # Construct helpful message
        base_message = f"Failed to delete {object_type} {object_id}: {e}" 
        if object_type in ["processor", "port", "connection"]:
            # Add specific hint for components that need to be stopped/disconnected
            hint = " Ensure the object and any connected components (processors/ports) are stopped and connections are empty before deletion."
            base_message += hint
        elif object_type == "process_group":
            hint = " Ensure the process group is stopped and completely empty (no processors, connections, ports, or child groups)."
            base_message += hint
            
        return {"status": "error", "message": base_message} # Include conflict reason and hint
    except (NiFiAuthenticationError, ConnectionError) as e:
        local_logger.error(f"API error deleting {object_type} {object_id}: {e}", exc_info=True)
        # --- Log NiFi Response (Error) ---
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug(f"Received error from NiFi API ({operation})")
        # --------------------------------
        return {"status": "error", "message": f"API error during deletion of {object_type} {object_id}: {e}"}
    except Exception as e:
        local_logger.error(f"Unexpected error deleting {object_type} {object_id}: {e}", exc_info=True)
        # --- Log NiFi Response (Error) ---
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug(f"Received unexpected error from NiFi API ({operation})")
        # --------------------------------
        return {"status": "error", "message": f"An unexpected error occurred during deletion of {object_type} {object_id}: {e}"}


@mcp.tool()
async def update_nifi_processor_properties(
    processor_id: str,
    processor_config_properties: Dict[str, Any] # Renamed argument
) -> Dict:
    """
    Updates a processor's configuration properties by *replacing* the existing property dictionary.

    It is crucial to fetch the current configuration first using `get_nifi_object_details`
    to understand the existing structure before attempting an update.

    Args:
        processor_id: The UUID of the processor to update.
        processor_config_properties: A complete dictionary representing the desired final state of all properties. e.g. {"Property1": "0", "Property2": "${filename:equalsIgnoreCase('hello.txt')} "}
                                     Cannot be empty.

    Returns:
        A dictionary representing the updated processor entity or an error status. Includes validation status.
    """
    local_logger = logger.bind(tool_name="update_nifi_processor_properties")
    await ensure_authenticated()

    # --- Input Validation ---
    if not processor_config_properties: # Updated check
        error_msg = "The 'processor_config_properties' argument cannot be empty. Please use 'get_nifi_object_details' first to fetch the current configuration, modify it, and then provide the complete desired property dictionary to this tool."
        local_logger.warning(f"Validation failed for update_nifi_processor_properties (processor_id={processor_id}): {error_msg}")
        raise ToolError(error_msg)

    if not isinstance(processor_config_properties, dict): # Updated check
         raise ToolError(f"Invalid 'processor_config_properties' type. Expected a dictionary, got {type(processor_config_properties)}.")
    # ------------------------

    # --- Correction for nested 'properties' key ---
    if isinstance(processor_config_properties, dict) and \
       list(processor_config_properties.keys()) == ["properties"] and \
       isinstance(processor_config_properties["properties"], dict):
        
        original_input = processor_config_properties # Keep original for logging
        processor_config_properties = processor_config_properties["properties"]
        local_logger.warning(f"Detected nested 'properties' key in input for processor {processor_id}. Correcting structure. Original input: {original_input}")
    # --------------------------------------------

    local_logger.info(f"Executing update_nifi_processor_properties for ID: {processor_id}, properties: {processor_config_properties}") # Updated log
    try:
        # --- Get current details for revision and state check ---
        local_logger.info(f"Fetching current details for processor {processor_id} before update.")
        nifi_get_req = {"operation": "get_processor_details", "processor_id": processor_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
        current_entity = await nifi_api_client.get_processor_details(processor_id)
        component_precheck = current_entity.get("component", {})
        current_state = component_precheck.get("state")
        current_revision = current_entity.get("revision") # Get revision here
        # Log minimal necessary info from pre-check
        precheck_resp = {"id": processor_id, "state": current_state, "version": current_revision.get('version') if current_revision else None}
        local_logger.bind(interface="nifi", direction="response", data=precheck_resp).debug("Received from NiFi API (for pre-check)")
        
        # --- State Pre-check ---
        if current_state == "RUNNING":
            error_msg = f"Processor '{component_precheck.get('name', processor_id)}' ({processor_id}) is currently RUNNING. It must be stopped before its properties can be updated."
            local_logger.warning(error_msg)
            return {"status": "error", "message": error_msg, "entity": None}
        # ----------------------
        
        # Check if revision was obtained
        if not current_revision:
             raise ToolError(f"Could not retrieve revision for processor {processor_id}.")
             
        # --- Log NiFi Request (Update Config) ---
        nifi_update_req = {
            "operation": "update_processor_config",
            "processor_id": processor_id,
            "update_type": "properties", # Hardcoded
            "update_data": processor_config_properties # Updated data passed
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API (update processor component)")
        # ----------------------------------------
        updated_entity = await nifi_api_client.update_processor_config(
            processor_id=processor_id,
            update_type="properties",     # Pass type
            update_data=processor_config_properties # Updated data passed
        )
        # --- Log NiFi Response (Update Config) ---
        filtered_updated_entity = filter_created_processor_data(updated_entity) # Reuse filter
        local_logger.bind(interface="nifi", direction="response", data=filtered_updated_entity).debug("Received from NiFi API (update processor component)")
        # -----------------------------------------

        local_logger.info(f"Successfully updated properties for processor {processor_id}")

        # ... (Validation check and return logic as before) ...
        component = updated_entity.get("component", {})
        validation_status = component.get("validationStatus", "UNKNOWN")
        validation_errors = component.get("validationErrors", [])
        name = component.get("name", processor_id)

        if validation_status == "VALID":
            return {
                "status": "success",
                "message": f"Processor '{name}' properties updated successfully.",
                "entity": filtered_updated_entity
            }
        else:
            error_msg_snippet = f" ({validation_errors[0]})" if validation_errors else ""
            local_logger.warning(f"Processor '{name}' properties updated, but validation status is {validation_status}{error_msg_snippet}.")
            return {
                "status": "warning",
                "message": f"Processor '{name}' properties updated, but validation status is {validation_status}{error_msg_snippet}. Check configuration.",
                "entity": filtered_updated_entity
            }

    except ValueError as e: # Catch 'not found' or 'conflict'
        local_logger.warning(f"Error updating processor properties {processor_id}: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update processor component)")
        return {"status": "error", "message": f"Error updating properties for processor {processor_id}: {e}", "entity": None}
    except (NiFiAuthenticationError, ConnectionError) as e:
        local_logger.error(f"API error updating processor properties: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update processor component)")
        return {"status": "error", "message": f"Failed to update NiFi processor properties: {e}", "entity": None}
    except Exception as e:
        local_logger.error(f"Unexpected error updating processor properties: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update processor component)")
        return {"status": "error", "message": f"An unexpected error occurred during processor properties update: {e}", "entity": None}


@mcp.tool()
async def delete_nifi_processor_properties(
    processor_id: str,
    property_names_to_delete: List[str]
) -> Dict:
    """
    Deletes specific properties from a processor's configuration by setting their values to null.

    This tool fetches the current configuration, marks the specified properties for deletion,
    and then submits the update.

    Args:
        processor_id: The UUID of the processor to modify.
        property_names_to_delete: A non-empty list of property names (strings) to delete from the processor's configuration.

    Returns:
        A dictionary representing the updated processor entity or an error status. Includes validation status.
    """
    local_logger = logger.bind(tool_name="delete_nifi_processor_properties", processor_id=processor_id)
    await ensure_authenticated()

    # --- Input Validation ---
    if not property_names_to_delete:
        raise ToolError("The 'property_names_to_delete' list cannot be empty.")
    if not isinstance(property_names_to_delete, list) or not all(isinstance(item, str) for item in property_names_to_delete):
        raise ToolError("Invalid 'property_names_to_delete' type. Expected a non-empty list of strings.")
    # ------------------------

    local_logger.info(f"Preparing to delete properties {property_names_to_delete} for processor {processor_id}")

    try:
        # 1. Get current processor entity to obtain the latest revision and properties
        local_logger.info(f"Fetching current details for processor {processor_id} before deleting properties.")
        # --- Log NiFi Request (Get Details) ---
        nifi_get_req = {"operation": "get_processor_details", "processor_id": processor_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
        # ------------------------------------------
        current_entity = await nifi_api_client.get_processor_details(processor_id)
        # --- Log NiFi Response (Get Details) ---
        # Log full entity as we need config
        local_logger.bind(interface="nifi", direction="response", data=current_entity).debug("Received from NiFi API (full details)")
        # -------------------------------------------

        current_revision = current_entity.get("revision")
        current_component = current_entity.get("component", {})
        current_config = current_component.get("config", {})
        current_properties = current_config.get("properties", {})
        current_state = current_component.get("state") # Get current state

        # --- State Pre-check ---
        if current_state == "RUNNING":
            error_msg = f"Processor '{current_component.get('name', processor_id)}' ({processor_id}) is currently RUNNING. It must be stopped before its properties can be deleted."
            local_logger.warning(error_msg)
            return {"status": "error", "message": error_msg, "entity": None}
        # ----------------------
        
        if not current_revision:
             raise ToolError(f"Could not retrieve revision for processor {processor_id}.")

        # 2. Prepare the modified properties dictionary
        modified_properties = current_properties.copy()
        properties_actually_deleted = []
        for prop_name in property_names_to_delete:
            if prop_name in modified_properties:
                modified_properties[prop_name] = None # Set to None for deletion via NiFi API
                properties_actually_deleted.append(prop_name)
            else:
                local_logger.warning(f"Property '{prop_name}' requested for deletion was not found in the current configuration of processor {processor_id}. Skipping.")

        if not properties_actually_deleted:
            local_logger.warning(f"None of the requested properties {property_names_to_delete} were found for deletion. No update will be sent.")
            # Return a success-like status indicating nothing needed to be done
            filtered_current_entity = filter_created_processor_data(current_entity) # Reuse filter
            return {
                "status": "success",
                "message": f"No properties needed deletion for processor '{current_component.get('name', processor_id)}'. Properties requested ({property_names_to_delete}) were not found.",
                "entity": filtered_current_entity
            }
            
        # 3. Call the update config method with the modified properties
        local_logger.info(f"Attempting to delete properties: {properties_actually_deleted}")
        # --- Log NiFi Request (Update Config) ---
        nifi_update_req = {
            "operation": "update_processor_config",
            "processor_id": processor_id,
            "update_type": "properties",
            "update_data": modified_properties # Send the dict with None values
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API (update processor component)")
        # ----------------------------------------
        updated_entity = await nifi_api_client.update_processor_config(
            processor_id=processor_id,
            update_type="properties",
            update_data=modified_properties
        )
        # --- Log NiFi Response (Update Config) ---
        filtered_updated_entity = filter_created_processor_data(updated_entity) # Reuse filter
        local_logger.bind(interface="nifi", direction="response", data=filtered_updated_entity).debug("Received from NiFi API (update processor component)")
        # -----------------------------------------

        local_logger.info(f"Successfully submitted update to delete properties for processor {processor_id}")

        # 4. Check validation status and return
        component = updated_entity.get("component", {})
        validation_status = component.get("validationStatus", "UNKNOWN")
        validation_errors = component.get("validationErrors", [])
        name = component.get("name", processor_id)

        if validation_status == "VALID":
            return {
                "status": "success",
                "message": f"Processor '{name}' properties ({properties_actually_deleted}) deleted successfully.",
                "entity": filtered_updated_entity
            }
        else:
            error_msg_snippet = f" ({validation_errors[0]})" if validation_errors else ""
            local_logger.warning(f"Processor '{name}' properties deleted, but validation status is {validation_status}{error_msg_snippet}.")
            return {
                "status": "warning",
                "message": f"Processor '{name}' properties ({properties_actually_deleted}) deleted, but validation status is {validation_status}{error_msg_snippet}. Check configuration.",
                "entity": filtered_updated_entity
            }

    except ValueError as e: # Catch 'not found' or 'conflict'
        local_logger.warning(f"Error deleting processor properties {processor_id}: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update processor component)")
        # Distinguish between not found and conflict
        if "not found" in str(e).lower():
             return {"status": "error", "message": f"Processor {processor_id} not found.", "entity": None}
        elif "conflict" in str(e).lower() or "revision mismatch" in str(e).lower():
             return {"status": "error", "message": f"Conflict deleting properties for processor {processor_id}. Revision mismatch: {e}", "entity": None}
        else:
            return {"status": "error", "message": f"Error deleting properties for processor {processor_id}: {e}", "entity": None}
    except (NiFiAuthenticationError, ConnectionError) as e:
        local_logger.error(f"API error deleting processor properties: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update processor component)")
        return {"status": "error", "message": f"Failed to delete NiFi processor properties: {e}", "entity": None}
    except Exception as e:
        local_logger.error(f"Unexpected error deleting processor properties: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update processor component)")
        return {"status": "error", "message": f"An unexpected error occurred during processor properties deletion: {e}", "entity": None}


@mcp.tool()
async def update_nifi_processor_relationships(
    processor_id: str,
    auto_terminated_relationships: List[str]
) -> Dict:
    """
    Updates a processor's auto-terminated relationships list by *replacing* the existing list.

    It is crucial to fetch the current configuration first using `get_nifi_object_details`
    to understand the existing structure before attempting an update.

    Args:
        processor_id: The UUID of the processor to update.
        auto_terminated_relationships: A list containing only the names (strings) of the relationships
                                       that *should* be auto-terminated. Relationships not included
                                       in the list will *not* be auto-terminated. Provide an empty
                                       list `[]` to remove all auto-terminations.

    Returns:
        A dictionary representing the updated processor entity or an error status. Includes validation status.
    """
    local_logger = logger.bind(tool_name="update_nifi_processor_relationships")
    await ensure_authenticated()

    # --- Input Validation ---
    if not isinstance(auto_terminated_relationships, list):
         raise ToolError(f"Invalid 'auto_terminated_relationships' type. Expected a list, got {type(auto_terminated_relationships)}.")
    # Add check for list elements being strings
    if not all(isinstance(item, str) for item in auto_terminated_relationships):
        raise ToolError("Invalid 'auto_terminated_relationships' elements. Expected a list of strings (relationship names).")
    # ------------------------

    local_logger.info(f"Executing update_nifi_processor_relationships for ID: {processor_id}, relationships: {auto_terminated_relationships}")
    try:
        # --- Get current details for revision and state check ---
        local_logger.info(f"Fetching current details for processor {processor_id} before update.")
        nifi_get_req = {"operation": "get_processor_details", "processor_id": processor_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
        current_entity = await nifi_api_client.get_processor_details(processor_id)
        component_precheck = current_entity.get("component", {})
        current_state = component_precheck.get("state")
        current_revision = current_entity.get("revision") # Get revision here too
        # Log minimal necessary info from pre-check
        precheck_resp = {"id": processor_id, "state": current_state, "version": current_revision.get('version') if current_revision else None} # Use processor_id
        local_logger.bind(interface="nifi", direction="response", data=precheck_resp).debug("Received from NiFi API (for pre-check)")
        
        # --- State Pre-check ---
        if current_state == "RUNNING":
            error_msg = f"Processor '{component_precheck.get('name', processor_id)}' ({processor_id}) is currently RUNNING. It must be stopped before its relationships can be updated."
            local_logger.warning(error_msg)
            return {"status": "error", "message": error_msg, "entity": None}
        # ----------------------

        # Check if revision was obtained
        if not current_revision:
            raise ToolError(f"Could not retrieve revision for processor {processor_id}.")
            
        # --- Log NiFi Request (Update Config) ---
        nifi_update_req = {
            "operation": "update_processor_config",
            "processor_id": processor_id,
            "update_type": "auto-terminatedrelationships", # Hardcoded
            "update_data": auto_terminated_relationships
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API (update processor component)")
        # ----------------------------------------
        updated_entity = await nifi_api_client.update_processor_config(
            processor_id=processor_id,
            update_type="auto-terminatedrelationships", # Pass type
            update_data=auto_terminated_relationships   # Pass data
        )
        # --- Log NiFi Response (Update Config) ---
        filtered_updated_entity = filter_created_processor_data(updated_entity) # Reuse filter
        local_logger.bind(interface="nifi", direction="response", data=filtered_updated_entity).debug("Received from NiFi API (update processor component)")
        # -----------------------------------------

        local_logger.info(f"Successfully updated auto-terminated relationships for processor {processor_id}")

        # ... (Validation check and return logic as before) ...
        component = updated_entity.get("component", {})
        validation_status = component.get("validationStatus", "UNKNOWN")
        validation_errors = component.get("validationErrors", [])
        name = component.get("name", processor_id)

        if validation_status == "VALID":
            return {
                "status": "success",
                "message": f"Processor '{name}' auto-terminated relationships updated successfully.",
                "entity": filtered_updated_entity
            }
        else:
            # Usually relationship changes don't cause validation errors, but check just in case
            error_msg_snippet = f" ({validation_errors[0]})" if validation_errors else ""
            local_logger.warning(f"Processor '{name}' auto-terminated relationships updated, but validation status is {validation_status}{error_msg_snippet}.")
            return {
                "status": "warning", # Keep as warning if validation is not VALID
                "message": f"Processor '{name}' auto-terminated relationships updated, but validation status is {validation_status}{error_msg_snippet}.",
                "entity": filtered_updated_entity
            }

    except ValueError as e: # Catch 'not found' or 'conflict'
        local_logger.warning(f"Error updating processor relationships {processor_id}: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update processor component)")
        return {"status": "error", "message": f"Error updating relationships for processor {processor_id}: {e}", "entity": None}
    except (NiFiAuthenticationError, ConnectionError) as e:
        local_logger.error(f"API error updating processor relationships: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update processor component)")
        return {"status": "error", "message": f"Failed to update NiFi processor relationships: {e}", "entity": None}
    except Exception as e:
        local_logger.error(f"Unexpected error updating processor relationships: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update processor component)")
        return {"status": "error", "message": f"An unexpected error occurred during processor relationships update: {e}", "entity": None}


@mcp.tool()
async def update_nifi_connection(
    connection_id: str,
    relationships: List[str]
) -> Dict:
    """
    Updates the selected relationships for a NiFi connection by *replacing* the existing list.

    It is crucial to fetch the current connection details first using `get_nifi_object_details` 
    to ensure the relationships provided are valid for the source component.

    Args:
        connection_id: The UUID of the connection to update.
        relationships: A non-empty list containing the names (strings) of the relationships that *should*
                       be selected for this connection. NiFi requires at least one relationship to be selected.
                       To remove the connection entirely (deselecting all relationships), use the `delete_nifi_object` tool instead.

    Returns:
        A dictionary representing the updated connection entity or an error status.
    """
    local_logger = logger.bind(tool_name="update_nifi_connection", connection_id=connection_id)
    await ensure_authenticated()

    # --- Input Validation ---
    if not isinstance(relationships, list):
         raise ToolError(f"Invalid 'relationships' type. Expected a list, got {type(relationships)}.")
    if not all(isinstance(item, str) for item in relationships):
        raise ToolError("Invalid 'relationships' elements. Expected a list of strings (relationship names).")
    # Add validation to prevent empty list
    if not relationships:
        raise ToolError("The 'relationships' list cannot be empty. NiFi connections require at least one selected relationship. To remove the connection, use the 'delete_nifi_object' tool.")
    # Note: We cannot easily validate relationship names here without knowing the source processor type/details.
    # The NiFi API will handle validation on the PUT request.
    # ------------------------

    local_logger.info(f"Executing update_nifi_connection for ID: {connection_id}, relationships: {relationships}")

    try:
        # 1. Get current connection entity to obtain the latest revision and component details
        local_logger.info(f"Fetching current details for connection {connection_id} before update.")
        # --- Log NiFi Request (Get Connection) ---
        nifi_get_req = {"operation": "get_connection", "connection_id": connection_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
        # ------------------------------------------
        current_entity = await nifi_api_client.get_connection(connection_id)
        # --- Log NiFi Response (Get Connection) ---
        local_logger.bind(interface="nifi", direction="response", data=current_entity).debug("Received from NiFi API (full details)")
        # -------------------------------------------
        current_revision = current_entity["revision"]
        current_component = current_entity["component"]

        # 2. Prepare the update payload
        # Copy the existing component data and modify only selectedRelationships
        update_component = current_component.copy()
        update_component["selectedRelationships"] = relationships
        
        # Construct the full update payload with the fetched revision
        update_payload = {
            "revision": current_revision,
            "component": update_component
        }

        # 3. Make the PUT request using the client method
        local_logger.info(f"Attempting to update connection {connection_id} with new relationships.")
        # --- Log NiFi Request (Update Connection) ---
        nifi_update_req = {
            "operation": "update_connection",
            "connection_id": connection_id,
            # Don't log full payload again, just relationships
            "selectedRelationships": relationships 
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API (update connection)")
        # -----------------------------------------
        updated_entity = await nifi_api_client.update_connection(connection_id, update_payload)
        # --- Log NiFi Response (Update Connection) ---
        filtered_updated_entity = filter_connection_data(updated_entity) # Reuse filter
        local_logger.bind(interface="nifi", direction="response", data=filtered_updated_entity).debug("Received from NiFi API (update connection)")
        # ------------------------------------------

        local_logger.info(f"Successfully updated relationships for connection {connection_id}")

        return {
            "status": "success",
            "message": f"Connection '{connection_id}' relationships updated successfully.",
            "entity": filtered_updated_entity
        }

    except ValueError as e: # Catches 'not found' or 'conflict' from get/update calls
        local_logger.warning(f"Error updating connection {connection_id}: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update connection)")
        # Distinguish between not found and conflict
        if "not found" in str(e).lower():
            return {"status": "error", "message": f"Connection {connection_id} not found.", "entity": None}
        elif "conflict" in str(e).lower() or "revision mismatch" in str(e).lower():
             return {"status": "error", "message": f"Conflict updating connection {connection_id}. Revision mismatch or invalid relationships provided: {e}", "entity": None}
        else:
             return {"status": "error", "message": f"Error updating connection {connection_id}: {e}", "entity": None}
    except (NiFiAuthenticationError, ConnectionError) as e:
        local_logger.error(f"API error updating connection relationships: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update connection)")
        return {"status": "error", "message": f"Failed to update NiFi connection relationships: {e}", "entity": None}
    except Exception as e:
        local_logger.error(f"Unexpected error updating connection relationships: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update connection)")
        return {"status": "error", "message": f"An unexpected error occurred during connection relationships update: {e}", "entity": None}


@mcp.tool()
async def operate_nifi_object(
    object_type: Literal["processor", "port"],
    object_id: str,
    operation_type: Literal["start", "stop"]
) -> Dict:
    """
    Starts or stops a specific NiFi processor or port.

    Args:
        object_type: The type of object to operate on ('processor' or 'port').
        object_id: The UUID of the object.
        operation_type: The operation to perform ('start' or 'stop').

    Returns:
        A dictionary indicating the status (success, warning, error) and potentially the updated entity.
    """
    local_logger = logger.bind(tool_name="operate_nifi_object", object_type=object_type, object_id=object_id, operation_type=operation_type)
    await ensure_authenticated()

    # Map operation type to NiFi state
    target_state = "RUNNING" if operation_type == "start" else "STOPPED"
    
    local_logger.info(f"Executing {operation_type} operation for {object_type} ID: {object_id} (Target State: {target_state})")

    try:
        updated_entity = None
        port_type_found = None # To track if input/output port for logging/errors
        operation_name_for_log = f"update_{object_type}_state"

        if object_type == "processor":
            # --- Pre-check for starting a processor ---
            if operation_type == "start":
                local_logger.info(f"Performing pre-checks for starting processor {object_id}...")
                try:
                    # Fetch details to check validation status and current state
                    # --- Log NiFi Request (Get Details for Pre-check) ---
                    nifi_get_req = {"operation": "get_processor_details", "processor_id": object_id}
                    local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API (for pre-check)")
                    # ---------------------------------------------------
                    proc_details = await nifi_api_client.get_processor_details(object_id)
                    # --- Log NiFi Response (Get Details for Pre-check) ---
                    # Log only relevant parts for pre-check
                    component_precheck = proc_details.get("component", {})
                    precheck_resp = {
                        "id": object_id,
                        "validationStatus": component_precheck.get("validationStatus"),
                        "state": component_precheck.get("state")
                    }
                    local_logger.bind(interface="nifi", direction="response", data=precheck_resp).debug("Received from NiFi API (for pre-check)")
                    # ----------------------------------------------------
                    
                    component = proc_details.get("component", {})
                    validation_status = component.get("validationStatus")
                    current_state = component.get("state")
                    validation_errors = component.get("validationErrors", [])
                    name = component.get("name", object_id)

                    if validation_status != "VALID":
                        error_list_str = ", ".join(validation_errors) if validation_errors else "No specific errors listed."
                        error_msg = f"Processor '{name}' ({object_id}) cannot be started because its validation status is {validation_status}. Errors: [{error_list_str}]"
                        local_logger.warning(error_msg)
                        return {"status": "error", "message": error_msg, "entity": None}
                        
                    if current_state == "DISABLED":
                        error_msg = f"Processor '{name}' ({object_id}) cannot be started because it is currently DISABLED. Enable it first."
                        local_logger.warning(error_msg)
                        return {"status": "error", "message": error_msg, "entity": None}
                        
                    local_logger.info(f"Processor {object_id} pre-checks passed (Validation: {validation_status}, State: {current_state}). Proceeding with start request.")

                except ValueError as e: # Handle processor not found during pre-check
                    local_logger.warning(f"Processor {object_id} not found during start pre-check: {e}")
                    local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (for pre-check)")
                    return {"status": "error", "message": f"Processor {object_id} not found.", "entity": None}
                except Exception as e:
                    local_logger.error(f"Error during start pre-check for processor {object_id}: {e}", exc_info=True)
                    local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (for pre-check)")
                    return {"status": "error", "message": f"Failed pre-start check for processor {object_id}: {e}", "entity": None}
            # --- End of Pre-check ---
            
            # Proceed with the actual state update API call
            # --- Log NiFi Request (Update State) --- 
            nifi_update_req = {"operation": operation_name_for_log, "processor_id": object_id, "state": target_state}
            local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API")
            # ---------------------------------------
            updated_entity = await nifi_api_client.update_processor_state(object_id, target_state)

        elif object_type == "port":
            # --- Pre-check for starting a port ---
            port_details_for_check = None
            if operation_type == "start":
                local_logger.info(f"Performing pre-checks for starting port {object_id}...")
                try:
                    # Fetch details to check validation status and current state
                    # Try input first
                    try:
                        # --- Log NiFi Request (Get Details for Pre-check) ---
                        nifi_get_req = {"operation": "get_input_port_details", "port_id": object_id}
                        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API (trying input port for pre-check)")
                        # ---------------------------------------------------
                        port_details_for_check = await nifi_api_client.get_input_port_details(object_id)
                        port_type_found = "input"
                    except ValueError: # Not input, try output
                        # --- Log NiFi Response (Input Not Found Pre-check) ---
                        local_logger.bind(interface="nifi", direction="response", data={"error": "Input port not found"}).debug("Received error from NiFi API (for pre-check)")
                        # -----------------------------------------------------
                        # --- Log NiFi Request (Get Details for Pre-check) ---
                        nifi_get_req = {"operation": "get_output_port_details", "port_id": object_id}
                        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API (trying output port for pre-check)")
                        # ----------------------------------------------------
                        port_details_for_check = await nifi_api_client.get_output_port_details(object_id)
                        port_type_found = "output"
                        
                    # --- Log NiFi Response (Get Details for Pre-check) ---
                    component_precheck = port_details_for_check.get("component", {})
                    precheck_resp = {
                        "id": object_id,
                        "type": port_type_found,
                        "validationStatus": component_precheck.get("validationStatus"),
                        "state": component_precheck.get("state")
                    }
                    local_logger.bind(interface="nifi", direction="response", data=precheck_resp).debug("Received from NiFi API (for pre-check)")
                    # ----------------------------------------------------
                    
                    component = port_details_for_check.get("component", {})
                    validation_status = component.get("validationStatus")
                    current_state = component.get("state")
                    validation_errors = component.get("validationErrors", [])
                    name = component.get("name", object_id)

                    if validation_status != "VALID":
                        error_list_str = ", ".join(validation_errors) if validation_errors else "No specific errors listed."
                        error_msg = f"Port '{name}' ({object_id}) cannot be started because its validation status is {validation_status}. Errors: [{error_list_str}]"
                        local_logger.warning(error_msg)
                        return {"status": "error", "message": error_msg, "entity": None}
                        
                    if current_state == "DISABLED":
                        error_msg = f"Port '{name}' ({object_id}) cannot be started because it is currently DISABLED. Enable it first."
                        local_logger.warning(error_msg)
                        return {"status": "error", "message": error_msg, "entity": None}

                    local_logger.info(f"Port {object_id} pre-checks passed (Validation: {validation_status}, State: {current_state}). Proceeding with start request.")

                except ValueError as e: # Handle port not found during pre-check (should cover both input/output attempts)
                    local_logger.warning(f"Port {object_id} not found during start pre-check: {e}")
                    local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (for pre-check)")
                    return {"status": "error", "message": f"Port {object_id} not found.", "entity": None}
                except Exception as e:
                    local_logger.error(f"Error during start pre-check for port {object_id}: {e}", exc_info=True)
                    local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (for pre-check)")
                    return {"status": "error", "message": f"Failed pre-start check for port {object_id}: {e}", "entity": None}
            # --- End of Port Pre-check ---
            
            # Need to determine if input or output port first to call correct API
            # This also implicitly fetches the revision needed by the update state methods
            # We might have already found the port type during pre-check
            if port_type_found is None: # If not starting, or pre-check failed unexpectedly, find type now
                local_logger.info("Determining port type (input/output) before changing state...")
                try:
                    _ = await nifi_api_client.get_input_port_details(object_id) 
                    port_type_found = "input"
                    local_logger.info(f"Port {object_id} identified as INPUT port.")
                except ValueError: # Not found as input
                    local_logger.warning(f"Port {object_id} not found as input, trying output.")
                    try:
                        _ = await nifi_api_client.get_output_port_details(object_id) 
                        port_type_found = "output"
                        local_logger.info(f"Port {object_id} identified as OUTPUT port.")
                    except ValueError: # Not found as output either
                        raise ToolError(f"Port with ID {object_id} not found (checked input and output). Cannot change state.")

            # Call the appropriate state update method
            if port_type_found == "input":
                operation_name_for_log = "update_input_port_state"
                # --- Log NiFi Request (Update State) --- 
                nifi_update_req = {"operation": operation_name_for_log, "port_id": object_id, "state": target_state}
                local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API")
                # ---------------------------------------
                updated_entity = await nifi_api_client.update_input_port_state(object_id, target_state)
            elif port_type_found == "output":
                operation_name_for_log = "update_output_port_state"
                # --- Log NiFi Request (Update State) --- 
                nifi_update_req = {"operation": operation_name_for_log, "port_id": object_id, "state": target_state}
                local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API")
                # ---------------------------------------
                updated_entity = await nifi_api_client.update_output_port_state(object_id, target_state)
            # No else needed, port_type_found guaranteed if we reach here
        else:
            raise ToolError(f"Invalid object_type specified: {object_type}. Must be 'processor' or 'port'.")

        # --- Log NiFi Response (Update State) --- 
        # Filter data for logging (using processor filter for now, might need port-specific one)
        filtered_entity = filter_created_processor_data(updated_entity)
        local_logger.bind(interface="nifi", direction="response", data=filtered_entity).debug(f"Received from NiFi API ({operation_name_for_log})")
        # ----------------------------------------
        
        # --- Process Result --- 
        component = updated_entity.get("component", {}) 
        current_state = component.get("state")
        name = component.get("name", object_id)
        validation_status = component.get("validationStatus", "UNKNOWN")
        
        if current_state == target_state:
             action = "started" if operation_type == "start" else "stopped"
             local_logger.info(f"Successfully {action} {object_type} '{name}'.")
             return {"status": "success", "message": f"{object_type.capitalize()} '{name}' {action} successfully.", "entity": filtered_entity}
        else:
            # Handle cases where state didn't change as expected
            if operation_type == "start" and (current_state == "DISABLED" or validation_status != "VALID"):
                local_logger.warning(f"{object_type.capitalize()} '{name}' could not be started. Current state: {current_state}, Validation: {validation_status}.")
                return {"status": "warning", "message": f"{object_type.capitalize()} '{name}' could not be started (State: {current_state}, Validation: {validation_status}). Check configuration and dependencies.", "entity": filtered_entity}
            else:
                 action = "start" if operation_type == "start" else "stop"
                 local_logger.warning(f"{object_type.capitalize()} '{name}' state is {current_state} after {action} request. Expected {target_state}.")
                 return {"status": "warning", "message": f"{object_type.capitalize()} '{name}' is {current_state} after {action} request. Check NiFi UI for details.", "entity": filtered_entity}

    except ValueError as e: # Catches 404s during initial get/revision fetch or 409 conflicts from state update
        local_logger.warning(f"Error operating on {object_type} {object_id}: {e}")
        # Log specific NiFi error if available
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug(f"Received error from NiFi API ({operation_name_for_log})")
        # Provide informative message based on error type
        if "not found" in str(e).lower():
             return {"status": "error", "message": f"{object_type.capitalize()} {object_id} not found.", "entity": None}
        elif "conflict" in str(e).lower():
             return {"status": "error", "message": f"Could not {operation_type} {object_type} {object_id} due to conflict: {e}. Check state and revision.", "entity": None}
        else:
            return {"status": "error", "message": f"Could not {operation_type} {object_type} {object_id}: {e}", "entity": None}
            
    except (NiFiAuthenticationError, ConnectionError) as e:
        local_logger.error(f"API error operating on {object_type} {object_id}: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug(f"Received error from NiFi API ({operation_name_for_log})")
        return {"status": "error", "message": f"Failed to {operation_type} NiFi {object_type}: {e}", "entity": None}
    except Exception as e:
        local_logger.error(f"Unexpected error operating on {object_type} {object_id}: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug(f"Received error from NiFi API ({operation_name_for_log})")
        return {"status": "error", "message": f"An unexpected error occurred operating on {object_type} {object_id}: {e}", "entity": None}


@mcp.tool()
async def document_nifi_flow(
    process_group_id: str | None = None,
    starting_processor_id: str | None = None,
    max_depth: int = 10,
    include_properties: bool = True,
    include_descriptions: bool = True
) -> Dict[str, Any]:
    """
    Documents a NiFi flow by traversing processors and connections within a specified process group.

    Parameters
    ----------
    process_group_id : str | None, optional
        The UUID of the process group to document. If None or omitted, defaults to the root process group. (default is None)
    starting_processor_id : str | None, optional
        Optional. The UUID of a processor to begin traversal from. If provided, documentation will be limited to components reachable within `max_depth` steps (incoming and outgoing) from this processor. If None, documents all components directly within the `process_group_id`. (default is None)
    max_depth : int, optional
        Maximum depth to traverse connections when `starting_processor_id` is specified. Defaults to 10. Ignored if `starting_processor_id` is None. (default is 10)
    include_properties : bool, optional
        Whether to include extracted key processor properties, dynamic properties, and expression analysis in the documentation. Defaults to True. (default is True)
    include_descriptions : bool, optional
        Whether to include processor description/comment fields in the documentation. Defaults to True. (default is True)

    Returns
    -------
    Dict[str, Any]
        A dictionary containing the flow documentation, including processors, connections, graph structure summary, identified paths, decision points, and parameter context (if `include_properties` is True).
    """
    local_logger = logger.bind(tool_name="document_nifi_flow") # Add bound logger
    await ensure_authenticated()

    # Get data from NiFi
    target_pg_id = process_group_id
    if target_pg_id is None:
        local_logger.info("No process_group_id provided, fetching root process group ID.") # Use local_logger
        try:
            target_pg_id = await nifi_api_client.get_root_process_group_id()
        except Exception as e:
            local_logger.error(f"Failed to get root process group ID: {e}", exc_info=True) # Use local_logger
            raise ToolError(f"Failed to determine root process group ID: {e}")
    
    local_logger = local_logger.bind(process_group_id=target_pg_id) # Re-bind with PG ID
    local_logger.info(f"Starting flow documentation for process group {target_pg_id}.") # Use local_logger
    
    try:
        # Get all processors in the process group
        # --- Log NiFi Request ---
        nifi_req_procs = {"operation": "list_processors", "process_group_id": target_pg_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req_procs).debug("Calling NiFi API")
        # -----------------------
        processors = await nifi_api_client.list_processors(target_pg_id)
        # --- Log NiFi Response ---
        nifi_resp_procs = {"processor_count": len(processors)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp_procs).debug("Received from NiFi API")
        # -----------------------
        
        # --- Log NiFi Request ---
        nifi_req_conns = {"operation": "list_connections", "process_group_id": target_pg_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req_conns).debug("Calling NiFi API")
        # -----------------------
        connections = await nifi_api_client.list_connections(target_pg_id)
        # --- Log NiFi Response ---
        nifi_resp_conns = {"connection_count": len(connections)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp_conns).debug("Received from NiFi API")
        # -----------------------

        # Filter processors if starting_processor_id is provided
        filtered_processors = processors
        if starting_processor_id:
            # Find the starting processor
            start_processor = next((p for p in processors if p["id"] == starting_processor_id), None)
            if not start_processor:
                raise ToolError(f"Starting processor with ID {starting_processor_id} not found")
            
            # Build graph and perform traversal to find connected processors
            processor_map = {p["id"]: p for p in processors}
            graph = build_graph_structure(processors, connections)
            
            # Build a set of processor IDs to include (breadth-first search)
            included_processors = set([starting_processor_id])
            to_visit = [starting_processor_id]
            visited = set()
            depth = 0
            
            while to_visit and depth < max_depth:
                current_level = to_visit
                to_visit = []
                depth += 1
                
                for proc_id in current_level:
                    visited.add(proc_id)
                    
                    # Add outgoing connections
                    if proc_id in graph["outgoing"]:
                        for conn in graph["outgoing"][proc_id]:
                            dest_id = conn["destinationId"] if "destinationId" in conn else conn["destination"]["id"]
                            if dest_id not in visited and dest_id not in to_visit:
                                included_processors.add(dest_id)
                                to_visit.append(dest_id)
                    
                    # Add incoming connections
                    if proc_id in graph["incoming"]:
                        for conn in graph["incoming"][proc_id]:
                            src_id = conn["sourceId"] if "sourceId" in conn else conn["source"]["id"]
                            if src_id not in visited and src_id not in to_visit:
                                included_processors.add(src_id)
                                to_visit.append(src_id)
            
            # Filter processors and connections
            filtered_processors = [p for p in processors if p["id"] in included_processors]
            filtered_connections = [
                c for c in connections if 
                (c["sourceId"] if "sourceId" in c else c["source"]["id"]) in included_processors and
                (c["destinationId"] if "destinationId" in c else c["destination"]["id"]) in included_processors
            ]
        else:
            filtered_connections = connections
        
        # Enrich processor data with important properties and expressions
        enriched_processors = []
        for processor in filtered_processors:
            proc_data = {
                "id": processor["id"],
                "name": processor["component"]["name"],
                "type": processor["component"]["type"],
                "state": processor["component"]["state"],
                "position": processor["position"],
                "relationships": [r["name"] for r in processor["component"].get("relationships", [])],
                "validation_status": processor["component"].get("validationStatus", "UNKNOWN")
            }
            
            if include_properties:
                # Extract and analyze properties
                property_info = extract_important_properties(processor)
                proc_data["properties"] = property_info["key_properties"]
                proc_data["dynamic_properties"] = property_info["dynamic_properties"]
                
                # Analyze expressions
                proc_data["expressions"] = analyze_expressions(property_info["all_properties"])
            
            if include_descriptions:
                proc_data["description"] = processor["component"].get("config", {}).get("comments", "")
            
            enriched_processors.append(proc_data)
        
        # Build graph structure for the filtered processors
        processor_map = {p["id"]: p for p in filtered_processors}
        graph = build_graph_structure(filtered_processors, filtered_connections)
        
        # Find common paths and decision points
        paths = find_source_to_sink_paths(processor_map, graph)
        decision_points = find_decision_branches(processor_map, graph)
        
        # Format connections
        formatted_connections = [format_connection(c, processor_map) for c in filtered_connections]
        
        # Assemble result
        result = {
            "processors": enriched_processors,
            "connections": formatted_connections,
            "graph_structure": {
                "outgoing_count": {p_id: len(conns) for p_id, conns in graph["outgoing"].items()},
                "incoming_count": {p_id: len(conns) for p_id, conns in graph["incoming"].items()}
            },
            "common_paths": paths,
            "decision_points": decision_points
        }
        
        # Include parameter context if available
        if include_properties:
            parameters = await nifi_api_client.get_parameter_context(target_pg_id)
            if parameters:
                result["parameters"] = parameters
        
        local_logger.info(f"Successfully documented flow for process group {target_pg_id}.") # Use local_logger
        return result
        
    except (NiFiAuthenticationError, ConnectionError) as e:
        local_logger.error(f"API error documenting flow: {e}", exc_info=True) # Use local_logger
        raise ToolError(f"Failed to document NiFi flow: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error documenting flow: {e}", exc_info=True) # Use local_logger
        raise ToolError(f"An unexpected error occurred while documenting the flow: {e}")

# Keep cleanup function
async def cleanup():
    """Perform cleanup tasks on server shutdown."""
    if nifi_api_client:
        logger.info("Closing NiFi API client connection.")
        await nifi_api_client.close()

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
        await ensure_authenticated()
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
async def get_tools(request: Request):
    """Retrieve the list of available MCP tools in OpenAI function format."""
    # Extract context IDs from request state
    user_request_id = request.state.user_request_id
    action_id = request.state.action_id
    
    # Create a bound logger with the context IDs
    bound_logger = logger.bind(user_request_id=user_request_id, action_id=action_id)
    
    try:
        bound_logger.debug(f"Inspecting mcp object attributes: {dir(mcp)}") # Keep debug for now
        formatted_tools = []
        # Access the ToolManager instance
        tool_manager = getattr(mcp, '_tool_manager', None)
        if tool_manager:
            # Call the ToolManager's list_tools method
            tools_info = tool_manager.list_tools() # Assuming this returns ToolInfo objects or similar
            
            for tool_info in tools_info: 
                tool_name = getattr(tool_info, 'name', 'unknown')
                # Get the full docstring (seems to be stored in 'description' by MCP here)
                raw_docstring = getattr(tool_info, 'description', '')
                # Parse the docstring
                parsed_docstring = parse(raw_docstring)
                # Extract just the short description for the main tool description
                base_description = parsed_docstring.short_description or raw_docstring.split('\n\n')[0]
                returns_description = ""
                if parsed_docstring.returns and parsed_docstring.returns.description:
                    # Format the returns section nicely
                    returns_description = f"\n\n**Returns:**\n{parsed_docstring.returns.description}"
                    
                tool_description = f"{base_description}{returns_description}"
                
                # Create a map of param name -> parsed description
                param_descriptions = {p.arg_name: p.description for p in parsed_docstring.params}
                
                # Extract only the necessary parts for the schema
                raw_params_schema = getattr(tool_info, 'parameters', {})
                
                # Build the schema explicitly for OpenAI/Gemini compatibility
                parameters_schema = {
                    "type": "object",
                    "properties": {}, # Initialize empty properties
                }
                raw_properties = raw_params_schema.get('properties', {})
                
                # Iterate through properties and clean them
                cleaned_properties = {}
                if isinstance(raw_properties, dict):
                    for prop_name, prop_schema in raw_properties.items():
                        if isinstance(prop_schema, dict):
                            # Create a copy to avoid modifying the original
                            cleaned_schema = prop_schema.copy()
                            # Remove problematic fields: anyOf, title, default, etc.
                            cleaned_schema.pop('anyOf', None) 
                            cleaned_schema.pop('title', None)
                            cleaned_schema.pop('default', None)  # Also remove default values
                            
                            # Add parsed description
                            cleaned_schema['description'] = param_descriptions.get(prop_name, '')
                            
                            cleaned_properties[prop_name] = cleaned_schema
                        else:
                            # Handle cases where a property schema isn't a dict
                            logger.warning(f"Property '{prop_name}' in tool '{tool_name}' has non-dict schema: {prop_schema}. Skipping property.")
                
                parameters_schema["properties"] = cleaned_properties
                
                # Only include required if it's non-empty and properties exist
                required_list = raw_params_schema.get('required', [])
                if required_list and cleaned_properties: # Only add required if there are properties
                     parameters_schema["required"] = required_list
                elif "required" in parameters_schema: # Clean up just in case
                     del parameters_schema["required"]

                # Remove properties/required fields entirely if properties dict is empty
                if not parameters_schema["properties"]:
                     del parameters_schema["properties"]
                     if "required" in parameters_schema: del parameters_schema["required"]

                # Make required properties all strings for now for schema compatibility
                # Check if 'required' exists before trying to list it
                if 'required' in raw_params_schema:
                    parameters_schema["required"] = list(raw_params_schema['required'])
                
                # Fix enum values to be valid strings
                # Check if properties exist before iterating
                if 'properties' in parameters_schema:
                    for prop_name, prop_data in parameters_schema['properties'].items():
                        if isinstance(prop_data, dict) and 'enum' in prop_data:
                            prop_data['enum'] = [str(val) for val in prop_data['enum']]

                formatted_tools.append({
                    "type": "function",
                    "function": {
                        "name": tool_name,
                        "description": tool_description,
                        "parameters": parameters_schema # Use the cleaned schema
                    }
                })
            bound_logger.info(f"Returning {len(formatted_tools)} tool definitions via ToolManager.")
            return formatted_tools
        else:
            bound_logger.warning("Could not find ToolManager (_tool_manager) on MCP instance.")
            return []
    except Exception as e:
        bound_logger.error(f"Error retrieving tool definitions via ToolManager: {e}", exc_info=True)
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
    # Extract context IDs from headers if present
    user_request_id = request.headers.get("X-Request-ID", "-")
    action_id = request.headers.get("X-Action-ID", "-")
    
    # Log the received context IDs for debugging
    if user_request_id != "-" or action_id != "-":
        logger.debug(f"Received request with context IDs: user_request_id={user_request_id}, action_id={action_id}")
    
    # Set some request state values we can access in route handlers
    request.state.user_request_id = user_request_id
    request.state.action_id = action_id
    
    # Proceed with the request
    response = await call_next(request)
    return response

@app.post("/tools/{tool_name}")
async def execute_tool(tool_name: str, payload: ToolExecutionPayload, request: Request) -> Dict[str, Any]:
    """Execute a specified MCP tool with the given arguments via ToolManager."""
    # Extract context IDs from both payload and headers (headers take precedence)
    user_request_id = request.state.user_request_id
    action_id = request.state.action_id
    
    # If context info exists in payload and no header values were found, use payload values
    if payload.context and user_request_id == "-":
        user_request_id = payload.context.user_request_id
    if payload.context and action_id == "-":
        action_id = payload.context.action_id
    
    # Bind the context IDs to logger for this request
    bound_logger = logger.bind(user_request_id=user_request_id, action_id=action_id)
    
    bound_logger.info(f"Received request to execute tool '{tool_name}' via ToolManager with arguments: {payload.arguments}")
    
    tool_manager = getattr(mcp, '_tool_manager', None)
    if not tool_manager:
        bound_logger.error("Could not find ToolManager on MCP instance during execution.")
        raise HTTPException(status_code=500, detail="Internal server configuration error: ToolManager not found.")

    try:
        # Ensure NiFi client is authenticated before execution
        await ensure_authenticated() 
        
        # Call the ToolManager's call_tool method - pass along the context IDs as dictionary
        tool_context = {"user_request_id": user_request_id, "action_id": action_id}
        result = await tool_manager.call_tool(tool_name, payload.arguments, context=tool_context)
            
        bound_logger.info(f"Execution of tool '{tool_name}' via ToolManager successful.")
        
        return {"result": result}
        
    except ToolError as e:
        # Log the specific ToolError details with bound context
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
        # Catch missing/invalid arguments specifically
        if isinstance(e, TypeError) and (
            "required positional argument" in str(e) or 
            "missing" in str(e) and "required argument" in str(e) or # More robust check for missing args
            "unexpected keyword argument" in str(e)
        ):
             bound_logger.warning(f"Invalid arguments provided for tool '{tool_name}': {e}") # Log as warning
             return JSONResponse(status_code=422, detail=f"Invalid or missing arguments for tool '{tool_name}': {e}")

        bound_logger.error(f"Unexpected error executing tool '{tool_name}' via ToolManager: {e}", exc_info=True) # Log other errors as error
        # Check if it's a context-related error
        if "Context is not available outside of a request" in str(e):
             bound_logger.error(f"Tool '{tool_name}' likely requires context, which is unavailable in this REST setup.")
             return JSONResponse(status_code=501, detail=f"Tool '{tool_name}' cannot be executed via REST API as it requires MCP context.")
        # For other truly unexpected errors, return 500
        return JSONResponse(status_code=500, detail=f"Internal server error executing tool '{tool_name}'.")

@mcp.tool()
async def create_nifi_port(
    port_type: Literal["input", "output"],
    name: str,
    position_x: int,
    position_y: int,
    process_group_id: str | None = None
) -> Dict:
    """
    Creates a new input or output port within a specified NiFi process group.

    Args:
        port_type: Whether to create an 'input' or 'output' port.
        name: The desired name for the new port.
        position_x: The desired X coordinate for the port on the canvas.
        position_y: The desired Y coordinate for the port on the canvas.
        process_group_id: The UUID of the process group where the port should be created. Defaults to the root group if None.

    Returns:
        A dictionary representing the result, including status and the created port entity.
    """
    local_logger = logger.bind(tool_name="create_nifi_port", port_type=port_type)
    await ensure_authenticated()

    target_pg_id = process_group_id
    if target_pg_id is None:
        local_logger.info("No process_group_id provided, fetching root process group ID.")
        try:
            nifi_get_req = {"operation": "get_root_process_group_id"}
            local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
            target_pg_id = await nifi_api_client.get_root_process_group_id()
            nifi_get_resp = {"root_pg_id": target_pg_id}
            local_logger.bind(interface="nifi", direction="response", data=nifi_get_resp).debug("Received from NiFi API")
        except Exception as e:
            local_logger.error(f"Failed to get root process group ID: {e}", exc_info=True)
            local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
            raise ToolError(f"Failed to determine root process group ID for port creation: {e}")

    position = {"x": position_x, "y": position_y}
    local_logger = local_logger.bind(process_group_id=target_pg_id) 
    local_logger.info(f"Executing create_nifi_port: Type='{port_type}', Name='{name}', Position={position} in group: {target_pg_id}")

    try:
        port_entity = None
        operation_name = f"create_{port_type}_port"
        nifi_create_req = {
            "operation": operation_name,
            "process_group_id": target_pg_id,
            "name": name,
            "position": position
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_create_req).debug("Calling NiFi API")
        
        if port_type == "input":
            port_entity = await nifi_api_client.create_input_port(
                pg_id=target_pg_id,
                name=name,
                position=position
            )
        elif port_type == "output":
            port_entity = await nifi_api_client.create_output_port(
                pg_id=target_pg_id,
                name=name,
                position=position
            )
        else:
            # Should be caught by Literal validation
            raise ToolError(f"Invalid port_type specified: {port_type}")

        # Filter and log response
        filtered_port = filter_port_data(port_entity)
        local_logger.bind(interface="nifi", direction="response", data=filtered_port).debug("Received from NiFi API")
        local_logger.info(f"Successfully created {port_type} port '{name}' with ID: {filtered_port.get('id', 'N/A')}")

        # Check validation status (similar to processor creation)
        validation_status = filtered_port.get("validationStatus", "UNKNOWN")
        validation_errors = filtered_port.get("validationErrors", [])

        if validation_status == "VALID":
            return {
                "status": "success",
                "message": f"{port_type.capitalize()} port '{name}' created successfully.",
                "entity": filtered_port
            }
        else:
            error_msg_snippet = f" ({validation_errors[0]})" if validation_errors else ""
            local_logger.warning(f"{port_type.capitalize()} port '{name}' created but is {validation_status}{error_msg_snippet}.")
            return {
                "status": "warning",
                "message": f"{port_type.capitalize()} port '{name}' created but is currently {validation_status}{error_msg_snippet}. Further configuration or connections likely required.",
                "entity": filtered_port
            }
            
    except (NiFiAuthenticationError, ConnectionError, ValueError) as e:
        local_logger.error(f"API error creating {port_type} port: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"Failed to create NiFi {port_type} port: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error creating {port_type} port: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
        raise ToolError(f"An unexpected error occurred during {port_type} port creation: {e}")


@mcp.tool()
async def create_nifi_process_group(
    name: str,
    position_x: int,
    position_y: int,
    parent_process_group_id: str | None = None
) -> Dict:
    """
    Creates a new, empty process group within a specified parent process group.

    Args:
        name: The desired name for the new process group.
        position_x: The desired X coordinate for the process group on the canvas.
        position_y: The desired Y coordinate for the process group on the canvas.
        parent_process_group_id: The UUID of the parent process group where the new group should be created. Defaults to the root group if None.

    Returns:
        A dictionary representing the result, including status and the created process group entity.
    """
    local_logger = logger.bind(tool_name="create_nifi_process_group")
    await ensure_authenticated()

    target_parent_pg_id = parent_process_group_id
    if target_parent_pg_id is None:
        local_logger.info("No parent_process_group_id provided, fetching root process group ID.")
        try:
            nifi_get_req = {"operation": "get_root_process_group_id"}
            local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
            target_parent_pg_id = await nifi_api_client.get_root_process_group_id()
            nifi_get_resp = {"root_pg_id": target_parent_pg_id}
            local_logger.bind(interface="nifi", direction="response", data=nifi_get_resp).debug("Received from NiFi API")
        except Exception as e:
            local_logger.error(f"Failed to get root process group ID: {e}", exc_info=True)
            local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
            raise ToolError(f"Failed to determine root process group ID for process group creation: {e}")

    position = {"x": position_x, "y": position_y}
    local_logger = local_logger.bind(parent_process_group_id=target_parent_pg_id) 
    local_logger.info(f"Executing create_nifi_process_group: Name='{name}', Position={position} in parent group: {target_parent_pg_id}")

    try:
        operation_name = "create_process_group"
        nifi_create_req = {
            "operation": operation_name,
            "parent_process_group_id": target_parent_pg_id,
            "name": name,
            "position": position
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_create_req).debug("Calling NiFi API")
        
        pg_entity = await nifi_api_client.create_process_group(
            parent_pg_id=target_parent_pg_id,
            name=name,
            position=position
        )
        
        # Filter and log response
        filtered_pg = filter_process_group_data(pg_entity)
        local_logger.bind(interface="nifi", direction="response", data=filtered_pg).debug("Received from NiFi API")
        local_logger.info(f"Successfully created process group '{name}' with ID: {filtered_pg.get('id', 'N/A')}")

        # Process groups are generally valid on creation, but return status for consistency
        return {
            "status": "success",
            "message": f"Process group '{name}' created successfully.",
            "entity": filtered_pg
        }
            
    except (NiFiAuthenticationError, ConnectionError, ValueError) as e:
        local_logger.error(f"API error creating process group: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"Failed to create NiFi process group: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error creating process group: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
        raise ToolError(f"An unexpected error occurred during process group creation: {e}")


def _format_processor_type_summary(processor_type_data: Dict) -> Dict:
    """Formats the processor type data for the lookup tool response."""
    bundle = processor_type_data.get("bundle", {})
    return {
        "type": processor_type_data.get("type"),
        "bundle_group": bundle.get("group"),
        "bundle_artifact": bundle.get("artifact"),
        "bundle_version": bundle.get("version"),
        "description": processor_type_data.get("description"),
        "tags": processor_type_data.get("tags", []), # Ensure tags is a list
    }

@mcp.tool()
async def lookup_nifi_processor_type(
    processor_name: str,
    bundle_artifact_filter: str | None = None
) -> Union[List[Dict], Dict]:
    """
    Looks up available NiFi processor types by display name, returning key details including the full class name.

    Args:
        processor_name: The display name (often the 'title' in NiFi UI, not the full Java class) of the processor type to search for (e.g., 'GenerateFlowFile', 'RouteOnAttribute'). Case-insensitive search.
        bundle_artifact_filter: Optional. Filters results to only include types from a specific bundle artifact (e.g., 'nifi-standard-nar', 'nifi-update-attribute-nar'). Case-insensitive search.

    Returns:
        - If one match found: A dictionary with details ('type', 'bundle_*', 'description', 'tags').
        - If multiple matches found: A list of matching dictionaries.
        - If no matches found: An empty list.
    """
    local_logger = logger.bind(tool_name="lookup_nifi_processor_type", processor_name=processor_name, bundle_artifact_filter=bundle_artifact_filter)
    await ensure_authenticated()
    
    local_logger.info(f"Looking up processor type details for name: '{processor_name}'")
    try:
        # --- Log NiFi Request ---
        nifi_req = {"operation": "get_processor_types"}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
        # -----------------------
        all_types = await nifi_api_client.get_processor_types()
        # --- Log NiFi Response ---
        nifi_resp = {"processor_type_count": len(all_types)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp).debug("Received from NiFi API")
        # -----------------------

        matches = []
        search_name_lower = processor_name.lower()
        filter_artifact_lower = bundle_artifact_filter.lower() if bundle_artifact_filter else None

        for proc_type in all_types:
            # Extract display name (adjust key if NiFi API differs, often 'title')
            display_name = proc_type.get("title", proc_type.get("type", "").split('.')[-1]) # Fallback to class name part
            
            if display_name.lower() == search_name_lower:
                # Check bundle filter if provided
                if filter_artifact_lower:
                    bundle = proc_type.get("bundle", {})
                    artifact = bundle.get("artifact", "")
                    if artifact.lower() == filter_artifact_lower:
                        matches.append(_format_processor_type_summary(proc_type))
                else:
                    # No bundle filter, add the match
                    matches.append(_format_processor_type_summary(proc_type))

        local_logger.info(f"Found {len(matches)} match(es) for processor name '{processor_name}' (Filter: {bundle_artifact_filter})")
        
        # Return single dict if one match, list otherwise (even if empty)
        if len(matches) == 1:
            return matches[0]
        else:
            return matches
        
    except (NiFiAuthenticationError, ConnectionError) as e:
        local_logger.error(f"API error looking up processor types: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"Failed to lookup NiFi processor types: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error looking up processor types: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
        raise ToolError(f"An unexpected error occurred looking up processor types: {e}")

@mcp.tool()
async def create_nifi_flow(
    nifi_objects: List[Dict[str, Any]],
    process_group_id: str | None = None
) -> List[Dict[str, Any]]:
    """
    Creates multiple NiFi components (processors, connections) within a specified process group based on a list of definitions.

    This tool attempts to create components sequentially: processors first, then connections.
    If a component fails to create, an error is recorded for that component, but the tool attempts to create subsequent components.
    Connection definitions MUST use the 'name' of the source/target processors defined earlier in the 'nifi_objects' list for 'source_id' and 'target_id'.

    Args:
        nifi_objects: (Required) A list where each dictionary defines a component to create.
                      This list must contain the definitions for ALL components (processors, connections) intended for the new flow.
                      Required fields per component type:
                      - Processor: `type="processor"`, `name` (str), `processor_type` (str, e.g., "org.apache.nifi.processors.standard.GenerateFlowFile"), `position` (dict{"x": int, "y": int}), `properties` (dict, optional).
                      - Connection: `type="connection"`, `source_id` (str, refers to the 'name' of a processor in this list), `target_id` (str, refers to the 'name' of a processor in this list), `relationships` (List[str]).
        process_group_id: The UUID of the process group where the flow should be created. Defaults to the root group if None.

    Example Payload for `nifi_objects`:
    ```json
    [
      {
        "type": "processor",
        "name": "Input Generator",
        "processor_type": "org.apache.nifi.processors.standard.GenerateFlowFile",
        "position": {"x": 0, "y": 0},
        "properties": {
          "File Size": "1 B",
          "Batch Size": "1"
        }
      },
      {
        "type": "processor",
        "name": "Output Logger",
        "processor_type": "org.apache.nifi.processors.standard.LogAttribute",
        "position": {"x": 400, "y": 0},
        "properties": {
          "Log Level": "info",
          "Attributes to Log": "uuid"
        }
      },
      {
        "type": "connection",
        "source_id": "Input Generator",  // Refers to the 'name' above
        "target_id": "Output Logger",    // Refers to the 'name' above
        "relationships": ["success"]
      }
    ]
    ```

    Returns:
        A list containing the results for each object defined in `nifi_objects`, in the same order.
        Each item will be either:
        - A dictionary with filtered details of the successfully created component (similar to 'list_nifi_objects').
        - A dictionary with `{"status": "error", "message": "..."}` indicating why creation failed for that specific component.
    """
    local_logger = logger.bind(tool_name="create_nifi_flow")
    await ensure_authenticated()

    # --- Input Validation (Basic) ---
    if not isinstance(nifi_objects, list) or not nifi_objects:
        raise ToolError("The 'nifi_objects' argument must be a non-empty list.")
    for i, obj_def in enumerate(nifi_objects):
        if not isinstance(obj_def, dict) or "type" not in obj_def:
            raise ToolError(f"Invalid definition at index {i}: Each object must be a dictionary with a 'type' key.")
        obj_type = obj_def.get("type")
        if obj_type == "processor":
            if not all(k in obj_def for k in ["name", "processor_type", "position"]):
                 raise ToolError(f"Invalid processor definition at index {i}: Missing required keys ('name', 'processor_type', 'position').")
            if not isinstance(obj_def.get("position"), dict) or not all(k in obj_def["position"] for k in ["x", "y"]):
                 raise ToolError(f"Invalid processor definition at index {i}: 'position' must be a dict with 'x' and 'y'.")
        elif obj_type == "connection":
            if not all(k in obj_def for k in ["source_id", "target_id", "relationships"]):
                raise ToolError(f"Invalid connection definition at index {i}: Missing required keys ('source_id', 'target_id', 'relationships').")
            if not isinstance(obj_def.get("relationships"), list) or not obj_def["relationships"]:
                 raise ToolError(f"Invalid connection definition at index {i}: 'relationships' must be a non-empty list.")
        # Add checks for other types if supported later (ports, PGs)
        elif obj_type not in ["processor", "connection"]:
             raise ToolError(f"Invalid definition at index {i}: Unsupported object 'type': {obj_type}.")
    # -----------------------------

    # --- Determine Target PG ---
    target_pg_id = process_group_id
    if target_pg_id is None:
        local_logger.info("No process_group_id provided, fetching root process group ID.")
        try:
            nifi_get_req = {"operation": "get_root_process_group_id"}
            local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
            target_pg_id = await nifi_api_client.get_root_process_group_id()
            nifi_get_resp = {"root_pg_id": target_pg_id}
            local_logger.bind(interface="nifi", direction="response", data=nifi_get_resp).debug("Received from NiFi API")
        except Exception as e:
            local_logger.error(f"Failed to get root process group ID: {e}", exc_info=True)
            local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
            # If we can't get the root PG, we can't proceed
            raise ToolError(f"Failed to determine root process group ID: {e}")
    local_logger = local_logger.bind(process_group_id=target_pg_id)
    # ---------------------------

    id_mapping = {} # Maps input 'name' to generated NiFi UUID
    # Initialize results list with None placeholders, same length as input
    results_list = [None] * len(nifi_objects) 

    # --- Phase 1: Create Processors ---
    local_logger.info("Starting Phase 1: Creating Processors...")
    for index, obj_def in enumerate(nifi_objects):
        if obj_def.get("type") == "processor":
            name = obj_def["name"]
            proc_type = obj_def["processor_type"]
            position = obj_def["position"]
            properties = obj_def.get("properties", {}) # Optional
            
            # Ensure properties is a dict
            if properties is None: properties = {}
            if not isinstance(properties, dict):
                 err_msg = f"Processor '{name}' (index {index}): 'properties' must be a dictionary or null."
                 local_logger.warning(err_msg)
                 results_list[index] = {"status": "error", "message": err_msg}
                 continue # Skip this processor

            local_logger.info(f"Attempting to create processor '{name}' (Type: {proc_type}) at index {index}")
            try:
                # --- Log NiFi Request ---
                nifi_create_req = {
                    "operation": "create_processor", 
                    "process_group_id": target_pg_id,
                    "processor_type": proc_type,
                    "name": name,
                    "position": position,
                    "config": {"properties": properties} if properties else None
                }
                local_logger.bind(interface="nifi", direction="request", data=nifi_create_req).debug("Calling NiFi API")
                # -----------------------
                processor_entity = await nifi_api_client.create_processor(
                    process_group_id=target_pg_id,
                    processor_type=proc_type,
                    name=name,
                    position=position,
                    config=properties # Pass properties dict directly
                )
                # --- Log NiFi Response ---
                filtered_processor = filter_processor_data(processor_entity) # Use the filter that includes properties
                local_logger.bind(interface="nifi", direction="response", data=filtered_processor).debug("Received from NiFi API")
                # -----------------------
                
                created_id = processor_entity.get("id")
                if created_id:
                    id_mapping[name] = created_id # Map name to generated ID
                    local_logger.info(f"Successfully created processor '{name}' with ID: {created_id}. Mapping name to ID.")
                    results_list[index] = filtered_processor # Store filtered success result
                else:
                     # Should not happen if create_processor was successful, but handle defensively
                     err_msg = f"Processor '{name}' (index {index}): Creation reported success but no ID found in response."
                     local_logger.error(err_msg)
                     results_list[index] = {"status": "error", "message": err_msg}
            
            except (NiFiAuthenticationError, ConnectionError, ValueError, ToolError) as e:
                err_msg = f"Processor '{name}' (index {index}): Failed to create - {type(e).__name__}: {e}"
                local_logger.error(err_msg, exc_info=True)
                local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
                results_list[index] = {"status": "error", "message": err_msg}
            except Exception as e:
                err_msg = f"Processor '{name}' (index {index}): Unexpected error during creation - {type(e).__name__}: {e}"
                local_logger.error(err_msg, exc_info=True)
                local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
                results_list[index] = {"status": "error", "message": err_msg}

    # --- Phase 2: Create Connections ---
    local_logger.info("Starting Phase 2: Creating Connections...")
    # Get all connections once for duplicate checking
    all_existing_connections = []
    try:
        all_existing_connections = await nifi_api_client.list_connections(target_pg_id)
    except Exception as e:
        local_logger.warning(f"Could not list existing connections for duplicate check in {target_pg_id}: {e}. Duplicate check skipped.")

    for index, obj_def in enumerate(nifi_objects):
         if obj_def.get("type") == "connection":
            source_name = obj_def["source_id"] # This is the logical name from input
            target_name = obj_def["target_id"] # This is the logical name from input
            relationships = obj_def["relationships"]
            
            local_logger.info(f"Attempting to create connection from '{source_name}' to '{target_name}' (index {index})")

            # Resolve IDs using the map
            source_uuid = id_mapping.get(source_name)
            target_uuid = id_mapping.get(target_name)

            if not source_uuid:
                err_msg = f"Connection (index {index}): Source processor '{source_name}' not found or failed creation. Cannot create connection."
                local_logger.warning(err_msg)
                results_list[index] = {"status": "error", "message": err_msg}
                continue # Skip this connection

            if not target_uuid:
                err_msg = f"Connection (index {index}): Target processor '{target_name}' not found or failed creation. Cannot create connection."
                local_logger.warning(err_msg)
                results_list[index] = {"status": "error", "message": err_msg}
                continue # Skip this connection
                
            local_logger.debug(f"Resolved connection IDs: {source_name} -> {source_uuid}, {target_name} -> {target_uuid}")

            # --- Duplicate Check ---
            duplicate_found = False
            for existing_conn_entity in all_existing_connections:
                existing_comp = existing_conn_entity.get("component", {})
                existing_source = existing_comp.get("source", {})
                existing_dest = existing_comp.get("destination", {})
                if existing_source.get("id") == source_uuid and existing_dest.get("id") == target_uuid:
                    duplicate_found = True
                    existing_conn_id = existing_conn_entity.get("id")
                    existing_rels = existing_comp.get("selectedRelationships", [])
                    err_msg = (
                        f"Connection (index {index}): A connection already exists between source '{source_name}' ({source_uuid}) "
                        f"and target '{target_name}' ({target_uuid}). Existing ID: {existing_conn_id}, Relationships: {existing_rels}. "
                        f"Skipping creation."
                    )
                    local_logger.warning(err_msg)
                    results_list[index] = {"status": "error", "message": err_msg}
                    break # Stop checking for duplicates for this pair
            if duplicate_found:
                continue # Skip to the next object in nifi_objects
            # --- End Duplicate Check ---

            try:
                # Assuming processors are the only connectable types for now
                # If ports/PGs are added, need to determine source/target type
                source_type = "PROCESSOR"
                target_type = "PROCESSOR"
                
                 # --- Log NiFi Request ---
                nifi_create_req = {
                    "operation": "create_connection",
                    "process_group_id": target_pg_id,
                    "source_id": source_uuid,
                    "target_id": target_uuid,
                    "relationships": relationships,
                    "source_type": source_type,
                    "target_type": target_type
                }
                local_logger.bind(interface="nifi", direction="request", data=nifi_create_req).debug("Calling NiFi API")
                # -----------------------
                connection_entity = await nifi_api_client.create_connection(
                    process_group_id=target_pg_id,
                    source_id=source_uuid,
                    target_id=target_uuid,
                    relationships=relationships,
                    source_type=source_type,
                    target_type=target_type
                )
                # --- Log NiFi Response ---
                filtered_connection = filter_connection_data(connection_entity)
                local_logger.bind(interface="nifi", direction="response", data=filtered_connection).debug("Received from NiFi API")
                # -----------------------
                
                local_logger.info(f"Successfully created connection from '{source_name}' to '{target_name}'.")
                results_list[index] = filtered_connection # Store success result

            except (NiFiAuthenticationError, ConnectionError, ValueError, ToolError) as e:
                err_msg = f"Connection from '{source_name}' to '{target_name}' (index {index}): Failed to create - {type(e).__name__}: {e}"
                local_logger.error(err_msg, exc_info=True)
                local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
                results_list[index] = {"status": "error", "message": err_msg}
            except Exception as e:
                err_msg = f"Connection from '{source_name}' to '{target_name}' (index {index}): Unexpected error during creation - {type(e).__name__}: {e}"
                local_logger.error(err_msg, exc_info=True)
                local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
                results_list[index] = {"status": "error", "message": err_msg}

    # --- Final Result ---
    # Replace any remaining None placeholders with an error (shouldn't happen if logic is correct)
    for i, result in enumerate(results_list):
        if result is None:
             obj_type = nifi_objects[i].get("type", "unknown")
             err_msg = f"Object at index {i} (type: {obj_type}) was not processed."
             local_logger.error(err_msg) # Log internal error
             results_list[i] = {"status": "error", "message": err_msg}

    local_logger.info("Finished creating NiFi flow components.")
    return results_list

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

import asyncio
from typing import List, Dict, Optional, Any, Union, Literal

# Import necessary components from parent/utils
from loguru import logger # Keep global logger for potential module-level logging if needed
# Import mcp ONLY (client comes from context)
from ..core import mcp
# Removed nifi_api_client import
from .utils import (
    tool_phases,
    # ensure_authenticated, # Removed - authentication handled by factory
    _format_processor_summary,
    _format_connection_summary,
    _format_port_summary,
    filter_processor_data # Keep if needed by helpers here
)
# Keep NiFiClient type hint and error imports
from nifi_mcp_server.nifi_client import NiFiClient, NiFiAuthenticationError
from mcp.server.fastmcp.exceptions import ToolError

# Import flow documentation tools specifically needed by document_nifi_flow
from nifi_mcp_server.flow_documenter import (
    extract_important_properties,
    analyze_expressions,
    build_graph_structure,
    format_connection,
    find_decision_branches
)

# Import context variables
from ..request_context import current_nifi_client, current_request_logger # Added
# Import new context variables for IDs
from ..request_context import current_user_request_id, current_action_id # Added


# --- Helper Functions (Now use context vars) --- 

async def _get_process_group_name(pg_id: str) -> str:
    """Helper to safely get a process group's name."""
    # Get client, logger, and IDs from context
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger
    user_request_id = current_user_request_id.get() or "-"
    action_id = current_action_id.get() or "-"

    if not nifi_client:
        local_logger.error("NiFi client not found in context for _get_process_group_name")
        return f"Error PG ({pg_id})"
        
    if pg_id == "root":
        return "Root"
    try:
        # Pass IDs directly to client method
        details = await nifi_client.get_process_group_details(
            pg_id, user_request_id=user_request_id, action_id=action_id
        )
        return details.get("component", {}).get("name", f"Unnamed PG ({pg_id})")
    except (ConnectionError, ValueError, NiFiAuthenticationError) as e:
        local_logger.warning(f"Could not fetch details for PG {pg_id} to get name: {e}")
        return f"Unknown PG ({pg_id})"
    except Exception as e:
        local_logger.error(f"Unexpected error fetching name for PG {pg_id}: {e}", exc_info=True)
        return f"Error PG ({pg_id})"

async def _get_process_group_contents_counts(pg_id: str) -> Dict[str, int]:
    """Fetches counts of components within a specific process group."""
    # Get client, logger, and IDs from context
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger
    user_request_id = current_user_request_id.get() or "-"
    action_id = current_action_id.get() or "-"
    
    if not nifi_client:
        local_logger.error("NiFi client not found in context for _get_process_group_contents_counts")
        return {"processors": -1, "connections": -1, "ports": -1, "process_groups": -1}

    counts = {"processors": 0, "connections": 0, "ports": 0, "process_groups": 0}
    # Extract context IDs from logger for the client calls
    # context = local_logger._context # REMOVED
    # user_request_id = context.get("user_request_id", "-") # REMOVED
    # action_id = context.get("action_id", "-") # REMOVED
    try:
        # Attempt to use the more efficient /flow endpoint first
        nifi_req = {"operation": "get_process_group_flow", "process_group_id": pg_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API (for counts)")
        pg_flow_details = await nifi_client.get_process_group_flow(pg_id) # Flow endpoint doesn't take context IDs
        nifi_resp = {"has_flow_details": bool(pg_flow_details and 'processGroupFlow' in pg_flow_details)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp).debug("Received from NiFi API (for counts)")
        
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
             # Pass IDs explicitly to client methods that need them
             processors = await nifi_client.list_processors(pg_id, user_request_id=user_request_id, action_id=action_id)
             connections = await nifi_client.list_connections(pg_id, user_request_id=user_request_id, action_id=action_id)
             input_ports = await nifi_client.get_input_ports(pg_id) # Doesn't take context IDs
             output_ports = await nifi_client.get_output_ports(pg_id) # Doesn't take context IDs
             process_groups = await nifi_client.get_process_groups(pg_id) # Doesn't take context IDs
             counts["processors"] = len(processors) if processors else 0
             counts["connections"] = len(connections) if connections else 0
             counts["ports"] = (len(input_ports) if input_ports else 0) + (len(output_ports) if output_ports else 0)
             counts["process_groups"] = len(process_groups) if process_groups else 0
             local_logger.debug(f"Got counts for PG {pg_id} via individual calls: {counts}")
             return counts
             
    except (ConnectionError, ValueError, NiFiAuthenticationError) as e:
        local_logger.error(f"Error fetching counts for PG {pg_id}: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (for counts)")
        return counts
    except Exception as e:
         local_logger.error(f"Unexpected error fetching counts for PG {pg_id}: {e}", exc_info=True)
         local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API (for counts)")
         return counts

async def _list_components_recursively(
    object_type: Literal["processors", "connections", "ports"],
    pg_id: str,
    depth: int = 0,
    max_depth: int = 3
) -> List[Dict]:
    """Recursively lists processors, connections, or ports within a process group hierarchy."""
    # Get client, logger, and IDs from context
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger
    user_request_id = current_user_request_id.get() or "-"
    action_id = current_action_id.get() or "-"

    if not nifi_client:
        local_logger.error("NiFi client not found in context for _list_components_recursively")
        return [{
             "process_group_id": pg_id,
             "process_group_name": "Unknown (Client Error)",
             "error": f"NiFi Client not available"
        }]

    all_results = [] 
    current_pg_name = await _get_process_group_name(pg_id)
    
    current_level_objects = []
    try:
        if object_type == "processors":
            raw_objects = await nifi_client.list_processors(pg_id, user_request_id=user_request_id, action_id=action_id)
            current_level_objects = _format_processor_summary(raw_objects)
        elif object_type == "connections":
            raw_objects = await nifi_client.list_connections(pg_id, user_request_id=user_request_id, action_id=action_id)
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

    # Check if max depth is reached before fetching child groups
    if depth >= max_depth:
        local_logger.debug(f"Max recursion depth ({max_depth}) reached for PG {pg_id}. Stopping recursion.")
        return all_results

    try:
        child_groups = await nifi_client.get_process_groups(pg_id) # Doesn't take context IDs
        if child_groups:
            for child_group_entity in child_groups:
                child_id = child_group_entity.get('id')
                if child_id:
                    # Call recursively without passing client/logger
                    recursive_results = await _list_components_recursively(
                        object_type=object_type,
                        pg_id=child_id,
                        depth=depth + 1,  # Increment depth
                        max_depth=max_depth # Pass max_depth down
                    )
                    all_results.extend(recursive_results)
                    
    except (ConnectionError, ValueError, NiFiAuthenticationError) as e:
        local_logger.error(f"Error fetching child groups for PG {pg_id} during recursion: {e}")
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
    recursive_search: bool
) -> Dict[str, Any]:
    """Fetches the hierarchy starting from pg_id, optionally recursively."""
    # Get client, logger, and IDs from context
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger
    user_request_id = current_user_request_id.get() or "-"
    action_id = current_action_id.get() or "-"

    if not nifi_client:
        local_logger.error("NiFi client not found in context for _get_process_group_hierarchy")
        return { "id": pg_id, "name": "Unknown (Client Error)", "child_process_groups": [], "error": "NiFi Client not available"}

    hierarchy_data = { "id": pg_id, "name": "Unknown", "child_process_groups": [] }
    # Extract context IDs from logger for the client calls - REMOVED
    # context = local_logger._context
    # user_request_id = context.get("user_request_id", "-")
    # action_id = context.get("action_id", "-")
    try:
        parent_name = await _get_process_group_name(pg_id) # Calls helper which now uses context
        hierarchy_data["name"] = parent_name

        nifi_req_children = {"operation": "get_process_groups", "process_group_id": pg_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req_children).debug("Calling NiFi API")
        child_groups_response = await nifi_client.get_process_groups(pg_id)
        child_count = len(child_groups_response) if child_groups_response else 0
        nifi_resp_children = {"child_group_count": child_count}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp_children).debug("Received from NiFi API")
        child_groups_list = child_groups_response

        if child_groups_list:
            for child_group_entity in child_groups_list:
                child_id = child_group_entity.get('id')
                child_component = child_group_entity.get('component', {})
                child_name = child_component.get('name', f"Unnamed PG ({child_id})")

                if child_id:
                    counts = await _get_process_group_contents_counts(child_id) # Calls helper which now uses context
                    child_data = {
                        "id": child_id,
                        "name": child_name,
                        "counts": counts
                    }
                    
                    if recursive_search:
                        local_logger.debug(f"Recursively fetching hierarchy for child PG: {child_id}")
                        # Call recursively without passing client/logger
                        child_hierarchy = await _get_process_group_hierarchy(
                            pg_id=child_id, 
                            recursive_search=True
                        )
                        child_data["children"] = child_hierarchy.get("child_process_groups", [])
                    
                    hierarchy_data["child_process_groups"].append(child_data)

        return hierarchy_data

    except (ConnectionError, ValueError, NiFiAuthenticationError) as e:
        local_logger.error(f"Error fetching process group hierarchy for {pg_id}: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        hierarchy_data["error"] = f"Failed to retrieve full hierarchy for process group {pg_id}: {e}"
        return hierarchy_data
    except Exception as e:
         local_logger.error(f"Unexpected error fetching hierarchy for {pg_id}: {e}", exc_info=True)
         local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
         hierarchy_data["error"] = f"Unexpected error retrieving hierarchy for {pg_id}: {e}"
         return hierarchy_data

# --- Tool Definitions --- 

@mcp.tool()
@tool_phases(["Review", "Build", "Modify", "Operate"])
async def list_nifi_objects(
    object_type: Literal["processors", "connections", "ports", "process_groups"],
    process_group_id: str | None = None,
    search_scope: Literal["current_group", "recursive"] = "current_group",
    # mcp_context: dict = {} # Removed context parameter
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
        The ID of the target process group. If None, defaults to the root process group.
    search_scope : Literal["current_group", "recursive"], optional
        Determines the scope of the search when listing objects.
        - 'current_group': Lists objects only directly within the target process group (default).
        - 'recursive': Lists objects within the target process group and all its descendants.
        Note: For 'process_groups' object_type, 'recursive' provides a nested hierarchy view.
    # Removed mcp_context from docstring

    Returns
    -------
    Union[List[Dict], Dict]
        - For object_type 'processors', 'connections', 'ports':
            - If search_scope='current_group': A list of simplified object summaries.
            - If search_scope='recursive': A list of dictionaries, each containing 'process_group_id', 'process_group_name', and a list of 'objects' found within that group (or an 'error' key).
        - For object_type 'process_groups':
            - If search_scope='current_group': A list of child process group summaries (id, name, counts).
            - If search_scope='recursive': A nested dictionary representing the process group hierarchy including names, IDs, component counts, and children.
    """
    # Get client and logger from context variables
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger # Fallback to global logger if needed

    if not nifi_client:
        raise ToolError("NiFi client not found in context.")
    if not isinstance(nifi_client, NiFiClient):
         raise ToolError(f"Invalid NiFi client type found in context: {type(nifi_client)}")

    # await ensure_authenticated(nifi_client, local_logger) # Removed - handled by factory
    
    # --- Get IDs from context --- 
    user_request_id = current_user_request_id.get() or "-"
    action_id = current_action_id.get() or "-"
    # --------------------------

    try:
        target_pg_id = process_group_id
        if not target_pg_id:
            local_logger.info("process_group_id not provided, defaulting to root.")
            target_pg_id = await nifi_client.get_root_process_group_id(user_request_id=user_request_id, action_id=action_id)
            local_logger.info(f"Resolved root process group ID: {target_pg_id}")

        local_logger.info(f"Listing NiFi objects of type '{object_type}' in scope '{search_scope}' for PG '{target_pg_id}'")

        # --- Process Group Handling --- 
        if object_type == "process_groups":
            local_logger.debug("Handling object_type 'process_groups'...")
            if search_scope == "current_group":
                local_logger.debug(f"Fetching direct children for PG {target_pg_id}")
                child_groups_response = await nifi_client.get_process_groups(target_pg_id)
                results = []
                if child_groups_response:
                    for child_group_entity in child_groups_response:
                        child_id = child_group_entity.get('id')
                        child_component = child_group_entity.get('component', {})
                        child_name = child_component.get('name', f"Unnamed PG ({child_id})")
                        if child_id:
                            counts = await _get_process_group_contents_counts(child_id)
                            results.append({"id": child_id, "name": child_name, "counts": counts})
                local_logger.info(f"Found {len(results)} direct child process groups in PG {target_pg_id}")
                return results
            else: # recursive
                local_logger.debug(f"Recursively fetching hierarchy starting from PG {target_pg_id}")
                hierarchy = await _get_process_group_hierarchy(target_pg_id, True)
                local_logger.info(f"Finished fetching recursive hierarchy for PG {target_pg_id}")
                return hierarchy

        # --- Processor, Connection, Port Handling --- 
        else:
            local_logger.debug(f"Handling object_type '{object_type}'...")
            if search_scope == "current_group":
                local_logger.debug(f"Fetching objects directly within PG {target_pg_id}")
                objects = []
                if object_type == "processors":
                    raw_objects = await nifi_client.list_processors(target_pg_id, user_request_id=user_request_id, action_id=action_id)
                    objects = _format_processor_summary(raw_objects)
                elif object_type == "connections":
                    raw_objects = await nifi_client.list_connections(target_pg_id, user_request_id=user_request_id, action_id=action_id)
                    objects = _format_connection_summary(raw_objects)
                elif object_type == "ports":
                    input_ports = await nifi_client.get_input_ports(target_pg_id)
                    output_ports = await nifi_client.get_output_ports(target_pg_id)
                    objects = _format_port_summary(input_ports, output_ports)
                    
                local_logger.info(f"Found {len(objects)} {object_type} directly within PG {target_pg_id}")
                return objects
            else: # recursive
                local_logger.debug(f"Recursively fetching {object_type} starting from PG {target_pg_id}")
                recursive_results = await _list_components_recursively(
                    object_type=object_type,
                    pg_id=target_pg_id,
                    max_depth=10 # Set a reasonable max depth
                )
                local_logger.info(f"Finished recursive search for {object_type} starting from PG {target_pg_id}")
                return recursive_results

    except NiFiAuthenticationError as e:
         local_logger.error(f"Authentication error during list_nifi_objects: {e}", exc_info=False)
         raise ToolError(f"Authentication error accessing NiFi: {e}") from e
    except (ValueError, ConnectionError, ToolError) as e:
        local_logger.error(f"Error listing NiFi objects: {e}", exc_info=False)
        raise ToolError(f"Error listing NiFi {object_type}: {e}") from e
    except Exception as e:
        local_logger.error(f"Unexpected error listing NiFi objects: {e}", exc_info=True)
        raise ToolError(f"An unexpected error occurred: {e}") from e

@mcp.tool()
@tool_phases(["Review", "Build", "Modify", "Operate"])
async def get_nifi_object_details(
    object_type: Literal["processor", "connection", "port", "process_group"],
    object_id: str,
    # mcp_context: dict = {} # Removed context parameter
) -> Dict:
    """
    Retrieves detailed information for a specific NiFi processor, connection, port, or process group.

    Parameters
    ----------
    object_type : Literal["processor", "connection", "port", "process_group"]
        The type of NiFi object to retrieve details for.
    object_id : str
        The ID of the specific NiFi object.
    # Removed mcp_context from docstring

    Returns
    -------
    Dict
        A dictionary containing the detailed entity information for the specified object, including component details, configuration, status, and revision.
    """
    # Get client and logger from context variables
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger

    if not nifi_client:
        raise ToolError("NiFi client not found in context.")
    if not isinstance(nifi_client, NiFiClient):
         raise ToolError(f"Invalid NiFi client type found in context: {type(nifi_client)}")
         
    # await ensure_authenticated(nifi_client, local_logger) # Removed
    
    # --- Get IDs from context --- 
    user_request_id = current_user_request_id.get() or "-"
    action_id = current_action_id.get() or "-"
    # --------------------------

    local_logger.info(f"Getting details for NiFi object type '{object_type}' with ID '{object_id}'")
    nifi_req = {"operation": f"get_{object_type}_details", "id": object_id}
    local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")

    try:
        details = {}
        if object_type == "processor":
            details = await nifi_client.get_processor_details(object_id)
        elif object_type == "connection":
            details = await nifi_client.get_connection(object_id)
        elif object_type == "process_group":
            # Use the method that passes context IDs
            details = await nifi_client.get_process_group_details(
                object_id, user_request_id=user_request_id, action_id=action_id
            )
        elif object_type == "port":
            # Need to determine if input or output port. Try input first.
            try:
                details = await nifi_client.get_input_port_details(object_id)
                local_logger.debug(f"Object {object_id} identified as an input port.")
            except ValueError:
                local_logger.debug(f"Object {object_id} not found as input port, trying output port.")
                try:
                    details = await nifi_client.get_output_port_details(object_id)
                    local_logger.debug(f"Object {object_id} identified as an output port.")
                except ValueError as e_out:
                    local_logger.error(f"Could not find port with ID '{object_id}' as either input or output port.")
                    raise ToolError(f"Port with ID '{object_id}' not found.") from e_out
        else:
            # Should not happen due to Literal validation, but good practice
            raise ToolError(f"Invalid object_type specified: {object_type}")
            
        local_logger.bind(interface="nifi", direction="response", data={
            "object_id": object_id, 
            "object_type": object_type, 
            "has_details": bool(details)
            }).debug("Received from NiFi API")
        local_logger.info(f"Successfully retrieved details for {object_type} {object_id}")
        return details

    except NiFiAuthenticationError as e:
         local_logger.error(f"Authentication error getting details for {object_type} {object_id}: {e}", exc_info=False)
         raise ToolError(f"Authentication error accessing NiFi: {e}") from e
    except ValueError as e:
        # Catch not found errors specifically
        local_logger.warning(f"Could not find {object_type} with ID '{object_id}': {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"{object_type.capitalize()} with ID '{object_id}' not found.") from e
    except (ConnectionError, ToolError) as e:
        local_logger.error(f"Error getting details for {object_type} {object_id}: {e}", exc_info=False)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"Error getting details for {object_type} {object_id}: {e}") from e
    except Exception as e:
        local_logger.error(f"Unexpected error getting details for {object_type} {object_id}: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
        raise ToolError(f"An unexpected error occurred: {e}") from e

@mcp.tool()
@tool_phases(["Review", "Build", "Modify", "Operate"])
async def document_nifi_flow(
    process_group_id: str | None = None,
    starting_processor_id: str | None = None,
    max_depth: int = 10,
    include_properties: bool = True,
    include_descriptions: bool = True,
    # mcp_context: dict = {} # Removed context parameter
) -> Dict[str, Any]:
    """
    Analyzes and documents a NiFi flow starting from a given process group or processor.

    This tool traverses the flow graph, extracts key information about processors and connections,
    identifies decision points, and generates a structured representation of the flow logic.

    Parameters
    ----------
    process_group_id : str, optional
        The ID of the process group to start documentation from. If None, `starting_processor_id` must be provided, and the tool will start from that processor's parent group.
    starting_processor_id : str, optional
        The ID of a specific processor to focus the documentation around. The tool will analyze the flow connected to this processor within its parent group.
    max_depth : int, optional
        The maximum depth to traverse the flow graph from the starting point. Defaults to 10.
    include_properties : bool, optional
        Whether to include important processor properties in the documentation. Defaults to True.
    include_descriptions : bool, optional
        Whether to include processor and connection descriptions/comments (if available). Defaults to True.
    # Removed mcp_context from docstring

    Returns
    -------
    Dict[str, Any]
        A dictionary containing the documented flow, including:
        - 'start_point': Information about the starting process group or processor.
        - 'flow_structure': A list representing the main flow path(s).
        - 'decision_branches': Information about identified decision branches (e.g., RouteOnAttribute).
        - 'unconnected_components': Lists of processors or ports not connected in the main flow.
        - 'errors': Any errors encountered during documentation.
    """
    # Get client and logger from context variables
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger

    if not nifi_client:
        raise ToolError("NiFi client not found in context.")
    if not isinstance(nifi_client, NiFiClient):
         raise ToolError(f"Invalid NiFi client type found in context: {type(nifi_client)}")
         
    # await ensure_authenticated(nifi_client, local_logger) # Removed
    
    # --- Get IDs from context --- 
    user_request_id = current_user_request_id.get() or "-"
    action_id = current_action_id.get() or "-"
    # --------------------------
    
    local_logger.info(f"Starting NiFi flow documentation. PG: {process_group_id}, Start Proc: {starting_processor_id}, Max Depth: {max_depth}")
    results = {
        "start_point": None,
        "flow_structure": [],
        "decision_branches": [],
        "unconnected_components": {"processors": [], "ports": []},
        "errors": []
    }

    target_pg_id = process_group_id
    
    try:
        # Determine the target process group ID
        if starting_processor_id and not target_pg_id:
            local_logger.info(f"No process_group_id provided, finding parent group for starting processor {starting_processor_id}")
            nifi_req = {"operation": "get_processor_details", "id": starting_processor_id}
            local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
            proc_details = await nifi_client.get_processor_details(starting_processor_id)
            nifi_resp = {"has_proc_details": bool(proc_details and 'component' in proc_details)}
            local_logger.bind(interface="nifi", direction="response", data=nifi_resp).debug("Received from NiFi API")
            
            target_pg_id = proc_details.get("component", {}).get("parentGroupId")
            if not target_pg_id:
                raise ToolError(f"Could not determine parent process group ID for processor {starting_processor_id}")
            results["start_point"] = {"type": "processor", "id": starting_processor_id, "name": proc_details.get("component", {}).get("name"), "process_group_id": target_pg_id}
            local_logger.info(f"Determined target process group ID: {target_pg_id} from starting processor.")
        elif not target_pg_id:
            local_logger.info("No process_group_id or starting_processor_id provided, defaulting to root process group.")
            target_pg_id = await nifi_client.get_root_process_group_id(user_request_id=user_request_id, action_id=action_id)
            if not target_pg_id:
                 raise ToolError("Could not retrieve the root process group ID.")
            results["start_point"] = {"type": "process_group", "id": target_pg_id, "name": "Root"}
            local_logger.info(f"Resolved root process group ID: {target_pg_id}")
        else:
            # PG ID was provided, get its name for the start point
            pg_name = await _get_process_group_name(target_pg_id)
            results["start_point"] = {"type": "process_group", "id": target_pg_id, "name": pg_name}
            local_logger.info(f"Using provided process group ID: {target_pg_id} ({pg_name})")
            
        if not target_pg_id:
             raise ToolError("Failed to determine a target process group ID for documentation.")

        # Fetch components for the target process group
        local_logger.info(f"Fetching components for process group {target_pg_id}...")
        nifi_req_components = {"operation": "list_all", "process_group_id": target_pg_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req_components).debug("Calling NiFi API (multiple calls)")
        
        processors_resp = await nifi_client.list_processors(target_pg_id, user_request_id=user_request_id, action_id=action_id)
        connections_resp = await nifi_client.list_connections(target_pg_id, user_request_id=user_request_id, action_id=action_id)
        input_ports_resp = await nifi_client.get_input_ports(target_pg_id)
        output_ports_resp = await nifi_client.get_output_ports(target_pg_id)
        
        nifi_resp_components = {
            "processor_count": len(processors_resp or []),
            "connection_count": len(connections_resp or []),
            "port_count": len(input_ports_resp or []) + len(output_ports_resp or [])
        }
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp_components).debug("Received from NiFi API (multiple calls)")

        processors = {p['id']: p for p in processors_resp if 'id' in p}
        connections = {c['id']: c for c in connections_resp if 'id' in c}
        input_ports = {p['id']: p for p in input_ports_resp if 'id' in p}
        output_ports = {p['id']: p for p in output_ports_resp if 'id' in p}
        all_components = {**processors, **input_ports, **output_ports}

        # Build the graph structure
        local_logger.info("Building graph structure from fetched components...")
        graph, nodes_by_id = build_graph_structure(processors, connections, input_ports, output_ports)

        # Analyze and document the flow
        local_logger.info("Analyzing flow structure...")
        start_node_id = starting_processor_id # Use provided start if available
        
        # If no specific processor start, try to find source nodes (input ports or processors with no incoming connections)
        if not start_node_id:
            source_nodes = [nid for nid, node in nodes_by_id.items() 
                            if node['type'] == 'INPUT_PORT' or 
                               (node['type'] == 'PROCESSOR' and not any(conn['source']['id'] == nid for conn in connections.values()))]
            if source_nodes:
                start_node_id = source_nodes[0] # Pick the first source node found
                local_logger.info(f"No starting_processor_id given, starting analysis from source node: {start_node_id}")
            else:
                # Fallback if no clear source, maybe pick first processor?
                if processors:
                    start_node_id = list(processors.keys())[0]
                    local_logger.warning(f"No source node found, starting analysis from arbitrary processor: {start_node_id}")
                else:
                    raise ToolError("Cannot document flow: No starting point specified and no processors found in the group.")
                    
        traversed_path = []
        visited_nodes = set()
        queue = [(start_node_id, 0)] # (node_id, current_depth)
        unconnected_processors = set(processors.keys())
        unconnected_ports = set(input_ports.keys()) | set(output_ports.keys())

        while queue:
            current_node_id, depth = queue.pop(0)

            if current_node_id in visited_nodes or depth > max_depth:
                continue
            visited_nodes.add(current_node_id)

            if current_node_id in unconnected_processors: unconnected_processors.remove(current_node_id)
            if current_node_id in unconnected_ports: unconnected_ports.remove(current_node_id)

            node_info = nodes_by_id.get(current_node_id)
            if not node_info:
                results["errors"].append(f"Node details not found for ID: {current_node_id}")
                continue

            component_details = all_components.get(current_node_id, {})
            component_config = component_details.get('component', {}).get('config', {})
            component_props = component_config.get('properties', {})

            doc_entry = {
                "id": current_node_id,
                "name": node_info["name"],
                "type": node_info["type"],
            }
            if include_descriptions:
                doc_entry["description"] = component_details.get("component", {}).get("comments", "") or component_details.get("component", {}).get("name", "")
            if include_properties and node_info["type"] == "PROCESSOR":
                 doc_entry["properties"] = extract_important_properties(component_props)
                 doc_entry["expressions"] = analyze_expressions(component_props)
                 
            # Add Relationship info here if needed
            traversed_path.append(doc_entry)
            
            # Add neighbors to queue
            if current_node_id in graph:
                 for neighbor_id, connection_details_list in graph[current_node_id].items():
                     if neighbor_id not in visited_nodes:
                         # Add connection details if needed
                         # Example: Add first connection detail found
                         if connection_details_list:
                              first_conn_id = connection_details_list[0]["connection_id"]
                              first_conn_details = connections.get(first_conn_id)
                              if first_conn_details:
                                   # Append connection info to the current node's entry or as a separate entry
                                   doc_entry["outgoing_connection"] = format_connection(first_conn_details, include_descriptions)
                                   pass # Append or handle connection documentation
                                   
                         queue.append((neighbor_id, depth + 1))

        results["flow_structure"] = traversed_path
        results["decision_branches"] = find_decision_branches(processors, connections)
        
        # Populate unconnected
        for proc_id in unconnected_processors:
             proc_info = processors.get(proc_id, {})
             results["unconnected_components"]["processors"].append({
                "id": proc_id,
                "name": proc_info.get('component', {}).get('name', 'Unknown Processor'),
                "type": proc_info.get('component', {}).get('type', 'Unknown Type')
             })
        for port_id in unconnected_ports:
             port_info = all_components.get(port_id, {})
             results["unconnected_components"]["ports"].append({
                "id": port_id,
                "name": port_info.get('component', {}).get('name', 'Unknown Port'),
                "type": port_info.get('component', {}).get('type', 'Unknown Port Type') 
             })
        
        local_logger.info("Flow documentation analysis complete.")
        return results

    except NiFiAuthenticationError as e:
         local_logger.error(f"Authentication error during document_nifi_flow: {e}", exc_info=False)
         results["errors"].append(f"Authentication error accessing NiFi: {e}")
         raise ToolError(f"Authentication error accessing NiFi: {e}") from e # Re-raise as ToolError
    except (ValueError, ConnectionError, ToolError) as e:
        local_logger.error(f"Error documenting NiFi flow: {e}", exc_info=False)
        results["errors"].append(f"Error documenting flow: {e}")
        # Don't re-raise, return partial results with errors
        return results
    except Exception as e:
        local_logger.error(f"Unexpected error documenting NiFi flow: {e}", exc_info=True)
        results["errors"].append(f"An unexpected error occurred: {e}")
        # Don't re-raise, return partial results with errors
        return results

@mcp.tool()
@tool_phases(["Review", "Build", "Modify", "Operate"])
async def search_nifi_flow(
    query: str,
    filter_object_type: Optional[Literal["processor", "connection", "port", "process_group"]] = None,
    filter_process_group_id: Optional[str] = None,
    # mcp_context: dict = {} # Removed context parameter
) -> Dict[str, List[Dict]]:
    """
    Performs a search across the entire NiFi flow for components matching the query.

    Optionally filters results by object type and/or containing process group ID.

    Parameters
    ----------
    query : str
        The search term (e.g., processor name, property value, comment text).
    filter_object_type : Optional[Literal["processor", "connection", "port", "process_group"]], optional
        Filter results to only include objects of this type. 'port' includes both input and output ports.
    filter_process_group_id : Optional[str], optional
        Filter results to only include objects within the specified process group (including nested groups).
    # Removed mcp_context from docstring

    Returns
    -------
    Dict[str, List[Dict]]
        A dictionary containing lists of matching objects, keyed by type (e.g., 'processorResults', 'connectionResults').
        Each result includes basic information like id, name, and parent group.
    """
    # Get client and logger from context variables
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger

    if not nifi_client:
        raise ToolError("NiFi client not found in context.")
    if not isinstance(nifi_client, NiFiClient):
         raise ToolError(f"Invalid NiFi client type found in context: {type(nifi_client)}")
         
    # await ensure_authenticated(nifi_client, local_logger) # Removed
    
    local_logger.info(f"Searching NiFi flow with query: '{query}'. Filters: type={filter_object_type}, group={filter_process_group_id}")
    nifi_req = {"operation": "search_flow", "query": query}
    local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")

    try:
        search_results_data = await nifi_client.search_flow(query)
        raw_results = search_results_data.get("searchResultsDTO", {})
        
        nifi_resp = {"has_results": bool(raw_results)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp).debug("Received from NiFi API")

        # --- Filtering Logic --- 
        filtered_results = {}
        object_type_map = {
            "processor": "processorResults",
            "connection": "connectionResults",
            "process_group": "processGroupResults",
            "input_port": "inputPortResults", # Map generic port to specific results
            "output_port": "outputPortResults" # Map generic port to specific results
        }

        # Determine which result keys to process based on filter_object_type
        keys_to_process = []
        if not filter_object_type:
            keys_to_process = list(object_type_map.values())
        elif filter_object_type == "port":
            keys_to_process = [object_type_map["input_port"], object_type_map["output_port"]]
        elif filter_object_type in object_type_map:
            keys_to_process = [object_type_map[filter_object_type]]
            
        if not keys_to_process:
            local_logger.warning(f"Invalid filter_object_type: '{filter_object_type}', returning all types.")
            keys_to_process = list(object_type_map.values()) # Fallback to all if filter is invalid
        
        for result_key in keys_to_process:
            if result_key in raw_results:
                filtered_list = []
                for item in raw_results[result_key]:
                    # Apply process group filter if specified
                    if filter_process_group_id:
                        item_pg_id = item.get("groupId")
                        if item_pg_id != filter_process_group_id:
                            # TODO: Implement recursive check if needed
                            # For now, only direct parent match
                            local_logger.trace(f"Skipping item {item.get('id')} due to PG filter mismatch (Item PG: {item_pg_id}, Filter PG: {filter_process_group_id})")
                            continue # Skip if PG ID doesn't match
                    
                    # Add basic info for the summary
                    filtered_list.append({
                        "id": item.get("id"),
                        "name": item.get("name"),
                        "groupId": item.get("groupId"),
                        "matches": item.get("matches", []) # Include matching fields
                    })
                if filtered_list:
                    filtered_results[result_key] = filtered_list
        # ---------------------
        
        local_logger.info(f"Flow search completed. Found results across {len(filtered_results)} types.")
        return filtered_results

    except NiFiAuthenticationError as e:
         local_logger.error(f"Authentication error during search_nifi_flow: {e}", exc_info=False)
         raise ToolError(f"Authentication error accessing NiFi: {e}") from e
    except (ConnectionError, ToolError) as e:
        local_logger.error(f"Error searching NiFi flow: {e}", exc_info=False)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"Error searching NiFi flow: {e}") from e
    except Exception as e:
        local_logger.error(f"Unexpected error searching NiFi flow: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
        raise ToolError(f"An unexpected error occurred: {e}") from e

import asyncio
from typing import List, Dict, Optional, Any, Union, Literal

# Import necessary components from parent/utils
from loguru import logger # Import the logger instance
# Import mcp and nifi_api_client from the new core module
from ..core import mcp, nifi_api_client
from .utils import (
    tool_phases,
    ensure_authenticated,
    _format_processor_summary,
    _format_connection_summary,
    _format_port_summary,
    filter_processor_data # Keep if needed by helpers here
)
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


# --- Helper Functions for list_nifi_objects --- 

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

async def _get_process_group_contents_counts(pg_id: str, nifi_client: NiFiClient, local_logger) -> Dict[str, int]:
    """Fetches counts of components within a specific process group."""
    counts = {"processors": 0, "connections": 0, "ports": 0, "process_groups": 0}
    try:
        # Attempt to use the more efficient /flow endpoint first
        nifi_req = {"operation": "get_process_group_flow", "process_group_id": pg_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API (for counts)")
        pg_flow_details = await nifi_client.get_process_group_flow(pg_id)
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
    nifi_client: NiFiClient,
    local_logger,
    depth: int = 0,
    max_depth: int = 3
) -> List[Dict]:
    """Recursively lists processors, connections, or ports within a process group hierarchy."""
    all_results = [] 
    current_pg_name = await _get_process_group_name(pg_id, nifi_client, local_logger)
    
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
        child_groups = await nifi_client.get_process_groups(pg_id)
        if child_groups:
            for child_group_entity in child_groups:
                child_id = child_group_entity.get('id')
                if child_id:
                    recursive_results = await _list_components_recursively(
                        object_type=object_type,
                        pg_id=child_id,
                        nifi_client=nifi_client,
                        local_logger=local_logger,
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
    nifi_client: NiFiClient, 
    local_logger,
    recursive_search: bool
) -> Dict[str, Any]:
    """Fetches the hierarchy starting from pg_id, optionally recursively."""
    hierarchy_data = { "id": pg_id, "name": "Unknown", "child_process_groups": [] }
    try:
        parent_name = await _get_process_group_name(pg_id, nifi_client, local_logger)
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
                    counts = await _get_process_group_contents_counts(child_id, nifi_client, local_logger)
                    child_data = {
                        "id": child_id,
                        "name": child_name,
                        "counts": counts
                    }
                    
                    if recursive_search:
                        local_logger.debug(f"Recursively fetching hierarchy for child PG: {child_id}")
                        child_hierarchy = await _get_process_group_hierarchy(
                            pg_id=child_id, 
                            nifi_client=nifi_client, 
                            local_logger=local_logger, 
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
    search_scope: Literal["current_group", "recursive"] = "current_group"
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
    search_scope : Literal["current_group"], optional
        Determines the scope of the listing. Defaults to 'current_group'.
        - 'current_group': Lists objects only within the specified `process_group_id`. For 'process_groups', shows only immediate children with counts.
        - 'recursive': For 'processors', 'connections', or 'ports', lists objects in the specified group and nested subgroups (up to 2 levels deep). For 'process_groups', provides the nested hierarchy including children of children, with counts at each level.

    Returns
    -------
    Union[List[Dict], Dict]
        A list or dictionary depending on the object_type and search_scope. See Args descriptions for specifics. Raises ToolError if an API error occurs.
    """
    local_logger = logger.bind(tool_name="list_nifi_objects", object_type=object_type, search_scope="current_group")
    await ensure_authenticated(nifi_api_client, local_logger)

    target_pg_id = process_group_id
    if target_pg_id is None:
        local_logger.info("No process_group_id provided, fetching root process group ID.")
        try:
            nifi_get_req = {"operation": "get_root_process_group_id"}
            local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
            target_pg_id = await nifi_api_client.get_root_process_group_id()
            nifi_get_resp = {"root_pg_id": target_pg_id}
            local_logger.bind(interface="nifi", direction="response", data=nifi_get_resp).debug("Received from NiFi API")
            local_logger.info(f"Using root process group ID: {target_pg_id}")
        except (ConnectionError, ValueError, NiFiAuthenticationError) as e:
            local_logger.error(f"Failed to get root process group ID: {e}", exc_info=True)
            local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
            raise ToolError(f"Failed to determine root process group ID: {e}")
        except Exception as e:
             local_logger.error(f"Unexpected error getting root process group ID: {e}", exc_info=True)
             local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
             raise ToolError(f"An unexpected error occurred determining root process group ID: {e}")

    local_logger = local_logger.bind(process_group_id=target_pg_id)
    local_logger.info(f"Executing list_nifi_objects for type '{object_type}' in group '{target_pg_id}'")

    try:
        if object_type == "processors":
            if search_scope == "recursive":
                local_logger.info(f"Performing recursive search for {object_type} starting from {target_pg_id} (max depth: 3)")
                # Pass initial depth and max_depth
                return await _list_components_recursively(object_type, target_pg_id, nifi_api_client, local_logger, depth=0, max_depth=3)
            else:
                nifi_req = {"operation": "list_processors", "process_group_id": target_pg_id}
                local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
                processors_list = await nifi_api_client.list_processors(target_pg_id)
                nifi_resp = {"processor_count": len(processors_list)}
                local_logger.bind(interface="nifi", direction="response", data=nifi_resp).debug("Received from NiFi API")
                return _format_processor_summary(processors_list)

        elif object_type == "connections":
            if search_scope == "recursive":
                local_logger.info(f"Performing recursive search for {object_type} starting from {target_pg_id} (max depth: 3)")
                 # Pass initial depth and max_depth
                return await _list_components_recursively(object_type, target_pg_id, nifi_api_client, local_logger, depth=0, max_depth=3)
            else:
                nifi_req = {"operation": "list_connections", "process_group_id": target_pg_id}
                local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
                connections_list = await nifi_api_client.list_connections(target_pg_id)
                nifi_resp = {"connection_count": len(connections_list)}
                local_logger.bind(interface="nifi", direction="response", data=nifi_resp).debug("Received from NiFi API")
                return _format_connection_summary(connections_list)

        elif object_type == "ports":
            if search_scope == "recursive":
                local_logger.info(f"Performing recursive search for {object_type} starting from {target_pg_id} (max depth: 3)")
                 # Pass initial depth and max_depth
                return await _list_components_recursively(object_type, target_pg_id, nifi_api_client, local_logger, depth=0, max_depth=3)
            else:
                nifi_req_in = {"operation": "get_input_ports", "process_group_id": target_pg_id}
                local_logger.bind(interface="nifi", direction="request", data=nifi_req_in).debug("Calling NiFi API")
                input_ports_list = await nifi_api_client.get_input_ports(target_pg_id)
                nifi_resp_in = {"input_port_count": len(input_ports_list)}
                local_logger.bind(interface="nifi", direction="response", data=nifi_resp_in).debug("Received from NiFi API")

                nifi_req_out = {"operation": "get_output_ports", "process_group_id": target_pg_id}
                local_logger.bind(interface="nifi", direction="request", data=nifi_req_out).debug("Calling NiFi API")
                output_ports_list = await nifi_api_client.get_output_ports(target_pg_id)
                nifi_resp_out = {"output_port_count": len(output_ports_list)}
                local_logger.bind(interface="nifi", direction="response", data=nifi_resp_out).debug("Received from NiFi API")
                return _format_port_summary(input_ports_list, output_ports_list)

        elif object_type == "process_groups":
            is_recursive = (search_scope == "recursive")
            local_logger.info(f"Building process group hierarchy for {target_pg_id} (Recursive: {is_recursive})")
            hierarchy_data = await _get_process_group_hierarchy(target_pg_id, nifi_api_client, local_logger, is_recursive)
            local_logger.info(f"Successfully built process group hierarchy for {target_pg_id}")
            return hierarchy_data
        else:
            local_logger.error(f"Invalid object_type provided: {object_type}")
            raise ToolError(f"Invalid object_type specified: {object_type}. Must be one of 'processors', 'connections', 'ports', 'process_groups'.")

    except (NiFiAuthenticationError, ConnectionError, ValueError) as e:
        local_logger.error(f"API error listing {object_type}: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"Failed to list NiFi {object_type}: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error listing {object_type}: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
        raise ToolError(f"An unexpected error occurred listing {object_type}: {e}")


@mcp.tool()
@tool_phases(["Review", "Build", "Modify", "Operate"])
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
    await ensure_authenticated(nifi_api_client, local_logger)

    local_logger.info(f"Executing get_nifi_object_details for {object_type} ID: {object_id}")
    try:
        details = None
        operation = f"get_{object_type}_details"
        if object_type == "processor":
            nifi_req = {"operation": "get_processor_details", "processor_id": object_id}
            local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
            details = await nifi_api_client.get_processor_details(object_id)
        
        elif object_type == "connection":
            nifi_req = {"operation": "get_connection", "connection_id": object_id}
            local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
            details = await nifi_api_client.get_connection(object_id)
        
        elif object_type == "port":
            try:
                nifi_req_in = {"operation": "get_input_port_details", "port_id": object_id}
                local_logger.bind(interface="nifi", direction="request", data=nifi_req_in).debug("Calling NiFi API (trying input port)")
                details = await nifi_api_client.get_input_port_details(object_id)
                operation = "get_input_port_details"
            except ValueError:
                 local_logger.warning(f"Input port {object_id} not found, trying output port.")
                 local_logger.bind(interface="nifi", direction="response", data={"error": "Input port not found"}).debug("Received error from NiFi API")
                 try:
                     nifi_req_out = {"operation": "get_output_port_details", "port_id": object_id}
                     local_logger.bind(interface="nifi", direction="request", data=nifi_req_out).debug("Calling NiFi API (trying output port)")
                     details = await nifi_api_client.get_output_port_details(object_id)
                     operation = "get_output_port_details"
                 except ValueError as e_out:
                     local_logger.warning(f"Output port {object_id} also not found.")
                     local_logger.bind(interface="nifi", direction="response", data={"error": "Output port not found"}).debug("Received error from NiFi API")
                     raise ToolError(f"Port with ID {object_id} not found (checked both input and output).") from e_out
        
        elif object_type == "process_group":
            nifi_req = {"operation": "get_process_group_details", "process_group_id": object_id}
            local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
            details = await nifi_api_client.get_process_group_details(object_id)
        
        else:
            local_logger.error(f"Invalid object_type specified: {object_type}")
            raise ToolError(f"Invalid object_type specified: {object_type}")

        local_logger.bind(interface="nifi", direction="response", data=details).debug(f"Received {object_type} details from NiFi API")
        local_logger.info(f"Successfully retrieved details for {object_type} {object_id}")
        return details

    except ValueError as e:
        local_logger.warning(f"{object_type.capitalize()} with ID {object_id} not found: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"{object_type.capitalize()} with ID {object_id} not found.") from e
    except (NiFiAuthenticationError, ConnectionError) as e:
        local_logger.error(f"API error getting {object_type} details: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"Failed to get NiFi {object_type} details: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error getting {object_type} details: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
        raise ToolError(f"An unexpected error occurred getting {object_type} details: {e}")


@mcp.tool()
@tool_phases(["Review", "Build", "Modify", "Operate"])
async def document_nifi_flow(
    process_group_id: str | None = None,
    starting_processor_id: str | None = None,
    max_depth: int = 10,
    include_properties: bool = True,
    include_descriptions: bool = True,
    user_request_id: str = "-",
    action_id: str = "-"
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
    user_request_id : str, optional
        A unique identifier for the user request. Defaults to "-".
    action_id : str, optional
        A unique identifier for the action being performed. Defaults to "-".

    Returns
    -------
    Dict[str, Any]
        A dictionary containing the flow documentation, including processors, connections, graph structure summary, identified paths, decision points, and parameter context (if `include_properties` is True).
    """
    local_logger = logger.bind(user_request_id=user_request_id, action_id=action_id)
    
    local_logger.debug("Starting document_nifi_flow execution.")
    target_pg_id = process_group_id
    if not target_pg_id:
        local_logger.info("No process_group_id provided, fetching root process group ID.")
        try:
            target_pg_id = await nifi_api_client.get_root_process_group_id(user_request_id=user_request_id, action_id=action_id)
        except Exception as e:
            local_logger.error(f"Failed to get root process group ID: {e}", exc_info=True)
            raise ToolError(f"Failed to determine root process group ID: {e}")
    
    local_logger = local_logger.bind(process_group_id=target_pg_id)
    local_logger.info(f"Starting flow documentation for process group {target_pg_id}.")
    
    try:
        # Fetch Process Group details to get variables later
        nifi_req_pg = {"operation": "get_process_group_details", "process_group_id": target_pg_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req_pg).debug("Calling NiFi API")
        pg_details = await nifi_api_client.get_process_group_details(target_pg_id, user_request_id=user_request_id, action_id=action_id)
        nifi_resp_pg = {"pg_details_retrieved": bool(pg_details)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp_pg).debug("Received from NiFi API")
        pg_variables = pg_details.get("component", {}).get("variables", None)

        nifi_req_procs = {"operation": "list_processors", "process_group_id": target_pg_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req_procs).debug("Calling NiFi API")
        processors = await nifi_api_client.list_processors(target_pg_id, user_request_id=user_request_id, action_id=action_id)
        nifi_resp_procs = {"processor_count": len(processors)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp_procs).debug("Received from NiFi API")
        
        nifi_req_conns = {"operation": "list_connections", "process_group_id": target_pg_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req_conns).debug("Calling NiFi API")
        connections = await nifi_api_client.list_connections(target_pg_id, user_request_id=user_request_id, action_id=action_id)
        nifi_resp_conns = {"connection_count": len(connections)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp_conns).debug("Received from NiFi API")

        filtered_processors = processors
        if starting_processor_id:
            start_processor = next((p for p in processors if p["id"] == starting_processor_id), None)
            if not start_processor:
                raise ToolError(f"Starting processor with ID {starting_processor_id} not found")
            
            processor_map = {p["id"]: p for p in processors}
            graph = build_graph_structure(processors, connections)
            
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
                    
                    if proc_id in graph["outgoing"]:
                        for conn in graph["outgoing"][proc_id]:
                            dest_id = conn["destinationId"] if "destinationId" in conn else conn["destination"]["id"]
                            if dest_id not in visited and dest_id not in to_visit:
                                included_processors.add(dest_id)
                                to_visit.append(dest_id)
                    
                    if proc_id in graph["incoming"]:
                        for conn in graph["incoming"][proc_id]:
                            src_id = conn["sourceId"] if "sourceId" in conn else conn["source"]["id"]
                            if src_id not in visited and src_id not in to_visit:
                                included_processors.add(src_id)
                                to_visit.append(src_id)
            
            filtered_processors = [p for p in processors if p["id"] in included_processors]
            filtered_connections = [
                c for c in connections if 
                (c["sourceId"] if "sourceId" in c else c["source"]["id"]) in included_processors and
                (c["destinationId"] if "destinationId" in c else c["destination"]["id"]) in included_processors
            ]
        else:
            filtered_connections = connections
        
        enriched_processors = []
        for processor in filtered_processors:
            proc_data = {
                "id": processor["id"],
                "name": processor["component"]["name"],
                "type": processor["component"]["type"],
                "state": processor["component"]["state"],
                "position": processor["position"],
                "relationships": processor["component"].get("relationships", []),
                "validation_status": processor["component"].get("validationStatus", "UNKNOWN")
            }
            
            if include_properties:
                property_info = extract_important_properties(processor)
                proc_data["properties"] = property_info["all_properties"]
                proc_data["expressions"] = property_info["expressions"]

            if include_descriptions:
                proc_data["description"] = processor["component"].get("config", {}).get("comments", "")
            
            enriched_processors.append(proc_data)
        
        # Build graph structure for decision point analysis and direct graph output
        processor_map_filtered = {p["id"]: p for p in filtered_processors} # Use filtered map
        graph_analysis_data = build_graph_structure(filtered_processors, filtered_connections)
        decision_points = find_decision_branches(processor_map_filtered, graph_analysis_data) # Keep decision points
        formatted_connections = [format_connection(c, processor_map_filtered) for c in filtered_connections] # Use filtered map

        # Prepare nodes for the graph output (using enriched data)
        graph_nodes = [
            {
                "id": p["id"], 
                "name": p["name"], 
                "type": p["type"] # Using the simplified type from enriched_processors
            } 
            for p in enriched_processors
        ]

        # Prepare edges for the graph output (using formatted connection data)
        graph_edges = [
            {
                "source": conn["source_id"],
                "target": conn["destination_id"],
                "relationship": conn["relationship"]
            }
            for conn in formatted_connections
        ]
        
        result = {
            "processors": enriched_processors,
            "connections": formatted_connections, # Keep formatted connections for potential detailed view
            # Removed graph_structure and common_paths
            # "graph_structure": { ... },
            # "common_paths": paths,
            "graph": { # Add the new graph structure
                "nodes": graph_nodes,
                "edges": graph_edges
            },
            "decision_points": decision_points # Keep decision points
        }
        
        # Add Process Group Variables if they exist
        if pg_variables:
            result["process_group_variables"] = pg_variables
            local_logger.debug(f"Added {len(pg_variables)} variables from process group {target_pg_id} to results.")
        
        if include_properties:
            parameters = await nifi_api_client.get_parameter_context(target_pg_id, user_request_id=user_request_id, action_id=action_id)
            if parameters:
                result["parameters"] = parameters
        
        local_logger.info(f"Successfully documented flow for process group {target_pg_id}.")
        return result
        
    except (NiFiAuthenticationError, ConnectionError) as e:
        local_logger.error(f"API error documenting flow: {e}", exc_info=True)
        raise ToolError(f"Failed to document NiFi flow: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error documenting flow: {e}", exc_info=True)
        raise ToolError(f"An unexpected error occurred while documenting the flow: {e}")


@mcp.tool()
@tool_phases(["Review", "Build", "Modify", "Operate"])
async def search_nifi_flow(
    query: str,
    filter_object_type: Optional[Literal["processor", "connection", "port", "process_group"]] = None,
    filter_process_group_id: Optional[str] = None
) -> Dict[str, List[Dict]]:
    """
    Searches the entire NiFi flow canvas for components matching a query string.

    This tool performs a global search across all accessible components (processors, connections, ports, process groups, etc.) 
    based on the provided query string. The search is case-insensitive and matches against names, IDs, comments, 
    property values, etc.

    Optionally, the results can be filtered client-side by object type and/or the parent process group ID 
    after the global search is performed.

    Args:
        query: The search term to look for across the NiFi canvas.
        filter_object_type: Optional. If provided, only return results of this type ('processor', 'connection', 'port', 'process_group').
        filter_process_group_id: Optional. If provided, only return results located directly within this process group ID.

    Returns:
        A dictionary containing lists of matching components, keyed by type ('processors', 'connections', 'ports', 'process_groups'). 
        Each component is represented by a dictionary with its 'id', 'name', and 'group_id'. For ports, 'type' ('input'/'output') is also included.
        Raises ToolError if the search fails or an API error occurs.
    """
    local_logger = logger.bind(
        tool_name="search_nifi_flow", 
        query=query, 
        filter_type=filter_object_type, 
        filter_group=filter_process_group_id
    )
    await ensure_authenticated(nifi_api_client, local_logger)

    local_logger.info(f"Executing global NiFi flow search for query: '{query}'")

    try:
        nifi_req = {"operation": "search_flow", "query": query}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API for search")
        
        # TODO: Implement search_flow method in nifi_client.py
        # search_results_dto = await nifi_api_client.search_flow(query) 
        search_results_dto = await nifi_api_client.search_flow(query) # Assuming this method exists/will exist

        nifi_resp = {"has_results": bool(search_results_dto)} # Basic check
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp).debug("Received search results from NiFi API")

        # Process and filter results
        filtered_results = {
            "processors": [],
            "connections": [],
            "ports": [],
            "process_groups": []
        }

        results_data = search_results_dto.get("searchResultsDTO", {}) # Structure based on typical NiFi response

        # --- Process Processors ---
        if not filter_object_type or filter_object_type == "processor":
            for proc in results_data.get("processorResults", []):
                 if not filter_process_group_id or proc.get("groupId") == filter_process_group_id:
                     filtered_results["processors"].append({
                         "id": proc.get("id"),
                         "name": proc.get("name"),
                         "group_id": proc.get("groupId")
                     })

        # --- Process Connections ---
        if not filter_object_type or filter_object_type == "connection":
            for conn in results_data.get("connectionResults", []):
                 if not filter_process_group_id or conn.get("groupId") == filter_process_group_id:
                      # Connection name might be less useful, often shows source/dest relationship names
                      conn_name = conn.get("name") or f"Connection from {conn.get('sourceName')} to {conn.get('destinationName')}"
                      filtered_results["connections"].append({
                          "id": conn.get("id"),
                          "name": conn_name, 
                          "group_id": conn.get("groupId")
                      })

        # --- Process Ports ---
        if not filter_object_type or filter_object_type == "port":
            # Input Ports
            for port in results_data.get("inputPortResults", []):
                 if not filter_process_group_id or port.get("groupId") == filter_process_group_id:
                     filtered_results["ports"].append({
                         "id": port.get("id"),
                         "name": port.get("name"),
                         "group_id": port.get("groupId"),
                         "type": "input" 
                     })
            # Output Ports
            for port in results_data.get("outputPortResults", []):
                 if not filter_process_group_id or port.get("groupId") == filter_process_group_id:
                     filtered_results["ports"].append({
                         "id": port.get("id"),
                         "name": port.get("name"),
                         "group_id": port.get("groupId"),
                         "type": "output"
                     })

        # --- Process Process Groups ---
        if not filter_object_type or filter_object_type == "process_group":
            for pg in results_data.get("processGroupResults", []):
                 # Filter out the root group if its ID is returned and we are filtering by a specific group
                 is_root = pg.get("id") == pg.get("parentGroupId") # Simple check, might need refinement based on API
                 if not filter_process_group_id or (pg.get("groupId") == filter_process_group_id and not is_root):
                     filtered_results["process_groups"].append({
                         "id": pg.get("id"),
                         "name": pg.get("name"),
                          # Parent group ID might be more relevant than the group ID itself here? 
                          # NiFi search result 'groupId' for a PG might be its *own* ID. Let's use parentGroupId if available.
                         "group_id": pg.get("parentGroupId") or pg.get("groupId") 
                     })
        
        # Remove empty lists if no filter was applied, or if filter resulted in no matches for a type
        final_results = {k: v for k, v in filtered_results.items() if v}

        local_logger.info(f"Search complete. Found {sum(len(v) for v in final_results.values())} matching components after filtering.")
        return final_results

    except (NiFiAuthenticationError, ConnectionError, ValueError) as e:
        local_logger.error(f"API error during NiFi search: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"Failed to perform NiFi search: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error during NiFi search: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
        raise ToolError(f"An unexpected error occurred during NiFi search: {e}")

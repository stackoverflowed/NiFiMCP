import asyncio
from typing import List, Dict, Optional, Any, Union, Literal

# Import necessary components from parent/utils
from loguru import logger
# Import mcp and nifi_api_client from the new core module
from ..core import mcp, nifi_api_client
# REMOVED from ..server import mcp, nifi_api_client
from .utils import (
    tool_phases,
    ensure_authenticated,
    filter_created_processor_data,
    filter_connection_data,
    filter_port_data,
    filter_process_group_data,
    filter_processor_data # Needed for create_nifi_flow
)
from nifi_mcp_server.nifi_client import NiFiClient, NiFiAuthenticationError
from mcp.server.fastmcp.exceptions import ToolError

# --- Tool Definitions --- 

@mcp.tool()
@tool_phases(["Modify"])
async def create_nifi_processor(
    processor_type: str,
    name: str,
    position_x: int,
    position_y: int,
    process_group_id: str | None = None,
    # config: Optional[Dict[str, Any]] = None # Keep commented for now
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
    await ensure_authenticated(nifi_api_client, local_logger)

    target_pg_id = process_group_id
    if target_pg_id is None:
        local_logger.info("No process_group_id provided for creation, fetching root process group ID.")
        try:
            nifi_request_data = {"operation": "get_root_process_group_id"}
            local_logger.bind(interface="nifi", direction="request", data=nifi_request_data).debug("Calling NiFi API")
            target_pg_id = await nifi_api_client.get_root_process_group_id()
            nifi_response_data = {"root_pg_id": target_pg_id}
            local_logger.bind(interface="nifi", direction="response", data=nifi_response_data).debug("Received from NiFi API")
        except Exception as e:
            local_logger.error(f"Failed to get root process group ID: {e}", exc_info=True)
            local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
            raise ToolError(f"Failed to determine root process group ID for creation: {e}")

    position = {"x": position_x, "y": position_y}
    local_logger.info(f"Executing create_nifi_processor: Type='{processor_type}', Name='{name}', Position={position} in group: {target_pg_id}")

    try:
        nifi_request_data = {
            "operation": "create_processor", 
            "process_group_id": target_pg_id,
            "processor_type": processor_type,
            "name": name,
            "position": position,
            "config": None # Log config when implemented
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_request_data).debug("Calling NiFi API")
        processor_entity = await nifi_api_client.create_processor(
            process_group_id=target_pg_id,
            processor_type=processor_type,
            name=name,
            position=position,
            config=None # Add config dict here when implemented
        )
        nifi_response_data = filter_created_processor_data(processor_entity)
        local_logger.bind(interface="nifi", direction="response", data=nifi_response_data).debug("Received from NiFi API")
        
        local_logger.info(f"Successfully created processor '{name}' with ID: {processor_entity.get('id', 'N/A')}")
        
        component = processor_entity.get("component", {})
        validation_status = component.get("validationStatus", "UNKNOWN")
        validation_errors = component.get("validationErrors", [])
        
        if validation_status == "VALID":
            return {
                "status": "success",
                "message": f"Processor '{name}' created successfully.",
                "entity": nifi_response_data
            }
        else:
            error_msg_snippet = f" ({validation_errors[0]})" if validation_errors else ""
            local_logger.warning(f"Processor '{name}' created but is {validation_status}{error_msg_snippet}. Requires configuration or connections.")
            return {
                "status": "warning",
                "message": f"Processor '{name}' created but is currently {validation_status}{error_msg_snippet}. Further configuration or connections likely required.",
                "entity": nifi_response_data
            }
            
    except (NiFiAuthenticationError, ConnectionError, ValueError) as e:
        local_logger.error(f"API error creating processor: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"Failed to create NiFi processor: {e}", "entity": None}
    except Exception as e:
        local_logger.error(f"Unexpected error creating processor: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"An unexpected error occurred during processor creation: {e}", "entity": None}


@mcp.tool()
@tool_phases(["Modify"])
async def create_nifi_connection(
    source_id: str,
    relationships: List[str],
    target_id: str,
) -> Dict:
    """
    Creates a connection between two components (processors or ports) within the same NiFi process group,
    selecting one or more relationships from the source component.
    The process group is automatically determined based on the parent group of the source and target components.

    Args:
        source_id: The UUID of the source component (processor, input port, or output port).
        relationships: A non-empty list of relationship names originating from the source component that should be selected for this connection.
        target_id: The UUID of the target component (processor, input port, or output port).

    Returns:
        A dictionary representing the created connection entity. Raises ToolError if components are not found,
        are in different process groups, or if the API call fails.
    """
    local_logger = logger.bind(tool_name="create_nifi_connection", source_id=source_id, target_id=target_id, relationships=relationships)
    await ensure_authenticated(nifi_api_client, local_logger)

    if not relationships:
        raise ToolError("The 'relationships' list cannot be empty. At least one relationship must be provided.")
    if not isinstance(relationships, list) or not all(isinstance(item, str) for item in relationships):
        raise ToolError("Invalid 'relationships' elements. Expected a non-empty list of strings (relationship names).")

    source_entity = None
    source_type = None
    target_entity = None
    target_type = None

    local_logger.info(f"Fetching details for source component {source_id}...")
    try:
        try:
            source_entity = await nifi_api_client.get_processor_details(source_id)
            source_type = "PROCESSOR"
            local_logger.info(f"Source component {source_id} identified as a PROCESSOR.")
        except ValueError:
            try:
                source_entity = await nifi_api_client.get_input_port_details(source_id)
                source_type = "INPUT_PORT"
                local_logger.info(f"Source component {source_id} identified as an INPUT_PORT.")
            except ValueError:
                try:
                    source_entity = await nifi_api_client.get_output_port_details(source_id)
                    source_type = "OUTPUT_PORT"
                    local_logger.info(f"Source component {source_id} identified as an OUTPUT_PORT.")
                except ValueError:
                    raise ToolError(f"Source component with ID {source_id} not found or is not a connectable type (Processor, Input Port, Output Port).")

        local_logger.info(f"Fetching details for target component {target_id}...")
        try:
            target_entity = await nifi_api_client.get_processor_details(target_id)
            target_type = "PROCESSOR"
            local_logger.info(f"Target component {target_id} identified as a PROCESSOR.")
        except ValueError:
            try:
                target_entity = await nifi_api_client.get_input_port_details(target_id)
                target_type = "INPUT_PORT"
                local_logger.info(f"Target component {target_id} identified as an INPUT_PORT.")
            except ValueError:
                try:
                    target_entity = await nifi_api_client.get_output_port_details(target_id)
                    target_type = "OUTPUT_PORT"
                    local_logger.info(f"Target component {target_id} identified as an OUTPUT_PORT.")
                except ValueError:
                     raise ToolError(f"Target component with ID {target_id} not found or is not a connectable type (Processor, Input Port, Output Port).")

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
        local_logger = local_logger.bind(process_group_id=common_parent_pg_id)
        local_logger.info(f"Validated that source and target are in the same process group: {common_parent_pg_id}")

    except (ValueError, NiFiAuthenticationError, ConnectionError) as e:
         local_logger.error(f"API error occurred while fetching component details: {e}", exc_info=True)
         raise ToolError(f"Failed to fetch details for source/target components: {e}")
    except Exception as e:
         local_logger.error(f"Unexpected error occurred while fetching component details: {e}", exc_info=True)
         raise ToolError(f"An unexpected error occurred while fetching component details: {e}")

    local_logger.info(f"Checking for existing connections between {source_id} and {target_id} in group {common_parent_pg_id}...")
    try:
        nifi_list_req = {"operation": "list_connections", "process_group_id": common_parent_pg_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_list_req).debug("Calling NiFi API (for duplicate check)")
        existing_connections = await nifi_api_client.list_connections(common_parent_pg_id)
        nifi_list_resp = {"connection_count": len(existing_connections)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_list_resp).debug("Received from NiFi API (for duplicate check)")
        
        for existing_conn_entity in existing_connections:
            existing_comp = existing_conn_entity.get("component", {})
            existing_source = existing_comp.get("source", {})
            existing_dest = existing_comp.get("destination", {})
            
            if existing_source.get("id") == source_id and existing_dest.get("id") == target_id:
                existing_conn_id = existing_conn_entity.get("id")
                local_logger.warning(f"Duplicate connection detected. Existing connection ID: {existing_conn_id}")
                try:
                    existing_conn_details = await nifi_api_client.get_connection(existing_conn_id)
                    existing_relationships = existing_conn_details.get("component", {}).get("selectedRelationships", [])
                except Exception as detail_err:
                    local_logger.error(f"Could not fetch details for existing connection {existing_conn_id} during duplicate check: {detail_err}")
                    existing_relationships = ["<error retrieving>"]
                error_msg = (
                    f"A connection already exists between source '{source_id}' and target '{target_id}'. "
                    f"Existing connection ID: {existing_conn_id}. "
                    f"Currently selected relationships: {existing_relationships}. "
                    f"Use the 'update_nifi_connection' tool to modify the relationships if needed."
                )
                return {"status": "error", "message": error_msg, "entity": None}
        local_logger.info("No duplicate connection found. Proceeding with creation.")
    except (NiFiAuthenticationError, ConnectionError, ValueError) as e:
        local_logger.error(f"API error listing connections during duplicate check: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (for duplicate check)")
        raise ToolError(f"Failed to check for existing connections before creation: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error listing connections during duplicate check: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (for duplicate check)")
        raise ToolError(f"An unexpected error occurred while checking for existing connections: {e}")

    local_logger.info(f"Executing create_nifi_connection: From {source_id} ({source_type}, {relationships}) To {target_id} ({target_type}) in derived group {common_parent_pg_id}")
    try:
        nifi_create_req = {
            "operation": "create_connection",
            "process_group_id": common_parent_pg_id,
            "source_id": source_id,
            "target_id": target_id,
            "relationships": relationships,
            "source_type": source_type,
            "target_type": target_type
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_create_req).debug("Calling NiFi API")
        connection_entity = await nifi_api_client.create_connection(
            process_group_id=common_parent_pg_id,
            source_id=source_id,
            target_id=target_id,
            relationships=relationships,
            source_type=source_type,
            target_type=target_type
        )
        local_logger.bind(interface="nifi", direction="response", data=connection_entity).debug("Received from NiFi API (full details)")
        filtered_connection = filter_connection_data(connection_entity)
        local_logger.info(f"Successfully created connection with ID: {filtered_connection.get('id', 'N/A')}. Returning filtered details.")
        return filtered_connection
    except (NiFiAuthenticationError, ConnectionError, ValueError) as e:
        local_logger.error(f"API error creating connection: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"Failed to create NiFi connection: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error creating connection: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"An unexpected error occurred during connection creation: {e}")


@mcp.tool()
@tool_phases(["Modify"])
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
            raise ToolError(f"Invalid port_type specified: {port_type}")

        filtered_port = filter_port_data(port_entity)
        local_logger.bind(interface="nifi", direction="response", data=filtered_port).debug("Received from NiFi API")
        local_logger.info(f"Successfully created {port_type} port '{name}' with ID: {filtered_port.get('id', 'N/A')}")

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
@tool_phases(["Modify"])
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
    await ensure_authenticated(nifi_api_client, local_logger)

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
        
        filtered_pg = filter_process_group_data(pg_entity)
        local_logger.bind(interface="nifi", direction="response", data=filtered_pg).debug("Received from NiFi API")
        local_logger.info(f"Successfully created process group '{name}' with ID: {filtered_pg.get('id', 'N/A')}")

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


@mcp.tool()
@tool_phases(["Build"])
async def create_nifi_flow(
    nifi_objects: List[Dict[str, Any]],
    process_group_id: str | None = None,
    create_process_group: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Creates multiple NiFi components (processors, connections) based on a list of definitions.
    Optionally creates a new containing process group first and places the components inside.

    **IMPORTANT:** The `nifi_objects` list requires a precise structure defined below.
    Attempts to create components sequentially: processors first, then connections.
    If a component fails to create, an error is recorded, but the tool attempts subsequent components.
    Connection definitions MUST use the 'name' of the source/target processors defined earlier in the 'nifi_objects' list for 'source_name' and 'target_name'.

    **Processor Definition Structure:**
    ```python
    {
        "type": "processor",          # MUST be the literal string "processor"
        "name": "Your Processor Name", # String: Unique name for this processor within the list
        "java_class_name": "org.apache.nifi.processors.standard.MyProcessor", # String: Full Java class name (MUST use this key, NOT 'type' or 'class')
        "position": {"x": 100, "y": 100}, # Dict: { "x": int, "y": int }
        "properties": { ... }         # Optional Dict: Processor-specific properties { "prop_name": "prop_value" }
    }
    ```
    **DO NOT USE 'component' or 'class' keys for processors.**

    **Connection Definition Structure:**
    ```python
    {
        "type": "connection",             # MUST be the literal string "connection"
        "source_name": "NameOfSourceProcessor", # String: MUST use 'source_name', refers to a processor 'name' in this list
        "target_name": "NameOfTargetProcessor", # String: MUST use 'target_name', refers to a processor 'name' in this list
        "relationships": ["rel_name"]     # List[str]: Non-empty list of relationship names from the source processor
    }
    ```
    **DO NOT USE 'source', 'destination', or 'relationship' keys for connections.**

    Args:
        nifi_objects: (Required) A list where each dictionary defines a component to create according to the structures defined above.
                      This list must contain the definitions for ALL components (processors, connections) intended for the new flow.
                      See structure examples above and the full payload example below.
        process_group_id: The UUID of the process group where the flow components should be created, OR where the new containing process group should be created if `create_process_group` is provided. Defaults to the root group if None.
                          Example: `"a1b2c3d4-01e2-1000-ffff-abcdef123456"`
        create_process_group: (Optional) If provided, a new process group will be created first.
                              Should be a dictionary with keys: `name` (str) and `position` (dict: {"x": int, "y": int}).
                              The components from `nifi_objects` will be created inside this new group.
                              If this is used, `process_group_id` acts as the parent for this new group.
                              Example: `{ "name": "My New Flow Group", "position": {"x": 100, "y": 100} }`

    Example Payload for `nifi_objects`:
    ```json
    [
      {
        "type": "processor",
        "name": "Generate Data",
        "java_class_name": "org.apache.nifi.processors.standard.GenerateFlowFile",
        "position": {"x": 100, "y": 100},
        "properties": {
          "File Size": "1 KB",
          "Batch Size": "1",
          "Data Format": "Text"
        }
      },
      {
        "type": "processor",
        "name": "Log Data Attributes",
        "java_class_name": "org.apache.nifi.processors.standard.LogAttribute",
        "position": {"x": 500, "y": 100},
        "properties": {
          "Log Level": "info",
          "Attributes to Log": "filename, uuid",
          "Log Payload": "false"
        }
      },
      {
        "type": "connection",
        "source_name": "Generate Data",
        "target_name": "Log Data Attributes",
        "relationships": ["success"]
      }
    ]
    ```

    Returns:
        A list containing the results for each object defined in `nifi_objects`, in the same order.
        Each item will be either:
        - A dictionary with filtered details of the successfully created component.
        - A dictionary with `{"status": "error", "message": "..."}` indicating why creation failed.
    """
    local_logger = logger.bind(tool_name="create_nifi_flow")
    await ensure_authenticated(nifi_api_client, local_logger)

    if not isinstance(nifi_objects, list) or not nifi_objects:
        raise ToolError("The 'nifi_objects' argument must be a non-empty list.")
    
    for i, obj_def in enumerate(nifi_objects):
        if not isinstance(obj_def, dict) or "type" not in obj_def:
            raise ToolError(f"Invalid definition at index {i}: Each object must be a dictionary with a 'type' key.")
        obj_type = obj_def.get("type")
        if obj_type == "processor":
            if not all(k in obj_def for k in ["name", "java_class_name", "position"]):
                 raise ToolError(f"Invalid processor definition at index {i}: Missing required keys ('name', 'java_class_name', 'position').")
            if not isinstance(obj_def.get("position"), dict) or not all(k in obj_def["position"] for k in ["x", "y"]):
                 raise ToolError(f"Invalid processor definition at index {i}: 'position' must be a dict with 'x' and 'y'.")
        elif obj_type == "connection":
            if not all(k in obj_def for k in ["source_name", "target_name", "relationships"]):
                raise ToolError(f"Invalid connection definition at index {i}: Missing required keys ('source_name', 'target_name', 'relationships').")
            if not isinstance(obj_def.get("relationships"), list) or not obj_def["relationships"]:
                 raise ToolError(f"Invalid connection definition at index {i}: 'relationships' must be a non-empty list.")
        elif obj_type not in ["processor", "connection"]:
             raise ToolError(f"Invalid definition at index {i}: Unsupported object 'type': {obj_type}.")
             
    if create_process_group is not None:
        if not isinstance(create_process_group, dict):
             raise ToolError("'create_process_group' must be a dictionary.")
        if not all(k in create_process_group for k in ["name", "position"]):
            raise ToolError("'create_process_group' dictionary missing required keys ('name', 'position').")
        if not isinstance(create_process_group.get("position"), dict) or not all(k in create_process_group["position"] for k in ["x", "y"]):
             raise ToolError("'create_process_group.position' must be a dict with 'x' and 'y'.")

    parent_pg_id = process_group_id
    if parent_pg_id is None:
        local_logger.info("No process_group_id provided, fetching root process group ID to use as parent.")
        try:
            nifi_get_req = {"operation": "get_root_process_group_id"}
            local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
            parent_pg_id = await nifi_api_client.get_root_process_group_id()
            nifi_get_resp = {"root_pg_id": parent_pg_id}
            local_logger.bind(interface="nifi", direction="response", data=nifi_get_resp).debug("Received from NiFi API")
        except Exception as e:
            local_logger.error(f"Failed to get root process group ID: {e}", exc_info=True)
            local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
            raise ToolError(f"Failed to determine root process group ID: {e}")
            
    target_pg_id = parent_pg_id
    local_logger = local_logger.bind(initial_parent_pg_id=parent_pg_id)

    if create_process_group:
        pg_name = create_process_group["name"]
        pg_pos = create_process_group["position"]
        local_logger.info(f"Attempting to create containing Process Group '{pg_name}' within parent {parent_pg_id}")
        try:
            nifi_create_pg_req = {
                "operation": "create_process_group",
                "parent_pg_id": parent_pg_id,
                "name": pg_name,
                "position": pg_pos
            }
            local_logger.bind(interface="nifi", direction="request", data=nifi_create_pg_req).debug("Calling NiFi API")
            new_pg_entity = await nifi_api_client.create_process_group(
                parent_pg_id=parent_pg_id,
                name=pg_name,
                position=pg_pos
            )
            filtered_pg = filter_process_group_data(new_pg_entity)
            local_logger.bind(interface="nifi", direction="response", data=filtered_pg).debug("Received from NiFi API")
            new_pg_id = new_pg_entity.get("id")
            if not new_pg_id:
                raise ToolError(f"Process Group '{pg_name}' creation reported success but API did not return an ID.")
            target_pg_id = new_pg_id
            local_logger.info(f"Successfully created containing PG '{pg_name}' (ID: {target_pg_id}). Subsequent components will be placed inside.")
        except (NiFiAuthenticationError, ConnectionError, ValueError, ToolError) as e:
            err_msg = f"Failed to create containing Process Group '{pg_name}': {e}"
            local_logger.error(err_msg, exc_info=True)
            local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
            raise ToolError(err_msg)
        except Exception as e:
            err_msg = f"Unexpected error creating containing Process Group '{pg_name}': {e}"
            local_logger.error(err_msg, exc_info=True)
            local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
            raise ToolError(err_msg)
    
    local_logger = local_logger.bind(final_target_pg_id=target_pg_id)

    id_mapping = {}
    results_list = [None] * len(nifi_objects)

    local_logger.info(f"Starting Phase 1: Creating Processors within PG: {target_pg_id}...")
    for index, obj_def in enumerate(nifi_objects):
        if obj_def.get("type") == "processor":
            name = obj_def["name"]
            proc_type = obj_def["java_class_name"]
            position = obj_def["position"]
            properties = obj_def.get("properties", {})
            if properties is None: properties = {}
            if not isinstance(properties, dict):
                 err_msg = f"Processor '{name}' (index {index}): 'properties' must be a dictionary or null."
                 local_logger.warning(err_msg)
                 results_list[index] = {"status": "error", "message": err_msg}
                 continue 

            local_logger.info(f"Attempting to create processor '{name}' (Type: {proc_type}) at index {index} in PG {target_pg_id}")
            try:
                nifi_create_req = {
                    "operation": "create_processor", 
                    "process_group_id": target_pg_id,
                    "processor_type": proc_type,
                    "name": name,
                    "position": position,
                    "config": {"properties": properties} if properties else None
                }
                local_logger.bind(interface="nifi", direction="request", data=nifi_create_req).debug("Calling NiFi API")
                processor_entity = await nifi_api_client.create_processor(
                    process_group_id=target_pg_id,
                    processor_type=proc_type,
                    name=name,
                    position=position,
                    config=properties
                )
                filtered_processor = filter_processor_data(processor_entity)
                local_logger.bind(interface="nifi", direction="response", data=filtered_processor).debug("Received from NiFi API")
                created_id = processor_entity.get("id")
                if created_id:
                    id_mapping[name] = created_id
                    local_logger.info(f"Successfully created processor '{name}' with ID: {created_id}. Mapping name to ID.")
                    results_list[index] = filtered_processor
                else:
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

    local_logger.info(f"Starting Phase 2: Creating Connections within PG: {target_pg_id}...")
    all_existing_connections = []
    try:
        all_existing_connections = await nifi_api_client.list_connections(target_pg_id)
    except Exception as e:
        local_logger.warning(f"Could not list existing connections for duplicate check in {target_pg_id}: {e}. Duplicate check skipped.")

    for index, obj_def in enumerate(nifi_objects):
         if obj_def.get("type") == "connection":
            source_name = obj_def["source_name"]
            target_name = obj_def["target_name"]
            relationships = obj_def["relationships"]
            local_logger.info(f"Attempting to create connection from '{source_name}' to '{target_name}' (index {index}) in PG {target_pg_id}")

            source_uuid = id_mapping.get(source_name)
            target_uuid = id_mapping.get(target_name)

            if not source_uuid:
                err_msg = f"Connection (index {index}): Source processor '{source_name}' not found or failed creation. Cannot create connection."
                local_logger.warning(err_msg)
                results_list[index] = {"status": "error", "message": err_msg}
                continue 

            if not target_uuid:
                err_msg = f"Connection (index {index}): Target processor '{target_name}' not found or failed creation. Cannot create connection."
                local_logger.warning(err_msg)
                results_list[index] = {"status": "error", "message": err_msg}
                continue 
                
            local_logger.debug(f"Resolved connection IDs: {source_name} -> {source_uuid}, {target_name} -> {target_uuid}")

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
                    break 
            if duplicate_found:
                continue 

            try:
                source_type = "PROCESSOR"
                target_type = "PROCESSOR"
                
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
                connection_entity = await nifi_api_client.create_connection(
                    process_group_id=target_pg_id,
                    source_id=source_uuid,
                    target_id=target_uuid,
                    relationships=relationships,
                    source_type=source_type,
                    target_type=target_type
                )
                filtered_connection = filter_connection_data(connection_entity)
                local_logger.bind(interface="nifi", direction="response", data=filtered_connection).debug("Received from NiFi API")
                local_logger.info(f"Successfully created connection from '{source_name}' to '{target_name}'.")
                results_list[index] = filtered_connection 

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

    for i, result in enumerate(results_list):
        if result is None:
             obj_type = nifi_objects[i].get("type", "unknown")
             err_msg = f"Object at index {i} (type: {obj_type}) was not processed."
             local_logger.error(err_msg)
             results_list[i] = {"status": "error", "message": err_msg}

    local_logger.info("Finished creating NiFi flow components.")
    return results_list

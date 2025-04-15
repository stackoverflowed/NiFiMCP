import asyncio
from typing import List, Dict, Optional, Any, Union, Literal

# Import necessary components from parent/utils
from loguru import logger
# Import mcp and nifi_api_client from the new core module
from ..core import mcp, nifi_api_client
from .utils import (
    tool_phases,
    ensure_authenticated,
    filter_created_processor_data,
    filter_connection_data
)
from nifi_mcp_server.nifi_client import NiFiClient, NiFiAuthenticationError
from mcp.server.fastmcp.exceptions import ToolError

# --- Tool Definitions --- 

@mcp.tool()
@tool_phases(["Modify"])
async def update_nifi_processor_properties(
    processor_id: str,
    processor_config_properties: Dict[str, Any]
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
    await ensure_authenticated(nifi_api_client, local_logger)

    if not processor_config_properties:
        error_msg = "The 'processor_config_properties' argument cannot be empty. Please use 'get_nifi_object_details' first to fetch the current configuration, modify it, and then provide the complete desired property dictionary to this tool."
        local_logger.warning(f"Validation failed for update_nifi_processor_properties (processor_id={processor_id}): {error_msg}")
        raise ToolError(error_msg)

    if not isinstance(processor_config_properties, dict):
         raise ToolError(f"Invalid 'processor_config_properties' type. Expected a dictionary, got {type(processor_config_properties)}.")

    if isinstance(processor_config_properties, dict) and \
       list(processor_config_properties.keys()) == ["properties"] and \
       isinstance(processor_config_properties["properties"], dict):
        original_input = processor_config_properties
        processor_config_properties = processor_config_properties["properties"]
        local_logger.warning(f"Detected nested 'properties' key in input for processor {processor_id}. Correcting structure. Original input: {original_input}")

    local_logger.info(f"Executing update_nifi_processor_properties for ID: {processor_id}, properties: {processor_config_properties}")
    try:
        local_logger.info(f"Fetching current details for processor {processor_id} before update.")
        nifi_get_req = {"operation": "get_processor_details", "processor_id": processor_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
        current_entity = await nifi_api_client.get_processor_details(processor_id)
        component_precheck = current_entity.get("component", {})
        current_state = component_precheck.get("state")
        current_revision = current_entity.get("revision")
        precheck_resp = {"id": processor_id, "state": current_state, "version": current_revision.get('version') if current_revision else None}
        local_logger.bind(interface="nifi", direction="response", data=precheck_resp).debug("Received from NiFi API (for pre-check)")
        
        if current_state == "RUNNING":
            error_msg = f"Processor '{component_precheck.get('name', processor_id)}' ({processor_id}) is currently RUNNING. It must be stopped before its properties can be updated."
            local_logger.warning(error_msg)
            return {"status": "error", "message": error_msg, "entity": None}
        
        if not current_revision:
             raise ToolError(f"Could not retrieve revision for processor {processor_id}.")
             
        nifi_update_req = {
            "operation": "update_processor_config",
            "processor_id": processor_id,
            "update_type": "properties",
            "update_data": processor_config_properties
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API (update processor component)")
        updated_entity = await nifi_api_client.update_processor_config(
            processor_id=processor_id,
            update_type="properties",
            update_data=processor_config_properties
        )
        filtered_updated_entity = filter_created_processor_data(updated_entity)
        local_logger.bind(interface="nifi", direction="response", data=filtered_updated_entity).debug("Received from NiFi API (update processor component)")

        local_logger.info(f"Successfully updated properties for processor {processor_id}")

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

    except ValueError as e:
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
@tool_phases(["Modify"])
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
    await ensure_authenticated(nifi_api_client, local_logger)

    if not property_names_to_delete:
        raise ToolError("The 'property_names_to_delete' list cannot be empty.")
    if not isinstance(property_names_to_delete, list) or not all(isinstance(item, str) for item in property_names_to_delete):
        raise ToolError("Invalid 'property_names_to_delete' type. Expected a non-empty list of strings.")

    local_logger.info(f"Preparing to delete properties {property_names_to_delete} for processor {processor_id}")

    try:
        local_logger.info(f"Fetching current details for processor {processor_id} before deleting properties.")
        nifi_get_req = {"operation": "get_processor_details", "processor_id": processor_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
        current_entity = await nifi_api_client.get_processor_details(processor_id)
        local_logger.bind(interface="nifi", direction="response", data=current_entity).debug("Received from NiFi API (full details)")

        current_revision = current_entity.get("revision")
        current_component = current_entity.get("component", {})
        current_config = current_component.get("config", {})
        current_properties = current_config.get("properties", {})
        current_state = current_component.get("state")

        if current_state == "RUNNING":
            error_msg = f"Processor '{current_component.get('name', processor_id)}' ({processor_id}) is currently RUNNING. It must be stopped before its properties can be deleted."
            local_logger.warning(error_msg)
            return {"status": "error", "message": error_msg, "entity": None}
        
        if not current_revision:
             raise ToolError(f"Could not retrieve revision for processor {processor_id}.")

        modified_properties = current_properties.copy()
        properties_actually_deleted = []
        for prop_name in property_names_to_delete:
            if prop_name in modified_properties:
                modified_properties[prop_name] = None
                properties_actually_deleted.append(prop_name)
            else:
                local_logger.warning(f"Property '{prop_name}' requested for deletion was not found in the current configuration of processor {processor_id}. Skipping.")

        if not properties_actually_deleted:
            local_logger.warning(f"None of the requested properties {property_names_to_delete} were found for deletion. No update will be sent.")
            filtered_current_entity = filter_created_processor_data(current_entity)
            return {
                "status": "success",
                "message": f"No properties needed deletion for processor '{current_component.get('name', processor_id)}'. Properties requested ({property_names_to_delete}) were not found.",
                "entity": filtered_current_entity
            }
            
        local_logger.info(f"Attempting to delete properties: {properties_actually_deleted}")
        nifi_update_req = {
            "operation": "update_processor_config",
            "processor_id": processor_id,
            "update_type": "properties",
            "update_data": modified_properties
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API (update processor component)")
        updated_entity = await nifi_api_client.update_processor_config(
            processor_id=processor_id,
            update_type="properties",
            update_data=modified_properties
        )
        filtered_updated_entity = filter_created_processor_data(updated_entity)
        local_logger.bind(interface="nifi", direction="response", data=filtered_updated_entity).debug("Received from NiFi API (update processor component)")

        local_logger.info(f"Successfully submitted update to delete properties for processor {processor_id}")

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

    except ValueError as e:
        local_logger.warning(f"Error deleting processor properties {processor_id}: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update processor component)")
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
@tool_phases(["Modify"])
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
    await ensure_authenticated(nifi_api_client, local_logger)

    if not isinstance(auto_terminated_relationships, list):
         raise ToolError(f"Invalid 'auto_terminated_relationships' type. Expected a list, got {type(auto_terminated_relationships)}.")
    if not all(isinstance(item, str) for item in auto_terminated_relationships):
        raise ToolError("Invalid 'auto_terminated_relationships' elements. Expected a list of strings (relationship names).")

    local_logger.info(f"Executing update_nifi_processor_relationships for ID: {processor_id}, relationships: {auto_terminated_relationships}")
    try:
        local_logger.info(f"Fetching current details for processor {processor_id} before update.")
        nifi_get_req = {"operation": "get_processor_details", "processor_id": processor_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
        current_entity = await nifi_api_client.get_processor_details(processor_id)
        component_precheck = current_entity.get("component", {})
        current_state = component_precheck.get("state")
        current_revision = current_entity.get("revision")
        precheck_resp = {"id": processor_id, "state": current_state, "version": current_revision.get('version') if current_revision else None}
        local_logger.bind(interface="nifi", direction="response", data=precheck_resp).debug("Received from NiFi API (for pre-check)")
        
        if current_state == "RUNNING":
            error_msg = f"Processor '{component_precheck.get('name', processor_id)}' ({processor_id}) is currently RUNNING. It must be stopped before its relationships can be updated."
            local_logger.warning(error_msg)
            return {"status": "error", "message": error_msg, "entity": None}

        if not current_revision:
            raise ToolError(f"Could not retrieve revision for processor {processor_id}.")
            
        nifi_update_req = {
            "operation": "update_processor_config",
            "processor_id": processor_id,
            "update_type": "auto-terminatedrelationships",
            "update_data": auto_terminated_relationships
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API (update processor component)")
        updated_entity = await nifi_api_client.update_processor_config(
            processor_id=processor_id,
            update_type="auto-terminatedrelationships",
            update_data=auto_terminated_relationships
        )
        filtered_updated_entity = filter_created_processor_data(updated_entity)
        local_logger.bind(interface="nifi", direction="response", data=filtered_updated_entity).debug("Received from NiFi API (update processor component)")

        local_logger.info(f"Successfully updated auto-terminated relationships for processor {processor_id}")

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
            error_msg_snippet = f" ({validation_errors[0]})" if validation_errors else ""
            local_logger.warning(f"Processor '{name}' auto-terminated relationships updated, but validation status is {validation_status}{error_msg_snippet}.")
            return {
                "status": "warning",
                "message": f"Processor '{name}' auto-terminated relationships updated, but validation status is {validation_status}{error_msg_snippet}.",
                "entity": filtered_updated_entity
            }

    except ValueError as e:
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
@tool_phases(["Modify"])
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
    await ensure_authenticated(nifi_api_client, local_logger)

    if not isinstance(relationships, list):
         raise ToolError(f"Invalid 'relationships' type. Expected a list, got {type(relationships)}.")
    if not all(isinstance(item, str) for item in relationships):
        raise ToolError("Invalid 'relationships' elements. Expected a list of strings (relationship names).")
    if not relationships:
        raise ToolError("The 'relationships' list cannot be empty. NiFi connections require at least one selected relationship. To remove the connection, use the 'delete_nifi_object' tool.")

    local_logger.info(f"Executing update_nifi_connection for ID: {connection_id}, relationships: {relationships}")

    try:
        local_logger.info(f"Fetching current details for connection {connection_id} before update.")
        nifi_get_req = {"operation": "get_connection", "connection_id": connection_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
        current_entity = await nifi_api_client.get_connection(connection_id)
        local_logger.bind(interface="nifi", direction="response", data=current_entity).debug("Received from NiFi API (full details)")
        current_revision = current_entity["revision"]
        current_component = current_entity["component"]

        update_component = current_component.copy()
        update_component["selectedRelationships"] = relationships
        
        update_payload = {
            "revision": current_revision,
            "component": update_component
        }

        local_logger.info(f"Attempting to update connection {connection_id} with new relationships.")
        nifi_update_req = {
            "operation": "update_connection",
            "connection_id": connection_id,
            "selectedRelationships": relationships
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API (update connection)")
        updated_entity = await nifi_api_client.update_connection(connection_id, update_payload)
        filtered_updated_entity = filter_connection_data(updated_entity)
        local_logger.bind(interface="nifi", direction="response", data=filtered_updated_entity).debug("Received from NiFi API (update connection)")

        local_logger.info(f"Successfully updated relationships for connection {connection_id}")

        return {
            "status": "success",
            "message": f"Connection '{connection_id}' relationships updated successfully.",
            "entity": filtered_updated_entity
        }

    except ValueError as e:
        local_logger.warning(f"Error updating connection {connection_id}: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (update connection)")
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
@tool_phases(["Modify"])
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
    await ensure_authenticated(nifi_api_client, local_logger)

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

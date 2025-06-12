import asyncio
from typing import List, Dict, Optional, Any, Union, Literal

# Import necessary components from parent/utils
from loguru import logger
# Import mcp ONLY
from ..core import mcp, handle_nifi_errors, _get_component_details_direct
# Removed nifi_api_client import
# Import context variables
from ..request_context import current_nifi_client, current_request_logger # Added
from config import settings as mcp_settings # Corrected import

from .utils import (
    tool_phases,
    # ensure_authenticated, # Removed
    filter_created_processor_data,
    filter_connection_data
)
from .review import get_nifi_object_details, list_nifi_objects  # Import both functions at the top
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
    
    Automatically stops running processors if Auto-Stop feature is enabled, then performs the update.
    If Auto-Stop is disabled, running processors will cause an error.

    Example:
    ```python
    {
        "processor_id": "123e4567-e89b-12d3-a456-426614174000",
        "processor_config_properties": {
            "Property1": "Value1",
            "Property2": "Value2",
            ...
        }
    }
    ```

    Args:
        processor_id: The UUID of the processor to update.
        processor_config_properties: A complete dictionary representing the desired final state of all properties. Cannot be empty.

    Returns:
        A dictionary representing the updated processor entity or an error status.
    """
    # Get client and logger from context
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger
    if not nifi_client:
        raise ToolError("NiFi client context is not set. This tool requires the X-Nifi-Server-Id header.")
    if not local_logger:
         raise ToolError("Request logger context is not set.")
         
    # Authentication handled by factory

    if not processor_config_properties:
        error_msg = "The 'processor_config_properties' argument cannot be empty. Fetch current config first."
        local_logger.warning(f"Validation failed for update_nifi_processor_properties (ID={processor_id}): {error_msg}")
        raise ToolError(error_msg)

    if not isinstance(processor_config_properties, dict):
         raise ToolError(f"Invalid 'processor_config_properties' type. Expected dict, got {type(processor_config_properties)}.")

    # Handle potential accidental nesting (e.g., passing {"properties": {...}})
    if isinstance(processor_config_properties, dict) and \
       list(processor_config_properties.keys()) == ["properties"] and \
       isinstance(processor_config_properties["properties"], dict):
        original_input = processor_config_properties
        processor_config_properties = processor_config_properties["properties"]
        local_logger.warning(f"Detected nested 'properties' key in input for processor {processor_id}. Correcting structure.")

    local_logger = local_logger.bind(processor_id=processor_id)
    local_logger.info(f"Executing update_nifi_processor_properties with properties: {processor_config_properties}")
    try:
        local_logger.info(f"Fetching current details for processor {processor_id} before update.")
        nifi_get_req = {"operation": "get_processor_details", "processor_id": processor_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
        current_entity = await nifi_client.get_processor_details(processor_id)
        component_precheck = current_entity.get("component", {})
        current_state = component_precheck.get("state")
        current_revision = current_entity.get("revision")
        precheck_resp = {"id": processor_id, "state": current_state, "version": current_revision.get('version') if current_revision else None}
        local_logger.bind(interface="nifi", direction="response", data=precheck_resp).debug("Received from NiFi API (pre-check)")
        
        # --- AUTO-STOP LOGIC ---
        if current_state == "RUNNING":
            local_logger.info(f"Processor '{component_precheck.get('name', processor_id)}' is RUNNING. Checking Auto-Stop feature.")
            
            # Get headers from request context
            from config.logging_setup import request_context
            context_data = request_context.get()
            request_headers = context_data.get('headers', {}) if context_data else {}
            
            # Convert header keys to lowercase for case-insensitive comparison
            if request_headers:
                request_headers = {k.lower(): v for k, v in request_headers.items()}
            
            is_auto_stop_feature_enabled = mcp_settings.get_feature_auto_stop_enabled(headers=request_headers)
            local_logger.info(f"[Auto-Stop] Feature flag check - headers: {request_headers}")
            local_logger.info(f"[Auto-Stop] Feature enabled: {is_auto_stop_feature_enabled}")

            if is_auto_stop_feature_enabled:
                # Stop the processor first
                local_logger.info(f"[Auto-Stop] Stopping processor {processor_id}")
                try:
                    nifi_request_data = {"operation": "stop_processor", "processor_id": processor_id}
                    local_logger.bind(interface="nifi", direction="request", data=nifi_request_data).debug("Calling NiFi API")
                    await nifi_client.stop_processor(processor_id)
                    local_logger.bind(interface="nifi", direction="response", data={"status": "success"}).debug("Received from NiFi API")
                    
                    # Wait for processor to fully stop
                    max_wait_seconds = 15
                    for attempt in range(max_wait_seconds):
                        updated_details = await nifi_client.get_processor_details(processor_id)
                        current_state = updated_details.get("component", {}).get("state")
                        if current_state == "STOPPED":
                            local_logger.info(f"[Auto-Stop] Confirmed processor {processor_id} is stopped")
                            # Update our references with the latest details
                            current_entity = updated_details
                            component_precheck = current_entity.get("component", {})
                            current_revision = current_entity.get("revision")
                            break
                        
                        if attempt == max_wait_seconds - 1:
                            raise ToolError(f"Processor {processor_id} did not stop after {max_wait_seconds} seconds")
                        
                        local_logger.info(f"[Auto-Stop] Waiting for processor to stop (attempt {attempt + 1}/{max_wait_seconds})")
                        await asyncio.sleep(1)

                except Exception as e:
                    local_logger.error(f"[Auto-Stop] Failed to stop processor: {e}", exc_info=True)
                    local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
                    return {"status": "error", "message": f"Failed to auto-stop processor for update: {e}", "entity": None}
            else:
                error_msg = f"Processor '{component_precheck.get('name', processor_id)}' is RUNNING. Stop it before updating properties or enable Auto-Stop."
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
        local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API")
        updated_entity = await nifi_client.update_processor_config(
            processor_id=processor_id,
            update_type="properties",
            update_data=processor_config_properties
        )
        filtered_updated_entity = filter_created_processor_data(updated_entity)
        local_logger.bind(interface="nifi", direction="response", data=filtered_updated_entity).debug("Received from NiFi API")

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
        local_logger.warning(f"Error updating processor properties: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"Error updating properties: {e}", "entity": None}
    except (NiFiAuthenticationError, ConnectionError, ToolError) as e: # Include ToolError
        local_logger.error(f"API/Tool error updating processor properties: {e}", exc_info=False)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"Failed to update properties: {e}", "entity": None}
    except Exception as e:
        local_logger.error(f"Unexpected error updating processor properties: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"An unexpected error occurred during update: {e}", "entity": None}


@mcp.tool()
@tool_phases(["Modify"])
async def delete_nifi_processor_properties(
    processor_id: str,
    property_names_to_delete: List[str]
) -> Dict:
    """
    Deletes specific properties from a processor's configuration by setting their values to null.
    
    Automatically stops running processors if Auto-Stop feature is enabled, then performs the update.
    If Auto-Stop is disabled, running processors will cause an error.

    Args:
        processor_id: The UUID of the processor to modify.
        property_names_to_delete: A non-empty list of property names (strings) to delete.

    Returns:
        A dictionary representing the updated processor entity or an error status.
    """
    # Get client and logger from context
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger
    if not nifi_client:
        raise ToolError("NiFi client context is not set. This tool requires the X-Nifi-Server-Id header.")
    if not local_logger:
         raise ToolError("Request logger context is not set.")
         
    # Authentication handled by factory

    if not property_names_to_delete:
        raise ToolError("The 'property_names_to_delete' list cannot be empty.")
    if not isinstance(property_names_to_delete, list) or not all(isinstance(item, str) for item in property_names_to_delete):
        raise ToolError("Invalid 'property_names_to_delete' type. Expected a non-empty list of strings.")

    local_logger = local_logger.bind(processor_id=processor_id)
    local_logger.info(f"Preparing to delete properties {property_names_to_delete}")

    try:
        local_logger.info(f"Fetching current details for processor {processor_id}...")
        nifi_get_req = {"operation": "get_processor_details", "processor_id": processor_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
        current_entity = await nifi_client.get_processor_details(processor_id)
        local_logger.bind(interface="nifi", direction="response", data=current_entity).debug("Received from NiFi API (full details)")

        current_revision = current_entity.get("revision")
        current_component = current_entity.get("component", {})
        current_config = current_component.get("config", {})
        current_properties = current_config.get("properties", {})
        current_state = current_component.get("state")

        # --- AUTO-STOP LOGIC ---
        if current_state == "RUNNING":
            local_logger.info(f"Processor '{current_component.get('name', processor_id)}' is RUNNING. Checking Auto-Stop feature.")
            
            # Get headers from request context
            from config.logging_setup import request_context
            context_data = request_context.get()
            request_headers = context_data.get('headers', {}) if context_data else {}
            
            # Convert header keys to lowercase for case-insensitive comparison
            if request_headers:
                request_headers = {k.lower(): v for k, v in request_headers.items()}
            
            is_auto_stop_feature_enabled = mcp_settings.get_feature_auto_stop_enabled(headers=request_headers)
            local_logger.info(f"[Auto-Stop] Feature flag check - headers: {request_headers}")
            local_logger.info(f"[Auto-Stop] Feature enabled: {is_auto_stop_feature_enabled}")

            if is_auto_stop_feature_enabled:
                # Stop the processor first
                local_logger.info(f"[Auto-Stop] Stopping processor {processor_id}")
                try:
                    nifi_request_data = {"operation": "stop_processor", "processor_id": processor_id}
                    local_logger.bind(interface="nifi", direction="request", data=nifi_request_data).debug("Calling NiFi API")
                    await nifi_client.stop_processor(processor_id)
                    local_logger.bind(interface="nifi", direction="response", data={"status": "success"}).debug("Received from NiFi API")
                    
                    # Wait for processor to fully stop
                    max_wait_seconds = 15
                    for attempt in range(max_wait_seconds):
                        updated_details = await nifi_client.get_processor_details(processor_id)
                        current_state = updated_details.get("component", {}).get("state")
                        if current_state == "STOPPED":
                            local_logger.info(f"[Auto-Stop] Confirmed processor {processor_id} is stopped")
                            # Update our references with the latest details
                            current_entity = updated_details
                            current_component = current_entity.get("component", {})
                            current_revision = current_entity.get("revision")
                            break
                        
                        if attempt == max_wait_seconds - 1:
                            raise ToolError(f"Processor {processor_id} did not stop after {max_wait_seconds} seconds")
                        
                        local_logger.info(f"[Auto-Stop] Waiting for processor to stop (attempt {attempt + 1}/{max_wait_seconds})")
                        await asyncio.sleep(1)

                except Exception as e:
                    local_logger.error(f"[Auto-Stop] Failed to stop processor: {e}", exc_info=True)
                    local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
                    return {"status": "error", "message": f"Failed to auto-stop processor for update: {e}", "entity": None}
            else:
                error_msg = f"Processor '{current_component.get('name', processor_id)}' is RUNNING. Stop it before deleting properties or enable Auto-Stop."
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
                local_logger.warning(f"Property '{prop_name}' not found for deletion. Skipping.")

        if not properties_actually_deleted:
            local_logger.warning(f"None of the requested properties {property_names_to_delete} were found. No update sent.")
            filtered_current_entity = filter_created_processor_data(current_entity)
            return {
                "status": "success",
                "message": f"No properties needed deletion for processor '{current_component.get('name', processor_id)}'. Requested properties not found.",
                "entity": filtered_current_entity
            }
            
        local_logger.info(f"Attempting to delete properties: {properties_actually_deleted}")
        nifi_update_req = {
            "operation": "update_processor_config",
            "processor_id": processor_id,
            "update_type": "properties",
            "update_data": modified_properties
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API")
        updated_entity = await nifi_client.update_processor_config(
            processor_id=processor_id,
            update_type="properties",
            update_data=modified_properties
        )
        filtered_updated_entity = filter_created_processor_data(updated_entity)
        local_logger.bind(interface="nifi", direction="response", data=filtered_updated_entity).debug("Received from NiFi API")

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
        local_logger.warning(f"Error deleting processor properties: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        if "not found" in str(e).lower():
             return {"status": "error", "message": f"Processor {processor_id} not found.", "entity": None}
        elif "conflict" in str(e).lower() or "revision mismatch" in str(e).lower():
             return {"status": "error", "message": f"Conflict deleting properties for processor {processor_id}. Revision mismatch: {e}", "entity": None}
        else:
            return {"status": "error", "message": f"Error deleting properties for processor {processor_id}: {e}", "entity": None}
    except (NiFiAuthenticationError, ConnectionError, ToolError) as e:
        local_logger.error(f"API/Tool error deleting processor properties: {e}", exc_info=False)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"Failed to delete properties: {e}", "entity": None}
    except Exception as e:
        local_logger.error(f"Unexpected error deleting processor properties: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"An unexpected error occurred during deletion: {e}", "entity": None}


@mcp.tool()
@tool_phases(["Modify"])
async def update_nifi_processor_relationships(
    processor_id: str,
    auto_terminated_relationships: List[str]
) -> Dict:
    """
    Updates the list of auto-terminated relationships for a processor.
    Replaces the entire existing list with the provided list.
    
    Automatically stops running processors if Auto-Stop feature is enabled, then performs the update.
    If Auto-Stop is disabled, running processors will cause an error.

    Args:
        processor_id: The UUID of the processor to update.
        auto_terminated_relationships: A list of relationship names (strings) to be auto-terminated.
                                        Use an empty list `[]` to clear all auto-terminations.

    Returns:
        A dictionary representing the updated processor entity or an error status.
    """
    # Get client and logger from context
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger
    if not nifi_client:
        raise ToolError("NiFi client context is not set. This tool requires the X-Nifi-Server-Id header.")
    if not local_logger:
         raise ToolError("Request logger context is not set.")
         
    # Authentication handled by factory

    if not isinstance(auto_terminated_relationships, list) or not all(isinstance(item, str) for item in auto_terminated_relationships):
         raise ToolError("Invalid 'auto_terminated_relationships' type. Expected a list of strings (can be empty).")

    local_logger = local_logger.bind(processor_id=processor_id)
    local_logger.info(f"Executing update_nifi_processor_relationships for ID: {processor_id}. Setting auto-terminate to: {auto_terminated_relationships}")

    try:
        # Fetch current entity first to check state (might not be strictly needed for relationships, but good practice)
        local_logger.info(f"Fetching current details for processor {processor_id} before update.")
        nifi_get_req = {"operation": "get_processor_details", "processor_id": processor_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
        current_entity = await nifi_client.get_processor_details(processor_id)
        component_precheck = current_entity.get("component", {})
        current_state = component_precheck.get("state")
        local_logger.bind(interface="nifi", direction="response", data=current_entity).debug("Received from NiFi API (pre-check)")

        # --- AUTO-STOP LOGIC ---
        if current_state == "RUNNING":
            local_logger.info(f"Processor '{component_precheck.get('name', processor_id)}' is RUNNING. Checking Auto-Stop feature.")
            
            # Get headers from request context
            from config.logging_setup import request_context
            context_data = request_context.get()
            request_headers = context_data.get('headers', {}) if context_data else {}
            
            # Convert header keys to lowercase for case-insensitive comparison
            if request_headers:
                request_headers = {k.lower(): v for k, v in request_headers.items()}
            
            is_auto_stop_feature_enabled = mcp_settings.get_feature_auto_stop_enabled(headers=request_headers)
            local_logger.info(f"[Auto-Stop] Feature flag check - headers: {request_headers}")
            local_logger.info(f"[Auto-Stop] Feature enabled: {is_auto_stop_feature_enabled}")

            if is_auto_stop_feature_enabled:
                # Stop the processor first
                local_logger.info(f"[Auto-Stop] Stopping processor {processor_id}")
                try:
                    nifi_request_data = {"operation": "stop_processor", "processor_id": processor_id}
                    local_logger.bind(interface="nifi", direction="request", data=nifi_request_data).debug("Calling NiFi API")
                    await nifi_client.stop_processor(processor_id)
                    local_logger.bind(interface="nifi", direction="response", data={"status": "success"}).debug("Received from NiFi API")
                    
                    # Wait for processor to fully stop
                    max_wait_seconds = 15
                    for attempt in range(max_wait_seconds):
                        updated_details = await nifi_client.get_processor_details(processor_id)
                        current_state = updated_details.get("component", {}).get("state")
                        if current_state == "STOPPED":
                            local_logger.info(f"[Auto-Stop] Confirmed processor {processor_id} is stopped")
                            # Update our references with the latest details
                            current_entity = updated_details
                            component_precheck = current_entity.get("component", {})
                            current_revision = current_entity.get("revision")
                            break
                        
                        if attempt == max_wait_seconds - 1:
                            raise ToolError(f"Processor {processor_id} did not stop after {max_wait_seconds} seconds")
                        
                        local_logger.info(f"[Auto-Stop] Waiting for processor to stop (attempt {attempt + 1}/{max_wait_seconds})")
                        await asyncio.sleep(1)

                except Exception as e:
                    local_logger.error(f"[Auto-Stop] Failed to stop processor: {e}", exc_info=True)
                    local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
                    return {"status": "error", "message": f"Failed to auto-stop processor for update: {e}", "entity": None}
            else:
                error_msg = f"Processor '{component_precheck.get('name', processor_id)}' is RUNNING. Stop it before updating relationships or enable Auto-Stop."
                local_logger.warning(error_msg)
                return {"status": "error", "message": error_msg, "entity": None}
            
        nifi_update_req = {
            "operation": "update_processor_config",
            "processor_id": processor_id,
            "update_type": "auto-terminatedrelationships",
            "update_data": auto_terminated_relationships
        }
        local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API")
        updated_entity = await nifi_client.update_processor_config(
            processor_id=processor_id,
            update_type="auto-terminatedrelationships",
            update_data=auto_terminated_relationships
        )
        filtered_updated_entity = filter_created_processor_data(updated_entity)
        local_logger.bind(interface="nifi", direction="response", data=filtered_updated_entity).debug("Received from NiFi API")

        local_logger.info(f"Successfully updated auto-terminated relationships for processor {processor_id}")
        name = updated_entity.get("component", {}).get("name", processor_id)
        return {
            "status": "success",
            "message": f"Processor '{name}' auto-terminated relationships updated successfully.",
            "entity": filtered_updated_entity
        }

    except ValueError as e:
        local_logger.warning(f"Error updating processor relationships {processor_id}: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"Error updating relationships for processor {processor_id}: {e}", "entity": None}
    except (NiFiAuthenticationError, ConnectionError, ToolError) as e:
        local_logger.error(f"API/Tool error updating processor relationships: {e}", exc_info=False)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"Failed to update relationships: {e}", "entity": None}
    except Exception as e:
        local_logger.error(f"Unexpected error updating processor relationships: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"An unexpected error occurred during relationship update: {e}", "entity": None}


@mcp.tool()
@tool_phases(["Modify"])
async def update_nifi_connection(
    connection_id: str,
    relationships: List[str]
) -> Dict:
    """
    Updates the selected relationships for an existing connection.
    Replaces the entire list of selected relationships with the provided list.

    Args:
        connection_id: The UUID of the connection to update.
        relationships: The complete list of relationship names (strings) that should be active for this connection.
                       Cannot be empty.

    Returns:
        A dictionary representing the updated connection entity or an error status.
    """
    # Get client and logger from context
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger
    if not nifi_client:
        raise ToolError("NiFi client context is not set. This tool requires the X-Nifi-Server-Id header.")
    if not local_logger:
         raise ToolError("Request logger context is not set.")
         
    # Authentication handled by factory

    if not relationships:
         raise ToolError("The 'relationships' list cannot be empty. Provide at least one relationship name.")
    if not isinstance(relationships, list) or not all(isinstance(item, str) for item in relationships):
         raise ToolError("Invalid 'relationships' type. Expected a non-empty list of strings.")
         
    local_logger = local_logger.bind(connection_id=connection_id)
    local_logger.info(f"Executing update_nifi_connection for ID: {connection_id}. Setting relationships to: {relationships}")

    try:
        local_logger.info(f"Fetching current details for connection {connection_id} before update.")
        nifi_get_req = {"operation": "get_connection", "connection_id": connection_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
        current_entity = await nifi_client.get_connection(connection_id)
        local_logger.bind(interface="nifi", direction="response", data=current_entity).debug("Received from NiFi API (full details)")

        current_revision = current_entity.get("revision")
        current_component = current_entity.get("component", {})
        
        if not current_revision:
            raise ToolError(f"Could not retrieve revision for connection {connection_id}.")

        # Prepare the update payload - only include fields that need updating + identity fields
        update_component = {
            "id": current_component.get("id"),
            "parentGroupId": current_component.get("parentGroupId"),
            # Include source and destination for potential validation/context by NiFi
            "source": current_component.get("source"),
            "destination": current_component.get("destination"),
            # The actual update
            "selectedRelationships": relationships
        }

        update_payload = {
            "revision": current_revision,
            "component": update_component
        }

        local_logger.info(f"Attempting update for connection {connection_id}")
        nifi_update_req = {"operation": "update_connection", "connection_id": connection_id, "payload": update_payload}
        local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API")
        updated_entity = await nifi_client.update_connection(connection_id, update_payload)
        filtered_updated_entity = filter_connection_data(updated_entity)
        local_logger.bind(interface="nifi", direction="response", data=filtered_updated_entity).debug("Received from NiFi API")

        local_logger.info(f"Successfully updated relationships for connection {connection_id}")
        return {
            "status": "success",
            "message": f"Connection {connection_id} relationships updated successfully.",
            "entity": filtered_updated_entity
        }

    except ValueError as e:
        local_logger.warning(f"Error updating connection {connection_id}: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        if "not found" in str(e).lower():
             return {"status": "error", "message": f"Connection {connection_id} not found.", "entity": None}
        elif "conflict" in str(e).lower() or "revision mismatch" in str(e).lower():
             return {"status": "error", "message": f"Conflict updating connection {connection_id}. Revision mismatch: {e}", "entity": None}
        else:
            return {"status": "error", "message": f"Error updating connection {connection_id}: {e}", "entity": None}
    except (NiFiAuthenticationError, ConnectionError, ToolError) as e:
        local_logger.error(f"API/Tool error updating connection: {e}", exc_info=False)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"Failed to update connection: {e}", "entity": None}
    except Exception as e:
        local_logger.error(f"Unexpected error updating connection: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"An unexpected error occurred during connection update: {e}", "entity": None}


@mcp.tool()
@tool_phases(["Modify"])
@handle_nifi_errors
async def delete_nifi_object(
    object_type: Literal["processor", "connection", "port", "process_group"],
    object_id: str,
    **kwargs # To potentially catch headers if passed by MCP
) -> Dict:
    """
    Deletes a specific NiFi object (processor, connection, port, or process group).
    Attempts Auto-Stop for running processors if enabled.
    Attempts Auto-Delete for processors with connections if enabled.

    Args:
        object_type: The type of the object to delete ('processor', 'connection', 'port', 'process_group').
        object_id: The UUID of the object to delete.
        **kwargs: May contain 'request_headers' for feature flag overrides.

    Returns:
        A dictionary indicating success or failure.
    """
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger
    if not nifi_client:
        raise ToolError("NiFi client context is not set. This tool requires the X-Nifi-Server-Id header.")
    if not local_logger:
         raise ToolError("Request logger context is not set.")
    
    local_logger = local_logger.bind(object_id=object_id, object_type=object_type)
    local_logger.info(f"Executing delete_nifi_object for {object_type} ID: {object_id}")

    try:
        # 1. Get current details using get_nifi_object_details
        local_logger.info(f"Fetching details for {object_type} {object_id}")
        current_entity = await get_nifi_object_details(object_type=object_type, object_id=object_id)
        if not current_entity:
            raise ToolError(f"Could not retrieve details for {object_type} {object_id}")

        # Refine port type if needed
        original_object_type = object_type
        if object_type == "port":
            port_type = current_entity.get("component", {}).get("type", "").lower()
            if "input" in port_type:
                object_type = "input_port"
            elif "output" in port_type:
                object_type = "output_port"
            else:
                raise ToolError(f"Unknown port type for port {object_id}: {port_type}")

        current_revision_dict = current_entity.get("revision")
        if not current_revision_dict or "version" not in current_revision_dict:
            raise ToolError(f"Could not retrieve current revision version for {object_type} {object_id}. Cannot delete.")
        current_version = current_revision_dict["version"]

        component = current_entity.get("component", {})
        state = component.get("state")
        name = component.get("name", object_id)

        # --- AUTO-DELETE PRE-EMPTIVE LOGIC ---
        if original_object_type == "processor":
            # Get headers from request context
            from config.logging_setup import request_context
            context_data = request_context.get()
            request_headers = context_data.get('headers', {}) if context_data else {}
            
            # Convert header keys to lowercase for case-insensitive comparison
            if request_headers:
                request_headers = {k.lower(): v for k, v in request_headers.items()}
            
            is_auto_delete_feature_enabled = mcp_settings.get_feature_auto_delete_enabled(headers=request_headers)
            local_logger.info(f"[Auto-Delete] Feature flag check - headers: {request_headers}")
            local_logger.info(f"[Auto-Delete] Feature enabled: {is_auto_delete_feature_enabled}")
            
            # Auto-Stop should have already run by this point and verified processor stopped
            # Now check for connections
            processor_details = await nifi_client.get_processor_details(object_id)
            processor_component = processor_details.get("component", {})
            processor_name = processor_component.get("name", "unknown")
            parent_pg_id = processor_component.get("parentGroupId")
            
            if not parent_pg_id:
                local_logger.error(f"[Auto-Delete] Could not determine parent process group for processor {processor_name} ({object_id})")
                return {"status": "error", "message": f"Failed to determine parent process group for processor {processor_name}", "entity": None}
            
            # Check if the processor is part of any connections
            local_logger.info(f"[Auto-Delete] Checking for connections to processor {processor_name} ({object_id}) in process group {parent_pg_id}")
            try:
                connections_list = await nifi_client.list_connections(parent_pg_id)
                
                connections = []
                connection_ids = []
                for connection_data in connections_list:
                    # The connection details are in the first item of the list
                    if not isinstance(connection_data, dict):
                        continue
                    component = connection_data.get('component', {})
                    source = component.get('source', {})
                    destination = component.get('destination', {})
                    if (source.get('id') == object_id or destination.get('id') == object_id):
                        connections.append(connection_data)
                        connection_id = connection_data.get('id')
                        if not connection_id:
                            # Try to get from component if not at root level
                            connection_id = component.get('id')
                        if connection_id:
                            connection_ids.append(connection_id)
                            local_logger.info(f"[Auto-Delete] Found connection: {connection_id} with source={source.get('id')} and destination={destination.get('id')}")
                
                if connections:
                    local_logger.info(f"[Auto-Delete] Found {len(connections)} connections for processor {processor_name}")
                    
                    if not is_auto_delete_feature_enabled:
                        # Auto-Delete is disabled, we should fail the deletion
                        local_logger.warning(f"[Auto-Delete] Feature disabled. Cannot delete processor with connections.")
                        return {"status": "error", "message": f"Processor {processor_name} has {len(connections)} connections. Auto-Delete is disabled. Please delete connections first or enable Auto-Delete.", "entity": None}
                    
                    # Auto-Delete is enabled, delete the connections first
                    local_logger.info(f"[Auto-Delete] Feature enabled. Automatically deleting {len(connections)} connections.")
                    
                    # Use the batch delete method to delete all connections at once
                    if connection_ids:
                        local_logger.info(f"[Auto-Delete] Attempting batch deletion of {len(connection_ids)} connections: {connection_ids}")
                        try:
                            # Make sure we have a valid client
                            if not nifi_client._client:
                                await nifi_client._get_client()
                            
                            # First, check if any source processors are running and stop them
                            processors_to_stop = set()
                            for connection_data in connections:
                                component = connection_data.get('component', {})
                                source = component.get('source', {})
                                source_id = source.get('id')
                                source_type = source.get('type', '').upper()
                                
                                # Only stop processors, not other component types
                                if source_id and source_type == 'PROCESSOR' and source_id != object_id:
                                    # Check if the processor is running
                                    try:
                                        proc_details = await nifi_client.get_processor_details(source_id)
                                        proc_state = proc_details.get('component', {}).get('state', '')
                                        if proc_state == 'RUNNING':
                                            processors_to_stop.add(source_id)
                                            local_logger.info(f"[Auto-Delete] Found running source processor {source_id} that needs to be stopped")
                                    except Exception as e:
                                        local_logger.warning(f"[Auto-Delete] Could not check processor {source_id} state: {e}")
                            
                            # Stop any running processors
                            for proc_id in processors_to_stop:
                                try:
                                    local_logger.info(f"[Auto-Delete] Stopping processor {proc_id} to allow connection deletion")
                                    await nifi_client.stop_processor(proc_id)
                                    # Wait a moment for the processor to fully stop
                                    await asyncio.sleep(1)
                                except Exception as e:
                                    local_logger.warning(f"[Auto-Delete] Failed to stop processor {proc_id}: {e}")

                            # Verify processors are fully stopped before proceeding with connection deletion
                            if processors_to_stop:
                                local_logger.info(f"[Auto-Delete] Verifying {len(processors_to_stop)} processors are fully stopped")
                                max_wait_seconds = 5
                                for attempt in range(max_wait_seconds):
                                    all_stopped = True
                                    for proc_id in processors_to_stop:
                                        try:
                                            proc_details = await nifi_client.get_processor_details(proc_id)
                                            proc_state = proc_details.get('component', {}).get('state')
                                            if proc_state != 'STOPPED':
                                                local_logger.info(f"[Auto-Delete] Processor {proc_id} still in state {proc_state} on attempt {attempt+1}")
                                                all_stopped = False
                                                break
                                        except Exception as e:
                                            local_logger.warning(f"[Auto-Delete] Error checking processor state: {e}")
                                            all_stopped = False
                                            break
                                    
                                    if all_stopped:
                                        local_logger.info(f"[Auto-Delete] All source processors verified as stopped")
                                        break
                                    
                                    if attempt < max_wait_seconds - 1:  # Don't sleep on last iteration
                                        local_logger.info(f"[Auto-Delete] Waiting for processors to stop (attempt {attempt+1}/{max_wait_seconds})")
                                        await asyncio.sleep(1)
                                
                                if not all_stopped:
                                    local_logger.warning(f"[Auto-Delete] Some processors may not be fully stopped yet. Proceeding with caution.")
                            
                            # Now try to delete the connections
                            auto_delete_errors = []  # Initialize the errors collection
                            batch_results = await nifi_client.delete_connections_batch(connection_ids)
                            
                            # Debug output
                            for conn_id, result in batch_results.items():
                                if result.get("success"):
                                    local_logger.info(f"[Auto-Delete] Successfully deleted connection {conn_id}")
                                else:
                                    error_msg = result.get('message', 'Unknown error')
                                    # Check if this is an expected error (processor still running)
                                    if "running" in error_msg.lower() or "active" in error_msg.lower():
                                        local_logger.warning(f"[Auto-Delete] Could not delete connection {conn_id}: {error_msg}")
                                    else:
                                        local_logger.error(f"[Auto-Delete] Failed to delete connection {conn_id}: {error_msg}")
                                    auto_delete_errors.append(f"{conn_id}: {error_msg}")
                                    if result.get("error"):
                                        # Log the actual error at debug level to reduce console noise
                                        local_logger.debug(f"[Auto-Delete] Error details: {str(result.get('error'))}")
                        except Exception as conn_delete_error:
                            error_msg = f"Error in batch deletion: {conn_delete_error}"
                            local_logger.error(f"[Auto-Delete] {error_msg}")
                            auto_delete_errors.append(error_msg)
                        
                        # Check for any failures
                        failures = []
                        for conn_id, result in batch_results.items():
                            if not result.get("success"):
                                failures.append(f"{conn_id}: {result.get('message')}")
                                
                        if failures:
                            error_message = f"Auto-Delete encountered {len(failures)} errors: {', '.join(failures)}"
                            # Log as warning if we're proceeding with processor deletion anyway
                            local_logger.warning(f"[Auto-Delete] {error_message}")
                            return {"status": "error", "message": error_message, "entity": None}
                        
                        local_logger.info(f"[Auto-Delete] Successfully deleted {len(connection_ids)} connections in batch.")
                else:
                    local_logger.info(f"[Auto-Delete] No connections found for processor {processor_name}")
            except Exception as e:
                local_logger.error(f"[Auto-Delete] Error checking for connections: {str(e)}")
                return {"status": "error", "message": f"Error checking for connections: {str(e)}", "entity": None}

        # --- AUTO-PURGE PRE-EMPTIVE LOGIC ---
        if object_type == "connection":
            # Get headers from request context
            from config.logging_setup import request_context
            context_data = request_context.get()
            request_headers = context_data.get('headers', {}) if context_data else {}
            
            # Convert header keys to lowercase for case-insensitive comparison
            if request_headers:
                request_headers = {k.lower(): v for k, v in request_headers.items()}
            
            is_auto_purge_feature_enabled = mcp_settings.get_feature_auto_purge_enabled(headers=request_headers)
            local_logger.info(f"[Auto-Purge] Feature flag check - headers: {request_headers}")
            local_logger.info(f"[Auto-Purge] Feature enabled: {is_auto_purge_feature_enabled}")

            # Check if connection has queued data
            connection_status = current_entity.get("status", {}).get("aggregateSnapshot", {})
            queued_count = int(connection_status.get("queuedCount", "0"))
            
            if queued_count > 0:
                if is_auto_purge_feature_enabled:
                    # Attempt to purge the queue
                    local_logger.info(f"[Auto-Purge] Connection has {queued_count} queued items. Attempting to purge.")
                    try:
                        # Create a drop request
                        drop_request = await nifi_client.create_drop_request(object_id)
                        drop_request_id = drop_request.get("id") # Get ID directly from the drop request object
                        if not drop_request_id:
                            raise ToolError("Failed to create drop request - no ID returned")
                        
                        # Wait for the drop request to complete
                        await nifi_client.handle_drop_request(object_id, timeout_seconds=30)
                        local_logger.info(f"[Auto-Purge] Successfully purged queue for connection {object_id}")
                    except Exception as e:
                        local_logger.error(f"[Auto-Purge] Failed to purge connection queue: {e}", exc_info=True)
                        raise ToolError(f"Failed to auto-purge connection queue: {e}")
                else:
                    error_msg = f"Cannot delete connection {object_id} with {queued_count} queued items when Auto-Purge is disabled"
                    local_logger.warning(error_msg)
                    return {"status": "error", "message": error_msg}

        # --- AUTO-STOP PRE-EMPTIVE LOGIC --- 
        if original_object_type == "processor" and state == "RUNNING":
            local_logger.info(f"Processor '{name}' is RUNNING. Checking Auto-Stop feature.")
            
            # Get headers from request context
            from config.logging_setup import request_context
            context_data = request_context.get()
            request_headers = context_data.get('headers', {}) if context_data else {}
            
            # Convert header keys to lowercase for case-insensitive comparison
            if request_headers:
                request_headers = {k.lower(): v for k, v in request_headers.items()}
            
            is_auto_stop_feature_enabled = mcp_settings.get_feature_auto_stop_enabled(headers=request_headers)
            local_logger.info(f"[Auto-Stop] Feature flag check - headers: {request_headers}")
            local_logger.info(f"[Auto-Stop] Feature enabled: {is_auto_stop_feature_enabled}")

            if is_auto_stop_feature_enabled:
                # Stop the component first
                local_logger.info(f"[Auto-Stop] Stopping {original_object_type} {object_id}")
                try:
                    async def verify_stopped(obj_type: str, obj_id: str, max_wait_seconds: int = 15) -> bool:
                        """Verify that a component (processor or process group) has stopped."""
                        for attempt in range(max_wait_seconds):
                            if obj_type == "processor":
                                details = await nifi_client.get_processor_details(obj_id)
                                current_state = details.get("component", {}).get("state")
                                if current_state == "STOPPED":
                                    local_logger.info(f"[Auto-Stop] Confirmed processor {obj_id} is stopped")
                                    return True, details  # Return both status and details
                            elif obj_type == "process_group":
                                # For PGs, we need to check all processors within
                                processors = await list_nifi_objects(
                                    object_type="processors",
                                    process_group_id=obj_id,
                                    search_scope="all"
                                )
                                all_stopped = True
                                for proc in processors:
                                    if isinstance(proc, dict):
                                        state = proc.get("component", {}).get("state")
                                        if state == "RUNNING":
                                            all_stopped = False
                                            break
                                if all_stopped:
                                    local_logger.info(f"[Auto-Stop] Confirmed all processors in PG {obj_id} are stopped")
                                    return True, None  # No specific details for PG
                            
                            if attempt == max_wait_seconds - 1:
                                local_logger.warning(f"[Auto-Stop] Maximum wait time reached without confirming stopped state for {obj_type} {obj_id}")
                                return False, None
                            
                            local_logger.info(f"[Auto-Stop] Waiting for {obj_type} to stop (attempt {attempt + 1}/{max_wait_seconds})")
                            await asyncio.sleep(1)
                        return False, None

                    if original_object_type == "processor":
                        nifi_request_data = {"operation": "stop_processor", "processor_id": object_id}
                        local_logger.bind(interface="nifi", direction="request", data=nifi_request_data).debug("Calling NiFi API")
                        await nifi_client.stop_processor(object_id)
                        local_logger.bind(interface="nifi", direction="response", data={"status": "success"}).debug("Received from NiFi API")
                        
                        is_stopped, updated_details = await verify_stopped("processor", object_id)
                        if not is_stopped:
                            raise ToolError(f"Processor {object_id} did not stop after 15 seconds")
                        # Update our state and component info with the latest details
                        if updated_details:
                            state = updated_details.get("component", {}).get("state")
                            component = updated_details.get("component", {})
                            name = component.get("name", object_id)
                            current_revision_dict = updated_details.get("revision")
                            current_version = current_revision_dict.get("version") if current_revision_dict else None
                            
                    elif original_object_type == "process_group":
                        nifi_request_data = {"operation": "stop_process_group", "process_group_id": object_id}
                        local_logger.bind(interface="nifi", direction="request", data=nifi_request_data).debug("Calling NiFi API")
                        await nifi_client.stop_process_group(object_id)
                        local_logger.bind(interface="nifi", direction="response", data={"status": "success"}).debug("Received from NiFi API")
                        
                        is_stopped, _ = await verify_stopped("process_group", object_id)
                        if not is_stopped:
                            raise ToolError(f"Process Group {object_id} did not fully stop after 15 seconds")

                except Exception as e:
                    local_logger.error(f"[Auto-Stop] Failed to stop {original_object_type}: {e}", exc_info=True)
                    local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
                    raise ToolError(f"Failed to auto-stop {original_object_type}: {e}")
            else:
                raise ToolError(f"Cannot delete running {original_object_type} {object_id} when Auto-Stop is disabled")

        # Final check on state before attempting deletion
        if object_type != "connection" and state == "RUNNING":
             error_msg = f"{object_type.capitalize()} '{name}' ({object_id}) is still RUNNING. It must be stopped before deletion. Auto-Stop may have failed or not fully stopped the component."
             local_logger.warning(error_msg)
             return {"status": "error", "message": error_msg} 

        # 2. Attempt deletion using the obtained version
        delete_op = f"delete_{object_type}" # Use potentially refined object_type for ports
        nifi_delete_req = {"operation": delete_op, "id": object_id, "version": current_version}
        local_logger.bind(interface="nifi", direction="request", data=nifi_delete_req).debug("Calling NiFi API (delete)")
        
        deleted = False
        try:
            if object_type == "processor":
                deleted = await nifi_client.delete_processor(object_id, current_version)
            elif object_type == "connection":
                deleted = await nifi_client.delete_connection(object_id, current_version)
            elif object_type in ["input_port", "output_port"]:
                deleted = await nifi_client.delete_port(object_id, current_version)
            elif object_type == "process_group":
                deleted = await nifi_client.delete_process_group(object_id, current_version)
            else:
                raise ToolError(f"Unsupported object type for deletion: {object_type}")
        except ValueError as e:
            if "not empty" in str(e).lower():
                error_msg = f"Cannot delete {object_type} '{name}' ({object_id}) because it is not empty."
                local_logger.warning(error_msg)
                return {"status": "error", "message": error_msg}
            elif "has data" in str(e).lower() or "active queue" in str(e).lower():
                error_msg = f"Cannot delete {object_type} '{name}' ({object_id}) because it has queued data."
                local_logger.warning(error_msg)
                return {"status": "error", "message": error_msg}
            else:
                raise  # Re-raise other ValueError types

        if deleted:
            success_msg = f"Successfully deleted {object_type} '{name}' ({object_id})"
            local_logger.info(success_msg)
            return {"status": "success", "message": success_msg}
        else:
            error_msg = f"Failed to delete {object_type} '{name}' ({object_id}). NiFi API returned false."
            local_logger.warning(error_msg)
            return {"status": "error", "message": error_msg}

    except ValueError as e:
        local_logger.warning(f"Error deleting {object_type} {object_id} (ValueError caught in main try-except): {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (delete)")
        
        error_message = str(e)
        error_message_lower = error_message.lower()

        if "not found" in error_message_lower:
             return {"status": "error", "message": f"{object_type.capitalize()} {object_id} not found."}
        elif "conflict" in error_message_lower or "revision mismatch" in error_message_lower or "is currently RUNNING" in error_message_lower:
             return {"status": "error", "message": f"Conflict or state issue deleting {object_type} '{name}' ({object_id}): {e}"}
        else:
            return {"status": "error", "message": f"Error deleting {object_type} '{name}' ({object_id}): {e}"}
            
    except (NiFiAuthenticationError, ConnectionError, ToolError) as e:
        local_logger.error(f"API/Tool error deleting {object_type} {object_id}: {e}", exc_info=False)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (delete)")
        return {"status": "error", "message": f"Failed to delete {object_type} {object_id}: {e}"}
    except Exception as e:
        local_logger.error(f"Unexpected error deleting {object_type} {object_id}: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (delete)")
        return {"status": "error", "message": f"An unexpected error occurred during deletion: {e}"}

import asyncio
from typing import List, Dict, Optional, Any, Union, Literal

# Import necessary components from parent/utils
from loguru import logger
# Import mcp ONLY
from ..core import mcp
# Removed nifi_api_client import
# Import context variables
from ..request_context import current_nifi_client, current_request_logger # Added

from .utils import (
    tool_phases,
    # ensure_authenticated, # Removed
    filter_created_processor_data # Need this for logging/return filtering
)
from nifi_mcp_server.nifi_client import NiFiClient, NiFiAuthenticationError
from mcp.server.fastmcp.exceptions import ToolError


@mcp.tool()
@tool_phases(["Operate"])
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
    # Get client and logger from context
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger
    if not nifi_client:
        raise ToolError("NiFi client context is not set. This tool requires the X-Nifi-Server-Id header.")
    if not local_logger:
         raise ToolError("Request logger context is not set.")
         
    # Authentication handled by factory
    
    # Bind args
    local_logger = local_logger.bind(object_type=object_type, object_id=object_id, operation_type=operation_type)

    # Map operation type to NiFi state
    target_state = "RUNNING" if operation_type == "start" else "STOPPED"
    
    local_logger.info(f"Executing {operation_type} operation (Target State: {target_state})")

    try:
        updated_entity = None
        port_type_found = None # To track if input/output port for logging/errors
        operation_name_for_log = f"update_{object_type}_state"

        if object_type == "processor":
            # --- Pre-check for starting a processor ---
            if operation_type == "start":
                local_logger.info(f"Performing pre-checks...")
                try:
                    nifi_get_req = {"operation": "get_processor_details", "processor_id": object_id}
                    local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API (pre-check)")
                    proc_details = await nifi_client.get_processor_details(object_id)
                    component_precheck = proc_details.get("component", {})
                    precheck_resp = {
                        "id": object_id,
                        "validationStatus": component_precheck.get("validationStatus"),
                        "state": component_precheck.get("state")
                    }
                    local_logger.bind(interface="nifi", direction="response", data=precheck_resp).debug("Received from NiFi API (pre-check)")
                    
                    component = proc_details.get("component", {})
                    validation_status = component.get("validationStatus")
                    current_state = component.get("state")
                    validation_errors = component.get("validationErrors", [])
                    name = component.get("name", object_id)

                    if validation_status != "VALID":
                        error_list_str = ", ".join(validation_errors) if validation_errors else "No specific errors listed."
                        error_msg = f"Processor '{name}' cannot be started. Validation status: {validation_status}. Errors: [{error_list_str}]"
                        local_logger.warning(error_msg)
                        return {"status": "error", "message": error_msg, "entity": None}
                        
                    if current_state == "DISABLED":
                        error_msg = f"Processor '{name}' cannot be started because it is DISABLED. Enable it first."
                        local_logger.warning(error_msg)
                        return {"status": "error", "message": error_msg, "entity": None}
                        
                    local_logger.info(f"Processor pre-checks passed (Validation: {validation_status}, State: {current_state}). Proceeding with start.")

                except ValueError as e:
                    local_logger.warning(f"Processor not found during start pre-check: {e}")
                    local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (pre-check)")
                    return {"status": "error", "message": f"Processor {object_id} not found.", "entity": None}
                except Exception as e:
                    local_logger.error(f"Error during start pre-check: {e}", exc_info=True)
                    local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (pre-check)")
                    return {"status": "error", "message": f"Failed pre-start check: {e}", "entity": None}
            # --- End of Pre-check ---
            
            nifi_update_req = {"operation": operation_name_for_log, "processor_id": object_id, "state": target_state}
            local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API")
            updated_entity = await nifi_client.update_processor_state(object_id, target_state)

        elif object_type == "port":
            # --- Pre-check for starting a port ---
            port_details_for_check = None
            if operation_type == "start":
                local_logger.info(f"Performing pre-checks...")
                try:
                    try:
                        nifi_get_req = {"operation": "get_input_port_details", "port_id": object_id}
                        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API (trying input for pre-check)")
                        port_details_for_check = await nifi_client.get_input_port_details(object_id)
                        port_type_found = "input"
                    except ValueError: # Not input, try output
                        local_logger.bind(interface="nifi", direction="response", data={"error": "Input port not found"}).debug("Received error from NiFi API (pre-check)")
                        nifi_get_req = {"operation": "get_output_port_details", "port_id": object_id}
                        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API (trying output for pre-check)")
                        port_details_for_check = await nifi_client.get_output_port_details(object_id)
                        port_type_found = "output"
                        
                    component_precheck = port_details_for_check.get("component", {})
                    precheck_resp = {
                        "id": object_id,
                        "type": port_type_found,
                        "validationStatus": component_precheck.get("validationStatus"),
                        "state": component_precheck.get("state")
                    }
                    local_logger.bind(interface="nifi", direction="response", data=precheck_resp).debug("Received from NiFi API (pre-check)")
                    
                    component = port_details_for_check.get("component", {})
                    validation_status = component.get("validationStatus")
                    current_state = component.get("state")
                    validation_errors = component.get("validationErrors", [])
                    name = component.get("name", object_id)

                    if validation_status != "VALID":
                        error_list_str = ", ".join(validation_errors) if validation_errors else "No specific errors listed."
                        error_msg = f"Port '{name}' cannot be started. Validation status: {validation_status}. Errors: [{error_list_str}]"
                        local_logger.warning(error_msg)
                        return {"status": "error", "message": error_msg, "entity": None}
                        
                    if current_state == "DISABLED":
                        error_msg = f"Port '{name}' cannot be started because it is DISABLED. Enable it first."
                        local_logger.warning(error_msg)
                        return {"status": "error", "message": error_msg, "entity": None}

                    local_logger.info(f"Port pre-checks passed (Validation: {validation_status}, State: {current_state}). Proceeding with start.")

                except ValueError as e:
                    local_logger.warning(f"Port not found during start pre-check: {e}")
                    local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (pre-check)")
                    return {"status": "error", "message": f"Port {object_id} not found.", "entity": None}
                except Exception as e:
                    local_logger.error(f"Error during start pre-check: {e}", exc_info=True)
                    local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API (pre-check)")
                    return {"status": "error", "message": f"Failed pre-start check for port {object_id}: {e}", "entity": None}
            # --- End of Port Pre-check ---
            
            if port_type_found is None:
                local_logger.info("Determining port type (input/output)...")
                try:
                    _ = await nifi_client.get_input_port_details(object_id) 
                    port_type_found = "input"
                    local_logger.info(f"Port {object_id} identified as INPUT.")
                except ValueError:
                    local_logger.warning(f"Port not found as input, trying output.")
                    try:
                        _ = await nifi_client.get_output_port_details(object_id) 
                        port_type_found = "output"
                        local_logger.info(f"Port {object_id} identified as OUTPUT.")
                    except ValueError:
                        raise ToolError(f"Port with ID {object_id} not found. Cannot change state.")

            if port_type_found == "input":
                operation_name_for_log = "update_input_port_state"
                nifi_update_req = {"operation": operation_name_for_log, "port_id": object_id, "state": target_state}
                local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API")
                updated_entity = await nifi_client.update_input_port_state(object_id, target_state)
            elif port_type_found == "output":
                operation_name_for_log = "update_output_port_state"
                nifi_update_req = {"operation": operation_name_for_log, "port_id": object_id, "state": target_state}
                local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API")
                updated_entity = await nifi_client.update_output_port_state(object_id, target_state)
        else:
            raise ToolError(f"Invalid object_type: {object_type}. Must be 'processor' or 'port'.")

        filtered_entity = filter_created_processor_data(updated_entity) # Re-use processor filter
        local_logger.bind(interface="nifi", direction="response", data=filtered_entity).debug(f"Received from NiFi API ({operation_name_for_log})")
        
        component = updated_entity.get("component", {}) 
        current_state = component.get("state")
        name = component.get("name", object_id)
        validation_status = component.get("validationStatus", "UNKNOWN")
        
        if current_state == target_state:
             action = "started" if operation_type == "start" else "stopped"
             local_logger.info(f"Successfully {action} {object_type} '{name}'.")
             return {"status": "success", "message": f"{object_type.capitalize()} '{name}' {action} successfully.", "entity": filtered_entity}
        else:
            # Check for specific error cases if start failed despite pre-check
            if operation_type == "start" and (current_state == "DISABLED" or validation_status != "VALID"):
                local_logger.warning(f"{object_type.capitalize()} '{name}' could not be started. State: {current_state}, Validation: {validation_status}.")
                return {"status": "warning", "message": f"{object_type.capitalize()} '{name}' could not be started (State: {current_state}, Validation: {validation_status}). Check config.", "entity": filtered_entity}
            else:
                 action = "start" if operation_type == "start" else "stop"
                 local_logger.warning(f"{object_type.capitalize()} '{name}' state is {current_state} after {action} request. Expected {target_state}.")
                 return {"status": "warning", "message": f"{object_type.capitalize()} '{name}' is {current_state} after {action} request. Check NiFi UI.", "entity": filtered_entity}

    except ValueError as e: # Catches 404s or 409 conflicts
        local_logger.warning(f"Error operating on {object_type}: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug(f"Received error from NiFi API ({operation_name_for_log})")
        if "not found" in str(e).lower():
             return {"status": "error", "message": f"{object_type.capitalize()} {object_id} not found.", "entity": None}
        elif "conflict" in str(e).lower():
             return {"status": "error", "message": f"Could not {operation_type} {object_type} due to conflict: {e}. Check state/revision.", "entity": None}
        else:
            return {"status": "error", "message": f"Could not {operation_type} {object_type}: {e}", "entity": None}
            
    except (NiFiAuthenticationError, ConnectionError, ToolError) as e:
        local_logger.error(f"API/Tool error operating on {object_type}: {e}", exc_info=False)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug(f"Received error from NiFi API ({operation_name_for_log})")
        return {"status": "error", "message": f"Failed to {operation_type} NiFi {object_type}: {e}", "entity": None}
    except Exception as e:
        local_logger.error(f"Unexpected error operating on {object_type}: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug(f"Received error from NiFi API ({operation_name_for_log})")
        return {"status": "error", "message": f"An unexpected error occurred: {e}", "entity": None}

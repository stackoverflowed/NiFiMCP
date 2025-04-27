import asyncio
from typing import List, Dict, Optional, Any, Union, Literal

# Import necessary components from parent/utils
from loguru import logger
# Import mcp ONLY
from ..core import mcp
# Removed nifi_api_client import
# Import context variables
from ..request_context import current_nifi_client, current_request_logger # Added
# Import utils helper for filtering PG data
from .utils import (
    tool_phases,
    # ensure_authenticated, # Removed
    filter_created_processor_data, # Keep for processor/port paths
    filter_process_group_data # Add for process group path
)
from nifi_mcp_server.nifi_client import NiFiClient, NiFiAuthenticationError
from mcp.server.fastmcp.exceptions import ToolError


@mcp.tool()
@tool_phases(["Operate"])
async def operate_nifi_object(
    object_type: Literal["processor", "port", "process_group"], # Added "process_group"
    object_id: str,
    operation_type: Literal["start", "stop"]
) -> Dict:
    """
    Starts or stops a specific NiFi processor, port, or all components within a process group.

    Args:
        object_type: The type of object to operate on ('processor', 'port', or 'process_group').
        object_id: The UUID of the object (processor, port, or process group).
        operation_type: The operation to perform ('start' or 'stop'). For 'process_group', applies to all eligible components within.

    Returns:
        A dictionary indicating the status (success, warning, error) and potentially the updated entity.
        The structure may vary slightly based on object_type.
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

    local_logger.info(f"Executing {operation_type} operation on {object_type} {object_id} (Target State: {target_state})")

    try:
        updated_entity = None
        port_type_found = None # To track if input/output port for logging/errors
        operation_name_for_log = f"update_{object_type}_state" # Default log name

        # --- Processor Logic ---
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

            # --- Format and return processor result ---
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

        # --- Port Logic ---
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

            # --- Format and return port result ---
            filtered_entity = filter_created_processor_data(updated_entity) # Re-use processor filter logic
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

        # --- Process Group Logic ---
        elif object_type == "process_group":
            operation_name_for_log = "update_process_group_state"
            nifi_update_req = {"operation": operation_name_for_log, "process_group_id": object_id, "state": target_state}
            local_logger.bind(interface="nifi", direction="request", data=nifi_update_req).debug("Calling NiFi API (Bulk Update)")
            # No pre-checks for group operation, API handles it
            updated_entity = await nifi_client.update_process_group_state(object_id, target_state)

            # Format and return process group result
            # Use filter_process_group_data helper
            filtered_entity = filter_process_group_data(updated_entity)
            local_logger.bind(interface="nifi", direction="response", data=filtered_entity).debug(f"Received from NiFi API ({operation_name_for_log})")

            action_verb = "started" if operation_type == "start" else "stopped"
            message = f"Successfully initiated {action_verb} operation for all components in process group {object_id}."
            # Note: NiFi API for group state change is async. We report success of the request submission.
            # We could potentially query status afterwards, but keeping it simple for now.
            local_logger.info(message)
            return {
                "status": "success",
                "message": message,
                "process_group_id": object_id,
                "operation_type": operation_type,
                "entity_summary": filtered_entity # Include summary status counts from response
            }

        # --- Invalid Object Type ---
        else:
            # This case should ideally not be reachable due to Literal typing, but defensive coding is good.
            raise ToolError(f"Invalid object_type: {object_type}. Must be 'processor', 'port', or 'process_group'.")

    # --- General Exception Handling ---
    except ValueError as e: # Catches specific errors like 404s or 409 conflicts raised by client methods
        local_logger.warning(f"Error operating on {object_type} {object_id}: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug(f"Received error from NiFi API ({operation_name_for_log})")
        error_context = f"{object_type.capitalize()} {object_id}" if object_type != 'process_group' else f"Process Group {object_id}"
        if "not found" in str(e).lower():
             return {"status": "error", "message": f"{error_context} not found.", "entity": None}
        elif "conflict" in str(e).lower():
             # More specific conflict messages are raised by client methods
             return {"status": "error", "message": f"Could not {operation_type} {error_context} due to conflict: {e}. Check state/revision.", "entity": None}
        else: # Other ValueErrors from client (e.g., invalid state)
            return {"status": "error", "message": f"Could not {operation_type} {error_context}: {e}", "entity": None}

    except (NiFiAuthenticationError, ConnectionError, ToolError) as e:
        # Handle specific known exceptions
        local_logger.error(f"API/Tool error operating on {object_type} {object_id}: {e}", exc_info=False) # No traceback for these expected errors
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug(f"Received error from NiFi API ({operation_name_for_log})")
        return {"status": "error", "message": f"Failed to {operation_type} NiFi {object_type} {object_id}: {e}", "entity": None}
    except Exception as e:
        # Catch any other unexpected errors
        local_logger.error(f"Unexpected error operating on {object_type} {object_id}: {e}", exc_info=True) # Include traceback
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug(f"Received error from NiFi API ({operation_name_for_log})")
        return {"status": "error", "message": f"An unexpected error occurred: {e}", "entity": None}


@mcp.tool()
@tool_phases(["Operate"])
async def run_processor_once(processor_id: str) -> Dict:
    """
    Attempts to run a single execution cycle for a specified processor.

    Ensures the processor is stopped first, then requests a single run using the RUN_ONCE state.
    Useful for step-by-step debugging.

    Args:
        processor_id: The ID of the target processor.

    Returns:
        A dictionary indicating the status:
        {
          "status": "success" | "warning" | "error",
          "message": "Processor XYZ successfully triggered for one run.",
          "processor_id": "...",
          "final_state": "STOPPED" | "RUNNING" | "DISABLED" | "INVALID" | "UNKNOWN"
        }
    """
    # Get client and logger from context
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger
    if not nifi_client:
        raise ToolError("NiFi client context is not set. This tool requires the X-Nifi-Server-Id header.")
    if not local_logger:
        raise ToolError("Request logger context is not set.")

    local_logger = local_logger.bind(processor_id=processor_id)
    local_logger.info(f"Attempting to run processor {processor_id} once.")

    proc_details = None
    latest_revision = None
    initial_state = "UNKNOWN"
    processor_name = processor_id # Default name

    try:
        # --- Step 1: Get Current State and Revision ---
        local_logger.info("Getting current processor state and revision...")
        nifi_get_req = {"operation": "get_processor_details", "processor_id": processor_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_get_req).debug("Calling NiFi API")
        proc_details = await nifi_client.get_processor_details(processor_id)
        latest_revision = proc_details["revision"]
        initial_state = proc_details.get("component", {}).get("state", "UNKNOWN")
        processor_name = proc_details.get("component", {}).get("name", processor_id)
        local_logger.info(f"Processor '{processor_name}' current state: {initial_state}, revision: {latest_revision.get('version')}")
        local_logger.bind(interface="nifi", direction="response", data=filter_created_processor_data(proc_details)).debug("Received from NiFi API")

        # --- Step 2: Stop if Necessary ---
        if initial_state == "RUNNING" or initial_state == "DISABLED":
            local_logger.info(f"Processor is {initial_state}, stopping it first...")
            nifi_stop_req = {"operation": "update_processor_state", "processor_id": processor_id, "state": "STOPPED"}
            local_logger.bind(interface="nifi", direction="request", data=nifi_stop_req).debug("Calling NiFi API")
            stop_result = await nifi_client.update_processor_state(processor_id, "STOPPED")
            latest_revision = stop_result["revision"] # Get the updated revision after stopping
            stopped_state = stop_result.get("component", {}).get("state")
            local_logger.bind(interface="nifi", direction="response", data=filter_created_processor_data(stop_result)).debug("Received from NiFi API")
            if stopped_state != "STOPPED":
                local_logger.warning(f"Processor state is {stopped_state} after stop request. Proceeding with RUN_ONCE anyway.")
            else:
                local_logger.info(f"Processor stopped successfully. New revision: {latest_revision.get('version')}")
        elif initial_state == "INVALID":
             local_logger.warning(f"Processor '{processor_name}' is INVALID. Cannot run once.")
             return {"status": "error", "message": f"Processor '{processor_name}' is INVALID and cannot be run.", "processor_id": processor_id, "final_state": "INVALID"}
        else: # Already STOPPED
            local_logger.info("Processor is already stopped.")

        # --- Step 3: Trigger RUN_ONCE ---
        local_logger.info(f"Triggering RUN_ONCE for processor '{processor_name}' (Revision: {latest_revision.get('version')})...")
        run_once_payload = {
            "revision": latest_revision,
            "state": "RUN_ONCE",
            "disconnectedNodeAcknowledged": False
        }
        client = await nifi_client._get_client() # Get httpx client
        endpoint = f"/processors/{processor_id}/run-status"
        nifi_runonce_req = {"operation": "set_run_once", "processor_id": processor_id, "state": "RUN_ONCE"}
        local_logger.bind(interface="nifi", direction="request", data=nifi_runonce_req).debug("Calling NiFi API")
        response = await client.put(endpoint, json=run_once_payload)
        response.raise_for_status()
        run_once_result = response.json()
        # Update revision in case it changed again, though unlikely for RUN_ONCE
        latest_revision = run_once_result.get("revision", latest_revision)
        local_logger.info(f"RUN_ONCE command submitted successfully for processor '{processor_name}'.")
        local_logger.bind(interface="nifi", direction="response", data=filter_created_processor_data(run_once_result)).debug("Received from NiFi API")

        # --- Step 4: Wait and Check Final State ---
        wait_seconds = 2 # Wait a couple of seconds for potential execution
        local_logger.info(f"Waiting {wait_seconds} seconds before checking final state...")
        await asyncio.sleep(wait_seconds)

        local_logger.info("Checking final processor state...")
        nifi_final_get_req = {"operation": "get_processor_details", "processor_id": processor_id}
        local_logger.bind(interface="nifi", direction="request", data=nifi_final_get_req).debug("Calling NiFi API")
        final_details = await nifi_client.get_processor_details(processor_id)
        final_state = final_details.get("component", {}).get("state", "UNKNOWN")
        local_logger.bind(interface="nifi", direction="response", data=filter_created_processor_data(final_details)).debug("Received from NiFi API")
        local_logger.info(f"Processor '{processor_name}' final state after RUN_ONCE attempt: {final_state}")

        return {
            "status": "success",
            "message": f"Processor '{processor_name}' successfully triggered for one run. Final state observed: {final_state}.",
            "processor_id": processor_id,
            "final_state": final_state
        }

    except ValueError as e:
        local_logger.warning(f"Value error during run-once operation for processor {processor_id}: {e}")
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        if "not found" in str(e).lower():
            return {"status": "error", "message": f"Processor {processor_id} not found.", "processor_id": processor_id, "final_state": "UNKNOWN"}
        elif "conflict" in str(e).lower():
            return {"status": "error", "message": f"Conflict operating on processor {processor_id}: {e}. Check revision/state.", "processor_id": processor_id, "final_state": initial_state}
        else:
            return {"status": "error", "message": f"Error operating on processor {processor_id}: {e}", "processor_id": processor_id, "final_state": initial_state}

    except (NiFiAuthenticationError, ConnectionError, ToolError) as e:
        local_logger.error(f"API/Tool error during run-once for processor {processor_id}: {e}", exc_info=False)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"Failed to run processor {processor_id} once: {e}", "processor_id": processor_id, "final_state": initial_state}
    except Exception as e:
        local_logger.error(f"Unexpected error during run-once for processor {processor_id}: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        return {"status": "error", "message": f"An unexpected error occurred: {e}", "processor_id": processor_id, "final_state": initial_state}

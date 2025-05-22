from loguru import logger
# from dotenv import load_dotenv # Removed

from mcp.server import FastMCP
from nifi_mcp_server.nifi_client import NiFiClient, NiFiAuthenticationError

# --- Import Config Settings --- #
from config.settings import get_nifi_server_config, get_nifi_servers # Added

# Load .env file - REMOVED (Handled by config.settings)
# load_dotenv()

# Initialize FastMCP server
# Shared instance for the application
mcp = FastMCP(
    "nifi_controller",
    description="An MCP server to interact with Apache NiFi.",
    protocol_version="2024-09-01",  # Explicitly set protocol version
    type_validation_mode="compat",  # Use compatibility mode for type validation
)
logger.info("MCP instance initialized in core.")

# REMOVED single shared NiFiClient instantiation
# nifi_api_client = None 

# --- NiFi Client Factory --- #
# Simple cache for authenticated clients within a request scope? (Could use contextvars or pass around)
# For now, create per request/call.

async def get_nifi_client(server_id: str, bound_logger = logger) -> NiFiClient:
    """Gets or creates an authenticated NiFi client for the specified server ID."""
    bound_logger.info(f"Requesting NiFi client for server ID: {server_id}")
    server_conf = get_nifi_server_config(server_id)
    if not server_conf:
        bound_logger.error(f"Configuration for NiFi server ID '{server_id}' not found.")
        raise ValueError(f"NiFi server configuration not found for ID: {server_id}")

    client = NiFiClient(
        base_url=server_conf.get('url'),
        username=server_conf.get('username'),
        password=server_conf.get('password'),
        tls_verify=server_conf.get('tls_verify', True)
    )
    bound_logger.debug(f"Instantiated NiFiClient for {server_conf.get('url')}")

    try:
        # Ensure client is authenticated
        if not client.is_authenticated:
            bound_logger.info(f"Authenticating NiFi client for {server_conf.get('url')}")
            await client.authenticate()
            bound_logger.info(f"Authentication successful for {server_conf.get('url')}")
        else:
            bound_logger.debug(f"NiFi client for {server_conf.get('url')} is already authenticated (cached?)")
        return client
    except NiFiAuthenticationError as e:
        bound_logger.error(f"Authentication failed for NiFi server {server_id} ({server_conf.get('url')}): {e}")
        # Close the client if auth fails to release resources
        await client.close()
        raise # Re-raise the authentication error
    except Exception as e:
        bound_logger.error(f"Unexpected error getting/authenticating NiFi client for {server_id}: {e}", exc_info=True)
        await client.close()
        raise # Re-raise other exceptions


# Ensure at least one NiFi server is configured on startup (Optional check)
try:
    if not get_nifi_servers():
        logger.warning("No NiFi servers defined in config.yaml. NiFi tools will likely fail.")
    else:
        logger.info(f"Found {len(get_nifi_servers())} NiFi server configurations.")
except Exception as e:
    logger.error(f"Failed to read NiFi server configurations on startup: {e}") 

# --- Enhanced NiFi Error Handling & Remediation ---

# Imports for error_handler (ensure these are at the top of the file if not already)
import asyncio
import functools
from typing import Callable, Any, Coroutine, Dict # Add Coroutine if not there
# from loguru import logger # Already imported
# from .nifi_client import NiFiClient, NiFiAuthenticationError # Already imported from this file's perspective

# Import settings and context, adjusting paths relative to nifi_mcp_server/core.py
from config import settings as mcp_settings # Assuming config is a top-level package or accessible
from .request_context import current_nifi_client, current_request_logger
from mcp.server.fastmcp.exceptions import ToolError # IMPORT ToolError


async def _get_component_details_direct(nifi_client, object_id: str, object_type: str, local_logger) -> dict | None:
    """Helper to directly fetch details for a NiFi component."""
    try:
        if object_type == "processor":
            details = await nifi_client.get_processor_details(object_id)
        elif object_type == "input_port":
            details = await nifi_client.get_input_port_details(object_id)
        elif object_type == "output_port":
            details = await nifi_client.get_output_port_details(object_id)
        elif object_type == "process_group":
            details = await nifi_client.get_process_group_details(object_id)
        else:
            local_logger.error(f"_get_component_details_direct: Unsupported object type: {object_type}")
            return None
        return details
    except ValueError as e:
        local_logger.warning(f"_get_component_details_direct: ValueError fetching details for {object_type} {object_id}: {e}")
        return None
    except Exception as e:
        local_logger.error(f"_get_component_details_direct: Unexpected error fetching details for {object_type} {object_id}: {e}", exc_info=True)
        return None

def handle_nifi_errors(original_func: Callable[..., Coroutine[Any, Any, Any]]):
    """
    Decorator to handle specific NiFi errors by attempting remediation (e.g., Auto-Stop).
    Applies to tool functions or other async functions that make NiFi client calls.
    The decorated function's arguments relevant for remediation (like `object_id`, `object_type`)
    need to be accessible via its signature or from kwargs. 
    `request_headers` should be passed in kwargs if header overrides for features are needed.
    """
    @functools.wraps(original_func)
    async def wrapper(*args, **kwargs):
        request_logger = kwargs.get('request_logger_override') or current_request_logger.get() or logger
        # NiFi client should be retrieved from context or passed via kwargs for the original_func
        # For functions NOT part of a tool (e.g. direct client calls elsewhere), ensure nifi_client is available.
        nifi_client = kwargs.get('nifi_client_override') or current_nifi_client.get()

        if not nifi_client:
            request_logger.error("Error Handler: NiFi client not available. Cannot proceed with enhanced error handling.")
            return await original_func(*args, **kwargs) # Fallback

        attempt = 0
        max_attempts = 2 # Original call + 1 retry
        
        object_id_for_remediation = kwargs.get("object_id")
        object_type_for_remediation = kwargs.get("object_type")

        # Heuristic for positional arguments if the decorator is applied to functions like delete_nifi_object(object_type, object_id, ...)
        if not object_id_for_remediation and len(args) >= 2 and isinstance(args[0], str) and isinstance(args[1], str):
            # This assumes the first two string args are object_type and object_id
            # This is true for delete_nifi_object tool's main signature.
            inferred_object_type = args[0]
            inferred_object_id = args[1]
            # Check if these look like valid types/ids before assigning (optional, basic check here)
            # For simplicity, let's assume if they are string, they are the intended ones for now.
            if not object_type_for_remediation: object_type_for_remediation = inferred_object_type
            if not object_id_for_remediation: object_id_for_remediation = inferred_object_id

        while attempt < max_attempts:
            attempt += 1
            try:
                return await original_func(*args, **kwargs)
            except (ValueError, ToolError) as e: # ADD ToolError HERE
                request_logger.info(f"DECORATOR CAUGHT EXCEPTION ({type(e).__name__}) -- ENTERING EXCEPTION BLOCK") 
                
                error_message = str(e) 
                # If it's a ToolError, the actual message might be nested or prefixed.
                # We need to find our original "is currently RUNNING" message within it.
                # A simple string search should suffice for now.

                error_message_lower = error_message.lower()
                request_logger.warning(f"NiFi operation failed (attempt {attempt}/{max_attempts}): {error_message}")

                # --- Auto-Stop Logic ---
                request_headers = kwargs.get("request_headers") 
                is_as_enabled = mcp_settings.get_feature_auto_stop_enabled(headers=request_headers)
                
                local_logger = request_logger # Use the same logger for consistent context

                if (is_as_enabled and
                   ("running" in error_message_lower or
                    "must be stopped" in error_message_lower or
                    "has running components" in error_message_lower) and
                   "bulletins" not in error_message_lower):

                    if attempt >= max_attempts:
                        local_logger.error(f"[Auto-Stop] Max attempts reached for '{object_id_for_remediation}' after error: {error_message}. Propagating error.")
                        raise

                    local_logger.info(f"[Auto-Stop] Detected runnable component error for '{object_id_for_remediation}' (type: {object_type_for_remediation}): {error_message}")

                    if not object_id_for_remediation or not object_type_for_remediation:
                        local_logger.warning("[Auto-Stop] Could not determine component_id or object_type from function arguments. Auto-Stop cannot proceed.")
                        raise 
                    
                    target_pg_to_stop_id = None
                    # Determine the target PG to stop
                    if object_type_for_remediation == "process_group":
                        target_pg_to_stop_id = object_id_for_remediation
                        local_logger.info(f"[Auto-Stop] Target for stop is the process group itself: {target_pg_to_stop_id}")
                    elif object_type_for_remediation in ["processor", "input_port", "output_port"]:
                        local_logger.info(f"[Auto-Stop] Attempting to find parent PG for {object_type_for_remediation} {object_id_for_remediation}")
                        details = await _get_component_details_direct(nifi_client, object_id_for_remediation, object_type_for_remediation, local_logger)
                        if details and details.get("component", {}).get("parentGroupId"):
                            target_pg_to_stop_id = details["component"]["parentGroupId"]
                            local_logger.info(f"[Auto-Stop] Identified parent PG ID: {target_pg_to_stop_id} for component {object_id_for_remediation}")
                        else:
                            local_logger.warning(f"[Auto-Stop] Could not get parentGroupId for {object_type_for_remediation} {object_id_for_remediation} to stop parent PG.")
                    else:
                        local_logger.info(f"[Auto-Stop] Object type {object_type_for_remediation} is not typically stopped directly or via parent PG for this error type. Auto-Stop may not apply.")

                    if target_pg_to_stop_id:
                        local_logger.info(f"[Auto-Stop] Attempting to stop PG: {target_pg_to_stop_id}")
                        try:
                            # Use update_process_group_state instead of _stop_pg_direct
                            await nifi_client.update_process_group_state(target_pg_to_stop_id, "STOPPED")
                            local_logger.info(f"[Auto-Stop] Parent PG {target_pg_to_stop_id} stop initiated. Waiting for delay before retry...")
                            await asyncio.sleep(mcp_settings.get_auto_stop_delay_seconds())
                            local_logger.info(f"[Auto-Stop] Retrying original operation for '{object_id_for_remediation}' ({original_func.__name__}).")
                            await asyncio.sleep(mcp_settings.get_auto_feature_retry_delay_seconds())
                            continue # Go to next attempt in the while loop
                        except Exception as e_stop_retry:
                            local_logger.error(f"[Auto-Stop] Exception during parent PG stop attempt for {target_pg_to_stop_id}: {e_stop_retry}", exc_info=True)
                            # Fall through to raise original error
                    else:
                        local_logger.warning(f"[Auto-Stop] Could not determine a Process Group to stop for component {object_id_for_remediation}. Auto-Stop cannot proceed with parent PG stop.")
                
                # If Auto-Stop didn't handle it or failed to remediate, raise the current error.
                raise 
            
            except Exception as e_generic: 
                request_logger.error(f"NiFi operation failed with unexpected error (attempt {attempt}/{max_attempts}): {e_generic}", exc_info=True)
                raise
        
        # This part should ideally not be reached if max_attempts >= 1 and errors always propagate
        request_logger.error("Error Handler: Exited retry loop unexpectedly. This indicates a logic flaw.") # Should not happen
        return None # Fallback, though an error should have been raised.

    return wrapper 

import asyncio
import time
from typing import Dict, Any, Optional
from loguru import logger

from .nifi_client import NiFiClient
from .api_tools.utils import filter_drop_request_data

async def _handle_drop_request(nifi_client: NiFiClient, connection_id: str, timeout_seconds: int, local_logger) -> Dict[str, Any]:
    """Helper function to handle the lifecycle of a drop request.
    
    Args:
        nifi_client: The authenticated NiFi client
        connection_id: The ID of the connection to drop FlowFiles from
        timeout_seconds: Maximum time to wait for the drop request to complete
        local_logger: Logger instance with request context
        
    Returns:
        Dict containing the drop request results
    """
    try:
        # Use the new NiFiClient method to handle the drop request
        result = await nifi_client.handle_drop_request(connection_id, timeout_seconds)
        
        # Format the response to match the expected structure
        return {
            "success": result["success"],
            "message": "Successfully purged connection" if result["success"] else result.get("error", "Failed to purge connection"),
            "results": [{
                "connection_id": connection_id,
                "success": result["success"],
                "dropped_count": result.get("dropped_count") if result["success"] else None,
                "error": result.get("error") if not result["success"] else None
            }]
        }

    except Exception as e:
        local_logger.error(f"Error during drop request for connection {connection_id}: {e}")
        return {
            "success": False,
            "message": f"Failed to purge connection: {e}",
            "results": [{
                "connection_id": connection_id,
                "success": False,
                "error": str(e)
            }]
        } 
import asyncio
import logging
import signal # Add signal import for cleanup
from typing import List, Dict, Optional, Any
import json

# Import our NiFi API client and exception (Absolute Import)
from nifi_client import NiFiClient, NiFiAuthenticationError

# Import MCP server components (Corrected for v1.6.0)
from mcp.server import FastMCP
# Corrected error import path based on file inspection for v1.6.0
from mcp.shared.exceptions import McpError # Base error
from mcp.server.fastmcp.exceptions import ToolError # Tool-specific errors

# Configure logging for the server - Set level to DEBUG
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("nifi_mcp_server")
logger.debug("nifi_mcp_server logger initialized with DEBUG level.")

# --- Server Setup ---

# Initialize FastMCP server - name should be descriptive
mcp = FastMCP("nifi_controller", description="An MCP server to interact with Apache NiFi.")

# Instantiate our NiFi API client (uses environment variables for config)
# Consider a more robust way to handle client lifecycle if needed
try:
    nifi_api_client = NiFiClient()
    logger.info("NiFi API Client instantiated.")
except ValueError as e:
    logger.error(f"Failed to instantiate NiFiClient: {e}. Ensure NIFI_API_URL is set.")
    # Decide how to handle this - maybe exit or have tools return errors
    nifi_api_client = None # Mark as unavailable

# --- Helper Function for Authentication ---

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


# --- MCP Tools ---

@mcp.tool()
async def list_nifi_processors(
    process_group_id: Optional[str] = None
) -> list:
    """
    Lists processors within a specified NiFi process group.

    If process_group_id is not provided, it will attempt to list processors
    in the root process group.

    Args:
        process_group_id: The UUID of the process group to inspect. Defaults to the root group if None.

    Returns:
        A list of dictionaries, where each dictionary
        represents a processor found in the specified process group.
    """
    await ensure_authenticated() # Ensure we are logged in

    target_pg_id = process_group_id
    if target_pg_id is None:
        logger.info("No process_group_id provided, fetching root process group ID.")
        try:
            target_pg_id = await nifi_api_client.get_root_process_group_id()
        except Exception as e:
            logger.error(f"Failed to get root process group ID: {e}", exc_info=True)
            raise ToolError(f"Failed to determine root process group ID: {e}")

    logger.info(f"Executing list_nifi_processors for group: {target_pg_id}")
    try:
        # The nifi_api_client.list_processors is expected to return the list directly
        processors_list = await nifi_api_client.list_processors(target_pg_id)
        # Ensure it's actually a list
        if not isinstance(processors_list, list):
            logger.error(f"API client list_processors did not return a list. Got: {type(processors_list)}")
            raise ToolError("Unexpected data format received from NiFi API client for list_processors.")
        
        # Return the list directly, let MCP handle serialization
        return processors_list
    except (NiFiAuthenticationError, ConnectionError) as e:
        logger.error(f"API error listing processors: {e}", exc_info=True)
        # Convert NiFi client errors to MCP errors
        raise ToolError(f"Failed to list NiFi processors: {e}")
    except Exception as e:
        logger.error(f"Unexpected error listing processors: {e}", exc_info=True)
        raise ToolError(f"An unexpected error occurred: {e}")


@mcp.tool()
async def create_nifi_processor(
    processor_type: str,
    name: str,
    position_x: int,
    position_y: int,
    process_group_id: Optional[str] = None,
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
        A dictionary representing the newly created processor entity, following the
        NiFi API response structure for ProcessorEntity.
    """
    await ensure_authenticated() # Ensure we are logged in

    target_pg_id = process_group_id
    if target_pg_id is None:
        logger.info("No process_group_id provided for creation, fetching root process group ID.")
        try:
            target_pg_id = await nifi_api_client.get_root_process_group_id()
        except Exception as e:
            logger.error(f"Failed to get root process group ID: {e}", exc_info=True)
            raise ToolError(f"Failed to determine root process group ID for creation: {e}")

    position = {"x": position_x, "y": position_y}
    logger.info(f"Executing create_nifi_processor: Type='{processor_type}', Name='{name}', Position={position} in group: {target_pg_id}")

    try:
        processor_entity = await nifi_api_client.create_processor(
            process_group_id=target_pg_id,
            processor_type=processor_type,
            name=name,
            position=position,
            config=None # Add config dict here when implemented
        )
        logger.info(f"Successfully created processor '{name}' with ID: {processor_entity.get('id', 'N/A')}")
        
        # Check validation status from the response
        component = processor_entity.get("component", {})
        validation_status = component.get("validationStatus", "UNKNOWN")
        validation_errors = component.get("validationErrors", [])
        
        if validation_status == "VALID":
            return {
                "status": "success",
                "message": f"Processor '{name}' created successfully.",
                "entity": processor_entity
            }
        else:
            error_msg_snippet = f" ({validation_errors[0]})" if validation_errors else ""
            logger.warning(f"Processor '{name}' created but is {validation_status}{error_msg_snippet}. Requires configuration or connections.")
            return {
                "status": "warning",
                "message": f"Processor '{name}' created but is currently {validation_status}{error_msg_snippet}. Further configuration or connections likely required.",
                "entity": processor_entity
            }
            
    except (NiFiAuthenticationError, ConnectionError, ValueError) as e: # Include ValueError for potential client-side validation issues
        logger.error(f"API error creating processor: {e}", exc_info=True)
        # raise ToolError(f"Failed to create NiFi processor: {e}")
        # Return error status
        return {"status": "error", "message": f"Failed to create NiFi processor: {e}", "entity": None}
    except Exception as e:
        logger.error(f"Unexpected error creating processor: {e}", exc_info=True)
        # raise ToolError(f"An unexpected error occurred during processor creation: {e}")
        return {"status": "error", "message": f"An unexpected error occurred during processor creation: {e}", "entity": None}


@mcp.tool()
async def create_nifi_connection(
    source_id: str,
    source_relationship: str,
    target_id: str,
    process_group_id: Optional[str] = None,
    # selected_relationships: Optional[List[str]] = None # Optional: More advanced config
) -> Dict:
    """
    Creates a connection between two components (processors, ports, etc.)
    within a specified NiFi process group.

    If process_group_id is not provided, it assumes the components exist in the
    root process group and attempts to fetch its ID.

    Args:
        source_id: The UUID of the source component.
        source_relationship: The name of the relationship originating from the source component (e.g., "success", "failure").
        target_id: The UUID of the target component.
        process_group_id: The UUID of the process group where the connection should be created. Defaults to the root group if None.
        # selected_relationships: Optionally specify which relationships to include if the source has multiple.

    Returns:
        A dictionary representing the newly created connection entity, following the
        NiFi API response structure for ConnectionEntity.
    """
    await ensure_authenticated() # Ensure we are logged in

    target_pg_id = process_group_id
    if target_pg_id is None:
        logger.info("No process_group_id provided for connection, fetching root process group ID.")
        try:
            target_pg_id = await nifi_api_client.get_root_process_group_id()
        except Exception as e:
            logger.error(f"Failed to get root process group ID for connection: {e}", exc_info=True)
            raise ToolError(f"Failed to determine root process group ID for connection creation: {e}")

    # NiFi API expects relationships to be provided as a list, even if it's just one
    relationships = [source_relationship]

    logger.info(f"Executing create_nifi_connection: Source={source_id}, Rel='{source_relationship}', Target={target_id} in group: {target_pg_id}")

    try:
        # Call the NiFi client method to create the connection
        connection_entity = await nifi_api_client.create_connection(
            process_group_id=target_pg_id,
            source_id=source_id,
            target_id=target_id,
            relationships=relationships # Pass the list of relationships
        )
        logger.info(f"Successfully created connection with ID: {connection_entity.get('id', 'N/A')}")
        return connection_entity
    except (NiFiAuthenticationError, ConnectionError, ValueError) as e:
        logger.error(f"API error creating connection: {e}", exc_info=True)
        raise ToolError(f"Failed to create NiFi connection: {e}")
    except Exception as e:
        logger.error(f"Unexpected error creating connection: {e}", exc_info=True)
        raise ToolError(f"An unexpected error occurred during connection creation: {e}")


@mcp.tool()
async def get_nifi_processor_details(processor_id: str) -> Dict:
    """
    Retrieves the detailed information and configuration for a specific processor.

    Args:
        processor_id: The UUID of the processor to inspect.

    Returns:
        A dictionary containing the processor's details and configuration,
        following the NiFi API response structure for ProcessorEntity.

    Raises:
        ToolError: If authentication fails, the processor is not found, or another API error occurs.
    """
    await ensure_authenticated() # Ensure we are logged in

    logger.info(f"Executing get_nifi_processor_details for processor ID: {processor_id}")

    try:
        # Call the NiFi client method to get processor details
        details = await nifi_api_client.get_processor_details(processor_id)
        return details
    except ValueError as e: # Catch the specific error for processor not found
        logger.warning(f"Processor not found error for ID {processor_id}: {e}")
        raise ToolError(f"Processor not found: {e}") from e
    except (NiFiAuthenticationError, ConnectionError) as e:
        logger.error(f"API error getting processor details: {e}", exc_info=True)
        raise ToolError(f"Failed to get NiFi processor details: {e}")
    except Exception as e:
        logger.error(f"Unexpected error getting processor details: {e}", exc_info=True)
        raise ToolError(f"An unexpected error occurred getting processor details: {e}")


@mcp.tool()
async def delete_nifi_processor(processor_id: str, version: int) -> Dict:
    """
    Deletes a specific processor from the NiFi canvas.

    Requires the current revision version of the processor to prevent conflicts.

    Args:
        processor_id: The UUID of the processor to delete.
        version: The current revision version of the processor.

    Returns:
        A dictionary containing a success message if deletion is successful.

    Raises:
        ToolError: If authentication fails, the processor is not found,
                   the version is incorrect (conflict), or another API error occurs.
    """
    await ensure_authenticated() # Ensure we are logged in

    logger.info(f"Executing delete_nifi_processor for processor ID: {processor_id} at version {version}")

    try:
        # Call the NiFi client method to delete the processor
        deleted = await nifi_api_client.delete_processor(processor_id, version)

        if deleted:
            return {"status": "success", "message": f"Processor {processor_id} deleted successfully."}
        else:
            # This might indicate a 404 handled within the client method
             raise ToolError(f"Processor {processor_id} could not be deleted (possibly not found).")

    except ValueError as e: # Catch the specific error for version conflict (409)
        logger.warning(f"Conflict deleting processor {processor_id} (version {version}): {e}")
        # Revert: Raise ToolError for client to catch as McpError
        raise ToolError(f"Conflict deleting processor: {e}. Ensure the correct version is provided.") from e
    except (NiFiAuthenticationError, ConnectionError) as e:
        logger.error(f"API error deleting processor {processor_id}: {e}", exc_info=True)
        # Revert: Raise ToolError
        raise ToolError(f"Failed to delete NiFi processor: {e}")
    except Exception as e:
        logger.error(f"Unexpected error deleting processor {processor_id}: {e}", exc_info=True)
        # Revert: Raise ToolError
        raise ToolError(f"An unexpected error occurred deleting processor: {e}")


@mcp.tool()
async def list_nifi_connections(
    process_group_id: Optional[str] = None
) -> list:
    """
    Lists connections within a specified NiFi process group.

    If process_group_id is not provided, it will attempt to list connections
    in the root process group.

    Args:
        process_group_id: The UUID of the process group to inspect. Defaults to the root group if None.

    Returns:
        A list of dictionaries, where each dictionary
        represents a connection found in the specified process group.
    """
    await ensure_authenticated()

    target_pg_id = process_group_id
    if target_pg_id is None:
        logger.info("No process_group_id provided for list_connections, fetching root ID.")
        try:
            target_pg_id = await nifi_api_client.get_root_process_group_id()
        except Exception as e:
            logger.error(f"Failed to get root process group ID for list_connections: {e}", exc_info=True)
            raise ToolError(f"Failed to determine root process group ID for list_connections: {e}")

    logger.info(f"Executing list_nifi_connections for group: {target_pg_id}")
    try:
        connections_list = await nifi_api_client.list_connections(target_pg_id)
        if not isinstance(connections_list, list):
            logger.error(f"API client list_connections did not return a list. Got: {type(connections_list)}")
            raise ToolError("Unexpected data format received from NiFi API client for list_connections.")
        
        return connections_list
    except (NiFiAuthenticationError, ConnectionError) as e:
        logger.error(f"API error listing connections: {e}", exc_info=True)
        raise ToolError(f"Failed to list NiFi connections: {e}")
    except Exception as e:
        logger.error(f"Unexpected error listing connections: {e}", exc_info=True)
        raise ToolError(f"An unexpected error occurred listing connections: {e}")

@mcp.tool()
async def delete_nifi_connection(connection_id: str, version: int) -> Dict:
    """
    Deletes a specific connection from the NiFi canvas.

    Requires the current revision version of the connection to prevent conflicts.

    Args:
        connection_id: The UUID of the connection to delete.
        version: The current revision version of the connection.

    Returns:
        A dictionary containing a success message if deletion is successful.

    Raises:
        ToolError: If authentication fails, the connection is not found,
                   the version is incorrect (conflict), or another API error occurs.
    """
    await ensure_authenticated()

    logger.info(f"Executing delete_nifi_connection for ID: {connection_id} at version {version}")

    try:
        deleted = await nifi_api_client.delete_connection(connection_id, version)

        if deleted:
            return {"status": "success", "message": f"Connection {connection_id} deleted successfully."}
        else:
            raise ToolError(f"Connection {connection_id} could not be deleted (possibly not found).")

    except ValueError as e: # Catch version conflict (409)
        logger.warning(f"Conflict deleting connection {connection_id} (version {version}): {e}")
        # Revert: Raise ToolError
        raise ToolError(f"Conflict deleting connection: {e}. Ensure the correct version is provided.") from e
    except (NiFiAuthenticationError, ConnectionError) as e:
        logger.error(f"API error deleting connection {connection_id}: {e}", exc_info=True)
        # Revert: Raise ToolError
        raise ToolError(f"Failed to delete NiFi connection: {e}")
    except Exception as e:
        logger.error(f"Unexpected error deleting connection {connection_id}: {e}", exc_info=True)
        # Revert: Raise ToolError
        raise ToolError(f"An unexpected error occurred deleting connection: {e}")


@mcp.tool()
async def update_nifi_processor_config(
    processor_id: str,
    config_properties: Dict[str, Any]
) -> Dict:
    """
    Updates the configuration properties for a specific processor.

    This requires fetching the processor's current revision first.

    Args:
        processor_id: The UUID of the processor to update.
        config_properties: A dictionary where keys are property names (as strings)
                           and values are the new property values (as strings,
                           booleans, numbers etc., depending on the property).

    Returns:
        A dictionary representing the updated processor entity.

    Raises:
        ToolError: If authentication fails, the processor is not found,
                   there's a revision conflict (409), or another API error occurs.
    """
    await ensure_authenticated()

    logger.info(f"Executing update_nifi_processor_config for ID: {processor_id} with props: {config_properties}")

    try:
        updated_entity = await nifi_api_client.update_processor_config(processor_id, config_properties)
        return updated_entity
    except ValueError as e: # Catch conflict or not found from client
        logger.error(f"Update processor config failed for {processor_id}: {e}")
        # Distinguish between conflict and not found if possible based on message
        if "Conflict updating processor" in str(e):
            raise ToolError(f"Conflict updating processor {processor_id}: Revision mismatch. Try again.") from e
        elif "not found" in str(e):
             raise ToolError(f"Processor {processor_id} not found for update.") from e
        else:
            raise ToolError(f"Update processor config failed: {e}") from e
    except (NiFiAuthenticationError, ConnectionError) as e:
        logger.error(f"API error updating processor config for {processor_id}: {e}", exc_info=True)
        raise ToolError(f"Failed to update NiFi processor config: {e}")
    except Exception as e:
        logger.error(f"Unexpected error updating processor config for {processor_id}: {e}", exc_info=True)
        raise ToolError(f"An unexpected error occurred updating processor config: {e}")


@mcp.tool()
async def start_nifi_processor(processor_id: str) -> Dict:
    """
    Starts a specific processor (sets state to RUNNING).

    Requires fetching the processor's current revision first.

    Args:
        processor_id: The UUID of the processor to start.

    Returns:
        A dictionary representing the updated processor entity with the new state.

    Raises:
        ToolError: If authentication fails, the processor is not found,
                   there's a revision conflict (409), or another API error occurs.
    """
    await ensure_authenticated()
    logger.info(f"Executing start_nifi_processor for ID: {processor_id}")
    try:
        updated_entity = await nifi_api_client.update_processor_state(processor_id, "RUNNING")
        # Check the actual state after the API call
        actual_state = updated_entity.get("component", {}).get("state", "UNKNOWN")
        if actual_state == "RUNNING":
            return {
                "status": "success",
                "message": f"Processor {processor_id} started successfully.",
                "entity": updated_entity
            }
        else:
            logger.warning(f"Processor {processor_id} state is {actual_state} after start request (expected RUNNING). Possible validation errors.")
            return {
                "status": "warning",
                "message": f"Processor {processor_id} state set to RUNNING via API, but current state is {actual_state}. Check NiFi bulletins for validation errors.",
                "entity": updated_entity
            }
    except ValueError as e: # Catch conflict or not found from client
        logger.error(f"Start processor failed for {processor_id}: {e}")
        # Return error status
        status = "error"
        message = f"Start processor failed: {e}"
        if "Conflict" in str(e):
            message = f"Conflict starting processor {processor_id}: Revision mismatch. Try again."
        elif "not found" in str(e):
             message = f"Processor {processor_id} not found for starting."
             status = "not_found"
        return {"status": status, "message": message, "entity": None}
    except (NiFiAuthenticationError, ConnectionError) as e:
        logger.error(f"API error starting processor {processor_id}: {e}", exc_info=True)
        # raise ToolError(f"Failed to start NiFi processor: {e}")
        return {"status": "error", "message": f"API error starting processor: {e}", "entity": None}
    except Exception as e:
        logger.error(f"Unexpected error starting processor {processor_id}: {e}", exc_info=True)
        # raise ToolError(f"An unexpected error occurred starting processor: {e}")
        return {"status": "error", "message": f"Unexpected error starting processor: {e}", "entity": None}

@mcp.tool()
async def stop_nifi_processor(processor_id: str) -> Dict:
    """
    Stops a specific processor (sets state to STOPPED).

    Requires fetching the processor's current revision first.

    Args:
        processor_id: The UUID of the processor to stop.

    Returns:
        A dictionary representing the updated processor entity with the new state.

    Raises:
        ToolError: If authentication fails, the processor is not found,
                   there's a revision conflict (409), or another API error occurs.
    """
    await ensure_authenticated()
    logger.info(f"Executing stop_nifi_processor for ID: {processor_id}")
    try:
        updated_entity = await nifi_api_client.update_processor_state(processor_id, "STOPPED")
        # Check the actual state after the API call
        actual_state = updated_entity.get("component", {}).get("state", "UNKNOWN")
        # NiFi might take a moment, so accept STOPPING or STOPPED
        if actual_state in ["STOPPED", "STOPPING"]:
            return {
                "status": "success",
                "message": f"Processor {processor_id} stopped (or stopping) successfully.",
                "entity": updated_entity
            }
        else:
            # This case is less common for stopping unless there's an error
            logger.warning(f"Processor {processor_id} state is {actual_state} after stop request (expected STOPPED/STOPPING).")
            return {
                "status": "warning",
                "message": f"Processor {processor_id} state set to STOPPED via API, but current state is {actual_state}. This might indicate an issue.",
                "entity": updated_entity
            }
    except ValueError as e: # Catch conflict or not found from client
        logger.error(f"Stop processor failed for {processor_id}: {e}")
        status = "error"
        message = f"Stop processor failed: {e}"
        if "Conflict" in str(e):
            message = f"Conflict stopping processor {processor_id}: Revision mismatch. Try again."
        elif "not found" in str(e):
             message = f"Processor {processor_id} not found for stopping."
             status = "not_found"
        return {"status": status, "message": message, "entity": None}
    except (NiFiAuthenticationError, ConnectionError) as e:
        logger.error(f"API error stopping processor {processor_id}: {e}", exc_info=True)
        # raise ToolError(f"Failed to stop NiFi processor: {e}")
        return {"status": "error", "message": f"API error stopping processor: {e}", "entity": None}
    except Exception as e:
        logger.error(f"Unexpected error stopping processor {processor_id}: {e}", exc_info=True)
        # raise ToolError(f"An unexpected error occurred stopping processor: {e}")
        return {"status": "error", "message": f"Unexpected error stopping processor: {e}", "entity": None}

# --- Server Run --- #

async def cleanup():
    """Perform cleanup tasks on server shutdown."""
    if nifi_api_client:
        logger.info("Closing NiFi API client connection.")
        await nifi_api_client.close()

if __name__ == "__main__":
    # Ensure .env is loaded if running directly (server loads its own when launched by client)
    from dotenv import load_dotenv
    load_dotenv()

    # Register cleanup function using loop (may need adjustment for different OS/Python versions)
    try:
        loop = asyncio.get_event_loop()
        # Add signal handlers if loop supports it (may not work on Windows)
        if hasattr(signal, 'SIGINT'):
            loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(cleanup()))
        if hasattr(signal, 'SIGTERM'):
            loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(cleanup()))
    except NotImplementedError:
        logger.warning("Signal handlers not supported on this platform/loop.")
    except Exception as e:
        logger.error(f"Error setting up signal handlers: {e}")

    logger.info("Starting NiFi MCP Server...")
    # Run the server using stdio transport
    try:
        mcp.run(transport='stdio')
    finally:
        # Ensure cleanup runs even if mcp.run exits unexpectedly
        # Note: This might run cleanup twice if signal handler also runs
        logger.info("Server run loop finished. Performing final cleanup...")
        asyncio.run(cleanup())

    logger.info("NiFi MCP Server stopped.")

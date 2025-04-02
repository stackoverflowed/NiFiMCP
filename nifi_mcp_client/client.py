import asyncio
import sys
import logging
from pathlib import Path
from contextlib import AsyncExitStack
from typing import Any, Dict, Optional
import json

# Import ClientSession and the stdio_client context manager and parameters class
from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters
# Import request/result types for tool execution
from mcp.types import CallToolRequest, CallToolRequestParams, CallToolResult
from mcp.shared.exceptions import McpError # Base error
# Configure logging for the client
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("nifi_mcp_client")

# --- Client Configuration ---

# Determine the path to the server script relative to this client script
SERVER_SCRIPT_NAME = "server.py"
# Assumes client.py is in nifi_mcp_client/ and server.py is in nifi_mcp_server/
# Go up one level from client.py's directory, then into nifi_mcp_server/
CLIENT_DIR = Path(__file__).parent.resolve()
SERVER_DIR = CLIENT_DIR.parent / "nifi_mcp_server"
SERVER_SCRIPT_PATH = SERVER_DIR / SERVER_SCRIPT_NAME

async def run_test_list_processors(client: ClientSession):
    """Tests the list_nifi_processors tool."""
    tool_name = "list_nifi_processors"
    params = {} # Call with default (root process group)

    logger.info(f"Attempting to call tool '{tool_name}' with params: {params}")
    try:
        # Construct the tool call request
        tool_request_params = CallToolRequestParams(name=tool_name, arguments=params)
        tool_request = CallToolRequest(method="tools/call", params=tool_request_params)

        # Send the request and specify the expected result type
        result: CallToolResult = await client.send_request(tool_request, CallToolResult)

        logger.info(f"Tool '{tool_name}' executed successfully.")

        # Process the CallToolResult - the actual data is in result.content
        if result.content and isinstance(result.content, list) and len(result.content) > 0:
            # Assuming the list contains TextContent objects and the first one holds our data
            content_item = result.content[0]
            if hasattr(content_item, 'text') and isinstance(content_item.text, str):
                raw_json_string = content_item.text
                # --- DEBUG LOGGING --- #
                logger.debug(f"RUN_TEST_LIST: Received raw JSON string: {raw_json_string[:500]}...") # Log first 500 chars
                # --- END DEBUG LOGGING --- #
                try:
                    loaded_data = json.loads(raw_json_string)

                    # --- REVERT FIX: Expect list directly --- #
                    if isinstance(loaded_data, list):
                        actual_processor_list = loaded_data
                        logger.debug("RUN_TEST_LIST: Successfully parsed JSON list.")
                    else:
                        actual_processor_list = []
                        logger.warning(f"RUN_TEST_LIST: Received unexpected data type from JSON: {type(loaded_data)}")
                    # --- END REVERT FIX --- #

                    if actual_processor_list: # Use the extracted list
                         logger.info(f"Received {len(actual_processor_list)} processors (parsed from JSON):")
                         for i, proc_entity in enumerate(actual_processor_list):
                             # Now proc_entity should be a dictionary
                             proc_id = proc_entity.get("id", "N/A")
                             proc_component = proc_entity.get("component", {})
                             proc_name = proc_component.get("name", "N/A")
                             proc_type = proc_component.get("type", "N/A")
                             logger.info(f"  {i+1}. ID: {proc_id}, Name: {proc_name}, Type: {proc_type}")
                    else:
                         # This case might now be redundant if extraction handles dicts/lists
                         logger.info(f"Parsed JSON content was not a list or extractable list from dict. Original type: {type(loaded_data)}, Content: {loaded_data}")
                except json.JSONDecodeError as json_err:
                     logger.error(f"Failed to parse JSON from TextContent: {json_err}")
                     logger.error(f"Raw text content was: {raw_json_string}")
            else:
                 logger.warning(f"First content item is not TextContent or missing 'text': {type(content_item)}")
        else:
             logger.warning(f"Tool executed but returned no content or content is not a non-empty list.")

    except McpError as e: # Catch MCP-specific errors
        logger.error(f"MCP Error executing tool '{tool_name}': Code={e.error.code}, Msg={e.error.message}, Data={e.error.data}", exc_info=True)
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"Unexpected error executing tool '{tool_name}': {e}", exc_info=True)

async def run_test_create_processor(
    client: ClientSession,
    processor_type: str,
    name: str,
    position_x: int,
    position_y: int
):
    """Tests the create_nifi_processor tool by creating a processor of the specified type."""
    tool_name = "create_nifi_processor"
    params = {
        "processor_type": processor_type,
        "name": name,
        "position_x": position_x,
        "position_y": position_y,
        # "process_group_id": "some_specific_id" # Optional: Test with a specific group later
    }

    logger.info(f"Attempting to call tool '{tool_name}' with params: {params}")
    try:
        # Construct the tool call request
        tool_request_params = CallToolRequestParams(name=tool_name, arguments=params)
        tool_request = CallToolRequest(method="tools/call", params=tool_request_params)

        # Send the request and specify the expected result type
        result: CallToolResult = await client.send_request(tool_request, CallToolResult)

        logger.info(f"Tool '{tool_name}' executed successfully.")

        # Process the CallToolResult - expect a dictionary in TextContent
        if result.content and isinstance(result.content, list) and len(result.content) > 0:
            content_item = result.content[0]
            if hasattr(content_item, 'text') and isinstance(content_item.text, str):
                raw_json_string = content_item.text
                try:
                    response_data = json.loads(raw_json_string)
                    if isinstance(response_data, dict):
                        status = response_data.get("status")
                        message = response_data.get("message", "No message provided.")
                        processor_entity = response_data.get("entity") # Entity might be None on error
                        
                        proc_id = None
                        if processor_entity and isinstance(processor_entity, dict):
                             proc_id = processor_entity.get("id", "N/A")
                             proc_component = processor_entity.get("component", {})
                             proc_name = proc_component.get("name", "N/A")
                             proc_type = proc_component.get("type", "N/A")

                        if status == "success":
                            logger.info(f"MCP Tool Status: SUCCESS - {message}")
                            logger.info(f"  Created Processor -> ID: {proc_id}, Name: {proc_name}, Type: {proc_type}")
                            return proc_id # Return ID on success
                        elif status == "warning":
                            logger.warning(f"MCP Tool Status: WARNING - {message}")
                            logger.info(f"  Created Processor -> ID: {proc_id}, Name: {proc_name}, Type: {proc_type}")
                            # Return ID even on warning, as processor was created
                            return proc_id 
                        else: # error or unknown status
                            logger.error(f"MCP Tool Status: {status.upper() if status else 'ERROR'} - {message}")
                            return None # Return None on failure
                    else:
                        logger.error(f"Create processor response was not a dictionary: {type(response_data)}")
                        return None
                except json.JSONDecodeError as json_err:
                    logger.error(f"Failed to parse JSON from create processor response: {json_err}")
                    logger.error(f"Raw text content was: {raw_json_string}")
                    return None
            else:
                logger.warning(f"Create processor response content item is not TextContent or missing 'text': {type(content_item)}")
                return None
        else:
            logger.warning(f"Create processor tool executed but returned no content.")
            return None

    except McpError as e: # Catch MCP framework errors
        logger.error(f"MCP Framework Error executing tool '{tool_name}': Code={e.error.code}, Msg={e.error.message}, Data={e.error.data}", exc_info=False)
        return None
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"Unexpected error executing tool '{tool_name}': {e}", exc_info=True)
        return None

async def run_test_create_connection(
    client: ClientSession,
    source_id: str,
    source_relationship: str,
    target_id: str,
    process_group_id: Optional[str] = None
):
    """Tests the create_nifi_connection tool."""
    tool_name = "create_nifi_connection"
    params = {
        "source_id": source_id,
        "source_relationship": source_relationship,
        "target_id": target_id,
    }
    if process_group_id:
        params["process_group_id"] = process_group_id

    logger.info(f"Attempting to call tool '{tool_name}' with params: {params}")
    try:
        # Construct the tool call request
        tool_request_params = CallToolRequestParams(name=tool_name, arguments=params)
        tool_request = CallToolRequest(method="tools/call", params=tool_request_params)

        # Send the request and specify the expected result type
        result: CallToolResult = await client.send_request(tool_request, CallToolResult)

        logger.info(f"Tool '{tool_name}' executed successfully.")

        # Process the CallToolResult - expect a dictionary in TextContent
        if result.content and isinstance(result.content, list) and len(result.content) > 0:
            content_item = result.content[0]
            if hasattr(content_item, 'text') and isinstance(content_item.text, str):
                raw_json_string = content_item.text
                try:
                    connection_entity = json.loads(raw_json_string)
                    if isinstance(connection_entity, dict):
                        conn_id = connection_entity.get("id", "N/A")
                        conn_component = connection_entity.get("component", {})
                        source_name = conn_component.get("source", {}).get("name", "N/A")
                        target_name = conn_component.get("destination", {}).get("name", "N/A")
                        rels = conn_component.get("selectedRelationships", [])
                        logger.info("Successfully created connection:")
                        logger.info(f"  ID: {conn_id}, From: '{source_name}', To: '{target_name}', Relationships: {rels}")
                        return conn_id # Return the connection ID
                    else:
                        logger.warning(f"Parsed JSON content is not a dictionary: {type(connection_entity)}, Content: {connection_entity}")
                except json.JSONDecodeError as json_err:
                    logger.error(f"Failed to parse JSON from TextContent: {json_err}")
                    logger.error(f"Raw text content was: {raw_json_string}")
            else:
                logger.warning(f"First content item is not TextContent or missing 'text': {type(content_item)}")
        else:
            logger.warning(f"Tool executed but returned no content or content is not a non-empty list.")

    except McpError as e: # Catch MCP-specific errors
        logger.error(f"MCP Error executing tool '{tool_name}': Code={e.error.code}, Msg={e.error.message}, Data={e.error.data}", exc_info=True)
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"Unexpected error executing tool '{tool_name}': {e}", exc_info=True)

    return None # Return None if creation failed or ID wasn't extracted

async def run_test_get_processor_details(client: ClientSession, processor_id: str):
    """Tests the get_nifi_processor_details tool."""
    tool_name = "get_nifi_processor_details"
    params = {"processor_id": processor_id}

    logger.info(f"Attempting to call tool '{tool_name}' with params: {params}")
    try:
        # Construct the tool call request
        tool_request_params = CallToolRequestParams(name=tool_name, arguments=params)
        tool_request = CallToolRequest(method="tools/call", params=tool_request_params)

        # Send the request and specify the expected result type
        result: CallToolResult = await client.send_request(tool_request, CallToolResult)

        logger.info(f"Tool '{tool_name}' executed successfully.")

        # Process the CallToolResult - expect a dictionary in TextContent
        if result.content and isinstance(result.content, list) and len(result.content) > 0:
            content_item = result.content[0]
            if hasattr(content_item, 'text') and isinstance(content_item.text, str):
                raw_json_string = content_item.text
                try:
                    processor_entity = json.loads(raw_json_string)
                    if isinstance(processor_entity, dict):
                        proc_id = processor_entity.get("id", "N/A")
                        proc_rev = processor_entity.get("revision", {}).get("version", "N/A")
                        proc_component = processor_entity.get("component", {})
                        proc_name = proc_component.get("name", "N/A")
                        proc_type = proc_component.get("type", "N/A")
                        proc_config = proc_component.get("config", {}).get("properties", {})
                        logger.info(f"Successfully fetched details for processor {proc_id} (Rev: {proc_rev}):")
                        logger.info(f"  Name: {proc_name}, Type: {proc_type}")
                        logger.info("  Configuration Properties:")
                        if proc_config:
                            for key, value in proc_config.items():
                                logger.info(f"    - {key}: {value}")
                        else:
                            logger.info("    (No configuration properties found)")
                        # Return the full entity if needed later
                        return processor_entity
                    else:
                        logger.warning(f"Parsed JSON content is not a dictionary: {type(processor_entity)}, Content: {processor_entity}")
                except json.JSONDecodeError as json_err:
                    logger.error(f"Failed to parse JSON from TextContent: {json_err}")
                    logger.error(f"Raw text content was: {raw_json_string}")
            else:
                logger.warning(f"First content item is not TextContent or missing 'text': {type(content_item)}")
        else:
            logger.warning(f"Tool executed but returned no content or content is not a non-empty list.")

    except McpError as e: # Catch MCP-specific errors
        # Check if it's a 'processor not found' error from the server
        if "Processor not found" in e.error.message:
             logger.warning(f"Tool '{tool_name}' failed: Processor with ID {processor_id} not found.")
        else:
            logger.error(f"MCP Error executing tool '{tool_name}': Code={e.error.code}, Msg={e.error.message}, Data={e.error.data}", exc_info=True)
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"Unexpected error executing tool '{tool_name}': {e}", exc_info=True)

    return None # Return None if failed

async def run_test_delete_processor(client: ClientSession, processor_id: str, version: int):
    """Tests the delete_nifi_processor tool."""
    tool_name = "delete_nifi_processor"
    params = {"processor_id": processor_id, "version": version}

    logger.info(f"Attempting to call tool '{tool_name}' with params: {params}")
    try:
        # Construct the tool call request
        tool_request_params = CallToolRequestParams(name=tool_name, arguments=params)
        tool_request = CallToolRequest(method="tools/call", params=tool_request_params)

        # Send the request and specify the expected result type
        result: CallToolResult = await client.send_request(tool_request, CallToolResult)

        logger.info(f"Tool '{tool_name}' executed successfully.")

        # Process the CallToolResult - expect a dictionary in TextContent
        if result.content and isinstance(result.content, list) and len(result.content) > 0:
            content_item = result.content[0]
            if hasattr(content_item, 'text') and isinstance(content_item.text, str):
                raw_json_string = content_item.text
                try:
                    # Attempt to parse as JSON (success case)
                    response_data = json.loads(raw_json_string)
                    if isinstance(response_data, dict) and response_data.get("status") == "success":
                        logger.info(f"Successfully deleted processor {processor_id} via MCP tool.")
                        return True
                    else:
                        # Got JSON, but not the expected success format
                        logger.warning(f"Delete tool reported success, but response format unexpected: {response_data}")
                        return False
                except json.JSONDecodeError:
                    # Failed to parse JSON - check if it's the framework's error string
                    if raw_json_string.startswith("Error executing tool"):
                        logger.error(f"Tool '{tool_name}' failed on server: {raw_json_string}")
                    else:
                        # Some other non-JSON string?
                        logger.error(f"Failed to parse non-JSON response from delete tool: {raw_json_string}")
                    return False
            else:
                logger.warning(f"Delete response content item is not TextContent or missing 'text': {type(content_item)}")
                return False
        else:
            logger.warning(f"Delete tool executed but returned no content or content is not a non-empty list.")
            return False

    except McpError as e: # Catch MCP framework errors (e.g., tool doesn't exist)
        logger.error(f"MCP Framework Error executing tool '{tool_name}': Code={e.error.code}, Msg={e.error.message}, Data={e.error.data}", exc_info=False)
        return False # Deletion failed
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"Unexpected error executing tool '{tool_name}': {e}", exc_info=True)
        return False # Deletion failed

async def run_test_delete_connection(client: ClientSession, connection_id: str, version: int):
    """Tests the delete_nifi_connection tool."""
    tool_name = "delete_nifi_connection"
    params = {"connection_id": connection_id, "version": version}

    logger.info(f"Attempting to call tool '{tool_name}' with params: {params}")
    try:
        tool_request_params = CallToolRequestParams(name=tool_name, arguments=params)
        tool_request = CallToolRequest(method="tools/call", params=tool_request_params)
        result: CallToolResult = await client.send_request(tool_request, CallToolResult)

        logger.info(f"Tool '{tool_name}' executed successfully.")

        if result.content and isinstance(result.content, list) and len(result.content) > 0:
            content_item = result.content[0]
            if hasattr(content_item, 'text') and isinstance(content_item.text, str):
                raw_json_string = content_item.text
                try:
                    # Attempt to parse as JSON (success case)
                    response_data = json.loads(raw_json_string)
                    if isinstance(response_data, dict) and response_data.get("status") == "success":
                        logger.info(f"Successfully deleted connection {connection_id} via MCP tool.")
                        return True
                    else:
                        # Got JSON, but not the expected success format
                        logger.warning(f"Delete connection tool reported success, but response format unexpected: {response_data}")
                        return False
                except json.JSONDecodeError:
                    # Failed to parse JSON - check if it's the framework's error string
                    if raw_json_string.startswith("Error executing tool"):
                        logger.error(f"Tool '{tool_name}' failed on server: {raw_json_string}")
                    else:
                        # Some other non-JSON string?
                        logger.error(f"Failed to parse non-JSON response from delete connection tool: {raw_json_string}")
                    return False
            else:
                 logger.warning(f"Delete connection response content item is not TextContent or missing 'text': {type(content_item)}")
                 return False
        else:
            logger.warning(f"Delete connection tool executed but returned no content or content is not a non-empty list.")
            return False

    except McpError as e:
        # Handle MCP framework errors (e.g., tool doesn't exist)
        logger.error(f"MCP Framework Error executing tool '{tool_name}': Code={e.error.code}, Msg={e.error.message}, Data={e.error.data}", exc_info=False)
        return False
    except Exception as e:
        logger.error(f"Unexpected error executing tool '{tool_name}': {e}", exc_info=True)
        return False

async def run_test_update_processor_config(
    client: ClientSession,
    processor_id: str,
    config_properties: Dict[str, Any]
):
    """Tests the update_nifi_processor_config tool."""
    tool_name = "update_nifi_processor_config"
    params = {"processor_id": processor_id, "config_properties": config_properties}

    logger.info(f"Attempting to call tool '{tool_name}' with params: {params}")
    try:
        tool_request_params = CallToolRequestParams(name=tool_name, arguments=params)
        tool_request = CallToolRequest(method="tools/call", params=tool_request_params)
        result: CallToolResult = await client.send_request(tool_request, CallToolResult)

        logger.info(f"Tool '{tool_name}' executed successfully.")

        if result.content and isinstance(result.content, list) and len(result.content) > 0:
            content_item = result.content[0]
            if hasattr(content_item, 'text') and isinstance(content_item.text, str):
                raw_json_string = content_item.text
                try:
                    updated_entity = json.loads(raw_json_string)
                    if isinstance(updated_entity, dict):
                        proc_id = updated_entity.get("id", "N/A")
                        new_rev = updated_entity.get("revision", {}).get("version", "N/A")
                        updated_props = updated_entity.get("component", {}).get("config", {}).get("properties", {})
                        logger.info(f"Successfully updated config for processor {proc_id}. New revision: {new_rev}")
                        logger.info("  Updated Properties (from response):")
                        # Log only the keys we tried to update for clarity
                        for key in config_properties.keys():
                            logger.info(f"    - {key}: {updated_props.get(key, '[Not in response properties]')}")
                        return updated_entity # Return the updated entity
                    else:
                        logger.warning(f"Update config response was not a dictionary: {type(updated_entity)}")
                        return None
                except json.JSONDecodeError as json_err:
                    logger.error(f"Failed to parse JSON from update config response: {json_err}")
                    logger.error(f"Raw text content was: {raw_json_string}")
                    return None
            else:
                 logger.warning(f"Update config response content item is not TextContent or missing 'text': {type(content_item)}")
                 return None
        else:
            logger.warning(f"Update config tool executed but returned no content.")
            return None

    except McpError as e:
        # Handle specific errors like conflict or not found
        if "Conflict updating processor" in e.error.message:
            logger.error(f"Tool '{tool_name}' failed: Revision conflict trying to update {processor_id}. State may be inconsistent.")
        elif "Processor not found" in e.error.message:
            logger.warning(f"Tool '{tool_name}' failed: Processor {processor_id} not found for update.")
        else:
            logger.error(f"MCP Error executing tool '{tool_name}': Code={e.error.code}, Msg={e.error.message}, Data={e.error.data}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error executing tool '{tool_name}': {e}", exc_info=True)
        return None

async def run_test_start_processor(client: ClientSession, processor_id: str):
    """Tests the start_nifi_processor tool."""
    tool_name = "start_nifi_processor"
    params = {"processor_id": processor_id}

    logger.info(f"Attempting to call tool '{tool_name}' with params: {params}")
    try:
        tool_request_params = CallToolRequestParams(name=tool_name, arguments=params)
        tool_request = CallToolRequest(method="tools/call", params=tool_request_params)
        result: CallToolResult = await client.send_request(tool_request, CallToolResult)

        logger.info(f"Tool '{tool_name}' executed successfully.")

        if result.content and isinstance(result.content, list) and len(result.content) > 0:
            content_item = result.content[0]
            if hasattr(content_item, 'text') and isinstance(content_item.text, str):
                raw_json_string = content_item.text
                try:
                    response_data = json.loads(raw_json_string)
                    if isinstance(response_data, dict):
                        status = response_data.get("status")
                        message = response_data.get("message", "No message provided.")
                        updated_entity = response_data.get("entity")

                        if status == "success":
                            logger.info(f"MCP Tool Status: SUCCESS - {message}")
                            return True
                        elif status == "warning":
                            logger.warning(f"MCP Tool Status: WARNING - {message}")
                            return True
                        else: # error, not_found, or other
                            logger.error(f"MCP Tool Status: {status.upper() if status else 'ERROR'} - {message}")
                            return False
                    else:
                        logger.error(f"Start processor response was not a dictionary: {type(response_data)}")
                        return False
                except json.JSONDecodeError as json_err:
                    logger.error(f"Failed to parse JSON from start processor response: {json_err}")
                    logger.error(f"Raw text content was: {raw_json_string}")
                    return False
            else:
                 logger.warning(f"Start processor response content item is not TextContent or missing 'text': {type(content_item)}")
                 return False
        else:
            logger.warning(f"Start processor tool executed but returned no content.")
            return False
    except McpError as e:
        # Handle MCP framework errors (less likely now with structured response)
        logger.error(f"MCP Framework Error executing tool '{tool_name}': Code={e.error.code}, Msg={e.error.message}, Data={e.error.data}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Unexpected error executing tool '{tool_name}': {e}", exc_info=True)
        return False

async def run_test_stop_processor(client: ClientSession, processor_id: str):
    """Tests the stop_nifi_processor tool."""
    tool_name = "stop_nifi_processor"
    params = {"processor_id": processor_id}

    logger.info(f"Attempting to call tool '{tool_name}' with params: {params}")
    try:
        tool_request_params = CallToolRequestParams(name=tool_name, arguments=params)
        tool_request = CallToolRequest(method="tools/call", params=tool_request_params)
        result: CallToolResult = await client.send_request(tool_request, CallToolResult)

        logger.info(f"Tool '{tool_name}' executed successfully.")

        if result.content and isinstance(result.content, list) and len(result.content) > 0:
            content_item = result.content[0]
            if hasattr(content_item, 'text') and isinstance(content_item.text, str):
                raw_json_string = content_item.text
                try:
                    response_data = json.loads(raw_json_string)
                    if isinstance(response_data, dict):
                        status = response_data.get("status")
                        message = response_data.get("message", "No message provided.")
                        updated_entity = response_data.get("entity")

                        if status == "success":
                            logger.info(f"MCP Tool Status: SUCCESS - {message}")
                            return True
                        elif status == "warning": # Less likely for stop, but handle anyway
                            logger.warning(f"MCP Tool Status: WARNING - {message}")
                            return True
                        else: # error, not_found, or other
                            logger.error(f"MCP Tool Status: {status.upper() if status else 'ERROR'} - {message}")
                            return False
                    else:
                        logger.error(f"Stop processor response was not a dictionary: {type(response_data)}")
                        return False
                except json.JSONDecodeError as json_err:
                    logger.error(f"Failed to parse JSON from stop processor response: {json_err}")
                    logger.error(f"Raw text content was: {raw_json_string}")
                    return False
            else:
                 logger.warning(f"Stop processor response content item is not TextContent or missing 'text': {type(content_item)}")
                 return False
        else:
            logger.warning(f"Stop processor tool executed but returned no content.")
            return False
    except McpError as e:
        # Handle MCP framework errors
        logger.error(f"MCP Framework Error executing tool '{tool_name}': Code={e.error.code}, Msg={e.error.message}, Data={e.error.data}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Unexpected error executing tool '{tool_name}': {e}", exc_info=True)
        return False

async def main():
    """Main function to launch server and run test calls."""
    if not SERVER_SCRIPT_PATH.exists():
        logger.error(f"Server script not found at expected path: {SERVER_SCRIPT_PATH}")
        sys.exit(1)

    # Command to launch the server using python's module execution flag (-m)
    python_executable = sys.executable
    server_module = "nifi_mcp_server.server"
    logger.info(f"Launching MCP server module: {python_executable} -m {server_module}")

    # Construct parameters using the StdioServerParameters class for module execution
    server_params_obj = StdioServerParameters(
        command=python_executable,
        args=["-m", server_module] # Use -m flag
        # env=os.environ.copy() # Example: Pass environment if needed
        # cwd=str(CLIENT_DIR.parent) # Set cwd to project root if needed for imports
    )

    async with AsyncExitStack() as stack:
        try:
            # Launch the server process and get streams using stdio_client
            read_stream, write_stream = await stack.enter_async_context(
                stdio_client(server_params_obj) # Pass the StdioServerParameters object
            )
            logger.info("stdio_client connected to server subprocess.")

            # Create the ClientSession using the streams
            client: ClientSession = await stack.enter_async_context(
                ClientSession(read_stream, write_stream)
            )
            logger.info("MCP ClientSession initialized.")

            # Initialize the MCP session (sends initialize request)
            # This might be necessary depending on the server implementation
            try:
                init_result = await client.initialize()
                logger.info(f"MCP session initialized. Server capabilities: {init_result.capabilities}")
            except Exception as init_err:
                logger.error(f"MCP session initialization failed: {init_err}", exc_info=True)
                # Depending on the error, we might not be able to proceed
                return # Exit if initialization fails

            # Wait briefly for server to be fully ready after init (optional)
            await asyncio.sleep(0.5)

            # --- Run Test Tool Calls ---
            # await run_test_list_processors(client)

            # 0. Cleanup previous test processors and their connection
            logger.info("---> Running Cleanup Phase <---")
            processors_to_delete = {}
            connection_to_delete = None
            generate_proc_id_cleanup = None
            log_proc_id_cleanup = None

            # --- List Processors --- #
            logger.info("Cleanup: Listing processors...")
            tool_name_list_p = "list_nifi_processors"
            try:
                list_p_request_params = CallToolRequestParams(name=tool_name_list_p, arguments={})
                list_p_request = CallToolRequest(method="tools/call", params=list_p_request_params)
                list_p_result: CallToolResult = await client.send_request(list_p_request, CallToolResult)
                if list_p_result.content and isinstance(list_p_result.content, list) and len(list_p_result.content) > 0:
                    content_item = list_p_result.content[0]
                    if hasattr(content_item, 'text') and isinstance(content_item.text, str):
                        raw_json_string = content_item.text
                        logger.debug(f"CLEANUP PHASE (Procs): Received raw JSON string: {raw_json_string[:500]}...")
                        loaded_data = json.loads(raw_json_string)
                        if isinstance(loaded_data, list):
                             actual_processor_list = loaded_data
                             logger.debug("CLEANUP PHASE: Successfully parsed processor JSON list.")
                             for proc_entity in actual_processor_list:
                                 proc_component = proc_entity.get("component", {})
                                 proc_name = proc_component.get("name", "")
                                 proc_id = proc_entity.get("id")
                                 proc_version = proc_entity.get("revision", {}).get("version")
                                 if proc_id and proc_version is not None:
                                     if proc_name == "GenerateFlowFile (MCP)":
                                         processors_to_delete[proc_id] = {"version": proc_version, "name": proc_name}
                                         generate_proc_id_cleanup = proc_id
                                         logger.info(f"Found '{proc_name}' (ID: {proc_id}, Version: {proc_version}) for deletion.")
                                     elif proc_name == "LogAttribute (MCP)":
                                         processors_to_delete[proc_id] = {"version": proc_version, "name": proc_name}
                                         log_proc_id_cleanup = proc_id
                                         logger.info(f"Found '{proc_name}' (ID: {proc_id}, Version: {proc_version}) for deletion.")
                        else:
                            logger.warning(f"CLEANUP PHASE (Procs): Received unexpected data type from JSON: {type(loaded_data)}")
                    else:
                        logger.warning("CLEANUP PHASE (Procs): List response content item is not TextContent or missing 'text'.")
                else:
                    logger.warning("CLEANUP PHASE (Procs): List tool executed but returned no content.")
            except Exception as list_err:
                logger.error(f"Failed to list processors during cleanup: {list_err}", exc_info=True)

            # --- List Connections (only if both processors were found) --- #
            if generate_proc_id_cleanup and log_proc_id_cleanup:
                logger.info("Cleanup: Listing connections to find the one between test processors...")
                tool_name_list_c = "list_nifi_connections"
                try:
                    list_c_request_params = CallToolRequestParams(name=tool_name_list_c, arguments={})
                    list_c_request = CallToolRequest(method="tools/call", params=list_c_request_params)
                    list_c_result: CallToolResult = await client.send_request(list_c_request, CallToolResult)
                    if list_c_result.content and isinstance(list_c_result.content, list) and len(list_c_result.content) > 0:
                        content_item = list_c_result.content[0]
                        if hasattr(content_item, 'text') and isinstance(content_item.text, str):
                            raw_json_string_c = content_item.text
                            logger.debug(f"CLEANUP PHASE (Conns): Received raw JSON string: {raw_json_string_c[:500]}...")
                            loaded_data_c = json.loads(raw_json_string_c)
                            if isinstance(loaded_data_c, list):
                                actual_connection_list = loaded_data_c
                                logger.debug("CLEANUP PHASE: Successfully parsed connection JSON list.")
                                for conn_entity in actual_connection_list:
                                    conn_component = conn_entity.get("component", {})
                                    source_id = conn_component.get("source", {}).get("id")
                                    dest_id = conn_component.get("destination", {}).get("id")
                                    # Check if this connection links our two specific processors
                                    if source_id == generate_proc_id_cleanup and dest_id == log_proc_id_cleanup:
                                        conn_id = conn_entity.get("id")
                                        conn_version = conn_entity.get("revision", {}).get("version")
                                        if conn_id and conn_version is not None:
                                            connection_to_delete = {"id": conn_id, "version": conn_version}
                                            logger.info(f"Found connection (ID: {conn_id}, Version: {conn_version}) between test processors for deletion.")
                                            break # Found the connection, no need to check others
                            else:
                                logger.warning(f"CLEANUP PHASE (Conns): Received unexpected data type from JSON: {type(loaded_data_c)}")
                        else:
                             logger.warning("CLEANUP PHASE (Conns): List response content item is not TextContent or missing 'text'.")
                    else:
                        logger.warning("CLEANUP PHASE (Conns): List tool executed but returned no content.")
                except Exception as list_c_err:
                    logger.error(f"Failed to list connections during cleanup: {list_c_err}", exc_info=True)

            # --- Perform Deletions --- #
            # Delete connection first if found
            if connection_to_delete:
                logger.info(f"Deleting connection ID: {connection_to_delete['id']} (Version: {connection_to_delete['version']})...")
                await run_test_delete_connection(client, connection_to_delete['id'], connection_to_delete['version'])
                await asyncio.sleep(0.5) # Wait after deleting connection
            elif generate_proc_id_cleanup and log_proc_id_cleanup:
                logger.info("Cleanup: Connection between test processors not found (maybe already deleted?).")

            # Delete processors
            if processors_to_delete:
                 logger.info(f"Deleting {len(processors_to_delete)} processors...")
                 # Delete LogAttribute first (if exists) as it might have caused issues before
                 if log_proc_id_cleanup and log_proc_id_cleanup in processors_to_delete:
                     proc_info = processors_to_delete[log_proc_id_cleanup]
                     logger.info(f"Deleting '{proc_info['name']}' (ID: {log_proc_id_cleanup}, Version: {proc_info['version']})...")
                     await run_test_delete_processor(client, log_proc_id_cleanup, proc_info['version'])
                     await asyncio.sleep(0.2)
                 # Delete GenerateFlowFile (if exists)
                 if generate_proc_id_cleanup and generate_proc_id_cleanup in processors_to_delete:
                     proc_info = processors_to_delete[generate_proc_id_cleanup]
                     logger.info(f"Deleting '{proc_info['name']}' (ID: {generate_proc_id_cleanup}, Version: {proc_info['version']})...")
                     await run_test_delete_processor(client, generate_proc_id_cleanup, proc_info['version'])
                     await asyncio.sleep(0.2)

                 logger.info("Cleanup phase finished.")
            else:
                 logger.info("No processors found matching test names for cleanup.")

            # Wait after cleanup before creating new ones
            await asyncio.sleep(1.0)

            # 1. Create GenerateFlowFile processor
            logger.info("---> Testing: Create GenerateFlowFile <---")
            generate_proc_id = await run_test_create_processor(
                client,
                processor_type="org.apache.nifi.processors.standard.GenerateFlowFile",
                name="GenerateFlowFile (MCP)",
                position_x=0,
                position_y=0
            )
            await asyncio.sleep(0.5) # Small delay

            # 2. Create LogAttribute processor
            logger.info("---> Testing: Create LogAttribute <---")
            log_proc_id = await run_test_create_processor(
                client,
                processor_type="org.apache.nifi.processors.standard.LogAttribute",
                name="LogAttribute (MCP)",
                position_x=0,
                position_y=200 # Place below the first one
            )
            await asyncio.sleep(0.5) # Small delay

            # 3. Connect them if both were created
            if generate_proc_id and log_proc_id:
                logger.info("---> Testing: Create Connection <---")
                await run_test_create_connection(
                    client,
                    source_id=generate_proc_id,
                    source_relationship="success", # Default relationship for GenerateFlowFile
                    target_id=log_proc_id
                )
                await asyncio.sleep(0.5)

                # 4. Get details of the LogAttribute processor
                logger.info("---> Testing: Get LogAttribute Processor Details <---")
                await run_test_get_processor_details(client, log_proc_id)
                await asyncio.sleep(0.5)

                # 5. Update LogAttribute config
                logger.info("---> Testing: Update LogAttribute Config <---")
                await run_test_update_processor_config(
                    client,
                    processor_id=log_proc_id,
                    config_properties={
                        "Log Level": "WARN", # Change from default INFO to WARN
                        "Log Payload": "true" # Example of another property
                    }
                )
                await asyncio.sleep(0.5)

                # 6. Get details again to verify update (Optional)
                logger.info("---> Testing: Get LogAttribute Details (After Update) <---")
                await run_test_get_processor_details(client, log_proc_id)
                await asyncio.sleep(0.5)

                # 7. Start processors
                logger.info("---> Testing: Start Processors <---")
                # Start GenerateFlowFile first, then LogAttribute
                await run_test_start_processor(client, generate_proc_id)
                await asyncio.sleep(0.5) # Give it a moment
                await run_test_start_processor(client, log_proc_id)
                await asyncio.sleep(1.0) # Let them run briefly

                # 8. Stop processors (before cleanup)
                logger.info("---> Testing: Stop Processors <---")
                # Stop LogAttribute first, then GenerateFlowFile (reverse of start)
                await run_test_stop_processor(client, log_proc_id)
                await asyncio.sleep(0.5) # Give it a moment
                await run_test_stop_processor(client, generate_proc_id)
                await asyncio.sleep(1.0) # Wait for them to stop

            else:
                logger.warning("Skipping connection, get details, update, and start/stop tests because one or both processors failed to create.")

            # List processors again to see the result
            logger.info("---> Testing: List Processors (After Creation) <---")
            await run_test_list_processors(client)

            logger.info("Test client finished.")

        except ConnectionRefusedError:
            logger.error("Connection refused. Is the server running correctly?")
        except Exception as e:
            logger.error(f"An error occurred in the client: {e}", exc_info=True)

if __name__ == "__main__":
    # Ensure environment variables (.env) are loaded if running this directly
    # The server process launched will load its own .env
    from dotenv import load_dotenv
    dotenv_path = CLIENT_DIR.parent / ".env" # Look for .env in the project root
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)
        logger.info(f"Loaded environment variables from {dotenv_path}")
    else:
        logger.warning(f".env file not found at {dotenv_path}. Server might fail if auth is needed.")

    asyncio.run(main())

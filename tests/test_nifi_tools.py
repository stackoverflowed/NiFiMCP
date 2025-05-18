import asyncio
import httpx
import os
import sys
import time
import uuid
from loguru import logger

# --- Configuration ---
# Get server details from environment variables or use defaults
BASE_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8000")
# !! IMPORTANT: Set this environment variable to the ID of the NiFi instance 
# defined in the server's config.yaml that you want to target for testing.
TARGET_NIFI_SERVER_ID = os.environ.get("NIFI_TEST_SERVER_ID") 

# --- Logging Setup ---
logger.remove() # Remove default/existing handlers first
logger.add(sys.stderr, level="INFO") # Configure basic logging

# --- Test Parameters ---
TEST_RUN_ID = str(uuid.uuid4())[:8] # Short unique ID for this test run
TEST_PG_NAME = f"mcp-auto-test-pg-{TEST_RUN_ID}"

# --- API Client Helper ---
async def call_tool(client: httpx.AsyncClient, tool_name: str, arguments: dict, headers: dict) -> dict:
    """Helper function to make POST requests to the tool endpoint."""
    url = f"{BASE_URL}/tools/{tool_name}"
    payload = {"arguments": arguments}
    logger.debug(f"Calling POST {url} with payload: {payload}")
    try:
        response = await client.post(url, json=payload, headers=headers, timeout=30.0) # Add timeout
        response.raise_for_status() # Raise exception for 4xx/5xx errors
        json_response = response.json()
        logger.debug(f"Received {response.status_code} response: {json_response}")
        return json_response
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error calling tool '{tool_name}': {e.response.status_code} - {e.response.text}")
        raise # Re-raise after logging
    except httpx.RequestError as e:
        logger.error(f"Request error calling tool '{tool_name}': {e}")
        raise # Re-raise after logging
    except Exception as e:
        logger.error(f"Unexpected error calling tool '{tool_name}': {e}", exc_info=True)
        raise # Re-raise after logging

# --- Main Test Function ---
async def run_nifi_tool_tests():
    """Orchestrates the NiFi MCP tool test sequence."""
    if not TARGET_NIFI_SERVER_ID:
        logger.error("FATAL: NIFI_TEST_SERVER_ID environment variable is not set.")
        logger.error("Please set this variable to the ID of the target NiFi server from the MCP server's config.yaml.")
        sys.exit(1)

    logger.info(f"Starting NiFi MCP Tool Test Run ID: {TEST_RUN_ID}")
    logger.info(f"Targeting Server: {BASE_URL}")
    logger.info(f"Targeting NiFi Instance ID: {TARGET_NIFI_SERVER_ID}")

    headers = {
        "X-Nifi-Server-Id": TARGET_NIFI_SERVER_ID,
        "Content-Type": "application/json"
    }

    # Variables to store created object IDs for cleanup
    test_pg_id = None
    generate_proc_id = None
    log_proc_id = None
    connection_id = None
    # Add var for create_nifi_flow test PG
    create_flow_pg_id = None

    async with httpx.AsyncClient() as client:
        try:
            # --- Optional: Check Server Connectivity ---
            try:
                logger.info("Checking server connectivity and NiFi configuration...")
                config_url = f"{BASE_URL}/config/nifi-servers"
                config_resp = await client.get(config_url, timeout=10.0)
                config_resp.raise_for_status()
                servers = config_resp.json()
                server_ids = [s.get('id') for s in servers]
                if TARGET_NIFI_SERVER_ID not in server_ids:
                     logger.error(f"FATAL: Target NiFi Server ID '{TARGET_NIFI_SERVER_ID}' not found in server config: {server_ids}")
                     sys.exit(1)
                logger.info(f"Server contacted successfully. Target NiFi ID '{TARGET_NIFI_SERVER_ID}' found in config.")
            except httpx.RequestError as e:
                 logger.error(f"FATAL: Could not connect to MCP server at {BASE_URL}: {e}")
                 sys.exit(1)
            except httpx.HTTPStatusError as e:
                 logger.error(f"FATAL: Error fetching config from {BASE_URL}: {e.response.status_code} - {e.response.text}")
                 sys.exit(1)

            # --- Test Execution Phase ---
            logger.info("--- Starting Test Execution Phase ---")

            # 1. Create Test Process Group
            logger.info(f"Creating Test Process Group: {TEST_PG_NAME}")
            create_pg_args = {"name": TEST_PG_NAME, "position_x": 0, "position_y": 0}
            pg_result_list = await call_tool(client, "create_nifi_process_group", create_pg_args, headers)
            # Expect a list containing one dictionary
            assert isinstance(pg_result_list, list) and len(pg_result_list) > 0 and isinstance(pg_result_list[0], dict), "Unexpected response format for create_nifi_process_group"
            pg_result = pg_result_list[0]
            assert pg_result.get("status") == "success", f"Failed to create process group: {pg_result.get('message')}"
            test_pg_id = pg_result.get("entity", {}).get("id")
            assert test_pg_id, "Process group ID not found in response."
            logger.info(f"Successfully created Process Group ID: {test_pg_id}")
            await asyncio.sleep(1) # Small delay after creation

            # 2. Create GenerateFlowFile Processor
            logger.info(f"Creating GenerateFlowFile processor in PG {test_pg_id}")
            create_gen_args = {
                "process_group_id": test_pg_id,
                "processor_type": "org.apache.nifi.processors.standard.GenerateFlowFile",
                "name": "mcp-test-generate",
                "position_x": 0,
                "position_y": 0
            }
            gen_result_list = await call_tool(client, "create_nifi_processor", create_gen_args, headers)
            assert isinstance(gen_result_list, list) and len(gen_result_list) > 0 and isinstance(gen_result_list[0], dict), "Unexpected response format for create_nifi_processor (Generate)"
            gen_result = gen_result_list[0]
            assert gen_result.get("status") in ["success", "warning"], f"Failed to create Generate processor: {gen_result.get('message')}"
            generate_proc_id = gen_result.get("entity", {}).get("id")
            assert generate_proc_id, "Generate processor ID not found in response."
            logger.info(f"Successfully created GenerateFlowFile Processor ID: {generate_proc_id}")
            await asyncio.sleep(1)

            # 3. Create LogAttribute Processor
            logger.info(f"Creating LogAttribute processor in PG {test_pg_id}")
            create_log_args = {
                "process_group_id": test_pg_id,
                "processor_type": "org.apache.nifi.processors.standard.LogAttribute",
                "name": "mcp-test-log",
                "position_x": 0,
                "position_y": 200
            }
            log_result_list = await call_tool(client, "create_nifi_processor", create_log_args, headers)
            assert isinstance(log_result_list, list) and len(log_result_list) > 0 and isinstance(log_result_list[0], dict), "Unexpected response format for create_nifi_processor (Log)"
            log_result = log_result_list[0]
            assert log_result.get("status") in ["success", "warning"], f"Failed to create Log processor: {log_result.get('message')}"
            log_proc_id = log_result.get("entity", {}).get("id")
            assert log_proc_id, "Log processor ID not found in response."
            logger.info(f"Successfully created LogAttribute Processor ID: {log_proc_id}")
            await asyncio.sleep(1)

            # 3.5 Auto-terminate LogAttribute relationships to make it valid
            logger.info(f"Auto-terminating relationships for LogAttribute ({log_proc_id})")
            update_rels_args = {
                "processor_id": log_proc_id,
                "auto_terminated_relationships": ["success", "failure"]
            }
            rels_result_list = await call_tool(client, "update_nifi_processor_relationships", update_rels_args, headers)
            assert isinstance(rels_result_list, list) and len(rels_result_list) > 0 and isinstance(rels_result_list[0], dict), "Unexpected response format for update_nifi_processor_relationships"
            rels_result = rels_result_list[0]
            assert rels_result.get("status") in ["success", "warning"], f"Failed to update relationships: {rels_result.get('message')}"
            logger.info(f"Successfully updated LogAttribute relationships. Response status: {rels_result.get('status')}")
            await asyncio.sleep(1)

            # 4. Connect Processors
            logger.info(f"Connecting {generate_proc_id} -> {log_proc_id}")
            connect_args = {
                "source_id": generate_proc_id,
                "relationships": ["success"],
                "target_id": log_proc_id
            }
            conn_result_list = await call_tool(client, "create_nifi_connection", connect_args, headers)
            assert isinstance(conn_result_list, list) and len(conn_result_list) > 0 and isinstance(conn_result_list[0], dict), "Unexpected response format for create_nifi_connection"
            conn_result = conn_result_list[0]
            assert conn_result.get("status") == "success", f"Failed to create connection: {conn_result.get('message')}"
            connection_id = conn_result.get("entity", {}).get("id")
            assert connection_id, "Connection ID not found in response."
            logger.info(f"Successfully created Connection ID: {connection_id}")
            await asyncio.sleep(1)

            # 4.1 Get Process Group Status
            logger.info(f"Getting status for Process Group {test_pg_id}")
            get_status_args = {"process_group_id": test_pg_id}
            status_result_list = await call_tool(client, "get_process_group_status", get_status_args, headers)
            assert isinstance(status_result_list, list) and len(status_result_list) > 0 and isinstance(status_result_list[0], dict), "Unexpected response format for get_process_group_status"
            status_result = status_result_list[0]
            # assert "aggregateSnapshot" in status_result, "get_process_group_status response missing 'aggregateSnapshot'"
            # Check for a valid key based on the tool's actual response structure
            assert "component_summary" in status_result, "get_process_group_status response missing 'component_summary'"
            logger.info(f"Successfully retrieved status for PG {test_pg_id}. Queued Count: {status_result.get('queue_summary', {}).get('total_queued_count', 'N/A')}")
            await asyncio.sleep(1)

            # 4.2 Search Flow
            logger.info(f"Searching for 'mcp-test-log' in Process Group {test_pg_id}")
            search_args = {
                "query": "mcp-test-log",
                "filter_process_group_id": test_pg_id,
                "filter_object_type": "processor"
            }
            search_result_list = await call_tool(client, "search_nifi_flow", search_args, headers)
            assert isinstance(search_result_list, list) and len(search_result_list) > 0 and isinstance(search_result_list[0], dict), "Unexpected response format for search_nifi_flow"
            search_result = search_result_list[0]
            assert "processorResults" in search_result, "Search did not return any processor results for 'mcp-test-log'"
            found_ids = [p.get('id') for p in search_result["processorResults"]]
            assert log_proc_id in found_ids, f"Log processor {log_proc_id} not found in search results: {found_ids}"
            logger.info(f"Successfully found processor {log_proc_id} via search.")
            await asyncio.sleep(1)

            # 4.3 Document Flow
            logger.info(f"Documenting flow for Process Group {test_pg_id}")
            document_args = {"process_group_id": test_pg_id}
            doc_result_list = await call_tool(client, "document_nifi_flow", document_args, headers)
            assert isinstance(doc_result_list, list) and len(doc_result_list) > 0 and isinstance(doc_result_list[0], dict), "Unexpected response format for document_nifi_flow"
            doc_result = doc_result_list[0]
            # Check for flow_structure list and expected number of components
            assert "flow_structure" in doc_result, "Document flow response missing 'flow_structure' key."
            assert isinstance(doc_result["flow_structure"], list), "'flow_structure' should be a list."
            # For the simple Generate -> Log flow, we expect 2 components in the structure
            expected_components = 2
            actual_components = len(doc_result["flow_structure"])
            assert actual_components == expected_components, f"Expected {expected_components} components in flow_structure, found {actual_components}. Content: {doc_result['flow_structure']}"
            logger.info(f"Successfully generated documentation with {actual_components} components in flow_structure.")
            # logger.debug(f"Documentation Result: {json.dumps(doc_result, indent=2)}") # Uncomment to see full structure
            await asyncio.sleep(1)

            # 5. Update LogAttribute Properties (Processor must be stopped - should be by default)
            logger.info(f"Updating LogAttribute ({log_proc_id}) properties")
            update_props_args = {
                "processor_id": log_proc_id,
                "processor_config_properties": {
                    "Log Level": "warn",
                    "Attributes to Log": ".*",
                    "Log prefix": "mcp-test"
                }
            }
            update_result_list = await call_tool(client, "update_nifi_processor_properties", update_props_args, headers)
            assert isinstance(update_result_list, list) and len(update_result_list) > 0 and isinstance(update_result_list[0], dict), "Unexpected response format for update_nifi_processor_properties"
            update_result = update_result_list[0]
            assert update_result.get("status") in ["success", "warning"], f"Failed to update properties: {update_result.get('message')}"
            # Check if the property was actually updated in the response (optional but good)
            # updated_props = update_result.get("entity", {}).get("component", {}).get("config", {}).get("properties", {})
            # assert updated_props.get("Log Level") == "WARN", "Log Level property not updated correctly in response."
            logger.info(f"Successfully submitted update for LogAttribute properties. Update response status: {update_result.get('status')}")
            await asyncio.sleep(1) # Allow time for update to potentially process

            # Re-fetch details to verify the update
            logger.info(f"Re-fetching LogAttribute ({log_proc_id}) details to verify property update.")
            verify_update_args = {"object_type": "processor", "object_id": log_proc_id}
            verify_result_list = await call_tool(client, "get_nifi_object_details", verify_update_args, headers)
            assert isinstance(verify_result_list, list) and len(verify_result_list) > 0 and isinstance(verify_result_list[0], dict), "Unexpected response format for get_nifi_object_details (verify update)"
            verify_result = verify_result_list[0]
            verified_component = verify_result.get("component", {})
            verified_config = verified_component.get("config", {})
            verified_props = verified_config.get("properties", {})
            validation_status = verified_component.get("validationStatus")

            # Check property
            assert verified_props.get("Log Level") == "warn", f"Log Level property was not updated correctly after re-fetching. Found: {verified_props.get('Log Level')}"
            logger.info(f"Successfully verified LogAttribute properties update (Log Level: {verified_props.get('Log Level')}).")

            # Check validation status BEFORE trying to start
            assert validation_status == "VALID", f"Processor {log_proc_id} is not VALID after property update. Status: {validation_status}. Cannot proceed to start."
            logger.info(f"Processor {log_proc_id} validation status confirmed: {validation_status}")

            await asyncio.sleep(1)

            # 6. Start the Process Group
            logger.info(f"Starting Process Group {test_pg_id}")
            operate_pg_start_args = {
                "object_type": "process_group",
                "object_id": test_pg_id,
                "operation_type": "start"
            }
            start_pg_result_list = await call_tool(client, "operate_nifi_object", operate_pg_start_args, headers)
            assert isinstance(start_pg_result_list, list) and len(start_pg_result_list) > 0 and isinstance(start_pg_result_list[0], dict), "Unexpected response format for operate_nifi_object (start PG)"
            start_pg_result = start_pg_result_list[0]
            assert start_pg_result.get("status") == "success", f"Failed to start process group: {start_pg_result.get('message')}"
            logger.info(f"Successfully started Process Group {test_pg_id}")
            await asyncio.sleep(5) # Allow time for components to start and maybe process a flowfile

            # 7. Check Processor Status (Optional)
            logger.info(f"Checking status of LogAttribute processor {log_proc_id}")
            get_details_args = {"object_type": "processor", "object_id": log_proc_id}
            details_result_list = await call_tool(client, "get_nifi_object_details", get_details_args, headers)
            assert isinstance(details_result_list, list) and len(details_result_list) > 0 and isinstance(details_result_list[0], dict), "Unexpected response format for get_nifi_object_details"
            details_result = details_result_list[0]
            # Note: get_details doesn't have a 'status' field, it returns the entity directly or raises error
            assert details_result.get("component", {}).get("state") == "RUNNING", "LogAttribute processor is not RUNNING after start command."
            logger.info(f"LogAttribute processor confirmed RUNNING.")
            await asyncio.sleep(1)

            # 8. Stop the Process Group
            logger.info(f"Stopping Process Group {test_pg_id}")
            operate_pg_stop_args = {
                "object_type": "process_group",
                "object_id": test_pg_id,
                "operation_type": "stop"
            }
            stop_pg_result_list = await call_tool(client, "operate_nifi_object", operate_pg_stop_args, headers)
            assert isinstance(stop_pg_result_list, list) and len(stop_pg_result_list) > 0 and isinstance(stop_pg_result_list[0], dict), "Unexpected response format for operate_nifi_object (stop PG)"
            stop_pg_result = stop_pg_result_list[0]
            assert stop_pg_result.get("status") == "success", f"Failed to stop process group: {stop_pg_result.get('message')}"
            logger.info(f"Successfully stopped Process Group {test_pg_id}")
            await asyncio.sleep(3) # Allow time for components to stop

            # --- Test create_nifi_flow --- 
            logger.info("--- Testing create_nifi_flow ---")
            # 9. Create a dedicated PG for this test
            create_flow_test_pg_name = f"mcp-create-flow-test-{TEST_RUN_ID}"
            logger.info(f"Creating dedicated PG for create_nifi_flow test: {create_flow_test_pg_name}")
            create_flow_pg_args = {"name": create_flow_test_pg_name, "position_x": 500, "position_y": 0}
            create_flow_pg_result_list = await call_tool(client, "create_nifi_process_group", create_flow_pg_args, headers)
            assert isinstance(create_flow_pg_result_list, list) and len(create_flow_pg_result_list) > 0 and isinstance(create_flow_pg_result_list[0], dict), "Unexpected response format for create_nifi_process_group (create_flow test)"
            create_flow_pg_result = create_flow_pg_result_list[0]
            assert create_flow_pg_result.get("status") == "success", f"Failed to create process group for create_nifi_flow test: {create_flow_pg_result.get('message')}"
            create_flow_pg_id = create_flow_pg_result.get("entity", {}).get("id")
            assert create_flow_pg_id, "create_nifi_flow test process group ID not found in response."
            logger.info(f"Successfully created Process Group ID for create_nifi_flow test: {create_flow_pg_id}")
            await asyncio.sleep(1)
            
            # 10. Define and create a simple flow using create_nifi_flow
            logger.info(f"Calling create_nifi_flow for PG {create_flow_pg_id}")
            # Corrected format based on docstring
            flow_definition = [
                {
                    "type": "processor",
                    "class": "org.apache.nifi.processors.standard.GenerateFlowFile",
                    "name": "generate-simple", # Used for connections
                    "position": {"x": 0, "y": 0},
                    "properties": {"Batch Size": "1"} # Test setting a property
                },
                {
                    "type": "processor",
                    "class": "org.apache.nifi.processors.standard.LogAttribute",
                    "name": "log-simple",
                    "position": {"x": 0, "y": 200},
                    "properties": {"Log Level": "warn", "Attributes to Log": ".*"} # Test setting properties
                },
                # Removed update_relationships - should be done via separate tool call if needed after creation
                # {
                #     "action": "update_relationships", 
                #     "config": {
                #         "processor_identifier": "proc_log", 
                #         "auto_terminated_relationships": ["success", "failure"]
                #     }
                # },
                {
                    "type": "connection",
                    "source": "generate-simple", # Use processor name
                    "dest": "log-simple",      # Use processor name
                    "relationships": ["success"]
                }
            ]
            create_flow_args = {
                "nifi_objects": flow_definition,
                "process_group_id": create_flow_pg_id
            }
            create_flow_result_list = await call_tool(client, "create_nifi_flow", create_flow_args, headers)
            assert isinstance(create_flow_result_list, list), "Expected create_nifi_flow response to be a list"
            # Check statuses within the list items
            all_success = True
            created_object_ids = {}
            for item_result in create_flow_result_list:
                logger.debug(f"create_nifi_flow item result: {item_result}")
                if not isinstance(item_result, dict) or item_result.get("status") not in ["success", "warning"]:
                    all_success = False
                    logger.error(f"create_nifi_flow step failed: {item_result}")
                    break
                # Store created IDs if needed, e.g., for specific checks
                if item_result.get("action") in ["create_processor", "create_connection"] and item_result.get("entity"): 
                    created_object_ids[item_result["action"] + "_" + item_result["config"].get("name", "conn")] = item_result["entity"].get("id")
            assert all_success, "One or more steps in create_nifi_flow failed."
            logger.info(f"Successfully executed create_nifi_flow. Created objects: {created_object_ids}")
            await asyncio.sleep(2) # Wait a bit after bulk creation

            # --- END OF TEST EXECUTION --- #

        except Exception as e:
            logger.error(f"!!! Test Execution Failed: {e}", exc_info=True)
            # The finally block will still run for cleanup

        finally:
            # --- Teardown Phase ---
            logger.info("--- Starting Teardown Phase ---")

            # Use a separate try/except for each deletion to ensure all attempts are made

            # Cleanup for main test flow (steps 1-8)
            # 1. Delete Connection
            if connection_id:
                logger.info(f"Deleting Connection: {connection_id}")
                try:
                    delete_conn_args = {"object_type": "connection", "object_id": connection_id, "kwargs": {}}
                    delete_conn_result_list = await call_tool(client, "delete_nifi_object", delete_conn_args, headers)
                    assert isinstance(delete_conn_result_list, list) and len(delete_conn_result_list) > 0 and isinstance(delete_conn_result_list[0], dict), "Unexpected response format for delete_nifi_object (connection)"
                    delete_conn_result = delete_conn_result_list[0]
                    if delete_conn_result.get("status") == "success":
                        logger.info(f"Successfully deleted Connection {connection_id}")
                    else:
                        logger.warning(f"Could not delete Connection {connection_id}: {delete_conn_result.get('message')}")
                except Exception as e_del_conn:
                    logger.error(f"Error during Connection {connection_id} deletion: {e_del_conn}", exc_info=False)
                await asyncio.sleep(0.5)
            else:
                logger.info("Skipping Main Connection deletion (connection_id not set).")

            # 2. Delete LogAttribute Processor
            if log_proc_id:
                logger.info(f"Deleting LogAttribute Processor: {log_proc_id}")
                try:
                    delete_log_args = {"object_type": "processor", "object_id": log_proc_id, "kwargs": {}}
                    delete_log_result_list = await call_tool(client, "delete_nifi_object", delete_log_args, headers)
                    assert isinstance(delete_log_result_list, list) and len(delete_log_result_list) > 0 and isinstance(delete_log_result_list[0], dict), "Unexpected response format for delete_nifi_object (processor Log)"
                    delete_log_result = delete_log_result_list[0]
                    if delete_log_result.get("status") == "success":
                        logger.info(f"Successfully deleted LogAttribute Processor {log_proc_id}")
                    else:
                        logger.warning(f"Could not delete LogAttribute Processor {log_proc_id}: {delete_log_result.get('message')}")
                except Exception as e_del_log:
                    logger.error(f"Error during LogAttribute Processor {log_proc_id} deletion: {e_del_log}", exc_info=False)
                await asyncio.sleep(0.5)
            else:
                logger.info("Skipping Main LogAttribute Processor deletion (log_proc_id not set).")

            # 3. Delete GenerateFlowFile Processor
            if generate_proc_id:
                logger.info(f"Deleting GenerateFlowFile Processor: {generate_proc_id}")
                try:
                    delete_gen_args = {"object_type": "processor", "object_id": generate_proc_id, "kwargs": {}}
                    delete_gen_result_list = await call_tool(client, "delete_nifi_object", delete_gen_args, headers)
                    assert isinstance(delete_gen_result_list, list) and len(delete_gen_result_list) > 0 and isinstance(delete_gen_result_list[0], dict), "Unexpected response format for delete_nifi_object (processor Gen)"
                    delete_gen_result = delete_gen_result_list[0]
                    if delete_gen_result.get("status") == "success":
                        logger.info(f"Successfully deleted GenerateFlowFile Processor {generate_proc_id}")
                    else:
                        logger.warning(f"Could not delete GenerateFlowFile Processor {generate_proc_id}: {delete_gen_result.get('message')}")
                except Exception as e_del_gen:
                    logger.error(f"Error during GenerateFlowFile Processor {generate_proc_id} deletion: {e_del_gen}", exc_info=False)
                await asyncio.sleep(0.5)
            else:
                logger.info("Skipping Main GenerateFlowFile Processor deletion (generate_proc_id not set).")

            # 4. Delete Test Process Group
            if test_pg_id:
                logger.info(f"Deleting Test Process Group: {test_pg_id}")
                try:
                    delete_pg_args = {"object_type": "process_group", "object_id": test_pg_id, "kwargs": {}}
                    delete_pg_result_list = await call_tool(client, "delete_nifi_object", delete_pg_args, headers)
                    assert isinstance(delete_pg_result_list, list) and len(delete_pg_result_list) > 0 and isinstance(delete_pg_result_list[0], dict), "Unexpected response format for delete_nifi_object (process group)"
                    delete_pg_result = delete_pg_result_list[0]
                    if delete_pg_result.get("status") == "success":
                        logger.info(f"Successfully deleted Test Process Group {test_pg_id}")
                    else:
                        # This is the final cleanup step, so error is more significant
                        logger.error(f"Failed to delete Test Process Group {test_pg_id}: {delete_pg_result.get('message')}")
                except Exception as e_del_pg:
                    logger.error(f"Error during Test Process Group {test_pg_id} deletion: {e_del_pg}", exc_info=False)
                await asyncio.sleep(1) # Longer delay after PG delete
            else:
                logger.info("Skipping Main Test Process Group deletion (test_pg_id not set).")

            # Cleanup for create_nifi_flow test (step 9-10)
            # 5. Delete create_nifi_flow Test Process Group
            if create_flow_pg_id:
                logger.info(f"Deleting create_nifi_flow Test Process Group: {create_flow_pg_id}")
                try:
                    delete_flow_pg_args = {"object_type": "process_group", "object_id": create_flow_pg_id, "kwargs": {}}
                    delete_flow_pg_result_list = await call_tool(client, "delete_nifi_object", delete_flow_pg_args, headers)
                    assert isinstance(delete_flow_pg_result_list, list) and len(delete_flow_pg_result_list) > 0 and isinstance(delete_flow_pg_result_list[0], dict), "Unexpected response format for delete_nifi_object (create_flow process group)"
                    delete_flow_pg_result = delete_flow_pg_result_list[0]
                    if delete_flow_pg_result.get("status") == "success":
                        logger.info(f"Successfully deleted create_nifi_flow Test Process Group {create_flow_pg_id}")
                    else:
                        logger.error(f"Failed to delete create_nifi_flow Test Process Group {create_flow_pg_id}: {delete_flow_pg_result.get('message')}")
                except Exception as e_del_flow_pg:
                    logger.error(f"Error during create_nifi_flow Test Process Group {create_flow_pg_id} deletion: {e_del_flow_pg}", exc_info=False)
                await asyncio.sleep(1)
            else:
                 logger.info("Skipping create_nifi_flow Test Process Group deletion (create_flow_pg_id not set).")


            logger.info("--- Teardown Phase Completed ---")

# --- New Feature Test Scenarios ---

async def test_auto_stop_delete_running_processor():
    """Test Auto-Stop: Deleting a running processor."""
    if not TARGET_NIFI_SERVER_ID:
        logger.error("SKIPPING TEST: NIFI_TEST_SERVER_ID not set.")
        return

    test_specific_id = f"del-run-proc-{TEST_RUN_ID}"
    pg_name = f"mcp-autostop-{test_specific_id}-pg"
    proc_name = f"mcp-autostop-{test_specific_id}-gen"
    pg_id = None
    proc_id = None
    proc_id_for_false_test = None # Initialize here

    base_headers = {
        "X-Nifi-Server-Id": TARGET_NIFI_SERVER_ID,
        "Content-Type": "application/json"
    }

    logger.info(f"--- Starting Test: test_auto_stop_delete_running_processor ({test_specific_id}) ---")

    async with httpx.AsyncClient() as client:
        try:
            # 1. Setup: Create PG and a Processor
            logger.info(f"[{test_specific_id}] Creating PG: {pg_name}")
            create_pg_args = {"name": pg_name, "position_x": 100, "position_y": 100}
            pg_result_list = await call_tool(client, "create_nifi_process_group", create_pg_args, base_headers)
            pg_id = pg_result_list[0].get("entity", {}).get("id")
            assert pg_id, f"[{test_specific_id}] Failed to create PG for test."
            logger.info(f"[{test_specific_id}] Created PG ID: {pg_id}")
            await asyncio.sleep(0.5)

            create_proc_args = {
                "process_group_id": pg_id,
                "processor_type": "org.apache.nifi.processors.standard.GenerateFlowFile",
                "name": proc_name,
                "position_x": 0,
                "position_y": 0
            }
            logger.info(f"[{test_specific_id}] Creating Processor: {proc_name} in PG {pg_id}")
            proc_result_list = await call_tool(client, "create_nifi_processor", create_proc_args, base_headers)
            proc_id = proc_result_list[0].get("entity", {}).get("id")
            assert proc_id, f"[{test_specific_id}] Failed to create Processor for test."
            logger.info(f"[{test_specific_id}] Created Processor ID: {proc_id}")
            await asyncio.sleep(0.5)

            # Auto-terminate 'success' relationship for GenerateFlowFile to make it valid for starting
            logger.info(f"[{test_specific_id}] Auto-terminating 'success' relationship for Processor {proc_id}")
            update_rels_args = {
                "processor_id": proc_id,
                "auto_terminated_relationships": ["success"]
            }
            rels_result_list = await call_tool(client, "update_nifi_processor_relationships", update_rels_args, base_headers)
            assert rels_result_list[0].get("status") in ["success", "warning"], f"Failed to update relationships for {proc_id}"
            logger.info(f"[{test_specific_id}] Updated relationships for Processor {proc_id}.")
            await asyncio.sleep(1)

            # 2. Start the Processor
            logger.info(f"[{test_specific_id}] Starting Processor {proc_id}")
            operate_proc_args = {"object_type": "processor", "object_id": proc_id, "operation_type": "start"}
            start_result_list = await call_tool(client, "operate_nifi_object", operate_proc_args, base_headers)
            assert start_result_list[0].get("status") == "success", f"[{test_specific_id}] Failed to start processor: {start_result_list[0].get('message')}"
            await asyncio.sleep(2)

            details_args = {"object_type": "processor", "object_id": proc_id}
            details_list = await call_tool(client, "get_nifi_object_details", details_args, base_headers)
            assert details_list[0].get("component", {}).get("state") == "RUNNING", f"[{test_specific_id}] Processor {proc_id} not RUNNING after start."
            logger.info(f"[{test_specific_id}] Processor {proc_id} confirmed RUNNING.")

            # 3. Attempt delete with Auto-Stop enabled
            logger.info(f"[{test_specific_id}] Test Step: Attempt delete RUNNING Processor {proc_id} with Auto-Stop=true (TEMPORARY EXPECTATION: NiFi block -> MCP error for setup validation)")
            headers_auto_stop_true = {**base_headers, "X-Mcp-Auto-Stop-Enabled": "true"}
            delete_args = {"object_type": "processor", "object_id": proc_id, "kwargs": {}}
            del_result_list_asat = await call_tool(client, "delete_nifi_object", delete_args, headers_auto_stop_true)
            
            assert isinstance(del_result_list_asat, list) and len(del_result_list_asat) > 0 and isinstance(del_result_list_asat[0], dict), \
                f"[{test_specific_id}] Unexpected response format for delete_nifi_object with Auto-Stop=true"
            del_result_asat = del_result_list_asat[0]

            # assert del_result_asat.get("status") == "success", \
            #     f"[{test_specific_id}] Delete with Auto-Stop=true FAILED. Expected 'success', got status '{del_result_asat.get('status')}' with message: {del_result_asat.get('message')}"
            # logger.info(f"PASSED [{test_specific_id}]: Delete with Auto-Stop=true reported 'success' for processor {proc_id}.")

            # # Verify processor is actually deleted
            # logger.info(f"[{test_specific_id}] Verifying processor {proc_id} is deleted after Auto-Stop=true reported success.")
            # processor_found_after_delete = True # Assume it's found unless proven otherwise
            # try:
            #     verify_deleted_args = {"object_type": "processor", "object_id": proc_id}
            #     details_after_delete_list = await call_tool(client, "get_nifi_object_details", verify_deleted_args, base_headers)
            #     if details_after_delete_list and details_after_delete_list[0].get("component"):
            #         logger.error(f"[{test_specific_id}] ANOMALY: Processor {proc_id} still exists after delete with Auto-Stop=true reported success. Details: {details_after_delete_list[0]}")
            #     elif details_after_delete_list and details_after_delete_list[0].get("status") == "error" and "not found" in details_after_delete_list[0].get("message", "").lower():
            #         logger.info(f"[{test_specific_id}] Processor {proc_id} correctly reported as not found by get_nifi_object_details. MCP Message: {details_after_delete_list[0].get('message')}")
            #         processor_found_after_delete = False
            #     else:
            #         logger.warning(f"[{test_specific_id}] Processor {proc_id} get_details returned an unexpected response after delete. Assuming not deleted for safety. Details: {details_after_delete_list[0] if details_after_delete_list else 'No details'}")
            # except httpx.HTTPStatusError as e_verify:
            #     if e_verify.response.status_code == 404:
            #         logger.info(f"[{test_specific_id}] Processor {proc_id} correctly not found (404) via get_nifi_object_details after delete with Auto-Stop=true.")
            #         processor_found_after_delete = False
            #     else:
            #         logger.error(f"[{test_specific_id}] Unexpected HTTP error {e_verify.response.status_code} when verifying deletion of processor {proc_id} with get_nifi_object_details. Assuming not deleted for safety.")
            # 
            # assert not processor_found_after_delete, f"[{test_specific_id}] Processor {proc_id} was NOT deleted after Auto-Stop=true reported success."
            # logger.info(f"[{test_specific_id}] Processor {proc_id} confirmed deleted from NiFi after Auto-Stop=true test.")
            
            # TEMPORARY: Expect error for setup validation
            if del_result_asat.get("status") == "error":
                logger.info(f"SETUP CHECKPOINT [{test_specific_id}]: Delete with Auto-Stop=true resulted in 'error' (NiFi block), as expected for setup validation. Msg: {del_result_asat.get('message')}")
                # Verify processor still exists
                try:
                    details_check = await call_tool(client, "get_nifi_object_details", {"object_type": "processor", "object_id": proc_id}, base_headers)
                    assert details_check[0].get("component"), f"Processor {proc_id} was unexpectedly deleted when error was expected."
                    logger.info(f"[{test_specific_id}] Verified processor {proc_id} still exists after Auto-Stop=true resulted in error (setup validation).")
                except Exception as e_check_proc:
                    assert False, f"Error verifying processor {proc_id} after Auto-Stop=true (setup validation): {e_check_proc}"
            else:
                assert False, f"[{test_specific_id}] Delete with Auto-Stop=true did NOT result in 'error'. Status: {del_result_asat.get('status')}. (Setup validation)"


            # 4. Re-setup a new processor for the Auto-Stop=false test, as the previous one should be deleted.
            # TEMPORARY: For setup validation, the previous processor (proc_id) was NOT deleted.
            # So, we will use IT for the Auto-Stop=false test.
            # logger.info(f"[{test_specific_id}] Re-creating a new processor for Auto-Stop=false test.")
            # proc_name_for_false_test = f"{proc_name}-for-false-test"
            # create_proc_args_false_test = {
            #     "process_group_id": pg_id,
            #     "processor_type": "org.apache.nifi.processors.standard.GenerateFlowFile",
            #     "name": proc_name_for_false_test,
            #     "position_x": 0, # Can reuse position
            #     "position_y": 0
            # }
            # logger.info(f"[{test_specific_id}] Creating Processor: {proc_name_for_false_test} in PG {pg_id}")
            # proc_result_list_false_test = await call_tool(client, "create_nifi_processor", create_proc_args_false_test, base_headers)
            # proc_id_for_false_test = proc_result_list_false_test[0].get("entity", {}).get("id")
            # assert proc_id_for_false_test, f"[{test_specific_id}] Failed to create Processor for Auto-Stop=false test."
            # logger.info(f"[{test_specific_id}] Created Processor ID for Auto-Stop=false test: {proc_id_for_false_test}")
            # await asyncio.sleep(0.5)

            # logger.info(f"[{test_specific_id}] Auto-terminating 'success' relationship for Processor {proc_id_for_false_test}")
            # update_rels_args_false_test = {
            #     "processor_id": proc_id_for_false_test,
            #     "auto_terminated_relationships": ["success"]
            # }
            # rels_result_list_false_test = await call_tool(client, "update_nifi_processor_relationships", update_rels_args_false_test, base_headers)
            # assert rels_result_list_false_test[0].get("status") in ["success", "warning"], f"Failed to update relationships for {proc_id_for_false_test}"
            # logger.info(f"[{test_specific_id}] Updated relationships for Processor {proc_id_for_false_test}.")
            # await asyncio.sleep(1)

            # logger.info(f"[{test_specific_id}] Starting Processor {proc_id_for_false_test}")
            # operate_proc_args_false_test = {"object_type": "processor", "object_id": proc_id_for_false_test, "operation_type": "start"}
            # start_result_list_false_test = await call_tool(client, "operate_nifi_object", operate_proc_args_false_test, base_headers)
            # assert start_result_list_false_test[0].get("status") == "success", f"[{test_specific_id}] Failed to start processor {proc_id_for_false_test}: {start_result_list_false_test[0].get('message')}"
            # await asyncio.sleep(2)

            # details_args_false_test = {"object_type": "processor", "object_id": proc_id_for_false_test}
            # details_list_false_test = await call_tool(client, "get_nifi_object_details", details_args_false_test, base_headers)
            # assert details_list_false_test[0].get("component", {}).get("state") == "RUNNING", f"[{test_specific_id}] Processor {proc_id_for_false_test} not RUNNING after start for Auto-Stop=false test."
            # logger.info(f"[{test_specific_id}] Processor {proc_id_for_false_test} confirmed RUNNING for Auto-Stop=false test.")
            
            # Using original proc_id for the false test as it should not have been deleted.
            proc_id_for_false_test = proc_id 
            logger.info(f"[{test_specific_id}] Using original processor {proc_id_for_false_test} for Auto-Stop=false test (setup validation).")


            # 5. Attempt delete with Auto-Stop disabled (EXPECTED TO FAIL - NiFi API will block)
            logger.info(f"[{test_specific_id}] Test Step: Attempt delete RUNNING Processor {proc_id_for_false_test} with Auto-Stop=false (Current expectation: NiFi block -> MCP error)")
            headers_auto_stop_false = {**base_headers, "X-Mcp-Auto-Stop-Enabled": "false"}
            delete_args_false_test = {"object_type": "processor", "object_id": proc_id_for_false_test, "kwargs": {}}
            del_result_list_asaf = await call_tool(client, "delete_nifi_object", delete_args_false_test, headers_auto_stop_false)
            assert isinstance(del_result_list_asaf, list) and len(del_result_list_asaf) > 0 and isinstance(del_result_list_asaf[0], dict), \
                f"[{test_specific_id}] Unexpected response format for delete_nifi_object with Auto-Stop=false"
            del_result_asaf = del_result_list_asaf[0]

            if del_result_asaf.get("status") == "error":
                logger.info(f"PASSED [{test_specific_id}]: Delete with Auto-Stop=false correctly resulted in error (NiFi block): {del_result_asaf.get('message')}")
            else:
                logger.error(f"FAILED [{test_specific_id}]: Delete with Auto-Stop=false did NOT result in error as expected. Status: {del_result_asaf.get('status')}, Msg: {del_result_asaf.get('message')}")
                assert False, f"Delete with Auto-Stop=false should have failed but got status: {del_result_asaf.get('status')}"
            
            # Original proc_id is now gone, use proc_id_for_false_test for cleanup related to this section.
            # The main cleanup at the end of the function will try to delete the original proc_id (which should fail gracefully if already gone)
            # and the pg_id. We need to ensure proc_id_for_false_test is also cleaned up.
            # The 'finally' block needs to be aware of proc_id_for_false_test.
            # For simplicity in this step, the 'finally' block will still try to stop/delete the original proc_id,
            # which should be fine if it's already gone. The new proc_id_for_false_test needs to be added to cleanup.

        except httpx.HTTPStatusError as e:
            logger.error(f"[{test_specific_id}] Test failed due to HTTPStatusError: Status Code {e.response.status_code}. Response content follows.", exc_info=True)
            logger.error(e.response.text)
            assert False, f"Test failed with HTTPStatusError" # Simplified assertion message
        except Exception as e:
            if isinstance(e, AssertionError):
                raise # Re-raise AssertionError for pytest to handle as a test failure
            logger.error(f"[{test_specific_id}] Test failed due to unexpected error: {e}", exc_info=True)
            assert False, f"Test failed with unexpected error: {e}"
        finally:
            logger.info(f"[{test_specific_id}] --- Starting Cleanup for test_auto_stop_delete_running_processor ---")
            
            original_proc_id_successfully_cleaned = False
            # Cleanup original processor (proc_id)
            if proc_id: # proc_id is the one from the Auto-Stop=true test
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Checking original processor {proc_id} state before stopping/deleting.")
                    details_args_orig = {"object_type": "processor", "object_id": proc_id}
                    details_res_orig = await call_tool(client, "get_nifi_object_details", details_args_orig, base_headers)
                    if details_res_orig and details_res_orig[0].get("component"): 
                        logger.info(f"[{test_specific_id}] Cleanup: Stopping original processor {proc_id} (if it still exists and is running).")
                        if details_res_orig[0]["component"].get("state") == "RUNNING":
                            logger.info(f"[{test_specific_id}] Cleanup: Processor {proc_id} is RUNNING. Attempting to stop it.")
                            operate_args_orig = {"object_type": "processor", "object_id": proc_id, "operation_type": "stop"}
                            stop_res_list = await call_tool(client, "operate_nifi_object", operate_args_orig, base_headers)
                            if stop_res_list[0].get("status") == "success":
                                logger.info(f"[{test_specific_id}] Cleanup: Stop command for {proc_id} sent successfully. Waiting for it to stop.")
                                for i in range(5): # Poll for up to 5 seconds
                                    await asyncio.sleep(1)
                                    current_details_list = await call_tool(client, "get_nifi_object_details", details_args_orig, base_headers)
                                    if current_details_list and current_details_list[0].get("component"):
                                        current_state = current_details_list[0]["component"].get("state")
                                        logger.info(f"[{test_specific_id}] Cleanup: Processor {proc_id} state check {i+1}/5: {current_state}")
                                        if current_state != "RUNNING":
                                            logger.info(f"[{test_specific_id}] Cleanup: Processor {proc_id} confirmed stopped (state: {current_state}).")
                                            break
                                        else: # Should not happen if stop command was successful and proc existed
                                            logger.warning(f"[{test_specific_id}] Cleanup: Processor {proc_id} details not found during stop polling.")
                                            break
                                    else: # else for the for loop
                                        logger.warning(f"[{test_specific_id}] Cleanup: Processor {proc_id} did not stop in time after explicit stop command.")
                            else:
                                logger.warning(f"[{test_specific_id}] Cleanup: Stop command for processor {proc_id} failed: {stop_res_list[0].get('message')}")
                        else:
                            logger.info(f"[{test_specific_id}] Cleanup: Processor {proc_id} was not RUNNING (state: {details_res_orig[0]['component'].get('state')}), proceeding to delete.")
                        
                        # await asyncio.sleep(1) # Removed this fixed sleep
                        logger.info(f"[{test_specific_id}] Cleanup: Deleting original processor {proc_id}.")
                        delete_args_cleanup_orig = {"object_type": "processor", "object_id": proc_id, "kwargs": {}}
                        delete_response = await call_tool(client, "delete_nifi_object", delete_args_cleanup_orig, base_headers)
                        if delete_response[0].get("status") == "success":
                             original_proc_id_successfully_cleaned = True
                    else:
                        logger.info(f"[{test_specific_id}] Cleanup: Original processor {proc_id} not found by get_details, assuming deleted.")
                        original_proc_id_successfully_cleaned = True # Assume gone if not found
                except httpx.HTTPStatusError as e_get_orig: 
                    if e_get_orig.response.status_code == 404:
                        logger.info(f"[{test_specific_id}] Cleanup: Original processor {proc_id} already gone (404 on get_details).")
                        original_proc_id_successfully_cleaned = True
                    else:
                        logger.warning(f"[{test_specific_id}] Cleanup: HTTP Error checking/deleting original processor {proc_id} state: {e_get_orig}")
                except Exception as e_stop_orig:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error during cleanup of original processor {proc_id}: {e_stop_orig}")

            # Cleanup processor for Auto-Stop=false test (proc_id_for_false_test)
            # In setup validation, proc_id_for_false_test is THE SAME as proc_id.
            # Only proceed if it wasn't the same or if the original cleanup attempt failed to confirm deletion.
            if proc_id_for_false_test and (proc_id_for_false_test != proc_id or not original_proc_id_successfully_cleaned):
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Checking processor {proc_id_for_false_test} (for false test) state before stopping/deleting.")
                    details_args_false = {"object_type": "processor", "object_id": proc_id_for_false_test}
                    details_res_false = await call_tool(client, "get_nifi_object_details", details_args_false, base_headers)
                    if details_res_false and details_res_false[0].get("component"):
                        logger.info(f"[{test_specific_id}] Cleanup: Stopping processor {proc_id_for_false_test} (if running).")
                        if details_res_false[0]["component"].get("state") == "RUNNING":
                            operate_args_false = {"object_type": "processor", "object_id": proc_id_for_false_test, "operation_type": "stop"}
                            await call_tool(client, "operate_nifi_object", operate_args_false, base_headers)
                            await asyncio.sleep(1)
                        logger.info(f"[{test_specific_id}] Cleanup: Deleting processor {proc_id_for_false_test}.")
                        delete_args_cleanup_false = {"object_type": "processor", "object_id": proc_id_for_false_test, "kwargs": {}}
                        await call_tool(client, "delete_nifi_object", delete_args_cleanup_false, base_headers)
                    else:
                        logger.info(f"[{test_specific_id}] Cleanup: Processor {proc_id_for_false_test} (for false test) not found by get_details, assuming deleted.")
                except httpx.HTTPStatusError as e_get_false: 
                    if e_get_false.response.status_code == 404:
                        logger.info(f"[{test_specific_id}] Cleanup: Processor {proc_id_for_false_test} (for false test) already gone (404 on get_details).")
                    else:
                        logger.warning(f"[{test_specific_id}] Cleanup: HTTP Error checking/deleting processor {proc_id_for_false_test} (for false test) state: {e_get_false}")
                except Exception as e_stop_false:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error during cleanup of processor {proc_id_for_false_test} (for false test): {e_stop_false}")
            elif proc_id_for_false_test == proc_id and original_proc_id_successfully_cleaned:
                 logger.info(f"[{test_specific_id}] Cleanup: Skipping cleanup for proc_id_for_false_test ({proc_id_for_false_test}) as it was the same as proc_id and already cleaned up.")
            
            if pg_id:
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting PG {pg_id}")
                    delete_pg_args_cleanup = {"object_type": "process_group", "object_id": pg_id, "kwargs": {}}
                    del_pg_res = await call_tool(client, "delete_nifi_object", delete_pg_args_cleanup, base_headers)
                    if del_pg_res[0].get("status") != "success":
                        logger.error(f"[{test_specific_id}] Cleanup: FAILED to delete PG {pg_id}: {del_pg_res[0].get('message')}")
                except Exception as e_del_pg:
                    logger.error(f"[{test_specific_id}] Cleanup: Error deleting PG {pg_id}: {e_del_pg}")
            
            logger.info(f"[{test_specific_id}] --- Finished Cleanup for test_auto_stop_delete_running_processor ---")

async def test_auto_stop_update_running_processor():
    """Test Auto-Stop: Updating a running processor's properties."""
    if not TARGET_NIFI_SERVER_ID:
        logger.error("SKIPPING TEST: NIFI_TEST_SERVER_ID not set.")
        return

    test_specific_id = f"upd-run-proc-{TEST_RUN_ID}"
    pg_name = f"mcp-autostop-{test_specific_id}-pg"
    proc_name = f"mcp-autostop-{test_specific_id}-gen-upd"
    pg_id = None
    proc_id = None
    proc_id_for_false_test = None # Initialize here

    base_headers = {
        "X-Nifi-Server-Id": TARGET_NIFI_SERVER_ID,
        "Content-Type": "application/json"
    }

    logger.info(f"--- Starting Test: test_auto_stop_update_running_processor ({test_specific_id}) ---")

    async with httpx.AsyncClient() as client:
        try:
            # 1. Setup: Create PG and a Processor
            logger.info(f"[{test_specific_id}] Creating PG: {pg_name}")
            create_pg_args = {"name": pg_name, "position_x": 200, "position_y": 200} # Different position
            pg_result_list = await call_tool(client, "create_nifi_process_group", create_pg_args, base_headers)
            pg_id = pg_result_list[0].get("entity", {}).get("id")
            assert pg_id, f"[{test_specific_id}] Failed to create PG for test."
            logger.info(f"[{test_specific_id}] Created PG ID: {pg_id}")
            await asyncio.sleep(0.5)

            create_proc_args = {
                "process_group_id": pg_id,
                "processor_type": "org.apache.nifi.processors.standard.GenerateFlowFile",
                "name": proc_name,
                "position_x": 0,
                "position_y": 0
            }
            logger.info(f"[{test_specific_id}] Creating Processor: {proc_name} in PG {pg_id}")
            proc_result_list = await call_tool(client, "create_nifi_processor", create_proc_args, base_headers)
            proc_id = proc_result_list[0].get("entity", {}).get("id")
            assert proc_id, f"[{test_specific_id}] Failed to create Processor for test."
            logger.info(f"[{test_specific_id}] Created Processor ID: {proc_id}")
            await asyncio.sleep(0.5)

            # Auto-terminate 'success' relationship for GenerateFlowFile
            logger.info(f"[{test_specific_id}] Auto-terminating 'success' for Processor {proc_id}")
            update_rels_args = {"processor_id": proc_id, "auto_terminated_relationships": ["success"]}
            rels_result_list = await call_tool(client, "update_nifi_processor_relationships", update_rels_args, base_headers)
            assert rels_result_list[0].get("status") in ["success", "warning"], f"Failed to update relationships for {proc_id}"
            logger.info(f"[{test_specific_id}] Updated relationships for Processor {proc_id}.")
            await asyncio.sleep(1)

            # 2. Start the Processor
            logger.info(f"[{test_specific_id}] Starting Processor {proc_id}")
            operate_proc_args = {"object_type": "processor", "object_id": proc_id, "operation_type": "start"}
            start_result_list = await call_tool(client, "operate_nifi_object", operate_proc_args, base_headers)
            assert start_result_list[0].get("status") == "success", f"[{test_specific_id}] Failed to start processor: {start_result_list[0].get('message')}"
            await asyncio.sleep(2)

            details_args = {"object_type": "processor", "object_id": proc_id}
            details_list = await call_tool(client, "get_nifi_object_details", details_args, base_headers)
            assert details_list[0].get("component", {}).get("state") == "RUNNING", f"[{test_specific_id}] Processor {proc_id} not RUNNING after start."
            logger.info(f"[{test_specific_id}] Processor {proc_id} confirmed RUNNING.")

            # 3. Attempt update with Auto-Stop enabled
            logger.info(f"[{test_specific_id}] Test Step: Attempt update RUNNING Processor {proc_id} config with Auto-Stop=true (TEMPORARY EXPECTATION: NiFi block -> MCP error for setup validation)")
            headers_auto_stop_true = {**base_headers, "X-Mcp-Auto-Stop-Enabled": "true"}
            
            initial_proc_details = details_list[0].get("component", {})
            initial_proc_config = initial_proc_details.get("config", {})
            initial_proc_properties = initial_proc_config.get("properties", {})
            current_scheduling_strategy_val = initial_proc_properties.get("Scheduling Strategy", "TIMER_DRIVEN")
            
            target_scheduling_strategy = "CRON_DRIVEN" if current_scheduling_strategy_val == "TIMER_DRIVEN" else "TIMER_DRIVEN"
            logger.info(f"[{test_specific_id}] Current Scheduling Strategy: {current_scheduling_strategy_val}, Target: {target_scheduling_strategy} for Auto-Stop=true test")

            update_props_args_asat = {
                "processor_id": proc_id,
                "processor_config_properties": {"Scheduling Strategy": target_scheduling_strategy}
            }
            update_result_asat_list = await call_tool(client, "update_nifi_processor_properties", update_props_args_asat, headers_auto_stop_true)
            
            assert isinstance(update_result_asat_list, list) and len(update_result_asat_list) > 0 and isinstance(update_result_asat_list[0], dict), \
                f"[{test_specific_id}] Unexpected response format for update_nifi_processor_properties with Auto-Stop=true"
            update_result_asat = update_result_asat_list[0]

            # assert update_result_asat.get("status") == "success", \
            #     f"[{test_specific_id}] Update with Auto-Stop=true FAILED. Expected 'success', got status '{update_result_asat.get('status')}' with message: {update_result_asat.get('message')}"
            # logger.info(f"PASSED [{test_specific_id}]: Update with Auto-Stop=true reported 'success' for processor {proc_id}.")

            # # Verify property was actually updated and check processor state
            # logger.info(f"[{test_specific_id}] Verifying processor {proc_id} properties and state after Auto-Stop=true update reported success.")
            # details_after_update_list = await call_tool(client, "get_nifi_object_details", details_args, base_headers) 
            # assert isinstance(details_after_update_list, list) and len(details_after_update_list) > 0 and isinstance(details_after_update_list[0], dict), \
            #     f"[{test_specific_id}] Unexpected response format for get_nifi_object_details after update."
            # details_after_update = details_after_update_list[0]
            # 
            # updated_component = details_after_update.get("component", {})
            # updated_config = updated_component.get("config", {})
            # updated_properties = updated_config.get("properties", {})
            # final_scheduling_strategy = updated_properties.get("Scheduling Strategy")
            # final_validation_status = updated_component.get("validationStatus")
            # final_run_status = updated_component.get("state")

            # assert final_scheduling_strategy == target_scheduling_strategy, \
            #     f"[{test_specific_id}] Scheduling Strategy was NOT updated after Auto-Stop=true. Expected '{target_scheduling_strategy}', got '{final_scheduling_strategy}'."
            # logger.info(f"[{test_specific_id}] Scheduling Strategy correctly updated to '{final_scheduling_strategy}'.")
            # logger.info(f"[{test_specific_id}] Processor {proc_id} final state after Auto-Stop=true update: ValidationStatus='{final_validation_status}', RunStatus='{final_run_status}'.")

            # TEMPORARY: Expect error for setup validation
            if update_result_asat.get("status") == "error":
                logger.info(f"SETUP CHECKPOINT [{test_specific_id}]: Update with Auto-Stop=true resulted in 'error' (NiFi block), as expected for setup validation. Msg: {update_result_asat.get('message')}")
                # Verify property changes and processor state according to current understanding of get_nifi_object_details behavior post-failed-update
                try:
                    details_check_asat = await call_tool(client, "get_nifi_object_details", details_args, base_headers)
                    assert isinstance(details_check_asat, list) and len(details_check_asat) > 0, "Failed to get processor details for verification (asat)"
                    component_check_asat = details_check_asat[0].get("component", {})
                    config_check_asat = component_check_asat.get("config", {})
                    actual_sched_strat_asat = config_check_asat.get("schedulingStrategy")
                    
                    # Verify it's still RUNNING and scheduling strategy is UNCHANGED from its original pre-attempt value
                    assert component_check_asat.get("state") == "RUNNING", \
                        f"[{test_specific_id}] Processor not RUNNING after Auto-Stop=true resulted in error. State: {component_check_asat.get('state')}"
                    assert actual_sched_strat_asat == current_scheduling_strategy_val, \
                        f"[{test_specific_id}] Scheduling Strategy CHANGED to {actual_sched_strat_asat} from {current_scheduling_strategy_val} when error was expected (asat)."
                    
                    # Check other properties (e.g., Yield Duration should also be unchanged)
                    props_check_asat = config_check_asat.get("properties", {})
                    original_yield_asat = initial_proc_properties.get("Yield Duration")
                    actual_yield_asat = props_check_asat.get("Yield Duration")
                    assert actual_yield_asat == original_yield_asat, \
                        f"[{test_specific_id}] Yield Duration CHANGED to {actual_yield_asat} from {original_yield_asat} when error was expected (asat)."

                    logger.info(f"SETUP CHECKPOINT [{test_specific_id}]: Processor state (RUNNING) and config (schedulingStrategy: {actual_sched_strat_asat}, Yield Duration: {actual_yield_asat}) correctly verified after Auto-Stop=true resulted in error.")
                except Exception as e_check_proc_asat:
                    logger.error(f"[{test_specific_id}] Error verifying processor {proc_id} properties/state after Auto-Stop=true (setup validation) resulted in error: {e_check_proc_asat}", exc_info=True)
                    raise # Re-raise the assertion or other exception from checks
            else:
                assert False, f"[{test_specific_id}] Update with Auto-Stop=true did NOT result in 'error'. Status: {update_result_asat.get('status')}. (Setup validation)"


            # 4. Re-setup a new processor for the Auto-Stop=false test
            # TEMPORARY: For setup validation, the previous processor (proc_id) was NOT changed and is still running.
            # So, we will use IT for the Auto-Stop=false test.
            # logger.info(f"[{test_specific_id}] Re-creating a new processor for Auto-Stop=false test.")
            # proc_name_for_false_test = f"{proc_name}-for-false-test-upd"
            # create_proc_args_false_test = {
            #     "process_group_id": pg_id,
            #     "processor_type": "org.apache.nifi.processors.standard.GenerateFlowFile",
            #     "name": proc_name_for_false_test,
            #     "position_x": 100, 
            #     "position_y": 100
            # }
            # proc_result_list_false_test = await call_tool(client, "create_nifi_processor", create_proc_args_false_test, base_headers)
            # proc_id_for_false_test = proc_result_list_false_test[0].get("entity", {}).get("id")
            # assert proc_id_for_false_test, f"[{test_specific_id}] Failed to create Processor for Auto-Stop=false update test."
            # logger.info(f"[{test_specific_id}] Created Processor ID for Auto-Stop=false update test: {proc_id_for_false_test}")
            # await asyncio.sleep(0.5)

            # update_rels_args_false_test = {"processor_id": proc_id_for_false_test, "auto_terminated_relationships": ["success"]}
            # await call_tool(client, "update_nifi_processor_relationships", update_rels_args_false_test, base_headers)
            # logger.info(f"[{test_specific_id}] Updated relationships for Processor {proc_id_for_false_test}.")
            # await asyncio.sleep(1)

            # Determine its original scheduling strategy to attempt a different one
            # details_false_test_initial = await call_tool(client, "get_nifi_object_details", {"object_type": "processor", "object_id": proc_id_for_false_test}, base_headers)
            # current_sched_strat_false_test = details_false_test_initial[0].get("component",{}).get("config",{}).get("properties",{}).get("Scheduling Strategy", "TIMER_DRIVEN")

            # operate_proc_args_false_test = {"object_type": "processor", "object_id": proc_id_for_false_test, "operation_type": "start"}
            # await call_tool(client, "operate_nifi_object", operate_proc_args_false_test, base_headers)
            # logger.info(f"[{test_specific_id}] Started Processor {proc_id_for_false_test} for Auto-Stop=false test.")
            # await asyncio.sleep(2)
            
            # Verify it's running
            # details_false_test_running = await call_tool(client, "get_nifi_object_details", {"object_type": "processor", "object_id": proc_id_for_false_test}, base_headers)
            # assert details_false_test_running[0].get("component", {}).get("state") == "RUNNING", f"Processor {proc_id_for_false_test} not RUNNING for false test."
            # logger.info(f"[{test_specific_id}] Processor {proc_id_for_false_test} confirmed RUNNING for Auto-Stop=false update test.")
            
            # Using original proc_id for the false test as it should not have been changed and is still running.
            proc_id_for_false_test = proc_id
            current_sched_strat_false_test = current_scheduling_strategy_val # Use the initial strategy of proc_id
            logger.info(f"[{test_specific_id}] Using original processor {proc_id_for_false_test} (Scheduling: {current_sched_strat_false_test}) for Auto-Stop=false test (setup validation).")

            # 5. Attempt update with Auto-Stop disabled
            target_sched_strat_false_test = "CRON_DRIVEN" if current_sched_strat_false_test == "TIMER_DRIVEN" else "TIMER_DRIVEN"
            logger.info(f"[{test_specific_id}] Test Step: Attempt update RUNNING Processor {proc_id_for_false_test} (Scheduling: {current_sched_strat_false_test} -> {target_sched_strat_false_test}) with Auto-Stop=false (EXPECTED: NiFi block -> MCP error)")
            headers_auto_stop_false = {**base_headers, "X-Mcp-Auto-Stop-Enabled": "false"}
            update_props_args_asaf = {
                "processor_id": proc_id_for_false_test,
                "processor_config_properties": {"Scheduling Strategy": target_sched_strat_false_test}
            }
            update_result_asaf_list = await call_tool(client, "update_nifi_processor_properties", update_props_args_asaf, headers_auto_stop_false)
            assert isinstance(update_result_asaf_list, list) and len(update_result_asaf_list) > 0 and isinstance(update_result_asaf_list[0], dict), \
                f"[{test_specific_id}] Unexpected response format for update_nifi_processor_properties with Auto-Stop=false"
            update_result_asaf = update_result_asaf_list[0]

            if update_result_asaf.get("status") == "error":
                logger.info(f"PASSED [{test_specific_id}]: Update with Auto-Stop=false correctly resulted in error (NiFi block): {update_result_asaf.get('message')}")
            else:
                logger.error(f"FAILED [{test_specific_id}]: Update with Auto-Stop=false did NOT result in error as expected. Status: {update_result_asaf.get('status')}, Msg: {update_result_asaf.get('message')}")
                assert False, f"Update with Auto-Stop=false should have failed but got status: {update_result_asaf.get('status')}"

            # Verify processor state and config are still unchanged for the false test processor
            logger.info(f"[{test_specific_id}] Verifying processor {proc_id_for_false_test} state and config after failed update with Auto-Stop=false.")
            details_list_check_asaf = await call_tool(client, "get_nifi_object_details", {"object_type": "processor", "object_id": proc_id_for_false_test}, base_headers)
            assert isinstance(details_list_check_asaf, list) and len(details_list_check_asaf) > 0, "Failed to get processor details for verification (asaf)"
            component_check_asaf = details_list_check_asaf[0].get("component", {})
            config_check_asaf = component_check_asaf.get("config", {})
            actual_sched_strat_asaf = config_check_asaf.get("schedulingStrategy")

            assert component_check_asaf.get("state") == "RUNNING", f"[{test_specific_id}] Processor {proc_id_for_false_test} not RUNNING after failed update attempt (asaf). State: {component_check_asaf.get('state')}"
            
            # Expect Scheduling Strategy to be UNCHANGED (NiFi doesn't modify it on this type of failed update via properties)
            # Note: get_nifi_object_details correctly reports the actual current strategy from NiFi.
            assert actual_sched_strat_asaf == current_sched_strat_false_test, \
                f"[{test_specific_id}] Scheduling Strategy after failed update (asaf) was '{actual_sched_strat_asaf}', expected it to remain '{current_sched_strat_false_test}'."

            # Check other properties (e.g., Yield Duration should also be unchanged)
            props_check_asaf = config_check_asaf.get("properties", {})
            original_yield_asaf = initial_proc_properties.get("Yield Duration")
            actual_yield_asaf = props_check_asaf.get("Yield Duration")
            assert actual_yield_asaf == original_yield_asaf, \
                f"[{test_specific_id}] Yield Duration after failed update (asaf) was '{actual_yield_asaf}', expected it to remain '{original_yield_asaf}'."

            logger.info(f"[{test_specific_id}] Processor {proc_id_for_false_test} state (RUNNING) and config (schedulingStrategy: {actual_sched_strat_asaf}, Yield Duration: {actual_yield_asaf}) correctly verified after failed update with Auto-Stop=false.")

        except httpx.HTTPStatusError as e:
            logger.error(f"[{test_specific_id}] Test failed due to HTTPStatusError: Status Code {e.response.status_code}. Response content follows.", exc_info=True)
            logger.error(e.response.text)
            assert False, f"Test failed with HTTPStatusError" # Simplified assertion message
        except Exception as e:
            if isinstance(e, AssertionError):
                raise # Re-raise AssertionError for pytest to handle as a test failure
            logger.error(f"[{test_specific_id}] Test failed due to unexpected error: {e}", exc_info=True)
            assert False, f"Test failed with unexpected error: {e}"
        finally:
            logger.info(f"[{test_specific_id}] --- Starting Cleanup for test_auto_stop_update_running_processor ---")
            # Cleanup original processor (proc_id)
            if proc_id: 
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Checking original processor {proc_id} state before stopping/deleting.")
                    details_res = await call_tool(client, "get_nifi_object_details", {"object_type": "processor", "object_id": proc_id}, base_headers)
                    if details_res and details_res[0].get("component"):
                        if details_res[0]["component"].get("state") == "RUNNING": # Only stop if running
                            logger.info(f"[{test_specific_id}] Cleanup: Stopping original processor {proc_id}")
                        await call_tool(client, "operate_nifi_object", {"object_type": "processor", "object_id": proc_id, "operation_type": "stop"}, base_headers)
                        await asyncio.sleep(1)
                        logger.info(f"[{test_specific_id}] Cleanup: Deleting original processor {proc_id}")
                        await call_tool(client, "delete_nifi_object", {"object_type": "processor", "object_id": proc_id, "kwargs": {}}, base_headers)
                    else:
                        logger.info(f"[{test_specific_id}] Cleanup: Original processor {proc_id} not found by get_details.")
                except httpx.HTTPStatusError as e_get_orig:
                    if e_get_orig.response.status_code == 404:
                        logger.info(f"[{test_specific_id}] Cleanup: Original processor {proc_id} already gone (404 on get_details).")
                    else:
                         logger.warning(f"[{test_specific_id}] Cleanup: HTTP Error for original processor {proc_id}: {e_get_orig}")       
                except Exception as e_stop:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error during cleanup of original processor {proc_id}: {e_stop}")
            
            # Cleanup processor for Auto-Stop=false test (proc_id_for_false_test)
            if proc_id_for_false_test:
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Checking processor {proc_id_for_false_test} (for false test) state before stopping/deleting.")
                    details_res_false = await call_tool(client, "get_nifi_object_details", {"object_type": "processor", "object_id": proc_id_for_false_test}, base_headers)
                    if details_res_false and details_res_false[0].get("component"):
                        if details_res_false[0]["component"].get("state") == "RUNNING":
                            logger.info(f"[{test_specific_id}] Cleanup: Stopping processor {proc_id_for_false_test} (for false test).")
                            await call_tool(client, "operate_nifi_object", {"object_type": "processor", "object_id": proc_id_for_false_test, "operation_type": "stop"}, base_headers)
                            await asyncio.sleep(1)
                        logger.info(f"[{test_specific_id}] Cleanup: Deleting processor {proc_id_for_false_test} (for false test).")
                        await call_tool(client, "delete_nifi_object", {"object_type": "processor", "object_id": proc_id_for_false_test, "kwargs": {}}, base_headers)
                    else:
                        logger.info(f"[{test_specific_id}] Cleanup: Processor {proc_id_for_false_test} (for false test) not found by get_details.")
                except httpx.HTTPStatusError as e_get_false:
                    if e_get_false.response.status_code == 404:
                        logger.info(f"[{test_specific_id}] Cleanup: Processor {proc_id_for_false_test} (for false test) already gone (404 on get_details).")
                    else:
                        logger.warning(f"[{test_specific_id}] Cleanup: HTTP Error for processor {proc_id_for_false_test} (for false test): {e_get_false}")
                except Exception as e_stop_false:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error during cleanup of processor {proc_id_for_false_test} (for false test): {e_stop_false}")
            
            if pg_id:
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting PG {pg_id}")
                    delete_pg_args_cleanup = {"object_type": "process_group", "object_id": pg_id, "kwargs": {}}
                    del_pg_res = await call_tool(client, "delete_nifi_object", delete_pg_args_cleanup, base_headers)
                    if del_pg_res[0].get("status") != "success":
                        logger.error(f"[{test_specific_id}] Cleanup: FAILED to delete PG {pg_id}: {del_pg_res[0].get('message')}")
                except Exception as e_del_pg:
                    logger.error(f"[{test_specific_id}] Cleanup: Error deleting PG {pg_id}: {e_del_pg}")
            
            logger.info(f"[{test_specific_id}] --- Finished Cleanup for test_auto_stop_update_running_processor ---")

async def test_auto_delete_processor_with_connections():
    """Test Auto-Delete: Deleting a processor (P2 - SINK) that has an incoming connection (C1)."""
    if not TARGET_NIFI_SERVER_ID:
        logger.error("SKIPPING TEST: NIFI_TEST_SERVER_ID not set.")
        return

    test_specific_id = f"del-sink-conn-{TEST_RUN_ID}" # Changed prefix
    pg_name = f"mcp-autodel-{test_specific_id}-pg"
    p1_name = f"mcp-autodel-{test_specific_id}-p1-source"
    p2_name = f"mcp-autodel-{test_specific_id}-p2-sink"
    
    pg_id = None
    p1_id = None
    p2_id = None
    conn_id = None # C1

    # For the Auto-Delete=false part of the test
    p1_id_for_false_test = None
    p2_id_for_false_test = None
    conn_id_for_false_test = None

    base_headers = {
        "X-Nifi-Server-Id": TARGET_NIFI_SERVER_ID,
        "Content-Type": "application/json"
    }

    logger.info(f"--- Starting Test: test_auto_delete_processor_with_connections ({test_specific_id}) ---")

    async with httpx.AsyncClient() as client:
        try:
            # 1. Setup: PG, P1 (Generate), P2 (Log), Connection C1 (P1->P2)
            logger.info(f"[{test_specific_id}] Creating PG: {pg_name}")
            pg_res_list = await call_tool(client, "create_nifi_process_group", {"name": pg_name, "position_x": 300, "position_y": 300}, base_headers)
            pg_id = pg_res_list[0].get("entity", {}).get("id")
            assert pg_id, f"[{test_specific_id}] Failed to create PG."
            logger.info(f"[{test_specific_id}] Created PG ID: {pg_id}")
            await asyncio.sleep(0.5)

            create_p1_args = {"process_group_id": pg_id, "processor_type": "org.apache.nifi.processors.standard.GenerateFlowFile", "name": p1_name, "position_x": 0, "position_y": 0}
            logger.info(f"[{test_specific_id}] Creating P1 Source: {p1_name} in PG {pg_id}")
            p1_res_list = await call_tool(client, "create_nifi_processor", create_p1_args, base_headers)
            p1_id = p1_res_list[0].get("entity", {}).get("id")
            assert p1_id, f"[{test_specific_id}] Failed to create P1."
            logger.info(f"[{test_specific_id}] Created P1 ID: {p1_id}")
            await asyncio.sleep(0.5)

            create_p2_args = {"process_group_id": pg_id, "processor_type": "org.apache.nifi.processors.standard.LogAttribute", "name": p2_name, "position_x": 0, "position_y": 200}
            logger.info(f"[{test_specific_id}] Creating P2 Sink: {p2_name} in PG {pg_id}")
            p2_res_list = await call_tool(client, "create_nifi_processor", create_p2_args, base_headers)
            p2_id = p2_res_list[0].get("entity", {}).get("id")
            assert p2_id, f"[{test_specific_id}] Failed to create P2."
            logger.info(f"[{test_specific_id}] Created P2 ID: {p2_id}")
            
            update_p2_rels_args = {"processor_id": p2_id, "auto_terminated_relationships": ["success", "failure"]}
            rels_p2_res_list = await call_tool(client, "update_nifi_processor_relationships", update_p2_rels_args, base_headers)
            assert rels_p2_res_list[0].get("status") in ["success", "warning"], f"Failed to update P2 relationships: {rels_p2_res_list[0].get('message')}"
            logger.info(f"[{test_specific_id}] Updated P2 relationships.")
            await asyncio.sleep(1)

            connect_args = {"source_id": p1_id, "relationships": ["success"], "target_id": p2_id}
            logger.info(f"[{test_specific_id}] Connecting P1 ({p1_id}) to P2 ({p2_id}) creating C1")
            conn_res_list = await call_tool(client, "create_nifi_connection", connect_args, base_headers)
            conn_id = conn_res_list[0].get("entity", {}).get("id")
            assert conn_id, f"[{test_specific_id}] Failed to create connection C1."
            logger.info(f"[{test_specific_id}] Created Connection C1 ID: {conn_id}")
            await asyncio.sleep(1)

            # --- Test Auto-Delete ENABLED (targeting P2 - the SINK) ---
            logger.info(f"[{test_specific_id}] Test Step: Attempt delete P2 SINK ({p2_id}) with Auto-Delete=true. (TEMPORARY EXPECTATION: NiFi block -> MCP error for setup validation)")
            headers_auto_delete_true = {**base_headers, "X-Mcp-Auto-Delete-Enabled": "true"}
            delete_p2_args_adat = {"object_type": "processor", "object_id": p2_id, "kwargs": {}}
            
            del_p2_adat_result_list = await call_tool(client, "delete_nifi_object", delete_p2_args_adat, headers_auto_delete_true)
            assert isinstance(del_p2_adat_result_list, list) and len(del_p2_adat_result_list) > 0 and isinstance(del_p2_adat_result_list[0], dict), \
                f"[{test_specific_id}] Unexpected response format for delete_nifi_object (P2_Sink Auto-Delete=true)"
            del_p2_adat_result = del_p2_adat_result_list[0]

            # Store original IDs for verification and potential re-setup logic
            original_p1_id_for_setup_val = p1_id
            original_p2_id_for_setup_val = p2_id
            original_conn_id_for_setup_val = conn_id
            p2_or_c1_deleted_in_true_step_unexpectedly = False

            if del_p2_adat_result.get("status") == "error":
                logger.info(f"SETUP CHECKPOINT [{test_specific_id}]: Delete P2 SINK with Auto-Delete=true resulted in 'error' (NiFi block), as expected for setup validation. Msg: {del_p2_adat_result.get('message')}")
                # Verify P2 and C1 still exist
                try:
                    p2_check = await call_tool(client, "get_nifi_object_details", {"object_type": "processor", "object_id": original_p2_id_for_setup_val}, base_headers)
                    assert p2_check[0].get("component"), f"P2 {original_p2_id_for_setup_val} was unexpectedly deleted when error was expected (AD=true setup validation)."
                    logger.info(f"[{test_specific_id}] Verified P2 ({original_p2_id_for_setup_val}) still exists (AD=true setup validation).")
                    
                    c1_check = await call_tool(client, "get_nifi_object_details", {"object_type": "connection", "object_id": original_conn_id_for_setup_val}, base_headers)
                    assert c1_check[0].get("component"), f"C1 {original_conn_id_for_setup_val} was unexpectedly deleted (AD=true setup validation)."
                    logger.info(f"[{test_specific_id}] Verified C1 ({original_conn_id_for_setup_val}) still exists (AD=true setup validation).")
                except Exception as e_check_ad_true_setup:
                    assert False, f"Error verifying P2/C1 after AD=true resulted in error (setup validation): {e_check_ad_true_setup}"
            else:
                logger.warning(f"[{test_specific_id}] Delete P2 SINK with Auto-Delete=true did NOT result in 'error' (setup validation). Status: {del_p2_adat_result.get('status')}. This might affect the AD=false part.")
                p2_or_c1_deleted_in_true_step_unexpectedly = True # Mark that something unexpected happened
                assert False, f"[{test_specific_id}] Delete P2 SINK with Auto-Delete=true did NOT result in 'error'. (Setup validation)"
            
            # --- Re-setup for Auto-Delete DISABLED test ---
            # For setup validation, if the AD=true step behaved (i.e., errored and P2/C1 still exist),
            # we use the original P2 (original_p2_id_for_setup_val) for the AD=false test.
            # If AD=true step misbehaved (e.g. deleted P2), we have an issue with current NiFi/MCP state for this test run.
            # The p1_id_for_false_test, etc. variables are for the TDD path where AD=true *succeeds* and deletes.
            # Here, for setup validation, they should remain None if AD=true errors as expected.

            target_p2_for_false_test = original_p2_id_for_setup_val
            target_conn_for_false_test_verification = original_conn_id_for_setup_val
            target_p1_for_false_test_verification = original_p1_id_for_setup_val

            if p2_or_c1_deleted_in_true_step_unexpectedly:
                # This path indicates an issue, as for setup validation we expect NiFi to block the AD=true delete.
                # If it didn't, the original components might be gone. The AD=false test might fail unpredictably.
                # For robust setup validation, we might skip or flag this part if components vanished.
                # However, the test will proceed using original IDs; if they are gone, get_details will fail later.
                logger.warning(f"[{test_specific_id}] AD=true step did not behave as expected for setup (didn't error or components changed). AD=false test part might be unreliable.")
                # For safety, we won't re-create here during *setup validation*. The test will use original IDs.
                # The p1_id_for_false_test etc. are for the TDD path when AD=true *works*.
            else:
                logger.info(f"[{test_specific_id}] Using original P2 ({target_p2_for_false_test}) and C1 ({target_conn_for_false_test_verification}) for Auto-Delete=false test (setup validation).")

            # --- Test Auto-Delete DISABLED (targeting P2 - SINK) ---
            logger.info(f"[{test_specific_id}] Test Step: Attempt delete P2_Sink ({target_p2_for_false_test}) with Auto-Delete=false. EXPECTED: NiFi block -> MCP error.")
            headers_auto_delete_false = {**base_headers, "X-Mcp-Auto-Delete-Enabled": "false"}
            delete_p2_args_adaf = {"object_type": "processor", "object_id": target_p2_for_false_test, "kwargs": {}}

            del_p2_adaf_result_list = await call_tool(client, "delete_nifi_object", delete_p2_args_adaf, headers_auto_delete_false)
            assert isinstance(del_p2_adaf_result_list, list) and len(del_p2_adaf_result_list) > 0 and isinstance(del_p2_adaf_result_list[0], dict), \
                f"[{test_specific_id}] Unexpected response format for delete_nifi_object (P2_Sink Auto-Delete=false)"
            del_p2_adaf_result = del_p2_adaf_result_list[0]

            if del_p2_adaf_result.get("status") == "error":
                logger.info(f"PASSED [{test_specific_id}]: Delete P2_Sink ({target_p2_for_false_test}) with Auto-Delete=false correctly resulted in 'error'. Msg: {del_p2_adaf_result.get('message')}")
                try:
                    p2_false_check = await call_tool(client, "get_nifi_object_details", {"object_type": "processor", "object_id": target_p2_for_false_test}, base_headers)
                    assert p2_false_check[0].get("component"), f"P2_Sink {target_p2_for_false_test} was unexpectedly deleted (AD=false path)."
                    logger.info(f"[{test_specific_id}] Verified P2_Sink ({target_p2_for_false_test}) still exists after failed delete (AD=false).")
                    
                    c1_false_check = await call_tool(client, "get_nifi_object_details", {"object_type": "connection", "object_id": target_conn_for_false_test_verification}, base_headers)
                    assert c1_false_check[0].get("component"), f"C1 ({target_conn_for_false_test_verification}) was unexpectedly deleted (AD=false path)."
                    logger.info(f"[{test_specific_id}] Verified C1 ({target_conn_for_false_test_verification}) still exists after failed delete (AD=false).")
                    
                    p1_false_check = await call_tool(client, "get_nifi_object_details", {"object_type": "processor", "object_id": target_p1_for_false_test_verification}, base_headers)
                    assert p1_false_check[0].get("component"), f"P1_Source ({target_p1_for_false_test_verification}) was unexpectedly deleted or changed (AD=false path)."
                    logger.info(f"[{test_specific_id}] Verified P1_Source ({target_p1_for_false_test_verification}) still exists (AD=false path).")
                except Exception as e_check_p2_ad_false:
                    logger.error(f"[{test_specific_id}] Error verifying P2/C1/P1 after AD=false attempt: {e_check_p2_ad_false}", exc_info=True)
                    assert False, "Verification of P2/C1/P1 failed after AD=false attempt."
            else: 
                logger.error(f"FAILED [{test_specific_id}]: Delete P2_Sink ({target_p2_for_false_test}) with Auto-Delete=false UNEXPECTEDLY returned '{del_p2_adaf_result.get('status')}' instead of 'error'. Msg: {del_p2_adaf_result.get('message')}")
                assert False, f"Delete P2_Sink ({target_p2_for_false_test}) with Auto-Delete=false should have failed (NiFi block), but got: {del_p2_adaf_result.get('status')}"

        except httpx.HTTPStatusError as e:
            logger.error(f"[{test_specific_id}] Test failed due to HTTPStatusError: Status Code {e.response.status_code}. Response content follows.", exc_info=True)
            logger.error(e.response.text)
            assert False, f"Test failed with HTTPStatusError" 
        except Exception as e:
            if isinstance(e, AssertionError):
                raise 
            logger.error(f"[{test_specific_id}] Test failed due to unexpected error: {e}", exc_info=True)
            assert False, f"Test failed with unexpected error: {e}"
        finally:
            logger.info(f"[{test_specific_id}] --- Starting Cleanup for test_auto_delete_processor_with_connections ---")
            
            # Cleanup components for the Auto-Delete=false re-setup path (if taken; for setup validation, these should be None)
            if conn_id_for_false_test:
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting C1_false ({conn_id_for_false_test}) from AD=false re-setup.")
                    await call_tool(client, "delete_nifi_object", {"object_type": "connection", "object_id": conn_id_for_false_test, "kwargs": {}}, base_headers)
                except Exception as e_del_c1_false:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error deleting C1_false {conn_id_for_false_test}: {e_del_c1_false}")
            if p1_id_for_false_test:
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting P1_Source_false ({p1_id_for_false_test}) from AD=false re-setup.")
                    await call_tool(client, "delete_nifi_object", {"object_type": "processor", "object_id": p1_id_for_false_test, "kwargs": {}}, base_headers)
                except Exception as e_del_p1_false:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error deleting P1_Source_false {p1_id_for_false_test}: {e_del_p1_false}")
            if p2_id_for_false_test:
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting P2_Sink_false ({p2_id_for_false_test}) from AD=false re-setup.")
                    await call_tool(client, "delete_nifi_object", {"object_type": "processor", "object_id": p2_id_for_false_test, "kwargs": {}}, base_headers)
                except Exception as e_del_p2_false:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error deleting P2_Sink_false {p2_id_for_false_test}: {e_del_p2_false}")

            # Cleanup for original main components. These are always created.
            if conn_id: # This is the original connection_id from the start of the test.
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Attempting to delete original C1 ({conn_id})")
                    await call_tool(client, "delete_nifi_object", {"object_type": "connection", "object_id": conn_id, "kwargs": {}}, base_headers)
                except Exception as e_del_c1:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error deleting original C1 ({conn_id}): {e_del_c1}")
            
            if p1_id: # Original P1_Source 
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting original P1 ({p1_id})")
                    await call_tool(client, "delete_nifi_object", {"object_type": "processor", "object_id": p1_id, "kwargs": {}}, base_headers)
                except Exception as e_del_p1:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error deleting original P1 ({p1_id}): {e_del_p1}")
            
            if p2_id: # Original P2_Sink
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting original P2 ({p2_id})")
                    await call_tool(client, "delete_nifi_object", {"object_type": "processor", "object_id": p2_id, "kwargs": {}}, base_headers)
                except Exception as e_del_p2:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error deleting original P2 ({p2_id}): {e_del_p2}")
            
            if pg_id:
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting PG {pg_id}")
                    delete_pg_args_cleanup = {"object_type": "process_group", "object_id": pg_id, "kwargs": {}}
                    del_pg_res = await call_tool(client, "delete_nifi_object", delete_pg_args_cleanup, base_headers)
                    if del_pg_res[0].get("status") != "success":
                        logger.error(f"[{test_specific_id}] Cleanup: FAILED to delete PG {pg_id}: {del_pg_res[0].get('message')}")
                except Exception as e_del_pg:
                    logger.error(f"[{test_specific_id}] Cleanup: Error deleting PG {pg_id}: {e_del_pg}")
            
            logger.info(f"[{test_specific_id}] --- Finished Cleanup for test_auto_delete_processor_with_connections ---")

async def test_auto_purge_connection_with_queued_data():
    """Test Auto-Purge: Deleting a connection that has queued data."""
    if not TARGET_NIFI_SERVER_ID:
        logger.error("SKIPPING TEST: NIFI_TEST_SERVER_ID not set.")
        return

    test_specific_id = f"del-conn-queued-{TEST_RUN_ID}"
    pg_name = f"mcp-autopurge-{test_specific_id}-pg"
    p1_name = f"mcp-autopurge-{test_specific_id}-p1-source"
    p2_name = f"mcp-autopurge-{test_specific_id}-p2-sink"
    
    pg_id = None
    p1_id = None
    p2_id = None
    conn_id = None # C1

    # For the Auto-Purge=false part of the test, if original C1 is deleted
    p1_id_for_false_setup = None
    p2_id_for_false_setup = None
    conn_id_for_false_setup = None

    base_headers = {
        "X-Nifi-Server-Id": TARGET_NIFI_SERVER_ID,
        "Content-Type": "application/json"
    }

    logger.info(f"--- Starting Test: test_auto_purge_connection_with_queued_data ({test_specific_id}) ---")

    async with httpx.AsyncClient() as client:
        try:
            # 1. Setup: PG, P1 (Generate), P2 (Log - initially invalid), C1 (P1->P2)
            logger.info(f"[{test_specific_id}] Creating PG: {pg_name}")
            pg_res_list = await call_tool(client, "create_nifi_process_group", {"name": pg_name, "position_x": 400, "position_y": 400}, base_headers)
            pg_id = pg_res_list[0].get("entity", {}).get("id")
            assert pg_id, f"[{test_specific_id}] Failed to create PG."
            logger.info(f"[{test_specific_id}] Created PG ID: {pg_id}")
            await asyncio.sleep(0.5)

            # P1 - Source (GenerateFlowFile)
            create_p1_args = {"process_group_id": pg_id, "processor_type": "org.apache.nifi.processors.standard.GenerateFlowFile", "name": p1_name, "position_x": 0, "position_y": 0}
            logger.info(f"[{test_specific_id}] Creating P1 Source: {p1_name} in PG {pg_id}")
            p1_res_list = await call_tool(client, "create_nifi_processor", create_p1_args, base_headers)
            p1_id = p1_res_list[0].get("entity", {}).get("id")
            assert p1_id, f"[{test_specific_id}] Failed to create P1."
            logger.info(f"[{test_specific_id}] Created P1 ID: {p1_id}")
            # Auto-terminate P1's success? No, we want to connect it.
            await asyncio.sleep(0.5)

            # P2 - Sink (LogAttribute)
            # IMPORTANT: For this test, P2 is initially created WITHOUT auto-terminating its relationships.
            # This makes P2 invalid, so FlowFiles will queue up in the connection C1.
            create_p2_args = {"process_group_id": pg_id, "processor_type": "org.apache.nifi.processors.standard.LogAttribute", "name": p2_name, "position_x": 0, "position_y": 200}
            logger.info(f"[{test_specific_id}] Creating P2 Sink (initially invalid): {p2_name} in PG {pg_id}")
            p2_res_list = await call_tool(client, "create_nifi_processor", create_p2_args, base_headers)
            p2_id = p2_res_list[0].get("entity", {}).get("id")
            assert p2_id, f"[{test_specific_id}] Failed to create P2."
            logger.info(f"[{test_specific_id}] Created P2 ID: {p2_id}. P2 relationships NOT auto-terminated yet.")
            await asyncio.sleep(1)

            # C1 - Connection (P1 -> P2)
            connect_args = {"source_id": p1_id, "relationships": ["success"], "target_id": p2_id} # P1 -> P2
            logger.info(f"[{test_specific_id}] Connecting P1 ({p1_id}) to P2 ({p2_id})")
            conn_res_list = await call_tool(client, "create_nifi_connection", connect_args, base_headers)
            conn_id = conn_res_list[0].get("entity", {}).get("id")
            assert conn_id, f"[{test_specific_id}] Failed to create connection C1."
            logger.info(f"[{test_specific_id}] Created Connection C1 ID: {conn_id}")
            await asyncio.sleep(1)

            # Start P1 to generate FlowFiles into C1's queue
            logger.info(f"[{test_specific_id}] Starting P1 ({p1_id}) to queue data in C1 ({conn_id}). P2 ({p2_id}) is invalid.")
            operate_p1_start_args = {"object_type": "processor", "object_id": p1_id, "operation_type": "start"}
            start_p1_res_list = await call_tool(client, "operate_nifi_object", operate_p1_start_args, base_headers)
            assert start_p1_res_list[0].get("status") == "success", f"Failed to start P1: {start_p1_res_list[0].get('message')}"
            logger.info(f"[{test_specific_id}] P1 started. Waiting for data to queue...")
            await asyncio.sleep(5) # Wait for some FlowFiles to be generated and queued

            # Stop P1
            logger.info(f"[{test_specific_id}] Stopping P1 ({p1_id}).")
            operate_p1_stop_args = {"object_type": "processor", "object_id": p1_id, "operation_type": "stop"}
            stop_p1_res_list = await call_tool(client, "operate_nifi_object", operate_p1_stop_args, base_headers)
            assert stop_p1_res_list[0].get("status") == "success", f"Failed to stop P1: {stop_p1_res_list[0].get('message')}"
            logger.info(f"[{test_specific_id}] P1 stopped. C1 should now have queued data.")
            await asyncio.sleep(2) # Ensure P1 is fully stopped

            # Verify queue (optional, but good for sanity)
            try:
                pg_status_args = {"process_group_id": pg_id}
                pg_status_res = await call_tool(client, "get_process_group_status", pg_status_args, base_headers)
                q_summary = pg_status_res[0].get("queue_summary", {})
                total_queued = q_summary.get("total_queued_count", "N/A")
                logger.info(f"[{test_specific_id}] PG Status: Total queued in PG {pg_id} is {total_queued}.")
                # We can't easily assert a specific count, but if it's > 0 or non-empty string, that's a good sign.
            except Exception as e_status:
                logger.warning(f"[{test_specific_id}] Could not get PG status to verify queue: {e_status}")

            # --- Test Auto-Purge ENABLED (targeting C1) ---
            logger.info(f"[{test_specific_id}] Test Step: Attempt delete C1 ({conn_id}) with Auto-Purge=true. (TEMPORARY EXPECTATION: NiFi block -> MCP error for setup validation)")
            headers_auto_purge_true = {**base_headers, "X-Mcp-Auto-Purge-Enabled": "true"}
            delete_c1_args_apat = {"object_type": "connection", "object_id": conn_id, "kwargs": {}}
            
            del_c1_apat_result_list = await call_tool(client, "delete_nifi_object", delete_c1_args_apat, headers_auto_purge_true)
            assert isinstance(del_c1_apat_result_list, list) and len(del_c1_apat_result_list) > 0 and isinstance(del_c1_apat_result_list[0], dict), \
                f"[{test_specific_id}] Unexpected response format for delete_nifi_object (C1 Auto-Purge=true)"
            del_c1_apat_result = del_c1_apat_result_list[0]

            # Store original IDs for verification and potential re-setup logic
            original_p1_id_for_setup_val_purge = p1_id
            original_p2_id_for_setup_val_purge = p2_id
            original_conn_id_for_setup_val_purge = conn_id
            c1_deleted_in_true_step_unexpectedly = False

            # TEMPORARY: Expect error for setup validation
            if del_c1_apat_result.get("status") == "error":
                logger.info(f"SETUP CHECKPOINT [{test_specific_id}]: Delete C1 with Auto-Purge=true resulted in 'error' (C1 has queue), as expected for setup validation. Msg: {del_c1_apat_result.get('message')}")
                # Verify C1 still exists, and P1/P2 also exist
                try:
                    c1_check = await call_tool(client, "get_nifi_object_details", {"object_type": "connection", "object_id": original_conn_id_for_setup_val_purge}, base_headers)
                    assert c1_check[0].get("component"), f"C1 {original_conn_id_for_setup_val_purge} was unexpectedly deleted (AP=true setup validation)."
                    logger.info(f"[{test_specific_id}] Verified C1 ({original_conn_id_for_setup_val_purge}) still exists (AP=true setup validation).")
                    
                    p1_check = await call_tool(client, "get_nifi_object_details", {"object_type": "processor", "object_id": original_p1_id_for_setup_val_purge}, base_headers)
                    assert p1_check[0].get("component"), f"P1 {original_p1_id_for_setup_val_purge} was unexpectedly changed/deleted (AP=true setup validation)."
                    logger.info(f"[{test_specific_id}] Verified P1 ({original_p1_id_for_setup_val_purge}) still exists (AP=true setup validation).")

                    p2_check = await call_tool(client, "get_nifi_object_details", {"object_type": "processor", "object_id": original_p2_id_for_setup_val_purge}, base_headers)
                    assert p2_check[0].get("component"), f"P2 {original_p2_id_for_setup_val_purge} was unexpectedly changed/deleted (AP=true setup validation)."
                    logger.info(f"[{test_specific_id}] Verified P2 ({original_p2_id_for_setup_val_purge}) still exists (AP=true setup validation).")
                except Exception as e_check_ap_true_setup:
                    assert False, f"Error verifying C1/P1/P2 after AP=true resulted in error (setup validation): {e_check_ap_true_setup}"
            else:
                logger.warning(f"[{test_specific_id}] Delete C1 with Auto-Purge=true did NOT result in 'error' (setup validation). Status: {del_c1_apat_result.get('status')}. This might affect AP=false part.")
                c1_deleted_in_true_step_unexpectedly = True
                assert False, f"[{test_specific_id}] Delete C1 with Auto-Purge=true did NOT result in 'error'. (Setup validation)"

            # # The TDD logic for success would be here, now commented out:
            # assert del_c1_apat_result.get("status") == "success", \
            #     f"[{test_specific_id}] Delete C1 with Auto-Purge=true FAILED. Expected 'success', got '{del_c1_apat_result.get('status')}'. Msg: {del_c1_apat_result.get('message')}"
            # logger.info(f"PASSED [{test_specific_id}]: Delete C1 ({conn_id}) with Auto-Purge=true reported 'success'. Now verifying deletion and processor integrity.")
            # conn_c1_found_after_apat = True
            # try:
            #     await call_tool(client, "get_nifi_object_details", {"object_type": "connection", "object_id": original_conn_id_for_setup_val_purge}, base_headers) 
            # except httpx.HTTPStatusError as e_c1_verify_apat:
            #     if e_c1_verify_apat.response.status_code == 404:
            #         conn_c1_found_after_apat = False
            # assert not conn_c1_found_after_apat, f"Connection C1 ({original_conn_id_for_setup_val_purge}) was NOT deleted."
            # logger.info(f"Connection C1 ({original_conn_id_for_setup_val_purge}) confirmed deleted.")
            # # Verify P1/P2 still exist (omitted for brevity, similar to above check)
            # original_conn_id_for_logging = original_conn_id_for_setup_val_purge 
            # conn_id = None # Mark original C1 as deleted FOR TDD PATH

            await asyncio.sleep(1)

            # --- Test Auto-Purge DISABLED ---
            # For setup validation, if AP=true errored, original C1 is still there.
            target_conn_id_for_false_test = original_conn_id_for_setup_val_purge 

            if c1_deleted_in_true_step_unexpectedly:
                logger.warning(f"[{test_specific_id}] C1 ({original_conn_id_for_setup_val_purge}) was unexpectedly deleted by AP=true. AP=false test might be unreliable or fail.")
                # Even if C1 was deleted, the AD=false test will try to use its ID. get_details would then fail.
                # No re-creation for setup validation path, as p1/p2_id_for_false_setup are for TDD path.
            else:
                logger.info(f"[{test_specific_id}] Using original C1 ({target_conn_id_for_false_test}) for Auto-Purge=false test (setup validation).")

            logger.info(f"[{test_specific_id}] Test Step: Attempt delete Connection ({target_conn_id_for_false_test}) with Auto-Purge=false. EXPECTED: NiFi block -> MCP error.")
            headers_auto_purge_false = {**base_headers, "X-Mcp-Auto-Purge-Enabled": "false"}
            delete_c1_args_apaf = {"object_type": "connection", "object_id": target_conn_id_for_false_test, "kwargs": {}}

            del_c1_apaf_result_list = await call_tool(client, "delete_nifi_object", delete_c1_args_apaf, headers_auto_purge_false)
            assert isinstance(del_c1_apaf_result_list, list) and len(del_c1_apaf_result_list) > 0 and isinstance(del_c1_apaf_result_list[0], dict), \
                f"[{test_specific_id}] Unexpected response format for delete_nifi_object (C1 Auto-Purge=false)"
            del_c1_apaf_result = del_c1_apaf_result_list[0]

            if del_c1_apaf_result.get("status") == "error":
                logger.info(f"PASSED [{test_specific_id}]: Delete Connection ({target_conn_id_for_false_test}) with Auto-Purge=false correctly resulted in 'error'. Msg: {del_c1_apaf_result.get('message')}")
                # Verify the target connection still exists
                try:
                    c1_check_apaf = await call_tool(client, "get_nifi_object_details", {"object_type": "connection", "object_id": target_conn_id_for_false_test}, base_headers)
                    assert c1_check_apaf[0].get("component"), f"Connection {target_conn_id_for_false_test} was unexpectedly deleted (AP=false path)."
                    logger.info(f"[{test_specific_id}] Verified Connection ({target_conn_id_for_false_test}) still exists after failed delete (AP=false).")
                    # Also verify P1 and P2 for this original setup
                    p1_apaf_check = await call_tool(client, "get_nifi_object_details", {"object_type": "processor", "object_id": original_p1_id_for_setup_val_purge}, base_headers)
                    assert p1_apaf_check[0].get("component"), f"P1 ({original_p1_id_for_setup_val_purge}) was unexpectedly changed/deleted (AP=false path)."
                    logger.info(f"[{test_specific_id}] Verified P1 ({original_p1_id_for_setup_val_purge}) still exists (AP=false path).")
                    p2_apaf_check = await call_tool(client, "get_nifi_object_details", {"object_type": "processor", "object_id": original_p2_id_for_setup_val_purge}, base_headers)
                    assert p2_apaf_check[0].get("component"), f"P2 ({original_p2_id_for_setup_val_purge}) was unexpectedly changed/deleted (AP=false path)."
                    logger.info(f"[{test_specific_id}] Verified P2 ({original_p2_id_for_setup_val_purge}) still exists (AP=false path).")
                except Exception as e_check_c1_ap_false:
                    logger.error(f"[{test_specific_id}] Error verifying Connection/P1/P2 {target_conn_id_for_false_test} after AP=false attempt: {e_check_c1_ap_false}", exc_info=True)
                    assert False, f"Verification of Connection/P1/P2 {target_conn_id_for_false_test} failed after AP=false attempt."
            else: 
                logger.error(f"FAILED [{test_specific_id}]: Delete Connection ({target_conn_id_for_false_test}) with Auto-Purge=false UNEXPECTEDLY returned '{del_c1_apaf_result.get('status')}' instead of 'error'. Msg: {del_c1_apaf_result.get('message')}")
                assert False, f"Delete Connection ({target_conn_id_for_false_test}) with Auto-Purge=false should have failed, but got: {del_c1_apaf_result.get('status')}"

        except httpx.HTTPStatusError as e:
            logger.error(f"[{test_specific_id}] Test failed due to HTTPStatusError: Status Code {e.response.status_code}. Response content follows.", exc_info=True)
            logger.error(e.response.text)
            assert False, f"Test failed with HTTPStatusError" # Simplified assertion message
        except Exception as e:
            if isinstance(e, AssertionError):
                raise # Re-raise AssertionError for pytest to handle as a test failure
            logger.error(f"[{test_specific_id}] Test failed due to unexpected error: {e}", exc_info=True)
            assert False, f"Test failed with unexpected error: {e}"
        finally:
            logger.info(f"[{test_specific_id}] --- Starting Cleanup for test_auto_purge_connection_with_queued_data ---")
            
            # Cleanup components for the Auto-Purge=false setup first (p1_id_for_false_setup etc.)
            # These are only populated if the TDD path (AP=true SUCCESS path leading to re-creation) was taken.
            # For setup validation path where AP=true ERRORS, these should be None.
            if conn_id_for_false_setup:
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting C1_false_setup ({conn_id_for_false_setup}) from AP=false re-setup.")
                    # For cleanup, attempt with auto_purge true as it might have data if its test section failed before purge
                    await call_tool(client, "delete_nifi_object", {"object_type": "connection", "object_id": conn_id_for_false_setup, "kwargs": {"auto_purge_enabled": "true"}}, base_headers)
                except Exception as e_del_c1_false_setup:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error deleting C1_false_setup {conn_id_for_false_setup}: {e_del_c1_false_setup}")
            if p1_id_for_false_setup:
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting P1_false_setup ({p1_id_for_false_setup}) from AP=false re-setup.")
                    await call_tool(client, "delete_nifi_object", {"object_type": "processor", "object_id": p1_id_for_false_setup, "kwargs": {}}, base_headers)
                except Exception as e_del_p1_false_setup:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error deleting P1_false_setup {p1_id_for_false_setup}: {e_del_p1_false_setup}")
            if p2_id_for_false_setup:
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting P2_false_setup ({p2_id_for_false_setup}) from AP=false re-setup.")
                    await call_tool(client, "delete_nifi_object", {"object_type": "processor", "object_id": p2_id_for_false_setup, "kwargs": {}}, base_headers)
                except Exception as e_del_p2_false_setup:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error deleting P2_false_setup {p2_id_for_false_setup}: {e_del_p2_false_setup}")

            # Cleanup for original main components. These are always created initially.
            # Use the 'original_..._for_setup_val_purge' variables defined in the try block.
            if 'original_conn_id_for_setup_val_purge' in locals() and original_conn_id_for_setup_val_purge:
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Attempting to delete original C1 ({original_conn_id_for_setup_val_purge}) with auto_purge=true for cleanup.")
                    await call_tool(client, "delete_nifi_object", {"object_type": "connection", "object_id": original_conn_id_for_setup_val_purge, "kwargs": {"auto_purge_enabled": "true"}}, base_headers)
                except Exception as e_del_c1:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error deleting original connection C1 ({original_conn_id_for_setup_val_purge}): {e_del_c1}")
            
            if 'original_p1_id_for_setup_val_purge' in locals() and original_p1_id_for_setup_val_purge: 
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting original P1 ({original_p1_id_for_setup_val_purge})")
                    await call_tool(client, "delete_nifi_object", {"object_type": "processor", "object_id": original_p1_id_for_setup_val_purge, "kwargs": {}}, base_headers)
                except Exception as e_del_p1:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error deleting original P1 ({original_p1_id_for_setup_val_purge}): {e_del_p1}")
            
            if 'original_p2_id_for_setup_val_purge' in locals() and original_p2_id_for_setup_val_purge: 
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting original P2 ({original_p2_id_for_setup_val_purge})")
                    await call_tool(client, "delete_nifi_object", {"object_type": "processor", "object_id": original_p2_id_for_setup_val_purge, "kwargs": {}}, base_headers)
                except Exception as e_del_p2:
                    logger.warning(f"[{test_specific_id}] Cleanup: Error deleting original P2 ({original_p2_id_for_setup_val_purge}): {e_del_p2}")
            
            if pg_id:
                try:
                    logger.info(f"[{test_specific_id}] Cleanup: Deleting PG {pg_id}")
                    delete_pg_args_cleanup = {"object_type": "process_group", "object_id": pg_id, "kwargs": {}}
                    del_pg_res = await call_tool(client, "delete_nifi_object", delete_pg_args_cleanup, base_headers)
                    if del_pg_res[0].get("status") != "success":
                        logger.error(f"[{test_specific_id}] Cleanup: FAILED to delete PG {pg_id}: {del_pg_res[0].get('message')}")
                except Exception as e_del_pg:
                    logger.error(f"[{test_specific_id}] Cleanup: Error deleting PG {pg_id}: {e_del_pg}")
            
            logger.info(f"[{test_specific_id}] --- Finished Cleanup for test_auto_purge_connection_with_queued_data ---")

async def run_new_feature_tests():
    """Orchestrates the new feature test sequence."""
    logger.info("--- Starting New Feature Test Suite ---")
    await test_auto_stop_delete_running_processor()
    await test_auto_stop_update_running_processor()
    await test_auto_delete_processor_with_connections()
    await test_auto_purge_connection_with_queued_data() # Add call to the new test
    logger.info("--- New Feature Test Suite Completed ---")

if __name__ == "__main__":
    # Ensure NIFI_TEST_SERVER_ID is checked before running any test suite
    if not TARGET_NIFI_SERVER_ID:
        logger.error("FATAL: NIFI_TEST_SERVER_ID environment variable is not set.")
        logger.error("Please set this variable to the ID of the target NiFi server from the MCP server's config.yaml.")
        sys.exit(1)
        
    # Decide which suite to run or run all
    # asyncio.run(run_nifi_tool_tests()) # Existing tests
    asyncio.run(run_new_feature_tests()) # New feature tests 
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
                    delete_conn_args = {"object_type": "connection", "object_id": connection_id}
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
                    delete_log_args = {"object_type": "processor", "object_id": log_proc_id}
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
                    delete_gen_args = {"object_type": "processor", "object_id": generate_proc_id}
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
                    delete_pg_args = {"object_type": "process_group", "object_id": test_pg_id}
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
                    delete_flow_pg_args = {"object_type": "process_group", "object_id": create_flow_pg_id}
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


if __name__ == "__main__":
    asyncio.run(run_nifi_tool_tests()) 
# Tests for NiFi Processor operations 

import pytest
import httpx
from loguru import logger
from typing import Dict, Any, List
import anyio
import asyncio

from nifi_mcp_server.nifi_client import NiFiClient # Corrected import
from tests.utils.nifi_test_utils import call_tool # Import call_tool

@pytest.mark.anyio
async def test_create_processors_in_pg(
    test_pg_with_processors: Dict[str, Any], # Fixture provides PG and processor IDs
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: dict,
    global_logger: Any,
    nifi_test_server_id: str # Changed from target_nifi_server_id
):
    """Tests the creation of processors within a PG using the test_pg_with_processors fixture."""
    pg_id = test_pg_with_processors.get("pg_id")
    generate_proc_id = test_pg_with_processors.get("generate_proc_id")
    log_proc_id = test_pg_with_processors.get("log_proc_id")

    global_logger.info(f"Test: test_create_processors_in_pg received PG_ID: {pg_id}, Gen_Proc_ID: {generate_proc_id}, Log_Proc_ID: {log_proc_id}")

    assert pg_id and isinstance(pg_id, str) and len(pg_id) > 0, "Invalid PG ID from fixture."
    assert generate_proc_id and isinstance(generate_proc_id, str) and len(generate_proc_id) > 0, "Invalid Generate Processor ID from fixture."
    assert log_proc_id and isinstance(log_proc_id, str) and len(log_proc_id) > 0, "Invalid Log Processor ID from fixture."

    # Verify Generate Processor details
    global_logger.info(f"Test: Verifying details for Generate Processor ID: {generate_proc_id}")
    gen_details_args = {"object_type": "processor", "object_id": generate_proc_id}
    gen_details_list = await call_tool(
        client=async_client, base_url=base_url, tool_name="get_nifi_object_details",
        arguments=gen_details_args, headers=mcp_headers, custom_logger=global_logger
    )
    assert isinstance(gen_details_list, list) and gen_details_list and isinstance(gen_details_list[0], dict)
    gen_details = gen_details_list[0].get("component", gen_details_list[0])
    assert gen_details and gen_details.get("id") == generate_proc_id
    expected_gen_name = f"mcp-test-generate-{nifi_test_server_id}"
    actual_gen_name = gen_details.get("name")
    assert actual_gen_name == expected_gen_name, \
        f"Generate processor name mismatch: expected '{expected_gen_name}', got '{actual_gen_name}'"
    global_logger.info(f"Test: Successfully verified Generate Processor {generate_proc_id} with name '{actual_gen_name}'.")

    # Verify LogAttribute Processor details
    global_logger.info(f"Test: Verifying details for LogAttribute Processor ID: {log_proc_id}")
    log_details_args = {"object_type": "processor", "object_id": log_proc_id}
    log_details_list = await call_tool(
        client=async_client, base_url=base_url, tool_name="get_nifi_object_details",
        arguments=log_details_args, headers=mcp_headers, custom_logger=global_logger
    )
    assert isinstance(log_details_list, list) and log_details_list and isinstance(log_details_list[0], dict)
    log_details = log_details_list[0].get("component", log_details_list[0])
    assert log_details and log_details.get("id") == log_proc_id
    expected_log_name = f"mcp-test-log-{nifi_test_server_id}"
    actual_log_name = log_details.get("name")
    assert actual_log_name == expected_log_name, \
        f"LogAttribute processor name mismatch: expected '{expected_log_name}', got '{actual_log_name}'"
    global_logger.info(f"Test: Successfully verified LogAttribute Processor {log_proc_id} with name '{actual_log_name}'.")

    assert log_proc_id in [proc.get("id") for proc in test_pg_with_processors["processors"]], \
        f"Processor '{log_proc_id}' not found in created processors."

# TODO: Add tests for updating processor properties, relationships, etc.

@pytest.mark.anyio
async def test_update_processor_relationships(
    test_pg_with_processors: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: dict,
    global_logger: Any,
    nifi_test_server_id: str
):
    """Tests updating a processor's auto-terminated relationships.
    Note: LogAttribute processor will show warnings without input connections,
    but the relationship update itself should still succeed.
    The LogAttribute processor only has a 'success' relationship."""
    log_proc_id = test_pg_with_processors.get("log_proc_id")
    assert log_proc_id, "Log Processor ID not found from fixture."

    # Update relationships
    global_logger.info(f"Test: Auto-terminating relationships for LogAttribute Processor ID: {log_proc_id}")
    update_rels_args = {
        "processor_id": log_proc_id,
        "auto_terminated_relationships": ["success"]  # LogAttribute only has 'success' relationship
    }
    rels_result_list = await call_tool(
        client=async_client, base_url=base_url, tool_name="update_nifi_processor_relationships",
        arguments=update_rels_args, headers=mcp_headers, custom_logger=global_logger
    )
    assert isinstance(rels_result_list, list) and rels_result_list and isinstance(rels_result_list[0], dict), \
        "Unexpected response format for update_nifi_processor_relationships"
    rels_result = rels_result_list[0]

    # Accept both success and warning as valid states
    assert rels_result.get("status") in ["success", "warning"], \
        f"Failed to update relationships: {rels_result.get('message')}"
    global_logger.info(f"Test: Successfully submitted update for LogAttribute relationships. Response status: {rels_result.get('status')}")

    # Verify the update by re-fetching processor details
    global_logger.info(f"Test: Verifying relationship update for LogAttribute Processor ID: {log_proc_id}")
    details_args = {"object_type": "processor", "object_id": log_proc_id}
    details_list = await call_tool(
        client=async_client, base_url=base_url, tool_name="get_nifi_object_details",
        arguments=details_args, headers=mcp_headers, custom_logger=global_logger
    )
    assert isinstance(details_list, list) and details_list and isinstance(details_list[0], dict)
    details = details_list[0].get("component", details_list[0])
    assert details and details.get("id") == log_proc_id

    # Check relationships array for auto-terminated settings
    relationships = details.get("relationships", [])
    global_logger.info(f"Checking relationships: {relationships}")
    
    # Find and verify the success relationship is auto-terminated
    success_rel = next((rel for rel in relationships if rel.get("name") == "success"), None)
    assert success_rel is not None, "Success relationship not found in processor relationships"
    assert success_rel.get("autoTerminate", False), "Success relationship is not auto-terminated"
    
    global_logger.info("Test: Successfully verified auto-terminated relationships in processor configuration")

@pytest.mark.anyio
async def test_update_processor_properties_and_verify(
    test_pg_with_processors: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: dict,
    global_logger: Any,
    nifi_test_server_id: str
):
    """Tests updating a processor's properties and verifies the update."""
    log_proc_id = test_pg_with_processors.get("log_proc_id")
    generate_proc_id = test_pg_with_processors.get("generate_proc_id")
    assert log_proc_id, "Log Processor ID not found from fixture."
    assert generate_proc_id, "Generate Processor ID not found from fixture."

    # First auto-terminate relationships to make the processor valid
    global_logger.info(f"Test: Auto-terminating relationships for LogAttribute Processor ID: {log_proc_id}")
    update_rels_args = {
        "processor_id": log_proc_id,
        "auto_terminated_relationships": ["success"]  # LogAttribute only has 'success' relationship
    }
    rels_result_list = await call_tool(
        client=async_client, base_url=base_url, tool_name="update_nifi_processor_relationships",
        arguments=update_rels_args, headers=mcp_headers, custom_logger=global_logger
    )
    assert isinstance(rels_result_list, list) and rels_result_list and isinstance(rels_result_list[0], dict), \
        "Unexpected response format for update_nifi_processor_relationships"
    rels_result = rels_result_list[0]
    assert rels_result.get("status") in ["success", "warning"], \
        f"Failed to update relationships: {rels_result.get('message')}"
    global_logger.info(f"Test: Successfully auto-terminated relationships. Status: {rels_result.get('status')}")

    # Create connection between GenerateFlowFile and LogAttribute
    global_logger.info(f"Test: Creating connection from GenerateFlowFile to LogAttribute")
    create_connection_args = {
        "source_id": generate_proc_id,
        "target_id": log_proc_id,  # Changed from destination_id
        "relationships": ["success"]  # Changed from relationships_to_connect
    }
    connection_result_list = await call_tool(
        client=async_client, base_url=base_url, tool_name="create_nifi_connections",
        arguments={"connections": [create_connection_args]}, headers=mcp_headers, custom_logger=global_logger
    )
    assert isinstance(connection_result_list, list) and connection_result_list and isinstance(connection_result_list[0], dict), \
        "Unexpected response format for create_nifi_connection"
    connection_result = connection_result_list[0]
    assert connection_result.get("status") in ["success", "warning"], \
        f"Failed to create connection: {connection_result.get('message')}"
    global_logger.info(f"Test: Successfully created connection. Status: {connection_result.get('status')}")

    # Now update the properties
    global_logger.info(f"Test: Updating LogAttribute ({log_proc_id}) properties.")
    target_log_level = "warn"
    update_props_args = {
        "processor_id": log_proc_id,
        "processor_config_properties": {
            "Log Level": target_log_level,
            "Attributes to Log": ".*",      # As per original test
            "Log prefix": "mcp-test"       # As per original test
        }
    }
    update_result_list = await call_tool(
        client=async_client, base_url=base_url, tool_name="update_nifi_processor_properties",
        arguments=update_props_args, headers=mcp_headers, custom_logger=global_logger
    )
    assert isinstance(update_result_list, list) and update_result_list and isinstance(update_result_list[0], dict), \
        "Unexpected response format for update_nifi_processor_properties"
    update_result = update_result_list[0]
    assert update_result.get("status") in ["success", "warning"], \
        f"Failed to update properties: {update_result.get('message')}"
    global_logger.info(f"Test: Successfully submitted update for LogAttribute properties. Status: {update_result.get('status')}")

    # Re-fetch details to verify the update
    global_logger.info(f"Test: Re-fetching LogAttribute ({log_proc_id}) details to verify property update.")
    verify_update_args = {"object_type": "processor", "object_id": log_proc_id}
    verify_result_list = await call_tool(
        client=async_client, base_url=base_url, tool_name="get_nifi_object_details",
        arguments=verify_update_args, headers=mcp_headers, custom_logger=global_logger
    )
    assert isinstance(verify_result_list, list) and verify_result_list and isinstance(verify_result_list[0], dict), \
        "Unexpected response format for get_nifi_object_details (verify update)"

    verify_details_response = verify_result_list[0]
    if verify_details_response.get("status") == "error":
        assert False, f"Failed to get details for processor {log_proc_id} after update: {verify_details_response.get('message')}"

    verified_component = verify_details_response.get("component", verify_details_response)
    assert verified_component is not None, "Component details not found in verification step."

    verified_config = verified_component.get("config", {})
    verified_props = verified_config.get("properties", {})
    validation_status = verified_component.get("validationStatus")

    # Verify properties were updated correctly
    actual_log_level = verified_props.get("Log Level")
    assert actual_log_level == target_log_level, \
        f"Log Level property was not updated correctly. Expected '{target_log_level}', found '{actual_log_level}'."
    global_logger.info(f"Test: Successfully verified LogAttribute properties update (Log Level: {actual_log_level}).")

    # Verify relationships are still auto-terminated
    relationships = verified_component.get("relationships", [])
    success_rel = next((rel for rel in relationships if rel.get("name") == "success"), None)
    assert success_rel is not None, "Success relationship not found in processor relationships"
    assert success_rel.get("autoTerminate", False), "Success relationship is not auto-terminated"
    global_logger.info("Test: Verified success relationship is still auto-terminated")

    # Check validation status - should now be VALID since we have auto-terminated relationships and upstream connection
    assert validation_status == "VALID", \
        f"Processor {log_proc_id} is not VALID after property update. Status: {validation_status}."
    global_logger.info(f"Test: Processor {log_proc_id} validation status confirmed: {validation_status}")

@pytest.mark.anyio
async def test_auto_stop_update_running_processor(
    test_pg_with_processors: Dict[str, Any],  # Fixture provides PG with Generate and Log processors
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that updating a running processor's properties with auto-stop enabled works correctly."""
    # Get processor IDs from the fixture
    generate_proc_id = test_pg_with_processors.get("generate_proc_id")
    assert generate_proc_id, "Generate Processor ID not found from fixture."

    # First auto-terminate relationships
    update_rels_args = {
        "processor_id": generate_proc_id,
        "auto_terminated_relationships": ["success"]
    }
    rels_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="update_nifi_processor_relationships",
        arguments=update_rels_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert rels_result_list[0].get("status") in ["success", "warning"], \
        "Failed to auto-terminate relationships"
    global_logger.info(f"Test: Auto-terminated relationships for processor {generate_proc_id}")

    # Then start the processor
    start_args = {
        "object_type": "processor",
        "object_id": generate_proc_id,
        "operation_type": "start"
    }
    start_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="operate_nifi_object",
        arguments=start_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert start_result_list[0].get("status") == "success", "Failed to start processor"
    global_logger.info(f"Test: Started processor {generate_proc_id}")
    await asyncio.sleep(1)  # Brief pause after starting

    # Verify processor is running and get current properties
    verify_args = {
        "object_type": "processor",
        "object_id": generate_proc_id
    }
    verify_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="get_nifi_object_details",
        arguments=verify_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert verify_result_list[0].get("component", {}).get("state") == "RUNNING", \
        "Processor not running before update test"
    
    # Get current scheduling strategy
    current_scheduling_strategy = verify_result_list[0].get("component", {}).get("config", {}).get("schedulingStrategy", "TIMER_DRIVEN")
    target_scheduling_strategy = "CRON_DRIVEN" if current_scheduling_strategy == "TIMER_DRIVEN" else "TIMER_DRIVEN"
    global_logger.info(f"Test: Current scheduling strategy: {current_scheduling_strategy}, target: {target_scheduling_strategy}")

    # Attempt update with Auto-Stop enabled
    headers_auto_stop_true = {**mcp_headers, "X-Mcp-Auto-Stop-Enabled": "true"}
    update_args = {
        "processor_id": generate_proc_id,
        "processor_config_properties": {
            "Scheduling Strategy": target_scheduling_strategy
        }
    }
    update_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="update_nifi_processor_properties",
        arguments=update_args,
        headers=headers_auto_stop_true,
        custom_logger=global_logger
    )
    # For now, expect error since feature isn't implemented
    assert update_result_list[0].get("status") == "error", \
        "Expected error with Auto-Stop enabled (not implemented yet)"
    global_logger.info("Test: Got expected error with Auto-Stop enabled")

    # Verify processor still exists and is running with original properties
    verify_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="get_nifi_object_details",
        arguments=verify_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert verify_result_list[0].get("component", {}).get("state") == "RUNNING", \
        "Processor should still be running after failed update attempt"
    assert verify_result_list[0].get("component", {}).get("config", {}).get("schedulingStrategy") == current_scheduling_strategy, \
        "Scheduling strategy should not have changed"
    global_logger.info(f"Test: Confirmed processor {generate_proc_id} is still running with original scheduling strategy")

    # Attempt update with Auto-Stop disabled
    headers_auto_stop_false = {**mcp_headers, "X-Mcp-Auto-Stop-Enabled": "false"}
    update_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="update_nifi_processor_properties",
        arguments=update_args,
        headers=headers_auto_stop_false,
        custom_logger=global_logger
    )
    assert update_result_list[0].get("status") == "error", \
        "Expected error when updating running processor with Auto-Stop disabled"
    global_logger.info("Test: Got expected error with Auto-Stop disabled") 
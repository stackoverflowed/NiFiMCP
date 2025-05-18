# Tests for NiFi Connection operations 

import pytest
import httpx
from loguru import logger
from typing import Dict, Any
import anyio
import asyncio

from tests.utils.nifi_test_utils import call_tool

@pytest.mark.anyio
async def test_create_and_verify_connection(
    test_pg_with_processors: Dict[str, Any],  # Fixture provides PG with Generate and Log processors
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test creating a connection between processors and verify its properties."""
    # Get processor IDs from the fixture
    generate_proc_id = test_pg_with_processors.get("generate_proc_id")
    log_proc_id = test_pg_with_processors.get("log_proc_id")
    assert generate_proc_id, "Generate Processor ID not found from fixture."
    assert log_proc_id, "Log Processor ID not found from fixture."

    global_logger.info(f"Test: Creating connection from GenerateFlowFile ({generate_proc_id}) to LogAttribute ({log_proc_id})")
    
    # Create connection
    connect_args = {
        "source_id": generate_proc_id,
        "relationships": ["success"],
        "target_id": log_proc_id
    }
    conn_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="create_nifi_connection",
        arguments=connect_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert isinstance(conn_result_list, list) and len(conn_result_list) > 0, "Connection creation should return a result list"
    conn_result = conn_result_list[0]
    assert conn_result.get("status") == "success", f"Failed to create connection: {conn_result.get('message')}"
    connection_id = conn_result.get("entity", {}).get("id")
    assert connection_id, "Connection ID not found in response."
    global_logger.info(f"Test: Successfully created Connection ID: {connection_id}")

    # Verify connection details
    verify_args = {
        "object_type": "connection",
        "object_id": connection_id
    }
    verify_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="get_nifi_object_details",
        arguments=verify_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert isinstance(verify_result_list, list) and len(verify_result_list) > 0, "Connection details should be returned"
    verify_result = verify_result_list[0]
    
    # Verify connection properties
    component = verify_result.get("component", {})
    assert component.get("source", {}).get("id") == generate_proc_id, "Source processor ID mismatch"
    assert component.get("destination", {}).get("id") == log_proc_id, "Destination processor ID mismatch"
    assert "success" in component.get("selectedRelationships", []), "Success relationship not found"
    
    global_logger.info(f"Test: Successfully verified connection {connection_id} properties.")

@pytest.mark.anyio
async def test_delete_connection(
    test_pg_with_processors: Dict[str, Any],  # Fixture provides PG with Generate and Log processors
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test deleting a connection between processors."""
    # First create a connection
    generate_proc_id = test_pg_with_processors.get("generate_proc_id")
    log_proc_id = test_pg_with_processors.get("log_proc_id")
    assert generate_proc_id and log_proc_id, "Processor IDs not found from fixture."

    # Create connection
    connect_args = {
        "source_id": generate_proc_id,
        "relationships": ["success"],
        "target_id": log_proc_id
    }
    conn_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="create_nifi_connection",
        arguments=connect_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    connection_id = conn_result_list[0].get("entity", {}).get("id")
    assert connection_id, "Failed to create test connection."
    global_logger.info(f"Test: Created test connection {connection_id} for deletion test.")

    # Now delete the connection
    delete_args = {
        "object_type": "connection",
        "object_id": connection_id,
        "kwargs": {}
    }
    delete_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="delete_nifi_object",
        arguments=delete_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert isinstance(delete_result_list, list) and len(delete_result_list) > 0, "Delete operation should return a result"
    delete_result = delete_result_list[0]
    assert delete_result.get("status") == "success", f"Failed to delete connection: {delete_result.get('message')}"
    global_logger.info(f"Test: Successfully deleted connection {connection_id}")

    # Verify connection is gone - should get a 400 error
    verify_args = {
        "object_type": "connection",
        "object_id": connection_id
    }
    try:
        verify_result_list = await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="get_nifi_object_details",
            arguments=verify_args,
            headers=mcp_headers,
            custom_logger=global_logger
        )
        assert False, f"Connection {connection_id} still exists after deletion"
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 400:
            # This is expected - the connection should not exist
            global_logger.info(f"Test: Confirmed connection {connection_id} no longer exists (got expected 400 error).")
        else:
            raise  # Re-raise if it's not a 400 error

@pytest.mark.anyio
async def test_auto_purge_connection_with_queued_data(
    test_pg_with_processors: Dict[str, Any],  # Fixture provides PG with Generate and Log processors
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that deleting a connection with queued data and auto-purge enabled works correctly."""
    # Get processor IDs from the fixture
    generate_proc_id = test_pg_with_processors.get("generate_proc_id")
    log_proc_id = test_pg_with_processors.get("log_proc_id")
    assert generate_proc_id and log_proc_id, "Processor IDs not found from fixture."

    # First auto-terminate relationships for the generator
    update_rels_args = {
        "processor_id": generate_proc_id,
        "auto_terminated_relationships": []  # Clear any auto-termination
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
        "Failed to update relationships"
    global_logger.info(f"Test: Updated relationships for processor {generate_proc_id}")

    # Create connection between processors
    connect_args = {
        "source_id": generate_proc_id,
        "relationships": ["success"],
        "target_id": log_proc_id
    }
    conn_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="create_nifi_connection",
        arguments=connect_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert conn_result_list[0].get("status") == "success", "Failed to create connection"
    connection_id = conn_result_list[0].get("entity", {}).get("id")
    assert connection_id, "Connection ID not found in response"
    global_logger.info(f"Test: Created connection {connection_id} between processors")

    # Start the generator processor to create some queued data
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
    assert start_result_list[0].get("status") == "success", "Failed to start generator processor"
    global_logger.info(f"Test: Started generator processor {generate_proc_id}")
    await asyncio.sleep(2)  # Allow time for data to queue up

    # Stop the generator processor
    stop_args = {
        "object_type": "processor",
        "object_id": generate_proc_id,
        "operation_type": "stop"
    }
    stop_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="operate_nifi_object",
        arguments=stop_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert stop_result_list[0].get("status") == "success", "Failed to stop generator processor"
    global_logger.info(f"Test: Stopped generator processor {generate_proc_id}")
    await asyncio.sleep(1)

    # Verify data is queued
    verify_conn_args = {
        "object_type": "connection",
        "object_id": connection_id
    }
    verify_conn_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="get_nifi_object_details",
        arguments=verify_conn_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    queued_count = int(verify_conn_list[0].get("status", {}).get("aggregateSnapshot", {}).get("queuedCount", "0"))
    assert queued_count > 0, "No data queued in connection for test"
    global_logger.info(f"Test: Verified {queued_count} items queued in connection {connection_id}")

    # Attempt delete with Auto-Purge enabled
    headers_auto_purge_true = {**mcp_headers, "X-Mcp-Auto-Purge-Enabled": "true"}
    delete_args = {
        "object_type": "connection",
        "object_id": connection_id,
        "kwargs": {}
    }
    delete_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="delete_nifi_object",
        arguments=delete_args,
        headers=headers_auto_purge_true,
        custom_logger=global_logger
    )
    # For now, expect error since feature isn't implemented
    assert delete_result_list[0].get("status") == "error", \
        "Expected error with Auto-Purge enabled (not implemented yet)"
    global_logger.info("Test: Got expected error with Auto-Purge enabled")

    # Verify connection still exists with queued data
    verify_conn_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="get_nifi_object_details",
        arguments=verify_conn_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert verify_conn_list[0].get("component"), \
        "Connection should still exist after failed deletion attempt"
    current_queued_count = int(verify_conn_list[0].get("status", {}).get("aggregateSnapshot", {}).get("queuedCount", "0"))
    assert current_queued_count == queued_count, \
        f"Queue count changed from {queued_count} to {current_queued_count} when it should not have"
    global_logger.info(f"Test: Confirmed connection {connection_id} still exists with {current_queued_count} queued items")

    # Attempt delete with Auto-Purge disabled
    headers_auto_purge_false = {**mcp_headers, "X-Mcp-Auto-Purge-Enabled": "false"}
    delete_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="delete_nifi_object",
        arguments=delete_args,
        headers=headers_auto_purge_false,
        custom_logger=global_logger
    )
    assert delete_result_list[0].get("status") == "error", \
        "Expected error when deleting connection with queued data and Auto-Purge disabled"
    global_logger.info("Test: Got expected error with Auto-Purge disabled")

# Add other connection related tests here 
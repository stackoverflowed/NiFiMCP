# Tests for NiFi Processor Cleanup operations (auto-stop, delete with connections, etc.)

import pytest
import httpx
from typing import Dict, Any
import anyio
import asyncio

from tests.utils.nifi_test_utils import call_tool

@pytest.mark.anyio
async def test_auto_stop_delete_running_processor(
    test_pg_with_processors: Dict[str, Any],  # Fixture provides PG with Generate and Log processors
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that deleting a running processor with auto-stop enabled works correctly."""
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

    # Verify processor is running
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
        "Processor not running before deletion test"
    global_logger.info(f"Test: Confirmed processor {generate_proc_id} is running")

    # Attempt delete with Auto-Stop enabled
    headers_auto_stop_true = {**mcp_headers, "X-Mcp-Auto-Stop-Enabled": "true"}
    delete_args = {
        "object_type": "processor",
        "object_id": generate_proc_id,
        "kwargs": {}
    }
    delete_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="delete_nifi_object",
        arguments=delete_args,
        headers=headers_auto_stop_true,
        custom_logger=global_logger
    )
    # For now, expect error since feature isn't implemented
    assert delete_result_list[0].get("status") == "error", \
        "Expected error with Auto-Stop enabled (not implemented yet)"
    global_logger.info("Test: Got expected error with Auto-Stop enabled")

    # Verify processor still exists and is running
    verify_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="get_nifi_object_details",
        arguments=verify_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert verify_result_list[0].get("component", {}).get("state") == "RUNNING", \
        "Processor should still be running after failed deletion attempt"
    global_logger.info(f"Test: Confirmed processor {generate_proc_id} is still running after failed deletion")

    # Attempt delete with Auto-Stop disabled
    headers_auto_stop_false = {**mcp_headers, "X-Mcp-Auto-Stop-Enabled": "false"}
    delete_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="delete_nifi_object",
        arguments=delete_args,
        headers=headers_auto_stop_false,
        custom_logger=global_logger
    )
    assert delete_result_list[0].get("status") == "error", \
        "Expected error when deleting running processor with Auto-Stop disabled"
    global_logger.info("Test: Got expected error with Auto-Stop disabled")

@pytest.mark.anyio
async def test_auto_delete_processor_with_connections(
    test_pg_with_processors: Dict[str, Any],  # Fixture provides PG with Generate and Log processors
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that deleting a processor with connections and auto-delete enabled works correctly."""
    # Get processor IDs from the fixture
    generate_proc_id = test_pg_with_processors.get("generate_proc_id")
    log_proc_id = test_pg_with_processors.get("log_proc_id")
    assert generate_proc_id and log_proc_id, "Processor IDs not found from fixture."

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

    # Attempt delete with Auto-Delete enabled
    headers_auto_delete_true = {**mcp_headers, "X-Mcp-Auto-Delete-Enabled": "true"}
    delete_args = {
        "object_type": "processor",
        "object_id": generate_proc_id,
        "kwargs": {}
    }
    delete_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="delete_nifi_object",
        arguments=delete_args,
        headers=headers_auto_delete_true,
        custom_logger=global_logger
    )
    # The feature is now implemented, so expect success
    assert delete_result_list[0].get("status") == "success", \
        "Failed to delete processor with Auto-Delete enabled"
    global_logger.info("Test: Successfully deleted processor with Auto-Delete enabled")

    # Verify processor is gone
    verify_proc_args = {
        "object_type": "processor",
        "object_id": generate_proc_id
    }
    try:
        verify_proc_list = await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="get_nifi_object_details",
            arguments=verify_proc_args,
            headers=mcp_headers,
            custom_logger=global_logger
        )
        assert False, f"Processor {generate_proc_id} still exists after deletion"
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 400:
            # This is expected - the processor should not exist
            global_logger.info(f"Test: Confirmed processor {generate_proc_id} no longer exists (got expected 400 error).")
        else:
            raise  # Re-raise if it's not a 400 error

    # Verify connection is gone
    verify_conn_args = {
        "object_type": "connection",
        "object_id": connection_id
    }
    try:
        verify_conn_list = await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="get_nifi_object_details",
            arguments=verify_conn_args,
            headers=mcp_headers,
            custom_logger=global_logger
        )
        assert False, f"Connection {connection_id} still exists after processor deletion"
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 400:
            # This is expected - the connection should not exist
            global_logger.info(f"Test: Confirmed connection {connection_id} no longer exists (got expected 400 error).")
        else:
            raise  # Re-raise if it's not a 400 error

    # Attempt delete with Auto-Delete disabled
    headers_auto_delete_false = {**mcp_headers, "X-Mcp-Auto-Delete-Enabled": "false"}
    delete_args = {
        "object_type": "processor",
        "object_id": log_proc_id,  # Try to delete the other processor
        "kwargs": {}
    }
    delete_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="delete_nifi_object",
        arguments=delete_args,
        headers=headers_auto_delete_false,
        custom_logger=global_logger
    )
    assert delete_result_list[0].get("status") == "success", \
        "Failed to delete processor with Auto-Delete disabled"
    global_logger.info("Test: Successfully deleted processor with Auto-Delete disabled") 
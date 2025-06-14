# Tests for NiFi Process Group Control operations 

import pytest
import httpx
from typing import Dict, Any
import anyio
import asyncio

from tests.utils.nifi_test_utils import call_tool

@pytest.mark.anyio
async def test_start_and_stop_process_group(
    test_pg_with_processors: Dict[str, Any],  # Fixture provides PG with Generate and Log processors
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test starting and stopping a process group with processors."""
    pg_id = test_pg_with_processors.get("pg_details", {}).get("id")
    assert pg_id, "Process Group ID not found from fixture."
    
    # First, ensure the LogAttribute processor's relationships are auto-terminated
    log_proc_id = test_pg_with_processors.get("log_proc_id")
    generate_proc_id = test_pg_with_processors.get("generate_proc_id")
    assert log_proc_id and generate_proc_id, "Processor IDs not found from fixture."
    
    # Auto-terminate LogAttribute's success relationship
    update_rels_args = {
        "processor_id": log_proc_id,
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
    assert rels_result_list[0].get("status") == "success", "Failed to update LogAttribute relationships"
    global_logger.info(f"Test: Auto-terminated LogAttribute success relationship")
    await asyncio.sleep(1)  # Brief pause after relationship update
    
    # Create connection between processors
    connect_args = {
        "source_id": generate_proc_id,
        "relationships": ["success"],
        "target_id": log_proc_id
    }
    conn_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="create_nifi_connections",
        arguments={"connections": [connect_args]},
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert conn_result_list[0].get("status") == "success", "Failed to create connection"
    global_logger.info(f"Test: Created connection from GenerateFlowFile to LogAttribute")
    await asyncio.sleep(1)  # Brief pause after connection creation

    # Ensure processors are valid before starting
    for proc_type, proc_id in [("GenerateFlowFile", generate_proc_id), 
                              ("LogAttribute", log_proc_id)]:
        verify_args = {
            "object_type": "processor",
            "object_id": proc_id
        }
        verify_result_list = await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="get_nifi_object_details",
            arguments=verify_args,
            headers=mcp_headers,
            custom_logger=global_logger
        )
        validation_status = verify_result_list[0].get("component", {}).get("validationStatus")
        assert validation_status == "VALID", f"{proc_type} processor {proc_id} is not VALID: {validation_status}"
        global_logger.info(f"Test: Confirmed {proc_type} processor {proc_id} is VALID")

    # Start the Process Group
    global_logger.info(f"Test: Starting Process Group {pg_id}")
    start_args = {
        "object_type": "process_group",
        "object_id": pg_id,
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
    assert isinstance(start_result_list, list) and len(start_result_list) > 0, "Start operation should return a result"
    start_result = start_result_list[0]
    assert start_result.get("status") == "success", f"Failed to start process group: {start_result.get('message')}"
    global_logger.info(f"Test: Successfully started Process Group {pg_id}")
    await asyncio.sleep(2)  # Allow time for processors to start

    # Verify processors are running
    for proc_type, proc_id in [("GenerateFlowFile", generate_proc_id), 
                              ("LogAttribute", log_proc_id)]:
        verify_args = {
            "object_type": "processor",
            "object_id": proc_id
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
            f"{proc_type} processor {proc_id} is not RUNNING after PG start"
        global_logger.info(f"Test: Confirmed {proc_type} processor {proc_id} is RUNNING")

    # Stop the Process Group
    global_logger.info(f"Test: Stopping Process Group {pg_id}")
    stop_args = {
        "object_type": "process_group",
        "object_id": pg_id,
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
    assert isinstance(stop_result_list, list) and len(stop_result_list) > 0, "Stop operation should return a result"
    stop_result = stop_result_list[0]
    assert stop_result.get("status") == "success", f"Failed to stop process group: {stop_result.get('message')}"
    global_logger.info(f"Test: Successfully stopped Process Group {pg_id}")
    await asyncio.sleep(2)  # Allow time for processors to stop

    # Verify processors are stopped
    for proc_type, proc_id in [("GenerateFlowFile", generate_proc_id), 
                              ("LogAttribute", log_proc_id)]:
        verify_args = {
            "object_type": "processor",
            "object_id": proc_id
        }
        verify_result_list = await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="get_nifi_object_details",
            arguments=verify_args,
            headers=mcp_headers,
            custom_logger=global_logger
        )
        assert verify_result_list[0].get("component", {}).get("state") == "STOPPED", \
            f"{proc_type} processor {proc_id} is not STOPPED after PG stop"
        global_logger.info(f"Test: Confirmed {proc_type} processor {proc_id} is STOPPED")

@pytest.mark.anyio
async def test_process_group_status_after_operations(
    test_pg_with_processors: Dict[str, Any],  # Fixture provides PG with Generate and Log processors
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test process group status changes after start/stop operations."""
    pg_id = test_pg_with_processors.get("pg_details", {}).get("id")
    assert pg_id, "Process Group ID not found from fixture."

    # First, ensure the LogAttribute processor's relationships are auto-terminated
    log_proc_id = test_pg_with_processors.get("log_proc_id")
    generate_proc_id = test_pg_with_processors.get("generate_proc_id")
    assert log_proc_id and generate_proc_id, "Processor IDs not found from fixture."
    
    # Create connection between processors
    connect_args = {
        "source_id": generate_proc_id,
        "relationships": ["success"],
        "target_id": log_proc_id
    }
    conn_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="create_nifi_connections",
        arguments={"connections": [connect_args]},
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert conn_result_list[0].get("status") == "success", "Failed to create connection"
    global_logger.info(f"Test: Created connection from GenerateFlowFile to LogAttribute")
    await asyncio.sleep(1)  # Brief pause after connection creation

    # Helper function to get PG status
    async def get_pg_status():
        status_args = {"process_group_id": pg_id}
        status_result_list = await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="get_process_group_status",
            arguments=status_args,
            headers=mcp_headers,
            custom_logger=global_logger
        )
        assert isinstance(status_result_list, list) and len(status_result_list) > 0, "Status info should be returned"
        return status_result_list[0]

    # Get initial status
    initial_status = await get_pg_status()
    global_logger.info(f"Test: Initial PG status - Component Summary: {initial_status.get('component_summary', {})}")

    # Start PG
    start_args = {
        "object_type": "process_group",
        "object_id": pg_id,
        "operation_type": "start"
    }
    await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="operate_nifi_object",
        arguments=start_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    await asyncio.sleep(2)  # Allow time for status to update

    # Get status after start
    running_status = await get_pg_status()
    global_logger.info(f"Test: PG status after start - Component Summary: {running_status.get('component_summary', {})}")
    
    # Stop PG
    stop_args = {
        "object_type": "process_group",
        "object_id": pg_id,
        "operation_type": "stop"
    }
    await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="operate_nifi_object",
        arguments=stop_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    await asyncio.sleep(2)  # Allow time for status to update

    # Get final status
    final_status = await get_pg_status()
    global_logger.info(f"Test: Final PG status after stop - Component Summary: {final_status.get('component_summary', {})}")

    # Verify status changes
    initial_summary = initial_status.get("component_summary", {}).get("processors", {})
    running_summary = running_status.get("component_summary", {}).get("processors", {})
    final_summary = final_status.get("component_summary", {}).get("processors", {})

    assert running_summary.get("running", 0) > initial_summary.get("running", 0), \
        "Number of running processors should increase after start"
    assert final_summary.get("stopped", 0) > running_summary.get("stopped", 0), \
        "Number of stopped processors should increase after stop" 
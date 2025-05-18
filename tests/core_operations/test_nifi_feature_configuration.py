import pytest
import httpx
from typing import Dict, Any
import anyio
import asyncio

from tests.utils.nifi_test_utils import call_tool

@pytest.mark.anyio
async def test_feature_configuration_headers(
    test_pg_with_processors: Dict[str, Any],  # Fixture provides PG with Generate and Log processors
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that feature configuration via headers works correctly."""
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
        "Processor not running before test"
    global_logger.info(f"Test: Confirmed processor {generate_proc_id} is running")

    # Test each feature header
    features = ["Auto-Stop", "Auto-Delete", "Auto-Purge"]
    for feature in features:
        # Test with feature enabled
        headers_enabled = {**mcp_headers, f"X-Mcp-{feature}-Enabled": "true"}
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
            headers=headers_enabled,
            custom_logger=global_logger
        )
        # For now, expect error since features aren't implemented
        assert delete_result_list[0].get("status") == "error", \
            f"Expected error with {feature} enabled (not implemented yet)"
        global_logger.info(f"Test: Got expected error with {feature} enabled")

        # Test with feature disabled
        headers_disabled = {**mcp_headers, f"X-Mcp-{feature}-Enabled": "false"}
        delete_result_list = await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="delete_nifi_object",
            arguments=delete_args,
            headers=headers_disabled,
            custom_logger=global_logger
        )
        assert delete_result_list[0].get("status") == "error", \
            f"Expected error with {feature} disabled"
        global_logger.info(f"Test: Got expected error with {feature} disabled")

@pytest.mark.anyio
async def test_feature_configuration_defaults(
    test_pg_with_processors: Dict[str, Any],  # Fixture provides PG with Generate and Log processors
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that feature configuration defaults work correctly when no headers are provided."""
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
        "Processor not running before test"
    global_logger.info(f"Test: Confirmed processor {generate_proc_id} is running")

    # Test without any feature headers (should use config defaults)
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
        headers=mcp_headers,
        custom_logger=global_logger
    )
    # For now, expect error since features aren't implemented
    assert delete_result_list[0].get("status") == "error", \
        "Expected error with default configuration (features not implemented yet)"
    global_logger.info("Test: Got expected error with default configuration") 
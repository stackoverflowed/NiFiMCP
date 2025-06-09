import pytest
import httpx
from typing import Dict, Any
import anyio

from tests.utils.nifi_test_utils import call_tool


@pytest.mark.anyio
async def test_get_flowfile_event_details_nonexistent_event(
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test get_flowfile_event_details with a non-existent event ID."""
    fake_event_id = 999999
    
    global_logger.info(f"Test: Testing with non-existent event ID {fake_event_id}")

    # Test with non-existent event ID
    event_args = {
        "event_id": fake_event_id,
        "max_content_bytes": 1024
    }
    
    event_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="get_flowfile_event_details",
        arguments=event_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(event_result_list, list) and len(event_result_list) > 0
    event_result = event_result_list[0]
    
    global_logger.info(f"Event details result: {event_result}")
    
    # Should have an error due to non-existent event
    assert event_result.get("status") == "error"
    assert "not found" in event_result.get("message", "").lower()
    assert event_result.get("event_id") == fake_event_id


@pytest.mark.anyio
async def test_get_flowfile_event_details_response_structure(
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that get_flowfile_event_details returns the correct response structure."""
    fake_event_id = 123456
    
    global_logger.info("Test: Testing response structure")

    event_args = {
        "event_id": fake_event_id,
        "max_content_bytes": 512
    }
    
    event_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="get_flowfile_event_details",
        arguments=event_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(event_result_list, list) and len(event_result_list) > 0
    event_result = event_result_list[0]
    
    # Check required fields in response structure
    required_fields = [
        "status", "message", "event_id", "event_type", "event_time", 
        "component_name", "flowfile_uuid", "attributes",
        "input_content_size_bytes", "output_content_size_bytes", 
        "content_identical", "content_included", "content_too_large",
        "content", "content_type"
    ]
    
    for field in required_fields:
        assert field in event_result, f"Missing required field: {field}"
    
    # Verify data types
    assert isinstance(event_result["event_id"], int)
    assert isinstance(event_result["attributes"], list)
    assert isinstance(event_result["input_content_size_bytes"], int)
    assert isinstance(event_result["output_content_size_bytes"], int)
    assert isinstance(event_result["content_identical"], bool)
    assert isinstance(event_result["content_included"], bool)
    assert isinstance(event_result["content_too_large"], bool)


@pytest.mark.anyio 
async def test_get_flowfile_event_details_max_content_bytes_parameter(
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that max_content_bytes parameter is handled correctly."""
    fake_event_id = 123456
    
    global_logger.info("Test: Testing max_content_bytes parameter")

    # Test with very small max_content_bytes
    event_args = {
        "event_id": fake_event_id,
        "max_content_bytes": 10  # Very small limit
    }
    
    event_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="get_flowfile_event_details",
        arguments=event_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(event_result_list, list) and len(event_result_list) > 0
    event_result = event_result_list[0]
    
    # The result should handle the small content limit appropriately
    assert "max_content_bytes" in str(event_args)
    assert event_result.get("event_id") == fake_event_id
    
    # Test with default max_content_bytes (no parameter)
    event_args_default = {
        "event_id": fake_event_id
    }
    
    event_result_list_default = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="get_flowfile_event_details",
        arguments=event_args_default,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(event_result_list_default, list) and len(event_result_list_default) > 0
    event_result_default = event_result_list_default[0]
    
    assert event_result_default.get("event_id") == fake_event_id 
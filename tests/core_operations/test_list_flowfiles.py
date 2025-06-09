import pytest
import httpx
from typing import Dict, Any
import anyio

from tests.utils.nifi_test_utils import call_tool


@pytest.mark.anyio
async def test_list_flowfiles_from_connection_empty_queue(
    test_connection: str,  # Fixture provides connection ID
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test listing flowfiles from a connection with no queued data."""
    global_logger.info(f"Test: Listing flowfiles from connection {test_connection} (empty queue)")

    # Test listing flowfiles from the connection
    list_args = {
        "target_id": test_connection,
        "target_type": "connection",
        "max_results": 5
    }
    
    list_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="list_flowfiles",
        arguments=list_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(list_result_list, list) and len(list_result_list) > 0
    list_result = list_result_list[0]
    
    global_logger.info(f"List flowfiles result: {list_result}")
    
    # Should succeed even with empty queue
    assert list_result.get("target_id") == test_connection
    assert list_result.get("target_type") == "connection"
    assert list_result.get("listing_source") == "queue"
    assert "flowfile_summaries" in list_result
    assert isinstance(list_result["flowfile_summaries"], list)
    
    # With empty queue, should have no flowfiles
    assert len(list_result["flowfile_summaries"]) == 0
    
    # Should not have an error
    assert list_result.get("error") is None


@pytest.mark.anyio
async def test_list_flowfiles_from_processor_no_history(
    test_pg_with_processors: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test listing flowfiles from a processor with no provenance history."""
    generate_proc_id = test_pg_with_processors["generate_proc_id"]
    
    global_logger.info(f"Test: Listing flowfiles from processor {generate_proc_id} (no history)")

    # Test listing flowfiles from the processor
    list_args = {
        "target_id": generate_proc_id,
        "target_type": "processor",
        "max_results": 5
    }
    
    list_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="list_flowfiles",
        arguments=list_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(list_result_list, list) and len(list_result_list) > 0
    list_result = list_result_list[0]
    
    global_logger.info(f"List flowfiles result: {list_result}")
    
    # Should have correct basic structure
    assert list_result.get("target_id") == generate_proc_id
    assert list_result.get("target_type") == "processor"
    assert list_result.get("listing_source") == "provenance"
    assert "flowfile_summaries" in list_result
    assert isinstance(list_result["flowfile_summaries"], list)
    
    # Check if provenance query succeeded or failed
    if list_result.get("error") is None:
        # Success case: should have no flowfiles in empty test environment
        assert len(list_result["flowfile_summaries"]) == 0
        global_logger.info("Provenance query succeeded (no flowfiles found)")
    else:
        # Error case: provenance might not be enabled or configured
        error_msg = list_result.get("error", "")
        # Common provenance errors in test environments
        assert any(phrase in error_msg.lower() for phrase in [
            "500",  # Server error
            "provenance",  # Provenance-related error
            "unexpected error"  # Generic NiFi error
        ]), f"Unexpected error type: {error_msg}"
        global_logger.info(f"Provenance query failed as expected in test environment: {error_msg}")
        # Should still have empty flowfile summaries even on error
        assert len(list_result["flowfile_summaries"]) == 0


@pytest.mark.anyio
async def test_list_flowfiles_invalid_target_type(
    test_pg_with_processors: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test list_flowfiles with invalid target_type."""
    generate_proc_id = test_pg_with_processors["generate_proc_id"]
    
    global_logger.info(f"Test: Testing invalid target_type")

    # Test with invalid target_type
    list_args = {
        "target_id": generate_proc_id,
        "target_type": "invalid_type",  # Invalid type
        "max_results": 5
    }
    
    # This should raise an HTTP error due to validation failure
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="list_flowfiles",
            arguments=list_args,
            headers=mcp_headers,
            custom_logger=global_logger
        )
    
    # Verify it's a 400 Bad Request due to validation error
    assert exc_info.value.response.status_code == 400
    response_text = exc_info.value.response.text
    assert "validation error" in response_text.lower()
    assert "literal_error" in response_text.lower()
    
    global_logger.info(f"Correctly received validation error: {response_text}")


@pytest.mark.anyio
async def test_list_flowfiles_invalid_connection_id(
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test list_flowfiles with non-existent connection ID."""
    fake_connection_id = "non-existent-connection-id"
    
    global_logger.info(f"Test: Testing with non-existent connection ID")

    # Test with non-existent connection ID
    list_args = {
        "target_id": fake_connection_id,
        "target_type": "connection",
        "max_results": 5
    }
    
    list_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="list_flowfiles",
        arguments=list_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(list_result_list, list) and len(list_result_list) > 0
    list_result = list_result_list[0]
    
    global_logger.info(f"List flowfiles result: {list_result}")
    
    # Should have an error due to non-existent connection
    assert list_result.get("error") is not None
    # The error could be a connection error or a specific not-found error
    assert list_result.get("target_id") == fake_connection_id
    assert list_result.get("target_type") == "connection"


@pytest.mark.anyio
async def test_list_flowfiles_invalid_processor_id(
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test list_flowfiles with non-existent processor ID."""
    fake_processor_id = "non-existent-processor-id"
    
    global_logger.info(f"Test: Testing with non-existent processor ID")

    # Test with non-existent processor ID
    list_args = {
        "target_id": fake_processor_id,
        "target_type": "processor",
        "max_results": 5
    }
    
    list_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="list_flowfiles",
        arguments=list_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(list_result_list, list) and len(list_result_list) > 0
    list_result = list_result_list[0]
    
    global_logger.info(f"List flowfiles result: {list_result}")
    
    # NiFi may accept the query but return no results for non-existent processor
    # This is valid behavior - either an error OR empty results with no error
    if list_result.get("error") is not None:
        # Error case - NiFi rejected the processor ID
        global_logger.info("NiFi rejected the non-existent processor ID with an error")
        assert "error" in list_result
    else:
        # No error case - NiFi accepted the query but returned no results
        global_logger.info("NiFi accepted the processor ID but returned no provenance events")
        assert list_result.get("error") is None
        assert list_result.get("flowfile_summaries") == []
        assert list_result.get("listing_source") == "provenance"
        assert list_result.get("target_id") == fake_processor_id


@pytest.mark.anyio
async def test_list_flowfiles_max_results_parameter(
    test_connection: str,
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that max_results parameter is properly handled."""
    global_logger.info(f"Test: Testing max_results parameter")

    # Test with different max_results values
    for max_results in [1, 10, 50]:
        list_args = {
            "target_id": test_connection,
            "target_type": "connection",
            "max_results": max_results
        }
        
        list_result_list = await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="list_flowfiles",
            arguments=list_args,
            headers=mcp_headers,
            custom_logger=global_logger
        )
        
        assert isinstance(list_result_list, list) and len(list_result_list) > 0
        list_result = list_result_list[0]
        
        # Should succeed regardless of max_results value
        assert list_result.get("error") is None
        assert list_result.get("target_type") == "connection"
        
        # Number of results should not exceed max_results (empty queue so should be 0)
        assert len(list_result["flowfile_summaries"]) <= max_results
        
        global_logger.info(f"max_results={max_results}: got {len(list_result['flowfile_summaries'])} flowfiles")


@pytest.mark.anyio
async def test_list_flowfiles_response_structure(
    test_connection: str,
    test_pg_with_processors: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that list_flowfiles returns the expected response structure."""
    generate_proc_id = test_pg_with_processors["generate_proc_id"]
    
    global_logger.info(f"Test: Verifying response structure")

    # Test both connection and processor
    test_cases = [
        {"target_id": test_connection, "target_type": "connection", "expected_source": "queue"},
        {"target_id": generate_proc_id, "target_type": "processor", "expected_source": "provenance"}
    ]
    
    for test_case in test_cases:
        list_args = {
            "target_id": test_case["target_id"],
            "target_type": test_case["target_type"],
            "max_results": 5
        }
        
        list_result_list = await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="list_flowfiles",
            arguments=list_args,
            headers=mcp_headers,
            custom_logger=global_logger
        )
        
        assert isinstance(list_result_list, list) and len(list_result_list) > 0
        list_result = list_result_list[0]
        
        # Verify required fields are present
        required_fields = ["target_id", "target_type", "listing_source", "flowfile_summaries"]
        for field in required_fields:
            assert field in list_result, f"Missing required field: {field}"
        
        # Verify field values
        assert list_result["target_id"] == test_case["target_id"]
        assert list_result["target_type"] == test_case["target_type"]
        assert list_result["listing_source"] == test_case["expected_source"]
        assert isinstance(list_result["flowfile_summaries"], list)
        
        # Error field should be present (may be None)
        assert "error" in list_result
        
        global_logger.info(f"Response structure verified for {test_case['target_type']}") 
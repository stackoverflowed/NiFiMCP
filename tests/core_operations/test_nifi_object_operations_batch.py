"""
Tests for the new batch operate_nifi_objects tool.
These tests validate the enhanced functionality from Phase 1.1 of the optimization plan.
"""

import pytest
import httpx
from typing import Dict, Any, List
from tests.utils.nifi_test_utils import call_tool


@pytest.mark.anyio
async def test_operate_nifi_objects_batch_validation(
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: dict,
    global_logger: Any
):
    """Tests the input validation for the operate_nifi_objects batch tool."""
    
    # Test empty list
    global_logger.info("Test: Testing empty operation_requests list")
    with pytest.raises(Exception) as exc_info:
        await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="operate_nifi_objects",
            arguments={"operations": []},
            headers=mcp_headers,
            custom_logger=global_logger
        )
    # Check that we get an HTTP 400 error (which indicates validation failed)
    assert "400" in str(exc_info.value)
    
    # Test invalid structure - not a list
    global_logger.info("Test: Testing invalid operation_requests type")
    with pytest.raises(Exception) as exc_info:
        await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="operate_nifi_objects",
            arguments={"operations": {"invalid": "structure"}},
            headers=mcp_headers,
            custom_logger=global_logger
        )
    assert "400" in str(exc_info.value)
    
    # Test missing required fields
    global_logger.info("Test: Testing missing required fields")
    with pytest.raises(Exception) as exc_info:
        await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="operate_nifi_objects",
            arguments={"operations": [{"object_type": "processor"}]},
            headers=mcp_headers,
            custom_logger=global_logger
        )
    assert "400" in str(exc_info.value)
    
    # Test invalid object_type
    global_logger.info("Test: Testing invalid object_type")
    with pytest.raises(Exception) as exc_info:
        await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="operate_nifi_objects",
            arguments={"operations": [{
                "object_type": "invalid_type",
                "object_id": "test-id",
                "operation_type": "start"
            }]},
            headers=mcp_headers,
            custom_logger=global_logger
        )
    assert "400" in str(exc_info.value)
    
    # Test invalid operation_type
    global_logger.info("Test: Testing invalid operation_type")
    with pytest.raises(Exception) as exc_info:
        await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="operate_nifi_objects",
            arguments={"operations": [{
                "object_type": "processor",
                "object_id": "test-id",
                "operation_type": "invalid_operation"
            }]},
            headers=mcp_headers,
            custom_logger=global_logger
        )
    assert "400" in str(exc_info.value)
    
    # Test incompatible operation for controller service
    global_logger.info("Test: Testing incompatible operation for controller service")
    with pytest.raises(Exception) as exc_info:
        await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="operate_nifi_objects",
            arguments={"operations": [{
                "object_type": "controller_service",
                "object_id": "test-id",
                "operation_type": "start"  # Invalid for controller service
            }]},
            headers=mcp_headers,
            custom_logger=global_logger
        )
    assert "400" in str(exc_info.value)
    
    global_logger.info("Test: All validation tests passed")


@pytest.mark.anyio
async def test_operate_nifi_objects_batch_mixed_operations(
    test_pg_with_processors: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: dict,
    global_logger: Any
):
    """Tests the operate_nifi_objects tool with a mix of operations that have different outcomes."""
    
    generate_proc_id = test_pg_with_processors["generate_proc_id"]
    log_proc_id = test_pg_with_processors["log_proc_id"]
    
    global_logger.info(f"Test: Testing batch operations with processor IDs: {generate_proc_id}, {log_proc_id}")
    
    # First, make the processors valid by auto-terminating their relationships
    global_logger.info("Making processors valid by auto-terminating relationships")
    
    # Auto-terminate success relationship for GenerateFlowFile processor
    await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="update_nifi_processor_relationships",
        arguments={
            "processor_id": generate_proc_id,
            "auto_terminated_relationships": ["success"]
        },
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    # Auto-terminate success relationship for LogAttribute processor (it also needs success terminated)
    await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="update_nifi_processor_relationships",
        arguments={
            "processor_id": log_proc_id,
            "auto_terminated_relationships": ["success"]
        },
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    # Create a mixed batch of operations - some valid, some invalid
    operation_requests = [
        {
            "object_type": "processor",
            "object_id": generate_proc_id,
            "operation_type": "start",
            "name": "GenerateFlowFile"
        },
        {
            "object_type": "processor", 
            "object_id": log_proc_id,
            "operation_type": "start",
            "name": "LogAttribute"
        },
        {
            "object_type": "processor",
            "object_id": "non-existent-processor-id",
            "operation_type": "start",
            "name": "NonExistentProcessor"
        }
    ]
    
    global_logger.info("Test: Executing batch operations")
    results_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="operate_nifi_objects",
        arguments={"operations": operation_requests},
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    # Validate results structure
    assert isinstance(results_list, list), "Results should be a list"
    assert len(results_list) == 3, f"Expected 3 results, got {len(results_list)}"
    
    # Validate each result has the expected metadata
    for i, result in enumerate(results_list):
        assert isinstance(result, dict), f"Result {i} should be a dictionary"
        assert "status" in result, f"Result {i} missing status field"
        assert "message" in result, f"Result {i} missing message field"
        assert "object_type" in result, f"Result {i} missing object_type field"
        assert "object_id" in result, f"Result {i} missing object_id field"
        assert "operation_type" in result, f"Result {i} missing operation_type field"
        assert "object_name" in result, f"Result {i} missing object_name field"
        assert "request_index" in result, f"Result {i} missing request_index field"
        assert result["request_index"] == i, f"Result {i} has incorrect request_index"
        
        global_logger.info(f"Test: Result {i}: {result['status']} - {result['message']}")
    
    # First processor (GenerateFlowFile) should succeed - it can run without upstream connections
    assert results_list[0]["status"] in ["success", "warning"], f"First processor operation failed: {results_list[0]['message']}"
    
    # Second processor (LogAttribute) should fail - it requires upstream connections in NiFi
    assert results_list[1]["status"] == "error", f"LogAttribute processor should fail without upstream connection: {results_list[1]['message']}"
    assert "upstream connection" in results_list[1]["message"].lower() or "validation" in results_list[1]["message"].lower(), "Error should mention upstream connection requirement"
    
    # Third should fail (non-existent processor)
    assert results_list[2]["status"] == "error", f"Non-existent processor should fail: {results_list[2]['message']}"
    assert "not found" in results_list[2]["message"].lower(), "Error message should indicate processor not found"
    
    # Clean up: Stop any processors that were successfully started to allow proper teardown
    global_logger.info("Test: Cleaning up started processors")
    cleanup_requests = []
    if results_list[0]["status"] == "success":
        cleanup_requests.append({
            "object_type": "processor",
            "object_id": generate_proc_id,
            "operation_type": "stop",
            "name": "GenerateFlowFile"
        })
    
    if cleanup_requests:
        await call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="operate_nifi_objects",
            arguments={"operations": cleanup_requests},
            headers=mcp_headers,
            custom_logger=global_logger
        )
        global_logger.info("Test: Processor cleanup complete")
    
    global_logger.info("Test: Batch mixed operations test passed - GenerateFlowFile started, LogAttribute failed validation (expected), non-existent processor not found")


@pytest.mark.anyio 
async def test_operate_nifi_objects_batch_stop_operations(
    test_pg_with_processors: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: dict,
    global_logger: Any
):
    """Tests stopping multiple processors in batch."""
    
    generate_proc_id = test_pg_with_processors["generate_proc_id"]
    log_proc_id = test_pg_with_processors["log_proc_id"]
    
    global_logger.info("Test: Testing batch stop operations")
    
    # First, make the processors valid by auto-terminating their relationships
    global_logger.info("Making processors valid by auto-terminating relationships")
    
    # Auto-terminate success relationship for GenerateFlowFile processor
    await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="update_nifi_processor_relationships",
        arguments={
            "processor_id": generate_proc_id,
            "auto_terminated_relationships": ["success"]
        },
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    # Auto-terminate success relationship for LogAttribute processor
    await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="update_nifi_processor_relationships",
        arguments={
            "processor_id": log_proc_id,
            "auto_terminated_relationships": ["success"]
        },
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    # Create stop operations for both processors
    operation_requests = [
        {
            "object_type": "processor",
            "object_id": generate_proc_id,
            "operation_type": "stop",
            "name": "GenerateFlowFile"
        },
        {
            "object_type": "processor",
            "object_id": log_proc_id,
            "operation_type": "stop",
            "name": "LogAttribute"
        }
    ]
    
    results_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="operate_nifi_objects",
        arguments={"operations": operation_requests},
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    # Validate results
    assert isinstance(results_list, list), "Results should be a list"
    assert len(results_list) == 2, f"Expected 2 results, got {len(results_list)}"
    
    for i, result in enumerate(results_list):
        assert result["status"] in ["success", "warning"], f"Stop operation {i} failed: {result['message']}"
        assert result["operation_type"] == "stop", f"Operation type should be 'stop' for result {i}"
        global_logger.info(f"Test: Stop result {i}: {result['status']} - {result['message']}")
    
    global_logger.info("Test: Batch stop operations test passed")


@pytest.mark.anyio
async def test_operate_nifi_objects_efficiency_comparison(
    test_pg_with_processors: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: dict,
    global_logger: Any
):
    """Tests performance of batch operations and validates batch functionality."""
    import time
    
    generate_proc_id = test_pg_with_processors["generate_proc_id"]
    log_proc_id = test_pg_with_processors["log_proc_id"]
    
    global_logger.info("Test: Batch operation performance validation")
    
    # NOTE: The original efficiency comparison test used the removed operate_nifi_object tool
    # This test now focuses on validating the batch operation functionality
    
    # Test batch call performance 
    start_time = time.time()
    
    batch_results = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="operate_nifi_objects",
        arguments={"operations": [
            {
                "object_type": "processor",
                "object_id": generate_proc_id,
                "operation_type": "stop",
                "name": "GenerateFlowFile"
            },
            {
                "object_type": "processor",
                "object_id": log_proc_id,
                "operation_type": "stop",
                "name": "LogAttribute"
            }
        ]},
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    batch_time = time.time() - start_time
    global_logger.info(f"Test: Batch call took {batch_time:.3f} seconds")
    
    # Validate batch results
    assert len(batch_results) == 2, "Batch should return 2 results"
    assert all(r["status"] in ["success", "warning"] for r in batch_results), "All operations should succeed or warn"
    
    # Verify all operations have the expected metadata
    for i, result in enumerate(batch_results):
        assert "object_type" in result, f"Result {i} should have object_type"
        assert "object_id" in result, f"Result {i} should have object_id"
        assert "operation_type" in result, f"Result {i} should have operation_type"
        assert "request_index" in result, f"Result {i} should have request_index"
        assert result["request_index"] == i, f"Result {i} should have correct request_index"
        global_logger.info(f"Test: Batch result {i}: {result['status']} - {result['message']}")
    
    # Validate the batch operation was reasonably performant (should complete in reasonable time)
    assert batch_time < 10.0, f"Batch operation took too long: {batch_time:.3f}s"
    
    global_logger.info("Test: Batch operation validation completed successfully") 
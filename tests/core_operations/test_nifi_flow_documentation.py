# Tests for NiFi Flow Documentation operations (search, document flow)

import pytest
import httpx
from typing import Dict, Any
import anyio
import asyncio

from tests.utils.nifi_test_utils import call_tool

@pytest.mark.anyio
async def test_search_flow_for_processor(
    test_pg_with_processors: Dict[str, Any],  # Fixture provides PG with Generate and Log processors
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test searching the flow for a processor."""
    # Get processor IDs and PG ID from the fixture
    log_proc_id = test_pg_with_processors.get("log_proc_id")
    pg_id = test_pg_with_processors.get("pg_details", {}).get("id")
    assert log_proc_id and pg_id, "Log Processor ID or PG ID not found from fixture."

    # Search for the LogAttribute processor by name pattern
    search_args = {
        "query": "mcp-test-log",
        "filter_process_group_id": pg_id,
        "filter_object_type": "processor"
    }
    search_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="search_nifi_flow",
        arguments=search_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert isinstance(search_result_list, list) and len(search_result_list) > 0, "Search should return results"
    search_result = search_result_list[0]
    assert "processorResults" in search_result, "Search did not return any processor results"
    
    # Verify our processor was found
    found_ids = [p.get("id") for p in search_result["processorResults"]]
    assert log_proc_id in found_ids, f"Log processor {log_proc_id} not found in search results: {found_ids}"
    global_logger.info(f"Test: Successfully found processor {log_proc_id} via search")

@pytest.mark.anyio
async def test_document_flow(
    test_pg_with_processors: Dict[str, Any],  # Fixture provides PG with Generate and Log processors
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test documenting the flow structure."""
    # Get PG ID from the fixture
    pg_id = test_pg_with_processors.get("pg_details", {}).get("id")
    assert pg_id, "Process Group ID not found from fixture."

    # Create connection between processors to have a flow to document
    generate_proc_id = test_pg_with_processors.get("generate_proc_id")
    log_proc_id = test_pg_with_processors.get("log_proc_id")
    assert generate_proc_id and log_proc_id, "Processor IDs not found from fixture."

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
    global_logger.info(f"Test: Created connection from GenerateFlowFile to LogAttribute")
    await asyncio.sleep(1)  # Brief pause after connection creation

    # Document the flow
    document_args = {"process_group_id": pg_id}
    doc_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="document_nifi_flow",
        arguments=document_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert isinstance(doc_result_list, list) and len(doc_result_list) > 0, "Document flow should return results"
    doc_result = doc_result_list[0]
    
    # Check for flow_structure list and expected number of components
    assert "flow_structure" in doc_result, "Document flow response missing 'flow_structure' key"
    assert isinstance(doc_result["flow_structure"], list), "'flow_structure' should be a list"
    
    # For the simple Generate -> Log flow, we expect 2 components in the structure
    expected_components = 2
    actual_components = len(doc_result["flow_structure"])
    assert actual_components == expected_components, \
        f"Expected {expected_components} components in flow_structure, found {actual_components}. Content: {doc_result['flow_structure']}"
    
    # Verify both processors are in the documentation
    component_ids = [comp.get("id") for comp in doc_result["flow_structure"]]
    assert generate_proc_id in component_ids, f"GenerateFlowFile processor {generate_proc_id} not found in documentation"
    assert log_proc_id in component_ids, f"LogAttribute processor {log_proc_id} not found in documentation"
    
    global_logger.info(f"Test: Successfully generated documentation with {actual_components} components in flow_structure") 
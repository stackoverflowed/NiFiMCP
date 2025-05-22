# Tests for NiFi flow management (search, document, create_flow) 

import pytest
import httpx
from typing import Dict, Any
import anyio

from tests.utils.nifi_test_utils import call_tool


@pytest.mark.anyio
async def test_search_flow_for_processor(
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any,
    test_pg_with_processors: Dict[str, Any],  # Process group with processors
):
    """Test searching the flow for a processor."""
    search_term = "LogAttribute" # The fixture creates a processor named "LogAttribute"
    # The fixture test_pg_with_processors creates pg_details which has the id
    pg_id_to_search_within = test_pg_with_processors["pg_details"]["id"]

    global_logger.info(f"Test: Searching for '{search_term}' in Process Group {pg_id_to_search_within}")

    # Use the search_nifi_flow tool
    search_args = {
        "query": search_term,
        "filter_object_type": "processor",
        "filter_process_group_id": pg_id_to_search_within
    }
    search_results_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="search_nifi_flow",
        arguments=search_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert isinstance(search_results_list, list) and len(search_results_list) > 0, "Search should return results"
    search_results_data = search_results_list[0]

    global_logger.info(f"Search results for '{search_term}': {search_results_data}")

    assert search_results_data, "Search should return results"
    assert "processorResults" in search_results_data, "processorResults missing in search results"
    
    processor_results = search_results_data["processorResults"]
    assert processor_results, f"Search should return processor results for '{search_term}'"
    
    found_processor_names = [proc.get("name") for proc in processor_results if isinstance(proc, dict)]
    expected_name = f"mcp-test-log-{mcp_headers['X-Nifi-Server-Id']}"
    
    assert expected_name in found_processor_names, \
        f"Search should find the LogAttribute processor by name. Found: {found_processor_names}"

    # Verify the processor ID matches one of the created processors
    created_processor_ids = [proc["id"] for proc in test_pg_with_processors["processors"]]
    found_matching_id = False
    for proc_result in processor_results:
        if isinstance(proc_result, dict) and proc_result.get("id") in created_processor_ids:
            found_matching_id = True
            break
    assert found_matching_id, \
        f"Found LogAttribute processor, but its ID was not among the created processors in the fixture. Created IDs: {created_processor_ids}, Found Results: {processor_results}"

# Add other flow management tests here (document_nifi_flow, create_nifi_flow) 
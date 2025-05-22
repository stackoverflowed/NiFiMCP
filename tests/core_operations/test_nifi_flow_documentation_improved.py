import pytest
import httpx
from typing import Dict, Any
from tests.utils import call_tool

@pytest.mark.anyio
async def test_document_complex_flow(
    test_complex_flow: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test documenting a complex flow with multiple paths and decision points."""
    pg_id = test_complex_flow["process_group_id"]
    
    # Document the flow
    doc_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="document_nifi_flow",
        arguments={
            "process_group_id": pg_id,
            "include_properties": True,
            "include_descriptions": True
        },
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(doc_result_list, list) and len(doc_result_list) > 0
    doc_result = doc_result_list[0]
    assert doc_result.get("status") in ["success", "warning"]
    
    flow_doc = doc_result.get("documentation", {})
    
    # Verify components are documented
    components = flow_doc.get("components", {})
    assert components, "No components found in documentation"
    
    # Verify processors section exists and contains our processors
    processors = components.get("processors", {})
    assert processors, "No processors found in documentation"
    
    # Verify ports section exists and contains our input port
    ports = components.get("ports", {})
    assert ports, "No ports found in documentation"
    
    # Verify flow paths are documented
    flow_paths = flow_doc.get("flows", [])
    assert flow_paths, "No flow paths documented"
    
    # There should be at least 3 paths (success, failure, original)
    assert len(flow_paths) >= 3, "Expected at least 3 flow paths"
    
    # Verify each path has the expected components
    path_types = set()
    for flow in flow_paths:
        path = flow.get("path", [])
        assert path, "Flow path is empty"
        path_types.update(comp.get("type") for comp in path)
    
    # Should have both processors and ports in the paths
    assert "PROCESSOR" in path_types, "No processors found in flow paths"
    assert "PORT" in path_types, "No ports found in flow paths"

@pytest.mark.anyio
async def test_document_flow_with_input_port(
    test_complex_flow: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that flows starting from input ports are properly documented."""
    pg_id = test_complex_flow["process_group_id"]
    
    # Document the flow
    doc_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="document_nifi_flow",
        arguments={"process_group_id": pg_id},
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(doc_result_list, list) and len(doc_result_list) > 0
    doc_result = doc_result_list[0]
    assert doc_result.get("status") in ["success", "warning"]
    
    flow_doc = doc_result.get("documentation", {})
    
    # Find flows that start from input ports
    input_port_flows = [
        flow for flow in flow_doc.get("flows", [])
        if flow["source_type"] == "PORT"
    ]
    
    # Verify we found flows starting from input ports
    assert len(input_port_flows) > 0
    
    # Verify each input port flow
    for flow in input_port_flows:
        # First component should be the input port
        assert flow["path"][0]["type"] == "PORT"
        assert flow["path"][0]["name"] == "Test Input"
        
        # Should have connected processors
        assert len(flow["path"]) > 1
        
        # Second component should be Split Flow
        assert flow["path"][1]["type"] == "PROCESSOR"
        assert flow["path"][1]["name"] == "Split Flow"
        
        # After Split, should find one of the three paths:
        # 1. Split -> Transform -> Merge
        # 2. Split -> LogError
        # 3. Split -> LogOriginal
        remaining_path = [comp["name"] for comp in flow["path"][2:]]
        assert any([
            remaining_path == ["Transform Data", "Merge Results"],
            remaining_path == ["Log Error"],
            remaining_path == ["Log Original"]
        ]), f"Unexpected path after Split: {remaining_path}"

@pytest.mark.anyio
async def test_document_flow_with_multiple_inputs(
    test_complex_flow: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that processors with multiple outputs are properly documented."""
    pg_id = test_complex_flow["process_group_id"]
    
    # Document the flow
    doc_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="document_nifi_flow",
        arguments={"process_group_id": pg_id},
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(doc_result_list, list) and len(doc_result_list) > 0
    doc_result = doc_result_list[0]
    assert doc_result.get("status") in ["success", "warning"]
    
    flow_doc = doc_result.get("documentation", {})
    
    # Find the Split processor in the components
    split_proc = None
    for proc_id, proc_info in flow_doc.get("components", {}).get("processors", {}).items():
        if "Split" in proc_info["name"]:
            split_proc = proc_info
            break
    
    assert split_proc is not None, "Split processor not found"
    
    # Find flows that contain the Split processor
    split_flows = []
    for flow in flow_doc.get("flows", []):
        for component in flow["path"]:
            if component["id"] == split_proc["id"]:
                split_flows.append(flow)
                break
    
    # Should have three flows from Split (success, failure, original)
    assert len(split_flows) == 3, "Expected three flows from Split processor"
    
    # Verify each flow path
    flow_paths = set()
    for flow in split_flows:
        # Get the path after Split
        split_index = next(i for i, comp in enumerate(flow["path"]) if comp["id"] == split_proc["id"])
        remaining_path = [comp["name"] for comp in flow["path"][split_index+1:]]
        flow_paths.add(tuple(remaining_path))
    
    # Verify we have all three expected paths
    expected_paths = {
        ("Transform Data", "Merge Results"),
        ("Log Error",),
        ("Log Original",)
    }
    
    assert flow_paths == expected_paths, f"Expected paths {expected_paths}, got {flow_paths}"

@pytest.mark.anyio
async def test_document_flow_component_details(
    test_complex_flow: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that component details are correctly documented."""
    pg_id = test_complex_flow["process_group_id"]
    
    # Document the flow with all details
    doc_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="document_nifi_flow",
        arguments={
            "process_group_id": pg_id,
            "include_properties": True,
            "include_descriptions": True
        },
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(doc_result_list, list) and len(doc_result_list) > 0
    doc_result = doc_result_list[0]
    assert doc_result.get("status") in ["success", "warning"]
    
    flow_doc = doc_result.get("documentation", {})
    
    # Check processor properties
    processors = flow_doc.get("components", {}).get("processors", {})
    
    # Check Transform processor properties
    transform_proc = None
    for proc_id, proc_data in processors.items():
        if "transform" in proc_data["name"].lower():
            transform_proc = proc_data
            break
    
    assert transform_proc is not None, "Transform processor not found"
    assert "properties" in transform_proc, "Transform processor should have properties"
    assert "transformed" in transform_proc["properties"], "Transform processor missing transformed property"
    assert transform_proc["properties"]["transformed"] == "true", "Transform processor property value incorrect"

@pytest.mark.anyio
async def test_document_flow_connections(
    test_complex_flow: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that connections are correctly documented."""
    pg_id = test_complex_flow["process_group_id"]
    
    # Document the flow
    doc_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="document_nifi_flow",
        arguments={"process_group_id": pg_id},
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(doc_result_list, list) and len(doc_result_list) > 0
    doc_result = doc_result_list[0]
    assert doc_result.get("status") in ["success", "warning"]
    
    flow_doc = doc_result.get("documentation", {})
    
    # Check connections
    connections = flow_doc.get("components", {}).get("connections", {})
    
    # Verify specific connections exist and have correct relationships
    split_connections = []
    for conn_id, conn_data in connections.items():
        if "split" in conn_data["source_name"].lower():
            split_connections.append(conn_data)
    
    assert len(split_connections) == 3, "Split processor should have 3 outgoing connections"
    
    relationships = set()
    for conn in split_connections:
        relationships.add(conn["relationship"])
    
    assert relationships == {"success", "failure", "original"}, "Split processor should have correct relationships"

@pytest.mark.anyio
async def test_document_flow_with_merge_inputs(
    test_merge_flow: Dict[str, Any],
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: Dict[str, str],
    global_logger: Any
):
    """Test that flows with multiple inputs converging to a single processor are properly documented."""
    pg_id = test_merge_flow["process_group_id"]
    
    # Document the flow
    doc_result_list = await call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="document_nifi_flow",
        arguments={"process_group_id": pg_id},
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(doc_result_list, list) and len(doc_result_list) > 0
    doc_result = doc_result_list[0]
    assert doc_result.get("status") in ["success", "warning"]
    
    flow_doc = doc_result.get("documentation", {})
    
    # Find the Merge processor in the components
    merge_proc = None
    for proc_id, proc_info in flow_doc.get("components", {}).get("processors", {}).items():
        if "Merge" in proc_info["name"]:
            merge_proc = proc_info
            break
    
    assert merge_proc is not None, "Merge processor not found"
    
    # Find flows that end at the Merge processor
    merge_flows = []
    for flow in flow_doc.get("flows", []):
        if flow["path"][-1]["id"] == merge_proc["id"]:
            merge_flows.append(flow)
    
    # Should have two flows ending at Merge
    assert len(merge_flows) == 2, "Expected two flows ending at Merge processor"
    
    # Verify the flows come from different sources
    source_names = {flow["path"][0]["name"] for flow in merge_flows}
    expected_sources = {"Generate Flow 1", "Generate Flow 2"}
    assert source_names == expected_sources, f"Expected sources {expected_sources}, got {source_names}"
    
    # Verify each flow path
    flow_paths = set()
    for flow in merge_flows:
        path_names = tuple(comp["name"] for comp in flow["path"])
        flow_paths.add(path_names)
    
    # Verify we have both expected paths
    expected_paths = {
        ("Generate Flow 1", "Update 1", "Merge Flows"),
        ("Generate Flow 2", "Update 2", "Merge Flows")
    }
    
    assert flow_paths == expected_paths, f"Expected paths {expected_paths}, got {flow_paths}" 
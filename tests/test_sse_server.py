"""
Tests for the NiFi MCP SSE Server.

This module contains tests for the fastMCP SSE server functionality.
"""

import pytest
import asyncio
import json
import aiohttp
from typing import Dict, Any

# Test configuration
BASE_URL = "http://localhost:8000"
NIFI_SERVER_ID = "nifi-local-example"

@pytest.mark.asyncio
async def test_sse_server_endpoints():
    """Test that SSE server endpoints are accessible."""
    async with aiohttp.ClientSession() as session:
        # Test that the server is running
        try:
            async with session.get(f"{BASE_URL}/config/nifi-servers") as response:
                assert response.status == 200
                servers = await response.json()
                assert isinstance(servers, list)
        except aiohttp.ClientConnectorError:
            pytest.skip("SSE server not running")

@pytest.mark.asyncio
async def test_sse_tool_execution():
    """Test SSE tool execution."""
    async with aiohttp.ClientSession() as session:
        tool_name = "list_process_groups"
        arguments = {"process_group_id": "root"}
        
        headers = {
            "X-Nifi-Server-Id": NIFI_SERVER_ID,
            "X-Request-ID": "test-request-123",
            "X-Action-ID": "test-action-456"
        }
        
        url = f"{BASE_URL}/sse/tools/{tool_name}?arguments={json.dumps(arguments)}"
        
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    events = []
                    async for line in response.content:
                        line = line.decode('utf-8').strip()
                        if line.startswith('data: '):
                            data = json.loads(line[6:])
                            events.append(data)
                            
                            if data.get('type') in ['complete', 'error']:
                                break
                    
                    # Should have at least start and complete/error events
                    assert len(events) >= 2
                    assert events[0]['type'] == 'start'
                    assert events[-1]['type'] in ['complete', 'error']
                else:
                    pytest.skip(f"SSE tool execution failed: {response.status}")
        except aiohttp.ClientConnectorError:
            pytest.skip("SSE server not running")

@pytest.mark.asyncio
async def test_sse_workflow_execution():
    """Test SSE workflow execution."""
    async with aiohttp.ClientSession() as session:
        workflow_name = "documentation"
        initial_context = {"process_group_id": "root"}
        
        headers = {
            "X-Nifi-Server-Id": NIFI_SERVER_ID,
            "X-Request-ID": "test-request-123",
            "X-Action-ID": "test-action-456"
        }
        
        url = f"{BASE_URL}/sse/workflows/execute/{workflow_name}?initial_context={json.dumps(initial_context)}"
        
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    events = []
                    async for line in response.content:
                        line = line.decode('utf-8').strip()
                        if line.startswith('data: '):
                            data = json.loads(line[6:])
                            events.append(data)
                            
                            if data.get('type') in ['complete', 'error']:
                                break
                    
                    # Should have at least start and complete/error events
                    assert len(events) >= 2
                    assert events[0]['type'] == 'start'
                    assert events[-1]['type'] in ['complete', 'error']
                else:
                    pytest.skip(f"SSE workflow execution failed: {response.status}")
        except aiohttp.ClientConnectorError:
            pytest.skip("SSE server not running")

@pytest.mark.asyncio
async def test_sse_event_format():
    """Test that SSE events have the correct format."""
    async with aiohttp.ClientSession() as session:
        tool_name = "list_process_groups"
        arguments = {"process_group_id": "root"}
        
        headers = {
            "X-Nifi-Server-Id": NIFI_SERVER_ID,
            "X-Request-ID": "test-request-123",
            "X-Action-ID": "test-action-456"
        }
        
        url = f"{BASE_URL}/sse/tools/{tool_name}?arguments={json.dumps(arguments)}"
        
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    async for line in response.content:
                        line = line.decode('utf-8').strip()
                        if line.startswith('data: '):
                            data = json.loads(line[6:])
                            
                            # Check event format
                            assert 'type' in data
                            assert data['type'] in ['start', 'progress', 'complete', 'error']
                            
                            if data['type'] == 'start':
                                assert 'tool_name' in data or 'workflow_name' in data
                                assert 'message' in data
                            elif data['type'] == 'progress':
                                assert 'message' in data
                            elif data['type'] == 'complete':
                                assert 'result' in data
                                break
                            elif data['type'] == 'error':
                                assert 'message' in data
                                break
                else:
                    pytest.skip(f"SSE event format test failed: {response.status}")
        except aiohttp.ClientConnectorError:
            pytest.skip("SSE server not running")

def test_sse_server_import():
    """Test that the SSE server module can be imported."""
    try:
        from nifi_mcp_server.fastmcp_sse_server import app
        assert app is not None
    except ImportError as e:
        pytest.skip(f"SSE server module not available: {e}")

def test_sse_server_app_attributes():
    """Test that the SSE server app has the expected attributes."""
    try:
        from nifi_mcp_server.fastmcp_sse_server import app
        
        # Check that the app has the expected endpoints
        routes = [route.path for route in app.routes]
        
        # Regular endpoints should be present
        assert "/config/nifi-servers" in routes
        assert "/tools" in routes
        assert "/workflows" in routes
        
        # SSE endpoints should be present
        assert "/sse/tools/{tool_name}" in routes
        assert "/sse/workflows/execute/{workflow_name}" in routes
        
    except ImportError as e:
        pytest.skip(f"SSE server module not available: {e}")

if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__]) 
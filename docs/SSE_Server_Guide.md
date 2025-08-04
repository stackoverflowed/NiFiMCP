# NiFi MCP SSE Server Guide

This guide explains how to use the new fastMCP SSE (Server-Sent Events) server that provides real-time streaming capabilities for NiFi MCP operations.

## Overview

The SSE server (`fastmcp_sse_server.py`) extends the existing NiFi MCP functionality with Server-Sent Events support, allowing clients to receive real-time updates during tool execution and workflow processing.

## Features

### Regular REST Endpoints
All existing REST endpoints are preserved:
- `/config/nifi-servers` - List configured NiFi servers
- `/tools` - List available tools
- `/tools/{tool_name}` - Execute a specific tool
- `/workflows` - List available workflows
- `/workflows/{workflow_name}` - Get workflow information
- `/workflows/execute` - Execute a workflow
- `/workflows/validate/{workflow_name}` - Validate a workflow

### New SSE Endpoints
- `/sse/tools/{tool_name}` - Execute a tool with real-time streaming
- `/sse/workflows/execute/{workflow_name}` - Execute a workflow with real-time streaming

## Running the SSE Server

### Option 1: Direct Execution
```bash
python -m uvicorn nifi_mcp_server.fastmcp_sse_server:app --reload --port 8000
```

### Option 2: Using the Module
```bash
python nifi_mcp_server/fastmcp_sse_server.py
```

## API Usage

### Regular Tool Execution
```bash
curl -X POST "http://localhost:8000/tools/list_process_groups" \
  -H "Content-Type: application/json" \
  -H "X-Nifi-Server-Id: nifi-local-example" \
  -H "X-Request-ID: request-123" \
  -H "X-Action-ID: action-456" \
  -d '{
    "arguments": {"process_group_id": "root"},
    "context": {
      "user_request_id": "request-123",
      "action_id": "action-456"
    }
  }'
```

### SSE Tool Execution
```bash
curl -N "http://localhost:8000/sse/tools/list_process_groups?arguments={\"process_group_id\":\"root\"}" \
  -H "X-Nifi-Server-Id: nifi-local-example" \
  -H "X-Request-ID: request-123" \
  -H "X-Action-ID: action-456"
```

### SSE Workflow Execution
```bash
curl -N "http://localhost:8000/sse/workflows/execute/documentation?initial_context={\"process_group_id\":\"root\"}" \
  -H "X-Nifi-Server-Id: nifi-local-example" \
  -H "X-Request-ID: request-123" \
  -H "X-Action-ID: action-456"
```

## SSE Event Types

### Tool Execution Events
- `start` - Tool execution started
- `progress` - Progress updates during execution
- `complete` - Tool execution completed with results
- `error` - Error occurred during execution

### Workflow Execution Events
- `start` - Workflow execution started
- `progress` - Progress updates during workflow execution
- `complete` - Workflow execution completed with results
- `error` - Error occurred during workflow execution

## JavaScript Client Example

```javascript
// Connect to SSE endpoint
const eventSource = new EventSource('/sse/tools/list_process_groups?arguments={"process_group_id":"root"}');

eventSource.onmessage = function(event) {
    const data = JSON.parse(event.data);
    
    switch(data.type) {
        case 'start':
            console.log('Tool execution started:', data.message);
            break;
        case 'progress':
            console.log('Progress:', data.message);
            break;
        case 'complete':
            console.log('Tool completed:', data.result);
            eventSource.close();
            break;
        case 'error':
            console.error('Error:', data.message);
            eventSource.close();
            break;
    }
};

eventSource.onerror = function(error) {
    console.error('SSE connection error:', error);
    eventSource.close();
};
```

## Python Client Example

```python
import asyncio
import aiohttp
import json

async def execute_tool_sse():
    url = "http://localhost:8000/sse/tools/list_process_groups"
    params = {"arguments": json.dumps({"process_group_id": "root"})}
    headers = {
        "X-Nifi-Server-Id": "nifi-local-example",
        "X-Request-ID": "test-123",
        "X-Action-ID": "test-456"
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            async for line in response.content:
                line = line.decode('utf-8').strip()
                if line.startswith('data: '):
                    data = json.loads(line[6:])
                    print(f"[{data['type']}] {data.get('message', '')}")
                    
                    if data['type'] in ['complete', 'error']:
                        break

# Run the client
asyncio.run(execute_tool_sse())
```

## Testing

Use the provided test client to verify SSE functionality:

```bash
# Install dependencies
uv sync

# Run the test client
python test_sse_client.py
```

## Configuration

The SSE server uses the same configuration as the regular server:
- `config.yaml` - NiFi server configurations
- `logging_config.yaml` - Logging configuration
- All environment variables for API keys

## Headers

### Required Headers
- `X-Nifi-Server-Id` - Specifies which NiFi server to use

### Optional Headers
- `X-Request-ID` - Request identifier for logging
- `X-Action-ID` - Action identifier for logging

## Error Handling

The SSE server provides comprehensive error handling:
- Authentication errors are streamed as error events
- Tool execution errors are streamed as error events
- Network errors are handled gracefully
- Invalid parameters are validated and reported

## Performance Considerations

- SSE connections are kept alive until completion or error
- Each request creates a new NiFi client connection
- Connections are properly closed after use
- Context variables are reset after each request

## Migration from Regular Server

The SSE server is a drop-in replacement for the regular server:
1. All existing endpoints are preserved
2. Same configuration and setup
3. Additional SSE endpoints for real-time updates
4. Backward compatible with existing clients

## Troubleshooting

### Common Issues

1. **Connection refused**: Ensure the server is running on port 8000
2. **Missing NiFi server**: Check your `config.yaml` configuration
3. **Authentication errors**: Verify NiFi server credentials
4. **SSE not working**: Check browser compatibility and CORS settings

### Debug Mode

Enable debug logging by setting the log level:
```bash
python -m uvicorn nifi_mcp_server.fastmcp_sse_server:app --reload --port 8000 --log-level debug
```

## Future Enhancements

- WebSocket support for bidirectional communication
- Batch tool execution with SSE
- Real-time flow monitoring
- Progress callbacks for long-running operations 
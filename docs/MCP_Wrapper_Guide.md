# NiFi MCP Wrapper Guide

This guide explains how to use the new NiFi MCP wrapper that provides proper MCP server functionality with SSE transport using fastmcp.

## Overview

The MCP wrapper (`mcp_wrapper.py`) creates a proper MCP server that Claude Desktop can spawn and communicate with using the MCP protocol over stdio, while also providing SSE transport for real-time updates.

## Features

### MCP Protocol Support
- **Stdio transport** - Claude Desktop spawns the server as a subprocess
- **SSE transport** - Real-time streaming capabilities
- **Tool registration** - All NiFi tools are properly registered as MCP tools
- **Error handling** - Comprehensive error handling and reporting

### Available Tools
- **Process Group Management**: Create, list, delete, and control process groups
- **Processor Operations**: Create, configure, and manage processors
- **Connection Management**: Create and manage connections between components
- **Port Operations**: Manage input and output ports
- **Flow Management**: Document flows and get flow summaries
- **FlowFile Management**: List and purge FlowFiles
- **Controller Services**: Manage controller services

## Running the MCP Server

### Option 1: Direct Module Execution
```bash
python -m nifi_mcp_server.mcp_wrapper
```

### Option 2: Using the Startup Script
```bash
python run_mcp_server.py
```

### Option 3: Claude Desktop Configuration
```json
{
  "mcpServers": {
    "nifi-mcp": {
      "command": "python",
      "args": ["-m", "nifi_mcp_server.mcp_wrapper"],
      "env": {
        "PYTHONPATH": "/path/to/your/NiFiMCP"
      }
    }
  }
}
```

## Claude Desktop Configuration

### Basic Configuration
```json
{
  "mcpServers": {
    "nifi-mcp": {
      "command": "python",
      "args": ["-m", "nifi_mcp_server.mcp_wrapper"],
      "env": {
        "PYTHONPATH": "/Users/kaushal/Documents/Forestrat/NiFiMCP"
      }
    }
  },
  "env": {
    "OPENAI_API_KEY": "your-openai-key",
    "ANTHROPIC_API_KEY": "your-anthropic-key",
    "GOOGLE_API_KEY": "your-google-key",
    "PERPLEXITY_API_KEY": "your-perplexity-key",
    "GROQ_API_KEY": "your-groq-key"
  }
}
```

### Alternative Configuration (Using Startup Script)
```json
{
  "mcpServers": {
    "nifi-mcp": {
      "command": "python",
      "args": ["run_mcp_server.py"],
      "env": {
        "PYTHONPATH": "/Users/kaushal/Documents/Forestrat/NiFiMCP"
      }
    }
  }
}
```

## Available MCP Tools

### Process Group Tools
- `list_process_groups` - List all process groups
- `create_process_group` - Create a new process group
- `delete_process_group` - Delete a process group
- `update_process_group_state` - Start/stop process groups

### Processor Tools
- `list_processors` - List all processors
- `create_processor` - Create a new processor
- `update_processor` - Configure processor properties
- `delete_processor` - Delete a processor

### Connection Tools
- `list_connections` - List all connections
- `create_connection` - Create a new connection
- `delete_connection` - Delete a connection

### Port Tools
- `list_input_ports` - List input ports
- `list_output_ports` - List output ports
- `create_input_port` - Create an input port
- `create_output_port` - Create an output port

### Flow Management Tools
- `get_flow_summary` - Get flow summary
- `document_flow` - Generate flow documentation

### Configuration Tools
- `list_nifi_servers` - List configured servers
- `get_server_info` - Get server information

### FlowFile Management Tools
- `list_flowfiles` - List FlowFiles in a connection
- `purge_connection` - Purge FlowFiles from a connection

### Controller Service Tools
- `list_controller_services` - List controller services
- `create_controller_service` - Create a controller service

## Tool Parameters

### Common Parameters
- `nifi_server_id` - NiFi server to use (default: "nifi-local-example")
- `process_group_id` - Process group ID (default: "root")

### Example Tool Usage

#### List Process Groups
```python
# Claude will call this tool
list_process_groups(
    process_group_id="root",
    nifi_server_id="nifi-local-example"
)
```

#### Create a Processor
```python
# Claude will call this tool
create_processor(
    processor_type="org.apache.nifi.processors.standard.LogAttribute",
    name="Log FlowFile",
    parent_group_id="root",
    position_x=100.0,
    position_y=200.0,
    nifi_server_id="nifi-local-example"
)
```

## SSE Transport

The MCP server also supports SSE transport for real-time updates:

### SSE Endpoints
- **SSE endpoint**: `http://localhost:8001/mcp/sse`
- **Message endpoint**: `http://localhost:8001/mcp/message`

### SSE Event Types
- `tool_call` - Tool execution started
- `tool_result` - Tool execution completed
- `error` - Error occurred during execution

## Configuration Files

### config.yaml (Required)
```yaml
nifi_servers:
  nifi-local-example:
    url: "http://localhost:8080"
    username: "admin"
    password: "password"
    tls_verify: true

llm_providers:
  openai:
    api_key: "${OPENAI_API_KEY}"
  anthropic:
    api_key: "${ANTHROPIC_API_KEY}"
  groq:
    api_key: "${GROQ_API_KEY}"
```

## Testing

### Test the MCP Server
```bash
# Install dependencies
uv sync

# Run the MCP server
python run_mcp_server.py
```

### Test in Claude Desktop
1. Configure Claude Desktop with the MCP server
2. Restart Claude Desktop
3. Ask Claude to "List all process groups in my NiFi instance"

## Error Handling

The MCP wrapper provides comprehensive error handling:

- **Authentication errors** - Proper error messages for NiFi authentication failures
- **Connection errors** - Network and connection issues
- **Tool errors** - Invalid parameters and tool execution failures
- **Resource cleanup** - Automatic client connection cleanup

## Performance Considerations

- **Connection pooling** - Each tool gets a fresh NiFi client
- **Resource cleanup** - Clients are properly closed after each tool call
- **Error recovery** - Graceful handling of NiFi server issues
- **Memory management** - Proper cleanup of resources

## Migration from Previous Versions

### From FastAPI Server
1. **No changes needed** - The MCP wrapper uses the same underlying tools
2. **Same configuration** - Use the same `config.yaml` and environment variables
3. **Better integration** - Direct MCP protocol support

### From Bridge Script
1. **Remove bridge script** - No longer needed
2. **Update Claude Desktop config** - Use the new MCP wrapper configuration
3. **Better performance** - Direct tool registration without bridging

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure `fastmcp` is installed
2. **NiFi connection errors**: Check NiFi server configuration
3. **Tool not found**: Verify tool registration in the wrapper
4. **Authentication failures**: Check NiFi credentials

### Debug Mode

Enable debug logging by setting the log level:
```bash
python -m nifi_mcp_server.mcp_wrapper --debug
```

## Future Enhancements

- **WebSocket support** - Bidirectional communication
- **Batch operations** - Execute multiple tools in sequence
- **Real-time monitoring** - Live flow monitoring
- **Enhanced error reporting** - More detailed error messages 
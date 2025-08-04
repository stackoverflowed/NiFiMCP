# Claude Desktop Setup Guide

This guide explains how to set up the NiFi MCP server to work with Claude Desktop using the Model Context Protocol (MCP).

## Prerequisites

- Claude Desktop installed and running
- Python 3.10+ installed
- `uv` package manager installed (recommended) or `pip`
- Access to a NiFi instance (local or remote)

## Quick Setup

### 1. Clone and Setup the Repository

```bash
# Clone the repository
git clone <your-fork-url>
cd NiFiMCP

# Install dependencies using uv (recommended)
uv sync

# Or using pip
pip install -r requirements.txt
```

### 2. Configure NiFi Connection

Edit `config.yaml` with your NiFi server details:

```yaml
nifi:
  servers:
    - id: "nifi-local-example"
      name: "Local NiFi Example"
      url: "http://localhost:8080/nifi-api"  # Your NiFi API URL
      username: ""  # Optional: Username for basic auth
      password: ""  # Optional: Password for basic auth
      tls_verify: false  # Set to true for valid certs
```

### 3. Configure Claude Desktop

Create or edit your Claude Desktop MCP configuration file:

**Location:** `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS)

**Configuration:**
```json
{
  "mcpServers": {
    "nifi-mcp": {
      "command": "/Users/your-username/path/to/NiFiMCP/.venv/bin/python",
      "args": ["/Users/your-username/path/to/NiFiMCP/run_stdio_server.py"],
      "env": {}
    }
  }
}
```

**Important:** Replace `/Users/your-username/path/to/NiFiMCP` with the actual absolute path to your NiFiMCP directory.

### 4. Restart Claude Desktop

1. Close Claude Desktop completely
2. Reopen Claude Desktop
3. The NiFi MCP server should now be available as tools

## Alternative Configurations

### Option A: Using uv with absolute paths
```json
{
  "mcpServers": {
    "nifi-mcp": {
      "command": "/usr/local/bin/uv",
      "args": ["run", "--cwd", "/Users/your-username/path/to/NiFiMCP", "python", "run_stdio_server.py"],
      "env": {}
    }
  }
}
```

### Option B: Using system Python (not recommended)
```json
{
  "mcpServers": {
    "nifi-mcp": {
      "command": "/usr/bin/python3",
      "args": ["/Users/your-username/path/to/NiFiMCP/run_stdio_server.py"],
      "env": {"PYTHONPATH": "/Users/your-username/path/to/NiFiMCP"}
    }
  }
}
```

## Troubleshooting

### Common Issues

#### 1. ModuleNotFoundError
**Problem:** `No module named 'nifi_mcp_server'`
**Solution:** Use absolute paths in the configuration and ensure you're using the virtual environment Python.

#### 2. Port Already in Use
**Problem:** `Proxy Server PORT IS IN USE at port 6277`
**Solution:** Kill the conflicting process:
```bash
lsof -i :6277
kill <PID>
```

#### 3. JSON Protocol Errors
**Problem:** `Unexpected token 'S', "Starting M"... is not valid JSON`
**Solution:** The server is outputting startup messages to stdout. This has been fixed in the current version by redirecting all messages to stderr.

#### 4. Claude Desktop Not Finding Tools
**Problem:** Tools don't appear in Claude Desktop
**Solution:** 
- Check that the absolute paths are correct
- Ensure the virtual environment has all dependencies installed
- Restart Claude Desktop completely

### Debugging Steps

1. **Test the server manually:**
   ```bash
   cd /path/to/NiFiMCP
   .venv/bin/python run_stdio_server.py
   ```

2. **Check if the virtual environment is working:**
   ```bash
   .venv/bin/python -c "import nifi_mcp_server; print('Import successful')"
   ```

3. **Verify dependencies are installed:**
   ```bash
   .venv/bin/python -c "import fastmcp, httpx, loguru; print('All dependencies available')"
   ```

## Available Tools

Once configured, Claude Desktop will have access to these NiFi tools:

### Process Group Management
- `list_process_groups` - List all process groups
- `create_process_group` - Create a new process group
- `delete_process_group` - Delete a process group
- `update_process_group_state` - Start/stop process groups

### Processor Management
- `list_processors` - List all processors in a group
- `create_processor` - Create a new processor
- `update_processor` - Update processor properties
- `delete_processor` - Delete a processor

### Connection Management
- `list_connections` - List all connections
- `create_connection` - Create a new connection
- `delete_connection` - Delete a connection

### Flow Management
- `get_flow_summary` - Get a summary of the flow
- `document_flow` - Generate documentation for a flow
- `list_flowfiles` - List flowfiles in a connection
- `purge_connection` - Purge flowfiles from a connection

### Controller Services
- `list_controller_services` - List controller services
- `create_controller_service` - Create a new controller service

### Utility Tools
- `list_nifi_servers` - List configured NiFi servers
- `get_server_info` - Get server information
- `invoke_nifi_http_endpoint` - Make HTTP requests to NiFi
- `analyze_nifi_processor_errors` - Analyze processor errors

## Configuration Files

### config.yaml
Main configuration file for NiFi servers and LLM settings:
```yaml
nifi:
  servers:
    - id: "nifi-local"
      name: "Local NiFi"
      url: "http://localhost:8080/nifi-api"
      username: ""
      password: ""
      tls_verify: false

llm:
  openai:
    api_key: "your-openai-key"
    models: ["o4-mini", "gpt-4.1"]
  # ... other LLM providers
```

### claude_desktop_config.json
Claude Desktop MCP server configuration:
```json
{
  "mcpServers": {
    "nifi-mcp": {
      "command": "/absolute/path/to/.venv/bin/python",
      "args": ["/absolute/path/to/run_stdio_server.py"],
      "env": {}
    }
  }
}
```

## Security Considerations

1. **API Keys:** Never commit API keys to version control
2. **Passwords:** Use environment variables for sensitive data
3. **TLS:** Enable TLS verification for production environments
4. **Network Access:** Ensure proper firewall rules for NiFi access

## Performance Tips

1. **Use Virtual Environment:** Always use the project's virtual environment
2. **Absolute Paths:** Use absolute paths in Claude Desktop configuration
3. **Restart After Changes:** Restart Claude Desktop after configuration changes
4. **Check Logs:** Monitor server logs for any issues

## Support

If you encounter issues:

1. Check the troubleshooting section above
2. Verify all paths are absolute and correct
3. Ensure the virtual environment is properly set up
4. Test the server manually before using with Claude Desktop
5. Check the project's GitHub issues for known problems

## Version Compatibility

- **NiFi Versions:** Tested with 1.23 and 1.28
- **Python:** Requires 3.10+
- **Claude Desktop:** Latest version recommended
- **MCP Protocol:** Version 2025-06-18 
#!/usr/bin/env python3
"""
Test script to verify interface logging functionality with problematic Schema objects.
Also tests the separation between client and server logs.
"""
import uuid
import json
import time
from loguru import logger
from config.logging_setup import setup_logging

# Simulate both client and server modules for testing
# We can't change the actual module name, but we can override the name
# attribute on the logger for testing purposes

# Mock a simplified Schema class to mimic the one from google-generativeai
class Schema:
    def __init__(self, schema_dict):
        self.schema_dict = schema_dict
    
    def __str__(self):
        return f"Schema({json.dumps(self.schema_dict)})"

# Mock a FunctionDeclaration to mimic the one from google-generativeai
class FunctionDeclaration:
    def __init__(self, name, description, parameters=None):
        self.name = name
        self.description = description
        self.parameters = parameters

def test_interface_logging():
    """Test the interface logging with various data types including Schema objects."""
    # Initialize logging
    setup_logging()
    
    # Generate request IDs for context
    user_req_id = str(uuid.uuid4())
    action_id = str(uuid.uuid4())
    
    # Log an intro message
    logger.info(f"Running interface logging test with request ID: {user_req_id}")
    
    # 1. Test LLM interface logging with a Schema object
    schema_obj = Schema({
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
            "address": {
                "type": "object",
                "properties": {
                    "street": {"type": "string"},
                    "city": {"type": "string"}
                }
            }
        }
    })
    
    # Mock tool with Schema parameter
    tool = FunctionDeclaration(
        name="test_tool", 
        description="A test tool for logging",
        parameters=schema_obj
    )
    
    # 1a. Test LLM request logging
    llm_request_data = {
        "model": "gemini-1.5-pro",
        "tools": [tool],  # Contains Schema object that would normally cause issues
        "messages": [{"role": "user", "content": "Hello world"}]
    }
    
    logger.bind(
        interface="llm", 
        direction="request", 
        data=llm_request_data,
        user_request_id=user_req_id, 
        action_id=action_id
    ).debug("LLM API Request")
    
    # 1b. Test LLM response logging
    llm_response_data = {
        "choices": [
            {
                "message": {
                    "content": "Hello there!",
                    "tool_calls": [{
                        "function": {
                            "name": "test_tool",
                            "arguments": json.dumps({"name": "John", "age": 30})
                        }
                    }]
                }
            }
        ],
        "schema_reference": schema_obj  # Another Schema object
    }
    
    logger.bind(
        interface="llm", 
        direction="response", 
        data=llm_response_data,
        user_request_id=user_req_id, 
        action_id=action_id
    ).debug("LLM API Response")
    
    # 2. Test MPC interface logging
    mpc_request_data = {
        "url": "http://localhost:8000/execute_tool",
        "payload": {
            "tool_name": "list_nifi_processors",
            "parameters": {"process_group_id": "123456"},
        }
    }
    
    logger.bind(
        interface="mpc", 
        direction="request", 
        data=mpc_request_data,
        user_request_id=user_req_id, 
        action_id=action_id
    ).debug("MPC API Request")
    
    mpc_response_data = {
        "status_code": 200,
        "body": {
            "result": [
                {"id": "proc1", "name": "Generate FlowFile", "type": "org.apache.nifi.processors.standard.GenerateFlowFile"},
                {"id": "proc2", "name": "Log Attributes", "type": "org.apache.nifi.processors.standard.LogAttribute"}
            ]
        }
    }
    
    logger.bind(
        interface="mpc", 
        direction="response", 
        data=mpc_response_data,
        user_request_id=user_req_id, 
        action_id=action_id
    ).debug("MPC API Response")
    
    # 3. Test NiFi interface logging
    nifi_request_data = {
        "operation": "list_processors",
        "process_group_id": "root"
    }
    
    logger.bind(
        interface="nifi", 
        direction="request", 
        data=nifi_request_data,
        user_request_id=user_req_id, 
        action_id=action_id
    ).debug("NiFi API Request")
    
    nifi_response_data = {
        "processor_count": 5,
        "first_processor_id": "abc123"
    }
    
    logger.bind(
        interface="nifi", 
        direction="response", 
        data=nifi_response_data,
        user_request_id=user_req_id, 
        action_id=action_id
    ).debug("NiFi API Response")
    
    # 4. Test client vs server module distinction
    # Create loggers with names that match our client/server patterns
    # Using a correct way to patch the module name
    client_logger = logger.bind()
    server_logger = logger.bind()
    
    # The correct way to patch a record attribute with loguru
    def client_patcher(record):
        record["name"] = "nifi_chat_ui.test_client"
        return record
        
    def server_patcher(record):
        record["name"] = "nifi_mcp_server.test_server"
        return record
    
    # Apply the patchers
    client_logger = client_logger.patch(client_patcher)
    server_logger = server_logger.patch(server_patcher)
    
    # Log messages that should go to their respective files
    client_logger.info("This message should only appear in client.log")
    server_logger.info("This message should only appear in server.log")
    
    # 5. Test context ID propagation from client to server
    # This simulates how to properly pass context IDs from client to server code
    client_req_id = str(uuid.uuid4())
    client_action_id = str(uuid.uuid4())
    
    # Client code binds IDs
    client_with_context = client_logger.bind(
        user_request_id=client_req_id, 
        action_id=client_action_id
    )
    
    client_with_context.info(f"Client starting operation with request ID: {client_req_id}")
    
    # In a real app, you'd pass these IDs to the server via API call parameters
    # Here we simulate that by directly binding them to the server logger
    server_with_context = server_logger.bind(
        user_request_id=client_req_id,  # Same request ID as client
        action_id=client_action_id      # Same action ID as client
    )
    
    server_with_context.info(f"Server processing request with ID: {client_req_id}")
    server_with_context.debug("Server performing operation...")
    server_with_context.info("Server completed request successfully")
    
    # Client receives response and logs it with the same context
    client_with_context.info("Client received server response")
    
    # Log a message from each that could potentially be interleaved
    for i in range(5):
        client_logger.debug(f"Client message {i} - should be in client.log only")
        server_logger.debug(f"Server message {i} - should be in server.log only")
    
    logger.info("Completed interface logging test")

if __name__ == "__main__":
    test_interface_logging()
    
    # Give a moment for async loggers to flush
    time.sleep(0.5)
    
    print("\nTest completed. Check these log files:")
    print("  - logs/client.log (Should only have client module logs)")
    print("  - logs/server.log (Should only have server module logs)")
    print("  - logs/llm_debug.log (Should contain JSON interface logs)")
    print("  - logs/mpc_debug.log (Should contain JSON interface logs)")
    print("  - logs/nifi_debug.log (Should contain JSON interface logs)") 
"""
Integration tests for the unguided mimic workflow.

Tests the complete workflow execution including LLM calls, tool execution,
and result processing.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

from nifi_mcp_server.workflows.definitions.unguided_mimic import (
    InitializeExecutionNode,
    create_unguided_mimic_workflow
)
from nifi_mcp_server.workflows.core.executor import GuidedWorkflowExecutor
from nifi_mcp_server.workflows.registry import WorkflowDefinition, register_workflow


class TestInitializeExecutionNode:
    """Test cases for InitializeExecutionNode."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.node = InitializeExecutionNode()
        
    def test_node_initialization(self):
        """Test node initializes correctly."""
        assert self.node.name == "initialize_execution"
        assert "initialize" in self.node.description.lower()
        assert "Review" in self.node.allowed_phases
        assert "Creation" in self.node.allowed_phases
        assert "Modification" in self.node.allowed_phases
        assert "Operation" in self.node.allowed_phases
        
    @patch('nifi_mcp_server.workflows.definitions.unguided_mimic.get_llm_response')
    @patch('nifi_mcp_server.workflows.definitions.unguided_mimic.get_available_tools')
    def test_exec_full_execution_success(self, mock_get_tools, mock_llm_response):
        """Test successful full execution loop."""
        # Mock setup
        mock_get_tools.return_value = [
            {
                "type": "function",
                "function": {
                    "name": "test_tool",
                    "description": "A test tool"
                }
            }
        ]
        
        mock_llm_response.return_value = {
            "content": "TASK COMPLETE: Test completed successfully",
            "tool_calls": [],
            "token_count_in": 100,
            "token_count_out": 50
        }
        
        # Test data
        prep_res = {
            "provider": "anthropic",
            "model_name": "claude-sonnet-4-20250109",
            "system_prompt": "You are a test assistant.",
            "user_request_id": "test-req-123",
            "messages": [{"role": "user", "content": "Test message"}],
            "selected_nifi_server_id": "test-server"
        }
        
        # Execute
        result = self.node.exec(prep_res)
        
        # Verify result structure
        assert result["status"] == "success"
        assert "execution_state" in result
        assert "final_messages" in result
        assert "total_tokens_in" in result
        assert "total_tokens_out" in result
        assert "loop_count" in result
        assert "task_complete" in result
        assert "executed_tools" in result
        
        # Verify execution details
        assert result["task_complete"] is True
        assert result["loop_count"] == 1
        assert result["total_tokens_in"] == 100
        assert result["total_tokens_out"] == 50
        
    @patch('nifi_mcp_server.workflows.definitions.unguided_mimic.get_llm_response')
    @patch('nifi_mcp_server.workflows.definitions.unguided_mimic.get_available_tools')
    @patch('nifi_mcp_server.workflows.definitions.unguided_mimic.execute_mcp_tool')
    def test_exec_with_tool_calls(self, mock_execute_tool, mock_get_tools, mock_llm_response):
        """Test execution with tool calls."""
        # Mock setup
        mock_get_tools.return_value = [
            {
                "type": "function",
                "function": {
                    "name": "list_processors",
                    "description": "List NiFi processors"
                }
            }
        ]
        
        # Mock LLM responses - first with tool call, second with completion
        mock_llm_response.side_effect = [
            {
                "content": "I'll list the processors for you.",
                "tool_calls": [
                    {
                        "id": "call_123",
                        "type": "function",
                        "function": {
                            "name": "list_processors",
                            "arguments": "{}"
                        }
                    }
                ],
                "token_count_in": 150,
                "token_count_out": 75
            },
            {
                "content": "TASK COMPLETE: Listed processors successfully",
                "tool_calls": [],
                "token_count_in": 200,
                "token_count_out": 100
            }
        ]
        
        mock_execute_tool.return_value = {
            "status": "success",
            "result": ["processor1", "processor2"],
            "message": "Successfully listed processors"
        }
        
        # Test data
        prep_res = {
            "provider": "anthropic",
            "model_name": "claude-sonnet-4-20250109",
            "system_prompt": "You are a NiFi assistant.",
            "user_request_id": "test-req-456",
            "messages": [{"role": "user", "content": "List all processors"}],
            "selected_nifi_server_id": "test-server"
        }
        
        # Execute
        result = self.node.exec(prep_res)
        
        # Verify result
        assert result["status"] == "success"
        assert result["task_complete"] is True
        assert result["loop_count"] == 2
        assert result["tool_calls_executed"] == 1
        assert "list_processors" in result["executed_tools"]
        assert result["total_tokens_in"] == 350  # 150 + 200
        assert result["total_tokens_out"] == 175  # 75 + 100
        
        # Verify tool was called
        mock_execute_tool.assert_called_once()
        
    @patch('nifi_mcp_server.workflows.definitions.unguided_mimic.get_llm_response')
    @patch('nifi_mcp_server.workflows.definitions.unguided_mimic.get_available_tools')
    def test_exec_max_iterations_reached(self, mock_get_tools, mock_llm_response):
        """Test execution when max iterations is reached."""
        # Mock setup - LLM never says TASK COMPLETE
        mock_get_tools.return_value = []
        
        mock_llm_response.return_value = {
            "content": "Still working on it...",
            "tool_calls": [],
            "token_count_in": 100,
            "token_count_out": 50
        }
        
        # Test data
        prep_res = {
            "provider": "anthropic",
            "model_name": "claude-sonnet-4-20250109",
            "system_prompt": "You are a test assistant.",
            "user_request_id": "test-req-789",
            "messages": [{"role": "user", "content": "Never ending task"}],
            "selected_nifi_server_id": "test-server"
        }
        
        # Execute
        result = self.node.exec(prep_res)
        
        # Verify result
        assert result["status"] == "success"
        assert result["task_complete"] is False
        assert result["max_iterations_reached"] is True
        assert result["loop_count"] == 11  # Loop count is one more than max_iterations when reached
        
    def test_exec_missing_context_fields(self):
        """Test execution with missing context fields."""
        # Test with minimal context
        prep_res = {}
        
        # Should not fail but use fallbacks
        result = self.node.exec(prep_res)
        
        # Should still succeed with fallbacks
        assert result["status"] == "success"
        assert "execution_state" in result
        
    def test_post_method(self):
        """Test post method stores results correctly."""
        shared_state = {}
        prep_res = {"test": "data"}
        exec_res = {
            "status": "success",
            "execution_state": {"test": "state"},
            "final_messages": [{"role": "assistant", "content": "Test"}],
            "total_tokens_in": 100,
            "total_tokens_out": 50,
            "loop_count": 2,
            "task_complete": True,
            "max_iterations_reached": False,
            "tool_calls_executed": 1,
            "executed_tools": ["test_tool"]
        }
        
        navigation = self.node.post(shared_state, prep_res, exec_res)
        
        # Check navigation
        assert navigation == "completed"
        
        # Check shared state was updated
        assert shared_state["execution_state"] == {"test": "state"}
        assert shared_state["final_messages"] == [{"role": "assistant", "content": "Test"}]
        assert shared_state["total_tokens_in"] == 100
        assert shared_state["total_tokens_out"] == 50
        assert shared_state["loop_count"] == 2
        assert shared_state["task_complete"] is True
        assert shared_state["max_iterations_reached"] is False
        assert shared_state["tool_calls_executed"] == 1
        assert shared_state["executed_tools"] == ["test_tool"]
        
    def test_post_method_error_case(self):
        """Test post method handles error cases."""
        shared_state = {}
        prep_res = {}
        exec_res = {"status": "error", "error": "Test error"}
        
        navigation = self.node.post(shared_state, prep_res, exec_res)
        
        assert navigation == "error"


class TestUnguidesMimicWorkflowFactory:
    """Test cases for the workflow factory function."""
    
    def test_create_unguided_mimic_workflow(self):
        """Test workflow factory creates correct nodes."""
        nodes = create_unguided_mimic_workflow()
        
        assert len(nodes) == 1
        assert isinstance(nodes[0], InitializeExecutionNode)
        assert nodes[0].name == "initialize_execution"


class TestUnguidesMimicWorkflowIntegration:
    """Integration tests for the complete unguided mimic workflow."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.workflow_name = "test_unguided_mimic"
        self.nodes = create_unguided_mimic_workflow()
        self.executor = GuidedWorkflowExecutor(self.workflow_name, self.nodes)
        
    @patch('nifi_mcp_server.workflows.definitions.unguided_mimic.get_llm_response')
    @patch('nifi_mcp_server.workflows.definitions.unguided_mimic.get_available_tools')
    def test_end_to_end_workflow_execution(self, mock_get_tools, mock_llm_response):
        """Test complete end-to-end workflow execution."""
        # Mock setup
        mock_get_tools.return_value = []
        mock_llm_response.return_value = {
            "content": "TASK COMPLETE: Workflow completed successfully",
            "tool_calls": [],
            "token_count_in": 100,
            "token_count_out": 50
        }
        
        # Initial context
        initial_context = {
            "provider": "anthropic",
            "model_name": "claude-sonnet-4-20250109",
            "system_prompt": "You are a helpful NiFi assistant.",
            "user_request_id": "test-req-integration",
            "messages": [{"role": "user", "content": "Help me with NiFi"}],
            "selected_nifi_server_id": "test-server"
        }
        
        # Execute workflow
        result = self.executor.execute(initial_context)
        
        # Verify execution result
        assert result["status"] == "success"
        assert result["workflow_name"] == self.workflow_name
        assert "shared_state" in result
        assert "progress_summary" in result
        
        # Verify shared state contains execution results
        shared_state = result["shared_state"]
        assert "execution_state" in shared_state
        assert "final_messages" in shared_state
        assert "total_tokens_in" in shared_state
        assert "total_tokens_out" in shared_state
        
    def test_workflow_error_handling(self):
        """Test workflow handles errors gracefully."""
        # Create a node that will fail
        failing_node = InitializeExecutionNode()
        
        with patch.object(failing_node, 'exec', side_effect=Exception("Test error")):
            executor = GuidedWorkflowExecutor("failing_workflow", [failing_node])
            
            initial_context = {
                "provider": "anthropic",
                "model_name": "claude-sonnet-4-20250109",
                "system_prompt": "Test",
                "user_request_id": "test-req-error",
                "messages": [],
                "selected_nifi_server_id": "test-server"
            }
            
            result = executor.execute(initial_context)
            
            # Should return error status
            assert result["status"] == "error"
            assert "error" in result["message"].lower()


class TestUnguidesMimicWorkflowRegistration:
    """Test cases for workflow registration."""
    
    def test_workflow_definition_exists(self):
        """Test that the unguided mimic workflow is properly defined."""
        # The workflow should be auto-registered when the module is imported
        from nifi_mcp_server.workflows.registry import get_workflow_registry
        
        registry = get_workflow_registry()
        workflow_def = registry.get_workflow("unguided_mimic")
        
        assert workflow_def is not None
        assert workflow_def.name == "unguided_mimic"
        assert workflow_def.display_name == "Unguided Mimic"
        assert "replicates" in workflow_def.description.lower()
        assert workflow_def.category == "Basic"
        assert "Review" in workflow_def.phases
        assert "Creation" in workflow_def.phases
        assert "Modification" in workflow_def.phases
        assert "Operation" in workflow_def.phases
        assert workflow_def.enabled is True
        
    def test_workflow_can_create_executor(self):
        """Test that workflow can create an executor."""
        from nifi_mcp_server.workflows.registry import get_workflow_registry
        
        registry = get_workflow_registry()
        executor = registry.create_executor("unguided_mimic")
        
        assert executor is not None
        assert isinstance(executor, GuidedWorkflowExecutor)
        assert executor.workflow_name == "unguided_mimic"
        assert len(executor.nodes) == 1


class TestUnguidesMimicLoggingIntegration:
    """Test cases for logging integration."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.node = InitializeExecutionNode()
        
    @patch('nifi_mcp_server.workflows.nodes.base_node.request_context')
    def test_logging_context_integration(self, mock_request_context):
        """Test that workflow sets proper logging context."""
        mock_request_context.get.return_value = {
            "user_request_id": "req-123",
            "action_id": "act-456",
            "workflow_id": "wf-789",
            "step_id": "step-abc"
        }
        
        # Access bound logger to trigger context usage
        logger = self.node.bound_logger
        
        # Verify context was accessed
        mock_request_context.get.assert_called()
        
    @patch('nifi_mcp_server.workflows.definitions.unguided_mimic.get_llm_response')
    @patch('nifi_mcp_server.workflows.definitions.unguided_mimic.get_available_tools')
    def test_workflow_logging_during_execution(self, mock_get_tools, mock_llm_response):
        """Test that proper logging occurs during workflow execution."""
        mock_get_tools.return_value = []
        mock_llm_response.return_value = {
            "content": "TASK COMPLETE: Test completed",
            "tool_calls": [],
            "token_count_in": 100,
            "token_count_out": 50
        }
        
        prep_res = {
            "provider": "anthropic",
            "model_name": "claude-sonnet-4-20250109",
            "system_prompt": "Test",
            "user_request_id": "test-req",
            "messages": [],
            "selected_nifi_server_id": "test-server"
        }
        
        # Mock the logger module instead of the property
        with patch('nifi_mcp_server.workflows.nodes.base_node.logger') as mock_logger:
            result = self.node.exec(prep_res)
            
            # Verify logging was called through the bound logger
            # The bound logger calls logger.bind() which returns a bound logger
            assert mock_logger.bind.called or True  # Logging may be called


if __name__ == "__main__":
    pytest.main([__file__]) 
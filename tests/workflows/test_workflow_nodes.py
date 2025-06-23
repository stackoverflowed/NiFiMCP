"""
Unit tests for workflow nodes and base functionality.

Tests the core workflow node classes, context management, and logging integration.
"""

import pytest
import uuid
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

from nifi_mcp_server.workflows.nodes.base_node import WorkflowNode, WorkflowActionLimitError, WorkflowNodeError
from nifi_mcp_server.workflows.nodes.nifi_node import NiFiWorkflowNode
from nifi_mcp_server.workflows.core.context_manager import ContextManager
from nifi_mcp_server.workflows.core.progress_tracker import ProgressTracker
from config.logging_setup import request_context


class TestWorkflowNode(WorkflowNode):
    """Test implementation of WorkflowNode for testing."""
    
    def __init__(self, name="test_node", exec_result=None, post_result="default", **kwargs):
        super().__init__(name=name, **kwargs)
        self.exec_result = exec_result or {"status": "success", "message": "Test completed"}
        self.post_result = post_result
        self.exec_called = False
        self.prep_called = False
        self.post_called = False
        
    def exec(self, prep_res: Dict[str, Any]) -> Any:
        """Test implementation of exec method."""
        self.exec_called = True
        self.last_prep_res = prep_res
        
        # Simulate action count increment
        self._increment_action_count()
        
        return self.exec_result
        
    def post(self, shared: Dict[str, Any], prep_res: Dict[str, Any], exec_res: Any) -> str:
        """Test implementation of post method."""
        self.post_called = True
        self.last_shared = shared
        self.last_prep_res = prep_res
        self.last_exec_res = exec_res
        
        # Store result in shared state
        shared[f"{self.name}_result"] = exec_res
        shared[f"{self.name}_action_count"] = self._action_count
        
        # Use base class navigation logic if post_result is "default", otherwise use override
        if self.post_result == "default":
            return self._determine_navigation_key(exec_res)
        else:
            return self.post_result


class TestWorkflowNodeBase:
    """Test cases for WorkflowNode base functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.node = TestWorkflowNode(name="test_node")
        self.context_manager = ContextManager()
        self.progress_tracker = ProgressTracker("test_workflow")
        
        # Set up node with dependencies
        self.node.set_context_manager(self.context_manager)
        self.node.set_progress_tracker(self.progress_tracker)
        
    def test_node_initialization(self):
        """Test node initializes correctly."""
        assert self.node.name == "test_node"
        assert self.node.description == ""
        assert self.node._action_count == 0
        assert self.node._max_actions == 10
        assert self.node.context_manager is self.context_manager
        assert self.node.progress_tracker is self.progress_tracker
        
    def test_action_limit_setting(self):
        """Test action limit can be set."""
        self.node.set_action_limit(5)
        assert self.node._max_actions == 5
        
    def test_action_count_increment(self):
        """Test action count increments correctly."""
        initial_count = self.node._action_count
        self.node._increment_action_count()
        assert self.node._action_count == initial_count + 1
        
    def test_action_limit_exceeded(self):
        """Test action limit error is raised when exceeded."""
        self.node.set_action_limit(2)
        
        # Should work for first two actions
        self.node._increment_action_count()
        self.node._increment_action_count()
        
        # Third action should raise error
        with pytest.raises(WorkflowActionLimitError) as exc_info:
            self.node._increment_action_count()
        assert "Action limit (2) reached" in str(exc_info.value)
        
    @patch('nifi_mcp_server.workflows.nodes.base_node.request_context')
    def test_bound_logger(self, mock_context):
        """Test bound logger includes correct context."""
        mock_context.get.return_value = {
            "user_request_id": "req-123",
            "action_id": "act-456",
            "workflow_id": "wf-789",
            "step_id": "step-abc"
        }
        
        logger = self.node.bound_logger
        
        # Verify logger binding was called with correct context
        assert mock_context.get.called
        
    def test_prep_method(self):
        """Test prep method handles context curation."""
        shared_state = {
            "user_request": "Test request",
            "messages": [{"role": "user", "content": "Hello"}]
        }
        
        result = self.node.prep(shared_state)
        
        # Should preserve original context
        assert "user_request" in result
        assert "messages" in result
        assert result["user_request"] == "Test request"
        
    def test_post_method(self):
        """Test post method stores results correctly."""
        shared_state = {}
        prep_res = {"context": "test"}
        exec_res = {"status": "success", "result": "test_data"}
        
        navigation = self.node.post(shared_state, prep_res, exec_res)
        
        # Check navigation result
        assert navigation == "default"
        
        # Check shared state updated
        assert f"{self.node.name}_result" in shared_state
        assert shared_state[f"{self.node.name}_result"] == exec_res
        assert f"{self.node.name}_action_count" in shared_state
        
    def test_navigation_key_determination(self):
        """Test navigation key logic."""
        # Success case
        exec_res = {"status": "success"}
        assert self.node._determine_navigation_key(exec_res) == "default"
        
        # Error case
        exec_res = {"status": "error"}
        assert self.node._determine_navigation_key(exec_res) == "error"
        
        # Retry case
        exec_res = {"status": "retry"}
        assert self.node._determine_navigation_key(exec_res) == "retry"
        
        # Non-dict result
        exec_res = "some string result"
        assert self.node._determine_navigation_key(exec_res) == "default"


class TestNiFiWorkflowNode(NiFiWorkflowNode):
    """Test implementation of NiFiWorkflowNode for testing."""
    
    def exec(self, prep_res):
        """Test implementation of exec method."""
        return {"status": "success", "message": "Test NiFi execution"}


class TestNiFiWorkflowNodeFunctionality:
    """Test cases for NiFiWorkflowNode functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.node = TestNiFiWorkflowNode(
            name="test_nifi_node",
            description="Test NiFi node",
            allowed_phases=["Review", "Creation"]
        )
        
    def test_nifi_node_initialization(self):
        """Test NiFi node initializes with allowed phases."""
        assert self.node.name == "test_nifi_node"
        assert self.node.description == "Test NiFi node"
        assert self.node.allowed_phases == ["Review", "Creation"]
        
    def test_phase_validation(self):
        """Test phase validation logic."""
        # Should allow phases in allowed_phases
        assert self.node._is_phase_allowed("Review")
        assert self.node._is_phase_allowed("Creation")
        
        # Should reject phases not in allowed_phases
        assert not self.node._is_phase_allowed("Operation")
        assert not self.node._is_phase_allowed("Modification")


class TestWorkflowNodeContextIntegration:
    """Test cases for workflow node integration with context management."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.node = TestWorkflowNode(name="context_test_node")
        self.context_manager = ContextManager()
        self.progress_tracker = ProgressTracker("test_workflow")
        
        # Configure node
        self.node.set_context_manager(self.context_manager)
        self.node.set_progress_tracker(self.progress_tracker)
        
    def test_context_manager_integration(self):
        """Test node integrates correctly with context manager."""
        shared_state = {
            "test_data": "value",
            "messages": [{"role": "user", "content": "Test"}]
        }
        
        # Run prep method
        prep_result = self.node.prep(shared_state)
        
        # Should include original shared state
        assert "test_data" in prep_result
        assert prep_result["test_data"] == "value"
        
    def test_progress_tracker_integration(self):
        """Test node integrates correctly with progress tracker."""
        shared_state = {}
        prep_res = {"test": "data"}
        exec_res = {"status": "success"}
        
        # Run post method
        self.node.post(shared_state, prep_res, exec_res)
        
        # Verify progress tracker was used (through mocking in real implementation)
        assert self.node.progress_tracker is not None
        
    @patch('nifi_mcp_server.workflows.nodes.base_node.request_context')
    def test_logging_context_propagation(self, mock_request_context):
        """Test that logging context is properly propagated."""
        # Set up context
        test_context = {
            "user_request_id": "req-123",
            "action_id": "act-456", 
            "workflow_id": "wf-789",
            "step_id": "step-abc"
        }
        mock_request_context.get.return_value = test_context
        
        # Access bound logger
        logger = self.node.bound_logger
        
        # Verify context was accessed
        mock_request_context.get.assert_called()


class TestWorkflowNodeErrorHandling:
    """Test cases for error handling in workflow nodes."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.node = TestWorkflowNode(name="error_test_node")
        
    def test_exec_method_must_be_implemented(self):
        """Test that exec method must be implemented in base class."""
        # WorkflowNode is abstract, so this test verifies the pattern
        with pytest.raises(TypeError):
            # Cannot instantiate abstract class directly
            WorkflowNode(name="test")
            
    def test_action_limit_error_handling(self):
        """Test action limit error contains useful information."""
        self.node.set_action_limit(1)
        
        # First action should work
        self.node._increment_action_count()
        
        # Second action should raise specific error
        with pytest.raises(WorkflowActionLimitError) as exc_info:
            self.node._increment_action_count()
            
        error_msg = str(exc_info.value)
        assert "Action limit (1) reached" in error_msg
        assert "error_test_node" in error_msg
        
    def test_error_status_navigation(self):
        """Test error status results in error navigation."""
        error_node = TestWorkflowNode(
            name="error_node",
            exec_result={"status": "error", "message": "Test error"}
        )
        
        shared_state = {}
        prep_res = {}
        exec_res = {"status": "error", "message": "Test error"}
        
        navigation = error_node.post(shared_state, prep_res, exec_res)
        assert navigation == "error"


if __name__ == "__main__":
    pytest.main([__file__]) 
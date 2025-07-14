"""
Base workflow node classes for the NiFi MCP workflow system.

This module provides the foundation classes for creating workflow nodes
that integrate with the PocketFlow framework and NiFi MCP tools.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from loguru import logger
from pocketflow import Node

# Import existing patterns from the NiFi MCP codebase
from config.logging_setup import request_context
from ..core.context_manager import ContextManager
from ..core.progress_tracker import ProgressTracker


class WorkflowNode(Node, ABC):
    """
    Base class for all workflow nodes in the NiFi MCP system.
    
    Integrates with PocketFlow Node and follows existing NiFi MCP patterns
    for logging, context management, and error handling.
    """
    
    def __init__(self, max_retries: int = 1, wait: int = 0, name: str = "", description: str = ""):
        """Initialize the workflow node."""
        super().__init__(max_retries=max_retries, wait=wait)
        self.name = name
        self.description = description
        self.context_manager: Optional[ContextManager] = None
        self.progress_tracker: Optional[ProgressTracker] = None
        self._action_count = 0
        self._max_actions = 10  # Default action limit
        
    def set_context_manager(self, context_manager: ContextManager):
        """Set the context manager for this node."""
        self.context_manager = context_manager
        
    def set_progress_tracker(self, progress_tracker: ProgressTracker):
        """Set the progress tracker for this node."""
        self.progress_tracker = progress_tracker
        
    def set_action_limit(self, max_actions: int):
        """Set the maximum number of actions allowed for this node."""
        self._max_actions = max_actions
        
    @property 
    def bound_logger(self):
        """Get a logger bound with current context."""
        ctx = request_context.get()
        return logger.bind(
            user_request_id=ctx.get("user_request_id", "-"),
            action_id=ctx.get("action_id", "-"),
            workflow_id=ctx.get("workflow_id", "-"),
            step_id=ctx.get("step_id", "-"),
            workflow_name=getattr(self, 'workflow_name', 'unknown'),
            node_name=self.name,
            component="workflow_node"
        )
        
    @property 
    def workflow_logger(self):
        """Get a logger specifically for workflow interface logging."""
        ctx = request_context.get()
        return logger.bind(
            user_request_id=ctx.get("user_request_id", "-"),
            action_id=ctx.get("action_id", "-"),
            workflow_id=ctx.get("workflow_id", "-"),
            step_id=ctx.get("step_id", "-"),
            workflow_name=getattr(self, 'workflow_name', 'unknown'),
            node_name=self.name,
            interface="workflow"
        )
        
    def _check_action_limit(self):
        """Check if action limit has been reached."""
        if self._action_count >= self._max_actions:
            raise WorkflowActionLimitError(
                f"Action limit ({self._max_actions}) reached for node '{self.name}'"
            )
    
    def _increment_action_count(self):
        """Increment action count and check limits."""
        # Check limit before incrementing
        if self._action_count >= self._max_actions:
            raise WorkflowActionLimitError(
                f"Action limit ({self._max_actions}) reached for node '{self.name}'"
            )
        self._action_count += 1
        
    def prep(self, shared: Dict[str, Any]) -> Dict[str, Any]:
        """
        PocketFlow prep hook - prepare context for node execution.
        
        Override this method to customize context preparation for your workflow node.
        """
        # Set step_id in request context for logging
        current_context = request_context.get()
        current_context["step_id"] = self.name
        request_context.set(current_context)
        
        self.bound_logger.info(f"Preparing workflow node: {self.name}")
        
        # Log workflow prep with interface logger middleware structure
        self.workflow_logger.bind(
            direction="prep",
            data={
                "node_name": self.name,
                "shared_keys": list(shared.keys()) if shared else [],
                "shared_values": {k: str(v)[:100] for k, v in (shared or {}).items()},  # Truncate values for logging
                "workflow_name": getattr(self, 'workflow_name', 'unknown')
            }
        ).info("workflow-prep")
        
        # Update progress if tracker is available
        if self.progress_tracker:
            self.progress_tracker.update_step_status(self.name, "preparing")
            
        # Preserve the original shared state for nodes that need it
        original_context = shared.copy() if shared else {}
        
        # Get curated context if context manager is available
        if self.context_manager:
            curated_context = self.context_manager.curate_context_for_step(self.name, shared=shared)
            # Merge: curated context as base, but preserve original context keys
            context = curated_context.copy()
            context.update(original_context)  # Original context takes precedence
        else:
            context = original_context
            
        self.bound_logger.debug(f"Node preparation complete. Context keys: {list(context.keys())}")
        return context
        
    def post(self, shared: Dict[str, Any], prep_res: Dict[str, Any], exec_res: Any) -> str:
        """
        PocketFlow post hook - handle results and update shared state.
        
        CRITICAL: This method must return a simple string (navigation key) for PocketFlow
        to use in successor lookup. Complex data should be stored in shared state.
        
        Override this method to customize result handling and state updates.
        """
        self.bound_logger.info(f"Post-processing workflow node: {self.name}")
        
        # Log workflow post with interface logger middleware structure
        exec_res_summary = {}
        if isinstance(exec_res, dict):
            exec_res_summary = {
                "status": exec_res.get("status", "unknown"),
                "message_count": len(exec_res.get("final_messages", [])),
                "has_messages": "final_messages" in exec_res,
                "keys": list(exec_res.keys())
            }
        
        self.workflow_logger.bind(
            direction="post",
            data={
                "node_name": self.name,
                "exec_result_type": type(exec_res).__name__,
                "exec_result_summary": exec_res_summary,
                "action_count": self._action_count,
                "workflow_name": getattr(self, 'workflow_name', 'unknown')
            }
        ).info("workflow-post")
        
        # Update progress if tracker is available
        if self.progress_tracker:
            if isinstance(exec_res, dict) and exec_res.get("status") == "error":
                self.progress_tracker.update_step_status(self.name, "failed", str(exec_res.get("message", "Unknown error")))
            else:
                self.progress_tracker.update_step_status(self.name, "completed")
                
        # Store result in shared state if context manager is available
        if self.context_manager:
            self.context_manager.store_step_result(self.name, exec_res)
            
        # Store complex execution results in shared state
        shared[f"{self.name}_result"] = exec_res
        shared[f"{self.name}_action_count"] = self._action_count
        
        # Determine navigation key based on execution result
        navigation_key = self._determine_navigation_key(exec_res)
        
        self.bound_logger.debug(f"Node post-processing complete. Navigation: '{navigation_key}'")
        return navigation_key  # Return simple string for PocketFlow navigation
        
    def _determine_navigation_key(self, exec_res: Any) -> str:
        """
        Determine the navigation key based on execution results.
        
        Override this method to customize navigation logic.
        Default behavior returns 'default' for success, 'error' for failures.
        """
        if isinstance(exec_res, dict):
            status = exec_res.get("status", "success")
            if status == "error":
                return "error"
            elif status == "retry":
                return "retry"
            else:
                return "default"
        else:
            # For non-dict results, assume success
            return "default"
        
    @abstractmethod
    def exec(self, prep_res: Dict[str, Any]) -> Any:
        """
        Execute the main logic of this workflow node.
        
        This method must be implemented by subclasses to define the specific
        workflow step logic.
        
        Args:
            prep_res: Result from the prep method
            
        Returns:
            Execution result
        """
        pass
        

class WorkflowActionLimitError(Exception):
    """Raised when a workflow node exceeds its action limit."""
    pass


class WorkflowNodeError(Exception):
    """Base exception for workflow node errors."""
    pass 
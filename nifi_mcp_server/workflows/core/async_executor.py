"""
Async Workflow Executor for NiFi MCP.

This module provides async workflow execution with real-time event emission
and support for both sync and async workflows.
"""

import sys
import os
import asyncio
from typing import Dict, Any, Optional, List, Callable, Union

# Import PocketFlow async classes
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'docs', 'pocketflow examples'))
from __init__ import AsyncFlow

from loguru import logger
from config.logging_setup import request_context
from ..core.event_system import get_event_emitter, emit_workflow_start, EventTypes
from ..nodes.base_node import WorkflowNode
from ..nodes.async_nifi_node import AsyncNiFiWorkflowNode


class AsyncWorkflowExecutor:
    """
    Async executor for both sync and async workflows with real-time events.
    """
    
    def __init__(self, workflow_name: str, workflow: Union[List[WorkflowNode], AsyncFlow]):
        """
        Initialize the async workflow executor.
        
        Args:
            workflow_name: Name of the workflow
            workflow: Either list of sync nodes or async flow
        """
        self.workflow_name = workflow_name
        self.workflow = workflow
        self.event_emitter = get_event_emitter()
        self.is_async_workflow = isinstance(workflow, AsyncFlow)
        
    @property
    def bound_logger(self):
        """Get a logger bound with current context."""
        ctx = request_context.get()
        return logger.bind(
            user_request_id=ctx.get("user_request_id", "-"),
            action_id=ctx.get("action_id", "-"),
            workflow_name=self.workflow_name,
            component="async_workflow_executor"
        )
    
    async def execute_async(self, initial_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute workflow asynchronously with real-time events.
        
        Args:
            initial_context: Initial context for the workflow
            
        Returns:
            Final workflow results
        """
        self.bound_logger.info(f"Starting async workflow execution: {self.workflow_name}")
        
        try:
            # Emit workflow start event
            await emit_workflow_start(
                self.workflow_name, 
                "workflow_start", 
                {
                    "workflow_type": "async" if self.is_async_workflow else "sync",
                    "initial_context_keys": list(initial_context.keys()) if initial_context else []
                },
                initial_context.get("user_request_id") if initial_context else None
            )
            
            # Set up shared state
            shared_state = initial_context.copy() if initial_context else {}
            shared_state["workflow_name"] = self.workflow_name
            
            if self.is_async_workflow:
                # Execute async workflow
                result = await self._execute_async_workflow(shared_state)
            else:
                # Execute sync workflow in thread pool
                result = await self._execute_sync_workflow_async(shared_state)
            
            # Emit workflow completion event
            await self.event_emitter.emit(EventTypes.WORKFLOW_COMPLETE, {
                "status": result.get("status", "success"),
                "workflow_name": self.workflow_name,
                "result_keys": list(result.keys())
            }, self.workflow_name, "workflow_complete", 
            initial_context.get("user_request_id") if initial_context else None)
            
            return result
            
        except Exception as e:
            # Emit workflow error event
            await self.event_emitter.emit(EventTypes.WORKFLOW_ERROR, {
                "error": str(e),
                "workflow_name": self.workflow_name
            }, self.workflow_name, "workflow_error", 
            initial_context.get("user_request_id") if initial_context else None)
            
            self.bound_logger.error(f"Async workflow execution failed: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "message": f"Async workflow {self.workflow_name} execution failed"
            }
    
    async def _execute_async_workflow(self, shared_state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an async workflow."""
        self.bound_logger.info(f"Executing async workflow: {self.workflow_name}")
        
        # Run the async flow
        result = await self.workflow.run_async(shared_state)
        
        return {
            "status": "success",
            "workflow_name": self.workflow_name,
            "shared_state": shared_state,
            "flow_result": result,
            "message": f"Async workflow {self.workflow_name} completed successfully"
        }
    
    async def _execute_sync_workflow_async(self, shared_state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a sync workflow in a thread pool."""
        self.bound_logger.info(f"Executing sync workflow in thread pool: {self.workflow_name}")
        
        # Import sync executor
        from .executor import GuidedWorkflowExecutor
        
        # Create sync executor
        sync_executor = GuidedWorkflowExecutor(self.workflow_name, self.workflow)
        
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: sync_executor.execute(shared_state)
        )
        
        return result


class AsyncWorkflowEventHandler:
    """
    Handler for workflow events that can update UI in real-time.
    """
    
    def __init__(self):
        self.event_emitter = get_event_emitter()
        self.ui_callbacks: List[Callable] = []
        
    def register_ui_callback(self, callback: Callable):
        """Register a callback for UI updates."""
        self.ui_callbacks.append(callback)
        self.event_emitter.on(self._handle_event_for_ui)
    
    async def _handle_event_for_ui(self, event):
        """Handle events and forward to UI callbacks."""
        for callback in self.ui_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(event)
                else:
                    callback(event)
            except Exception as e:
                logger.error(f"UI callback error: {e}", exc_info=True) 
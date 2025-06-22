"""
Context management for NiFi workflows.

This module provides context curation and management for workflow steps,
following existing NiFi MCP patterns for logging and state management.
"""

from typing import Dict, Any, List, Optional
from loguru import logger
from config.logging_setup import request_context


class ContextManager:
    """
    Manages context and state for workflow execution.
    
    Provides context curation, state storage, and context filtering
    to optimize LLM performance and reduce token usage.
    """
    
    def __init__(self, max_context_messages: int = 20):
        """
        Initialize the context manager.
        
        Args:
            max_context_messages: Maximum number of recent messages to keep in context
        """
        self.max_context_messages = max_context_messages
        self.shared_state: Dict[str, Any] = {}
        self.step_results: Dict[str, Any] = {}
        self.key_milestones: List[Dict[str, Any]] = []
        
    @property
    def bound_logger(self):
        """Get a logger bound with current context."""
        ctx = request_context.get()
        return logger.bind(
            user_request_id=ctx.get("user_request_id", "-"),
            action_id=ctx.get("action_id", "-"),
            component="context_manager"
        )
        
    def curate_context_for_step(self, step_name: str, **kwargs) -> Dict[str, Any]:
        """
        Curate context relevant for a specific workflow step.
        
        Args:
            step_name: Name of the workflow step
            **kwargs: Additional context from the workflow
            
        Returns:
            Curated context dictionary
        """
        self.bound_logger.debug(f"Curating context for step: {step_name}")
        
        context = {
            "step_name": step_name,
            "shared_state": self.shared_state.copy(),
            "previous_results": self._get_relevant_previous_results(step_name),
            "key_milestones": self.key_milestones.copy(),
            "workflow_progress": self._get_workflow_progress()
        }
        
        # Add any additional context from kwargs
        context.update(kwargs)
        
        # Apply context filtering to reduce token usage
        filtered_context = self._filter_context_for_step(step_name, context)
        
        self.bound_logger.debug(f"Context curated for {step_name}. Keys: {list(filtered_context.keys())}")
        return filtered_context
        
    def store_step_result(self, step_name: str, result: Any):
        """
        Store the result of a workflow step.
        
        Args:
            step_name: Name of the workflow step
            result: Result from the step execution
        """
        self.bound_logger.debug(f"Storing result for step: {step_name}")
        
        self.step_results[step_name] = {
            "result": result,
            "timestamp": self._get_current_timestamp(),
            "step_name": step_name
        }
        
        # Check if this is a key milestone
        if self._is_key_milestone(step_name, result):
            self._add_key_milestone(step_name, result)
            
    def update_shared_state(self, key: str, value: Any):
        """
        Update shared state that persists across workflow steps.
        
        Args:
            key: State key
            value: State value
        """
        self.bound_logger.debug(f"Updating shared state: {key}")
        self.shared_state[key] = value
        
    def get_shared_state(self, key: str, default: Any = None) -> Any:
        """
        Get value from shared state.
        
        Args:
            key: State key
            default: Default value if key not found
            
        Returns:
            State value or default
        """
        return self.shared_state.get(key, default)
        
    def _get_relevant_previous_results(self, current_step: str) -> Dict[str, Any]:
        """Get previous step results that are relevant to the current step."""
        # For Phase 1, return the last few step results
        # In future phases, this can be made more intelligent
        recent_results = {}
        step_names = list(self.step_results.keys())
        
        # Get last 3 step results (excluding current step if it exists)
        for step_name in step_names[-3:]:
            if step_name != current_step:
                recent_results[step_name] = self.step_results[step_name]
                
        return recent_results
        
    def _filter_context_for_step(self, step_name: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Filter context to include only relevant information for the step."""
        # For Phase 1, minimal filtering - just ensure reasonable size
        # In future phases, this can be made more sophisticated
        
        filtered = {}
        
        # Always include essential context
        for key in ["step_name", "shared_state", "workflow_progress"]:
            if key in context:
                filtered[key] = context[key]
                
        # Include previous results but limit size
        if "previous_results" in context:
            prev_results = context["previous_results"]
            if len(prev_results) > 3:
                # Keep only the 3 most recent
                sorted_results = sorted(
                    prev_results.items(),
                    key=lambda x: x[1].get("timestamp", 0),
                    reverse=True
                )
                filtered["previous_results"] = dict(sorted_results[:3])
            else:
                filtered["previous_results"] = prev_results
                
        # Include key milestones
        if "key_milestones" in context:
            # Limit to 5 most recent milestones
            milestones = context["key_milestones"]
            filtered["key_milestones"] = milestones[-5:] if len(milestones) > 5 else milestones
            
        return filtered
        
    def _is_key_milestone(self, step_name: str, result: Any) -> bool:
        """Determine if a step result represents a key milestone."""
        # For Phase 1, consider steps with successful results as milestones
        # In future phases, this can be more sophisticated
        
        if isinstance(result, dict):
            status = result.get("status", "").lower()
            return status in ["success", "completed"]
            
        # If result is not a dict, consider non-None results as potential milestones
        return result is not None
        
    def _add_key_milestone(self, step_name: str, result: Any):
        """Add a key milestone to the milestone list."""
        milestone = {
            "step_name": step_name,
            "timestamp": self._get_current_timestamp(),
            "summary": self._summarize_result(result)
        }
        
        self.key_milestones.append(milestone)
        self.bound_logger.debug(f"Added key milestone: {step_name}")
        
    def _summarize_result(self, result: Any) -> str:
        """Create a brief summary of a step result."""
        if isinstance(result, dict):
            if "message" in result:
                return str(result["message"])[:200]  # Truncate long messages
            elif "status" in result:
                return f"Status: {result['status']}"
            else:
                return f"Dict result with {len(result)} keys"
        elif isinstance(result, str):
            return result[:200]  # Truncate long strings
        else:
            return f"{type(result).__name__} result"
            
    def _get_workflow_progress(self) -> Dict[str, Any]:
        """Get summary of workflow progress."""
        total_steps = len(self.step_results)
        completed_steps = sum(
            1 for result in self.step_results.values()
            if isinstance(result.get("result"), dict) and 
            result["result"].get("status", "").lower() in ["success", "completed"]
        )
        
        return {
            "total_steps_executed": total_steps,
            "completed_steps": completed_steps,
            "milestones_count": len(self.key_milestones)
        }
        
    def _get_current_timestamp(self) -> float:
        """Get current timestamp."""
        import time
        return time.time() 
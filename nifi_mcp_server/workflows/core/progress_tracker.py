"""
Progress tracking for NiFi workflows.

This module provides multi-level progress tracking for workflow execution,
following existing NiFi MCP patterns for logging and state management.
"""

from typing import Dict, Any, List, Optional, Literal
from datetime import datetime
from loguru import logger
from config.logging_setup import request_context

StepStatus = Literal["pending", "preparing", "running", "completed", "failed", "skipped"]


class ProgressTracker:
    """
    Tracks progress of workflow execution at multiple levels.
    
    Provides step-by-step progress tracking with status updates,
    timing information, and error tracking.
    """
    
    def __init__(self, workflow_name: str):
        """
        Initialize the progress tracker.
        
        Args:
            workflow_name: Name of the workflow being tracked
        """
        self.workflow_name = workflow_name
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.current_step: Optional[str] = None
        self.steps: Dict[str, Dict[str, Any]] = {}
        self.workflow_status: StepStatus = "pending"
        self.error_message: Optional[str] = None
        
    @property
    def bound_logger(self):
        """Get a logger bound with current context."""
        ctx = request_context.get()
        return logger.bind(
            user_request_id=ctx.get("user_request_id", "-"),
            action_id=ctx.get("action_id", "-"),
            workflow_name=self.workflow_name,
            component="progress_tracker"
        )
        
    def start_workflow(self):
        """Mark the start of workflow execution."""
        self.start_time = datetime.now()
        self.workflow_status = "running"
        self.bound_logger.info(f"Workflow started: {self.workflow_name}")
        
    def complete_workflow(self, success: bool = True, error_message: Optional[str] = None):
        """
        Mark the completion of workflow execution.
        
        Args:
            success: Whether the workflow completed successfully
            error_message: Error message if workflow failed
        """
        self.end_time = datetime.now()
        self.workflow_status = "completed" if success else "failed"
        self.error_message = error_message
        
        duration = self.get_total_duration()
        status_msg = "completed successfully" if success else f"failed: {error_message}"
        self.bound_logger.info(f"Workflow {status_msg}: {self.workflow_name} (duration: {duration:.2f}s)")
        
    def start_step(self, step_name: str, description: str = ""):
        """
        Mark the start of a workflow step.
        
        Args:
            step_name: Name of the step
            description: Optional description of the step
        """
        self.current_step = step_name
        self.steps[step_name] = {
            "name": step_name,
            "description": description,
            "status": "running",
            "start_time": datetime.now(),
            "end_time": None,
            "error_message": None,
            "action_count": 0
        }
        
        self.bound_logger.info(f"Step started: {step_name}")
        
    def update_step_status(self, step_name: str, status: StepStatus, error_message: Optional[str] = None):
        """
        Update the status of a workflow step.
        
        Args:
            step_name: Name of the step
            status: New status of the step
            error_message: Error message if step failed
        """
        if step_name not in self.steps:
            # Initialize step if it doesn't exist
            self.steps[step_name] = {
                "name": step_name,
                "description": "",
                "status": status,
                "start_time": datetime.now(),
                "end_time": None,
                "error_message": None,
                "action_count": 0
            }
        else:
            self.steps[step_name]["status"] = status
            
        if status in ["completed", "failed", "skipped"]:
            self.steps[step_name]["end_time"] = datetime.now()
            
        if error_message:
            self.steps[step_name]["error_message"] = error_message
            
        self.bound_logger.debug(f"Step status updated: {step_name} -> {status}")
        
        # If this was the current step and it's finished, clear current step
        if step_name == self.current_step and status in ["completed", "failed", "skipped"]:
            self.current_step = None
            
    def increment_step_actions(self, step_name: str):
        """
        Increment the action count for a step.
        
        Args:
            step_name: Name of the step
        """
        if step_name in self.steps:
            self.steps[step_name]["action_count"] += 1
            self.bound_logger.trace(f"Action count incremented for {step_name}: {self.steps[step_name]['action_count']}")
            
    def get_step_duration(self, step_name: str) -> Optional[float]:
        """
        Get the duration of a step in seconds.
        
        Args:
            step_name: Name of the step
            
        Returns:
            Duration in seconds, or None if step is not completed
        """
        if step_name not in self.steps:
            return None
            
        step = self.steps[step_name]
        start_time = step.get("start_time")
        end_time = step.get("end_time")
        
        if start_time and end_time:
            return (end_time - start_time).total_seconds()
        elif start_time and step["status"] == "running":
            # Step is still running, return current duration
            return (datetime.now() - start_time).total_seconds()
            
        return None
        
    def get_total_duration(self) -> Optional[float]:
        """
        Get the total duration of the workflow in seconds.
        
        Returns:
            Duration in seconds, or None if workflow hasn't started
        """
        if not self.start_time:
            return None
            
        end_time = self.end_time or datetime.now()
        return (end_time - self.start_time).total_seconds()
        
    def get_progress_summary(self) -> Dict[str, Any]:
        """
        Get a summary of workflow progress.
        
        Returns:
            Dictionary containing progress information
        """
        total_steps = len(self.steps)
        completed_steps = sum(1 for step in self.steps.values() if step["status"] == "completed")
        failed_steps = sum(1 for step in self.steps.values() if step["status"] == "failed")
        running_steps = sum(1 for step in self.steps.values() if step["status"] == "running")
        
        progress_percentage = (completed_steps / total_steps * 100) if total_steps > 0 else 0
        
        summary = {
            "workflow_name": self.workflow_name,
            "workflow_status": self.workflow_status,
            "current_step": self.current_step,
            "total_steps": total_steps,
            "completed_steps": completed_steps,
            "failed_steps": failed_steps,
            "running_steps": running_steps,
            "progress_percentage": round(progress_percentage, 1),
            "total_duration": self.get_total_duration(),
            "error_message": self.error_message
        }
        
        return summary
        
    def get_step_details(self) -> List[Dict[str, Any]]:
        """
        Get detailed information about all steps.
        
        Returns:
            List of step detail dictionaries
        """
        step_details = []
        
        for step_name, step_info in self.steps.items():
            detail = {
                "name": step_name,
                "description": step_info.get("description", ""),
                "status": step_info["status"],
                "duration": self.get_step_duration(step_name),
                "action_count": step_info.get("action_count", 0),
                "error_message": step_info.get("error_message")
            }
            step_details.append(detail)
            
        return step_details
        
    def get_current_step_info(self) -> Optional[Dict[str, Any]]:
        """
        Get information about the currently running step.
        
        Returns:
            Current step information, or None if no step is running
        """
        if not self.current_step or self.current_step not in self.steps:
            return None
            
        step_info = self.steps[self.current_step]
        return {
            "name": self.current_step,
            "description": step_info.get("description", ""),
            "status": step_info["status"],
            "duration": self.get_step_duration(self.current_step),
            "action_count": step_info.get("action_count", 0)
        }
        
    def export_progress_log(self) -> Dict[str, Any]:
        """
        Export complete progress information for logging or debugging.
        
        Returns:
            Complete progress information
        """
        return {
            "workflow_name": self.workflow_name,
            "workflow_status": self.workflow_status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "total_duration": self.get_total_duration(),
            "current_step": self.current_step,
            "error_message": self.error_message,
            "progress_summary": self.get_progress_summary(),
            "step_details": self.get_step_details()
        } 
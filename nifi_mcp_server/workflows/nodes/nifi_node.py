"""
NiFi-specific workflow node classes.

This module provides workflow nodes that are specifically designed for
NiFi operations and have access to NiFi MCP tools.
"""

from typing import Dict, Any, List, Optional
from .base_node import WorkflowNode, WorkflowNodeError
from nifi_mcp_server.request_context import current_nifi_client, current_request_logger
from nifi_mcp_server.core import mcp


class NiFiWorkflowNode(WorkflowNode):
    """
    Base class for workflow nodes that interact with NiFi.
    
    Provides access to the NiFi client and MCP tools while following
    existing NiFi MCP patterns for authentication and error handling.
    """
    
    def __init__(self, max_retries: int = 1, wait: int = 0, name: str = "", description: str = "", allowed_phases: Optional[List[str]] = None):
        """
        Initialize the NiFi workflow node.
        
        Args:
            max_retries: Number of retry attempts for the node
            wait: Wait time between retries
            name: Name of the workflow node
            description: Description of what this node does
            allowed_phases: List of NiFi phases this node can operate in (Review, Creation, Modification, Operation)
        """
        super().__init__(max_retries=max_retries, wait=wait, name=name, description=description)
        self.allowed_phases = allowed_phases or ["Review", "Creation", "Modification", "Operation"]
        
    @property
    def nifi_client(self):
        """Get the current NiFi client from context."""
        client = current_nifi_client.get()
        if not client:
            raise WorkflowNodeError("NiFi client context is not set")
        return client
        
    async def call_mcp_tool(self, tool_name: str, **kwargs) -> Any:
        """
        Call an MCP tool with action limit checking.
        
        Args:
            tool_name: Name of the MCP tool to call
            **kwargs: Arguments to pass to the tool
            
        Returns:
            Result from the MCP tool
            
        Raises:
            WorkflowNodeError: If tool call fails
        """
        # Check action limit before making the call
        self._check_action_limit()
        
        try:
            self.bound_logger.debug(f"Calling MCP tool: {tool_name} with args: {list(kwargs.keys())}")
            
            # Log workflow tool call with interface logger middleware structure
            self.workflow_logger.bind(
                direction="tool_call",
                data={
                    "node_name": self.name,
                    "tool_name": tool_name,
                    "tool_args": list(kwargs.keys()),
                    "tool_args_values": {k: str(v)[:100] for k, v in kwargs.items()},  # Truncate values
                    "action_count": self._action_count + 1,  # Show what it will be after increment
                    "workflow_name": getattr(self, 'workflow_name', 'unknown')
                }
            ).info("workflow-tool_call")
            
            # Increment action count
            self._increment_action_count()
            
            # Call the tool via MCP
            result = await mcp.call_tool(tool_name, kwargs)
            
            # Log workflow tool result with interface logger middleware structure
            result_summary = {}
            if isinstance(result, dict):
                result_summary = {
                    "status": result.get("status", "unknown"),
                    "has_content": "content" in result,
                    "content_length": len(str(result.get("content", ""))),
                    "keys": list(result.keys())
                }
            
            self.workflow_logger.bind(
                direction="tool_result",
                data={
                    "node_name": self.name,
                    "tool_name": tool_name,
                    "result_type": type(result).__name__,
                    "result_summary": result_summary,
                    "workflow_name": getattr(self, 'workflow_name', 'unknown')
                }
            ).info("workflow-tool_result")
            
            self.bound_logger.debug(f"MCP tool {tool_name} completed successfully")
            return result
            
        except Exception as e:
            # Log workflow tool error with interface logger middleware structure
            self.workflow_logger.bind(
                direction="tool_error",
                data={
                    "node_name": self.name,
                    "tool_name": tool_name,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "workflow_name": getattr(self, 'workflow_name', 'unknown')
                }
            ).info("workflow-tool_error")
            
            self.bound_logger.error(f"Error calling MCP tool {tool_name}: {e}", exc_info=True)
            raise WorkflowNodeError(f"Failed to call MCP tool {tool_name}: {e}")
            
    def get_available_tools(self, phase_filter: Optional[str] = None) -> List[str]:
        """
        Get list of available MCP tools, optionally filtered by phase.
        
        Args:
            phase_filter: Optional phase to filter tools by
            
        Returns:
            List of available tool names
        """
        try:
            # Get tool manager from MCP instance
            tool_manager = getattr(mcp, '_tool_manager', None)
            if not tool_manager:
                self.bound_logger.warning("Could not access MCP tool manager")
                return []
                
            tools_info = tool_manager.list_tools()
            tool_names = [getattr(tool, 'name', 'unknown') for tool in tools_info]
            
            # Apply phase filter if specified
            if phase_filter:
                from nifi_mcp_server.api_tools.utils import _tool_phase_registry
                filtered_tools = []
                for tool_name in tool_names:
                    tool_phases = _tool_phase_registry.get(tool_name, [])
                    if not tool_phases or phase_filter.lower() in [p.lower() for p in tool_phases]:
                        filtered_tools.append(tool_name)
                return filtered_tools
                
            return tool_names
            
        except Exception as e:
            self.bound_logger.error(f"Error getting available tools: {e}", exc_info=True)
            return []
            
    def prep(self, shared: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare context for NiFi workflow node execution."""
        context = super().prep(shared)
        
        # Add NiFi-specific context
        context.update({
            "nifi_client_available": current_nifi_client.get() is not None,
            "allowed_phases": self.allowed_phases,
            "available_tools": self.get_available_tools()
        })
        
        self.bound_logger.debug(f"NiFi workflow node context prepared. Tools available: {len(context.get('available_tools', []))}")
        return context
        
    def post(self, shared: Dict[str, Any], prep_res: Dict[str, Any], exec_res: Any) -> str:
        """Handle results from NiFi workflow node execution."""
        navigation_key = super().post(shared, prep_res, exec_res)
        
        # Add NiFi-specific result information to shared state
        shared.update({
            f"{self.name}_nifi_node_type": self.__class__.__name__,
            f"{self.name}_allowed_phases": self.allowed_phases
        })
        
        return navigation_key
        
    def _is_phase_allowed(self, phase: str) -> bool:
        """Check if a phase is allowed for this node."""
        return phase in self.allowed_phases 
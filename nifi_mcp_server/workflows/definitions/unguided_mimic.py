"""
Unguided Mimic Workflow for NiFi MCP.

This workflow replicates the existing unguided LLM execution loop behavior
using multiple nodes to better demonstrate PocketFlow's capabilities.
"""

import json
import uuid
from typing import Dict, Any, List, Optional
from loguru import logger

from ..nodes.nifi_node import NiFiWorkflowNode
from ..registry import WorkflowDefinition, register_workflow
from nifi_chat_ui.chat_manager import get_llm_response, get_formatted_tool_definitions
from nifi_chat_ui.mcp_handler import get_available_tools, execute_mcp_tool
from config.logging_setup import request_context


class InitializeExecutionNode(NiFiWorkflowNode):
    """
    Initialize execution context and prepare for LLM iterations.
    """
    
    def __init__(self):
        super().__init__(
            name="initialize_execution",
            description="Set up execution context and initialize iteration state",
            allowed_phases=["Review", "Creation", "Modification", "Operation"]
        )
        # Simplified successors for basic flow
        self.successors = {
            "ready": "llm_iteration",
            "error": "finalize_execution"
        }
        
    def exec(self, prep_res: Dict[str, Any]) -> Dict[str, Any]:
        """Initialize execution context."""
        try:
            # Extract context from UI with fallbacks instead of strict validation
            provider = prep_res.get("provider", "anthropic") 
            model_name = prep_res.get("model_name", "claude-sonnet-4-20250109")
            system_prompt = prep_res.get("system_prompt", "You are a helpful assistant.")
            user_req_id = prep_res.get("user_request_id", "unknown")
            initial_messages = prep_res.get("messages", [])
            nifi_server_id = prep_res.get("selected_nifi_server_id")
            
            # Log warnings for missing fields instead of failing
            missing_fields = []
            if not provider: missing_fields.append("provider")
            if not model_name: missing_fields.append("model_name") 
            if not system_prompt: missing_fields.append("system_prompt")
            if not user_req_id: missing_fields.append("user_request_id")
            if not initial_messages: missing_fields.append("messages")
            if not nifi_server_id: missing_fields.append("selected_nifi_server_id")
            
            if missing_fields:
                self.bound_logger.warning(f"Missing context fields: {missing_fields}, using fallbacks")
            
            self.bound_logger.info(f"Initializing execution with provider: {provider}, model: {model_name}")
            
            # Initialize state
            execution_state = {
                "provider": provider,
                "model_name": model_name,
                "system_prompt": system_prompt,
                "user_request_id": user_req_id,
                "nifi_server_id": nifi_server_id,
                "messages": initial_messages.copy() if initial_messages else [],
                "loop_count": 0,
                "max_iterations": 10,
                "tool_results": [],
                "request_tokens_in": 0,
                "request_tokens_out": 0,
                "execution_complete": False
            }
            
            return {
                "status": "success",
                "execution_state": execution_state,
                "message": "Execution initialized successfully"
            }
            
        except Exception as e:
            self.bound_logger.error(f"Error initializing execution: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "message": "Failed to initialize execution"
            }
    
    def post(self, shared, prep_res, exec_res):
        """Store initialization results."""
        shared["execution_state"] = exec_res.get("execution_state", {})
        
        if exec_res.get("status") == "error":
            return "error"
        # For single-node workflow, return 'completed' instead of 'ready'
        return "completed"


class LLMIterationNode(NiFiWorkflowNode):
    """
    Execute a single LLM iteration with tool processing.
    """
    
    def __init__(self):
        super().__init__(
            name="llm_iteration", 
            description="Execute LLM call and process any tool calls",
            allowed_phases=["Review", "Creation", "Modification", "Operation"]
        )
        # Simplified successors - just continue to next node
        self.successors = {
            "continue": "evaluate_completion",
            "error": "finalize_execution"
        }
        
    def exec(self, prep_res: Dict[str, Any]) -> Dict[str, Any]:
        """Execute single LLM iteration."""
        try:
            execution_state = prep_res.get("execution_state", {})
            execution_state["loop_count"] += 1
            loop_count = execution_state["loop_count"]
            max_iterations = execution_state.get("max_iterations", 10)
            
            self.bound_logger.info(f"Starting LLM iteration {loop_count}/{max_iterations}")
            
            # Check max iterations
            if loop_count > max_iterations:
                return {
                    "status": "max_iterations_reached",
                    "execution_state": execution_state,
                    "message": f"Reached maximum iterations ({max_iterations})"
                }
            
            # Prepare tools
            formatted_tools = self._prepare_tools(execution_state)
            
            # Prepare LLM context
            llm_context_messages = [{"role": "system", "content": execution_state["system_prompt"]}]
            llm_context_messages.extend(execution_state["messages"])
            
            # Call LLM
            response_data = self._call_llm(llm_context_messages, formatted_tools, execution_state)
            
            if "error" in response_data:
                return {
                    "status": "error",
                    "execution_state": execution_state,
                    "error": response_data["error"]
                }
            
            # Update token counts
            execution_state["request_tokens_in"] += response_data.get("token_count_in", 0)
            execution_state["request_tokens_out"] += response_data.get("token_count_out", 0)
            
            # Add assistant message to history
            llm_content = response_data.get("content")
            tool_calls = response_data.get("tool_calls")
            
            assistant_message = {"role": "assistant"}
            if llm_content:
                assistant_message["content"] = llm_content
            if tool_calls:
                assistant_message["tool_calls"] = tool_calls
            execution_state["messages"].append(assistant_message)
            
            # Process tool calls if any
            if tool_calls:
                tool_results = self._process_tool_calls(tool_calls, execution_state)
                execution_state["tool_results"].extend(tool_results)
                
                # Add tool results to messages
                for tool_result in tool_results:
                    execution_state["messages"].append(tool_result["message"])
            
            # Check for completion
            task_complete = llm_content and "TASK COMPLETE" in llm_content
            
            return {
                "status": "success", 
                "execution_state": execution_state,
                "llm_content": llm_content,
                "task_complete": task_complete,
                "tool_calls_processed": len(tool_calls) if tool_calls else 0,
                "max_iterations_reached": loop_count >= max_iterations
            }
                
        except Exception as e:
            self.bound_logger.error(f"Error in LLM iteration: {e}", exc_info=True)
            return {
                "status": "error",
                "execution_state": execution_state,
                "error": str(e)
            }
    
    def post(self, shared, prep_res, exec_res):
        """Update shared state and determine next step."""
        shared["execution_state"] = exec_res.get("execution_state", {})
        shared["last_iteration_result"] = exec_res
        
        status = exec_res.get("status")
        if status == "error":
            return "error"
        elif exec_res.get("max_iterations_reached") or exec_res.get("task_complete"):
            # Skip evaluation, go directly to finalization
            return "complete"
        else:
            return "continue"
    
    def _prepare_tools(self, execution_state: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """Prepare tools for LLM call."""
        try:
            user_req_id = execution_state.get("user_request_id")
            provider = execution_state.get("provider")
            nifi_server_id = execution_state.get("nifi_server_id")
            
            raw_tools_list = get_available_tools(
                user_request_id=user_req_id,
                selected_nifi_server_id=nifi_server_id
            )
            
            if not raw_tools_list:
                self.bound_logger.warning("No raw tools available")
                return None
                
            formatted_tools = get_formatted_tool_definitions(
                provider=provider,
                raw_tools=raw_tools_list,
                user_request_id=user_req_id
            )
            
            if formatted_tools:
                self.bound_logger.debug(f"Tools prepared for {provider} ({len(formatted_tools)} tools)")
            
            return formatted_tools
            
        except Exception as e:
            self.bound_logger.error(f"Error preparing tools: {e}", exc_info=True)
            return None
    
    def _call_llm(self, messages: List[Dict[str, Any]], tools: Optional[List[Dict[str, Any]]], 
                  execution_state: Dict[str, Any]) -> Dict[str, Any]:
        """Call LLM with the prepared context."""
        try:
            provider = execution_state.get("provider")
            model_name = execution_state.get("model_name") 
            system_prompt = execution_state.get("system_prompt")
            user_req_id = execution_state.get("user_request_id")
            
            self.bound_logger.info(f"Calling LLM ({provider} - {model_name})")
            
            response_data = get_llm_response(
                messages=messages,
                system_prompt=system_prompt,
                tools=tools,
                provider=provider,
                model_name=model_name,
                user_request_id=user_req_id
            )
            
            if response_data is None:
                return {"error": "LLM response was empty"}
            
            return response_data
            
        except Exception as e:
            self.bound_logger.error(f"Error calling LLM: {e}", exc_info=True)
            return {"error": f"Error calling LLM: {e}"}
    
    def _process_tool_calls(self, tool_calls: List[Dict[str, Any]], 
                           execution_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process tool calls and return results."""
        tool_results = []
        user_req_id = execution_state.get("user_request_id")
        nifi_server_id = execution_state.get("nifi_server_id")
        
        for tool_call in tool_calls:
            try:
                tool_id = tool_call.get("id")
                function_call = tool_call.get("function", {})
                function_name = function_call.get("name")
                function_args_str = function_call.get("arguments", "{}")
                
                self.bound_logger.info(f"Executing tool: {function_name} (ID: {tool_id})")
                
                arguments = json.loads(function_args_str)
                
                tool_result = execute_mcp_tool(
                    tool_name=function_name,
                    params=arguments,
                    selected_nifi_server_id=nifi_server_id,
                    user_request_id=user_req_id,
                    action_id=str(uuid.uuid4())
                )
                
                tool_result_content = json.dumps(tool_result) if tool_result is not None else "null"
                
                tool_message = {
                    "role": "tool",
                    "tool_call_id": tool_id,
                    "content": tool_result_content
                }
                
                tool_results.append({
                    "tool_name": function_name,
                    "tool_id": tool_id,
                    "arguments": arguments,
                    "result": tool_result,
                    "message": tool_message
                })
                
            except Exception as e:
                self.bound_logger.error(f"Error executing tool {function_name}: {e}", exc_info=True)
                error_message = {
                    "role": "tool", 
                    "tool_call_id": tool_id,
                    "content": f"Error executing tool: {e}"
                }
                tool_results.append({
                    "tool_name": function_name,
                    "tool_id": tool_id,
                    "error": str(e),
                    "message": error_message
                })
        
        return tool_results


class EvaluateCompletionNode(NiFiWorkflowNode):
    """
    Evaluate whether the workflow should continue or complete.
    """
    
    def __init__(self):
        super().__init__(
            name="evaluate_completion",
            description="Determine if execution should continue or complete",
            allowed_phases=["Review"]
        )
        self.successors = {
            "continue": "llm_iteration",
            "complete": "finalize_execution"
        }
    
    def exec(self, prep_res: Dict[str, Any]) -> Dict[str, Any]:
        """Evaluate completion status."""
        try:
            execution_state = prep_res.get("execution_state", {})
            last_result = prep_res.get("last_iteration_result", {})
            
            loop_count = execution_state.get("loop_count", 0)
            max_iterations = execution_state.get("max_iterations", 10)
            task_complete = last_result.get("task_complete", False)
            
            # Simple completion logic
            if loop_count >= max_iterations or task_complete:
                return {
                    "status": "complete",
                    "decision": "complete",
                    "reason": "Task complete or max iterations reached"
                }
            else:
                return {
                    "status": "continue",
                    "decision": "continue",
                    "reason": "More iterations needed"
                }
                
        except Exception as e:
            self.bound_logger.error(f"Error evaluating completion: {e}", exc_info=True)
            return {
                "status": "error",
                "decision": "complete",
                "error": str(e)
            }
    
    def post(self, shared, prep_res, exec_res):
        """Determine next step based on evaluation."""
        shared["completion_evaluation"] = exec_res
        decision = exec_res.get("decision", "complete")
        return decision


class FinalizeExecutionNode(NiFiWorkflowNode):
    """
    Finalize execution and prepare final results.
    """
    
    def __init__(self):
        super().__init__(
            name="finalize_execution",
            description="Generate final results and completion summary",
            allowed_phases=["Review"]
        )
        # No successors - this is the final node
        self.successors = {}
    
    def exec(self, prep_res: Dict[str, Any]) -> Dict[str, Any]:
        """Finalize execution and prepare results."""
        try:
            execution_state = prep_res.get("execution_state", {})
            
            loop_count = execution_state.get("loop_count", 0)
            max_iterations = execution_state.get("max_iterations", 10)
            max_iterations_reached = loop_count >= max_iterations
            
            # Prepare final results
            final_result = {
                "status": "success",
                "message": f"Unguided mimic execution completed after {loop_count} iterations",
                "loop_count": loop_count,
                "max_iterations_reached": max_iterations_reached,
                "messages": execution_state.get("messages", []),
                "total_tokens_in": execution_state.get("request_tokens_in", 0),
                "total_tokens_out": execution_state.get("request_tokens_out", 0),
                "tool_calls_executed": len(execution_state.get("tool_results", [])),
                "tool_results": execution_state.get("tool_results", [])
            }
            
            if max_iterations_reached:
                final_result["status"] = "warning"
                final_result["message"] = f"Reached maximum iterations ({max_iterations})"
            
            self.bound_logger.info(f"Execution finalized: {final_result['status']}")
            return final_result
            
        except Exception as e:
            self.bound_logger.error(f"Error finalizing execution: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Execution failed: {e}",
                "loop_count": 0,
                "messages": []
            }
    
    def post(self, shared, prep_res, exec_res):
        """Store final results."""
        shared["unguided_mimic_result"] = exec_res
        shared["execution_completed"] = True
        return "completed"


def create_unguided_mimic_workflow() -> List[NiFiWorkflowNode]:
    """Factory function to create the unguided mimic workflow - temporarily simplified to debug."""
    # Temporarily use only the initialization node to test basic execution
    return [
        InitializeExecutionNode()
    ]


# Register the unguided mimic workflow
unguided_mimic_workflow = WorkflowDefinition(
    name="unguided_mimic",
    display_name="Unguided Mimic",
    description="Replicates the existing unguided LLM execution behavior using multi-node workflow",
    category="Basic",
    phases=["Review", "Creation", "Modification", "Operation"],
    factory=create_unguided_mimic_workflow,
    enabled=True
)

# Auto-register the workflow when this module is imported
register_workflow(unguided_mimic_workflow) 
"""
Unguided Mimic Workflow for NiFi MCP.

This workflow replicates the existing unguided LLM execution loop behavior
to ensure backward compatibility and identical results.
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


class UnguidedMimicNode(NiFiWorkflowNode):
    """
    Single node that replicates the entire unguided LLM execution loop.
    
    This node mimics the existing run_execution_loop function to ensure
    identical behavior and results in guided mode.
    """
    
    def __init__(self):
        super().__init__(
            name="unguided_mimic",
            description="Replicates existing unguided LLM execution loop behavior",
            allowed_phases=["Review", "Creation", "Modification", "Operation"]
        )
        self.max_iterations = 10  # Default, can be overridden
        # For single-node workflow, ensure no successors to avoid hashing issues
        self.successors = {}
        
    def post(self, shared, prep_res, exec_res):
        """Override post to store results and return navigation key."""
        # Store the complex execution results in shared state
        shared["unguided_mimic_result"] = exec_res
        shared["execution_completed"] = True
        
        # For single-node workflow, always navigate to completion
        # In multi-node workflows, this would determine the next step
        return "completed"  # Simple string for PocketFlow navigation
        
    def exec(self, prep_res: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the unguided mimic workflow step."""
        # Extract context 
        provider = prep_res.get("provider", "anthropic")
        model_name = prep_res.get("model_name", "claude-sonnet-4-20250109")
        system_prompt = prep_res.get("system_prompt", "You are a helpful assistant.")
        user_req_id = prep_res.get("user_request_id", "unknown")
        initial_messages = prep_res.get("messages", [])
        nifi_server_id = prep_res.get("selected_nifi_server_id")
        
        self.bound_logger.info(f"Starting unguided mimic execution with {len(initial_messages)} initial messages")
        
        # Initialize execution state
        loop_count = 0
        max_iterations = 10
        messages = initial_messages.copy() if initial_messages else []
        tool_results = []
        request_tokens_in = 0
        request_tokens_out = 0
        
        try:
            # Main execution loop - replicating run_execution_loop logic
            while loop_count < max_iterations:
                loop_count += 1
                llm_action_id = str(uuid.uuid4())
                current_loop_logger = self.bound_logger.bind(loop=loop_count, action_id=llm_action_id)
                current_loop_logger.info(f"Starting execution loop iteration {loop_count}")
                
                # Prepare tools
                formatted_tools = self._prepare_tools(provider, user_req_id, current_loop_logger)
                
                # Prepare LLM context
                llm_context_messages = [{"role": "system", "content": system_prompt}]
                llm_context_messages.extend(messages)
                
                # Call LLM
                response_data = self._call_llm(
                    llm_context_messages, system_prompt, formatted_tools,
                    provider, model_name, user_req_id, current_loop_logger
                )
                
                # Handle LLM response
                if "error" in response_data:
                    # LLM error - return error result
                    self.workflow_logger.bind(
                        direction="response",
                        data={"error": response_data["error"], "iteration": loop_count}
                    ).error("LLM returned error response")
                    
                    final_result = {
                        "status": "error",
                        "message": f"LLM error: {response_data['error']}",
                        "loop_count": loop_count,
                        "messages": messages
                    }
                    break
                    
                llm_content = response_data.get("content")
                tool_calls = response_data.get("tool_calls")
                token_count_in = response_data.get("token_count_in", 0)
                token_count_out = response_data.get("token_count_out", 0)
                
                # Log detailed LLM response for debugging
                self.workflow_logger.bind(
                    direction="response", 
                    data={
                        "iteration": loop_count,
                        "content_length": len(llm_content) if llm_content else 0,
                        "content_preview": llm_content[:200] if llm_content else None,
                        "tool_calls_count": len(tool_calls) if tool_calls else 0,
                        "token_count_in": token_count_in,
                        "token_count_out": token_count_out,
                        "has_content": bool(llm_content),
                        "has_tool_calls": bool(tool_calls)
                    }
                ).info("LLM response received")
                
                request_tokens_in += token_count_in
                request_tokens_out += token_count_out
                
                # Add assistant message to history
                assistant_message = {"role": "assistant"}
                if llm_content:
                    assistant_message["content"] = llm_content
                if tool_calls:
                    assistant_message["tool_calls"] = tool_calls
                messages.append(assistant_message)
                
                # Handle tool calls or completion
                if not tool_calls:
                    # No tool calls - check for completion
                    if llm_content and "TASK COMPLETE" in llm_content:
                        self.workflow_logger.bind(
                            direction="decision",
                            data={"reason": "task_complete_detected", "content_preview": llm_content[:100]}
                        ).info("Ending execution loop - TASK COMPLETE detected")
                        current_loop_logger.info("TASK COMPLETE detected. Ending execution loop.")
                        break
                    else:
                        # For unguided mimic, if LLM responds without tool calls and doesn't say complete,
                        # it means the response is complete for this user request. 
                        # This mimics the original behavior where each user message gets one execution cycle.
                        self.workflow_logger.bind(
                            direction="decision",
                            data={
                                "reason": "no_tool_calls_no_task_complete", 
                                "content_preview": llm_content[:100] if llm_content else None,
                                "has_content": bool(llm_content)
                            }
                        ).info("Ending execution loop - LLM response without tool calls")
                        current_loop_logger.info("LLM provided response without tool calls. Execution complete for this request.")
                        break
                else:
                    # Process tool calls
                    self.workflow_logger.bind(
                        direction="decision",
                        data={"reason": "processing_tool_calls", "tool_calls_count": len(tool_calls)}
                    ).info("Processing tool calls - continuing execution loop")
                    current_loop_logger.info(f"Processing {len(tool_calls)} tool call(s)")
                    tool_results_this_iteration = []
                    
                    for tool_call in tool_calls:
                        tool_result = self._execute_tool_call(
                            tool_call, nifi_server_id, user_req_id, 
                            llm_action_id, current_loop_logger
                        )
                        
                        if tool_result:
                            messages.append(tool_result["message"])
                            tool_results_this_iteration.append(tool_result)
                            
                    tool_results.extend(tool_results_this_iteration)
                    current_loop_logger.debug(f"Completed {len(tool_results_this_iteration)} tool calls")
                    
            # Prepare final results
            final_result = {
                "status": "success",
                "message": f"Unguided mimic execution completed after {loop_count} iterations",
                "loop_count": loop_count,
                "max_iterations_reached": loop_count >= max_iterations,
                "messages": messages,
                "total_tokens_in": request_tokens_in,
                "total_tokens_out": request_tokens_out,
                "tool_calls_executed": len(tool_results),
                "tool_results": tool_results
            }
            
            if loop_count >= max_iterations:
                final_result["status"] = "warning"
                final_result["message"] = f"Reached maximum iterations ({max_iterations})"
                
            self.bound_logger.info(f"Unguided mimic execution completed: {final_result['status']}")
            return final_result
            
        except Exception as e:
            self.bound_logger.error(f"Error in unguided mimic execution: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Execution failed: {e}",
                "loop_count": loop_count,
                "messages": messages
            }
            
    def _prepare_tools(self, provider: str, user_req_id: str, logger) -> Optional[List[Dict[str, Any]]]:
        """Prepare tools for LLM call, replicating the existing logic."""
        try:
            # Get available tools - check both possible keys for nifi_server_id
            nifi_server_id = None
            if self.context_manager:
                # Try both keys - server passes "nifi_server_id", UI might pass "selected_nifi_server_id"
                nifi_server_id = (
                    self.context_manager.shared_state.get("nifi_server_id") or 
                    self.context_manager.shared_state.get("selected_nifi_server_id")
                )
            
            raw_tools_list = get_available_tools(
                user_request_id=user_req_id,
                selected_nifi_server_id=nifi_server_id
            )
            if not raw_tools_list:
                logger.warning("No raw tools available")
                return None
                
            # Format tools for the provider
            formatted_tools = get_formatted_tool_definitions(
                provider=provider,
                raw_tools=raw_tools_list,
                user_request_id=user_req_id
            )
            
            if formatted_tools:
                logger.debug(f"Tools successfully formatted for {provider} ({len(formatted_tools)} tools)")
            else:
                logger.warning(f"Tool formatting returned None for {provider}")
                
            return formatted_tools
            
        except Exception as e:
            logger.error(f"Error preparing tools for {provider}: {e}", exc_info=True)
            return None
            
    def _call_llm(self, messages: List[Dict[str, Any]], system_prompt: str, 
                  tools: Optional[List[Dict[str, Any]]], provider: str, 
                  model_name: str, user_req_id: str, logger) -> Dict[str, Any]:
        """Call LLM with the prepared context."""
        try:
            logger.info(f"Calling LLM ({provider} - {model_name})")
            logger.debug(f"LLM request - Messages: {len(messages)}, Tools: {len(tools) if tools else 0}")
            logger.debug(f"LLM request - System prompt length: {len(system_prompt)}")
            
            response_data = get_llm_response(
                messages=messages,
                system_prompt=system_prompt,
                tools=tools,
                provider=provider,
                model_name=model_name,
                user_request_id=user_req_id
            )
            
            logger.debug(f"LLM raw response type: {type(response_data)}")
            if response_data:
                logger.debug(f"LLM response keys: {list(response_data.keys()) if isinstance(response_data, dict) else 'not dict'}")
                logger.debug(f"LLM response content length: {len(str(response_data.get('content', ''))) if isinstance(response_data, dict) else 'N/A'}")
                logger.debug(f"LLM response token counts: in={response_data.get('token_count_in', 'N/A')}, out={response_data.get('token_count_out', 'N/A')}")
            else:
                logger.warning("LLM response_data is None/empty")
            
            if response_data is None:
                logger.error("LLM response data was None")
                return {"error": "LLM response was empty"}
            else:
                logger.info(f"Received response from LLM ({provider})")
                return response_data
                
        except Exception as e:
            logger.error(f"Error calling LLM API ({provider}): {e}", exc_info=True)
            return {"error": f"Error calling LLM API: {e}"}
            
    def _execute_tool_call(self, tool_call: Dict[str, Any], nifi_server_id: Optional[str],
                          user_req_id: str, action_id: str, logger) -> Optional[Dict[str, Any]]:
        """Execute a single tool call, replicating the existing logic."""
        try:
            tool_id = tool_call.get("id")
            function_call = tool_call.get("function")
            
            if not tool_id or not function_call or not isinstance(function_call, dict):
                logger.error(f"Invalid tool call structure: {tool_call}")
                return None
                
            function_name = function_call.get("name")
            function_args_str = function_call.get("arguments", "{}")
            
            logger.info(f"Executing tool: {function_name} (ID: {tool_id})")
            
            # Parse arguments
            arguments = json.loads(function_args_str)
            logger.debug(f"Parsed arguments for {function_name}: {arguments}")
            
            # Execute the tool using mcp_handler
            tool_result = execute_mcp_tool(
                tool_name=function_name,
                params=arguments,
                selected_nifi_server_id=nifi_server_id,
                user_request_id=user_req_id,
                action_id=action_id
            )
            
            # Format result for the LLM
            tool_result_content = json.dumps(tool_result) if tool_result is not None else "null"
            logger.debug(f"Tool {function_name} execution result: {tool_result_content[:200]}...")
            
            # Create tool result message for LLM
            tool_message = {
                "role": "tool",
                "tool_call_id": tool_id,
                "content": tool_result_content
            }
            
            return {
                "tool_name": function_name,
                "tool_id": tool_id,
                "arguments": arguments,
                "result": tool_result,
                "message": tool_message
            }
            
        except json.JSONDecodeError as json_err:
            error_content = f"Error parsing arguments for {function_name}: {json_err}"
            logger.error(error_content)
            return {
                "tool_name": function_name,
                "tool_id": tool_id,
                "error": error_content,
                "message": {
                    "role": "tool",
                    "tool_call_id": tool_id,
                    "content": error_content
                }
            }
        except Exception as tool_err:
            error_content = f"Error executing tool {function_name}: {tool_err}"
            logger.error(error_content, exc_info=True)
            return {
                "tool_name": function_name,
                "tool_id": tool_id,
                "error": error_content,
                "message": {
                    "role": "tool",
                    "tool_call_id": tool_id,
                    "content": error_content
                }
            }


def create_unguided_mimic_workflow() -> List[NiFiWorkflowNode]:
    """Factory function to create the unguided mimic workflow."""
    return [UnguidedMimicNode()]


# Register the unguided mimic workflow
unguided_mimic_workflow = WorkflowDefinition(
    name="unguided_mimic",
    display_name="Unguided Mimic",
    description="Replicates the existing unguided LLM execution behavior for backward compatibility",
    category="Basic",
    phases=["Review", "Creation", "Modification", "Operation"],
    factory=create_unguided_mimic_workflow,
    enabled=True
)

# Auto-register the workflow when this module is imported
register_workflow(unguided_mimic_workflow) 
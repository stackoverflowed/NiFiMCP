"""
Groq LLM Provider implementation.

This module implements the LLMProvider interface for Groq using the Groq SDK.
"""

from typing import List, Dict, Any, Optional
from groq import Groq
from loguru import logger
from ..base import LLMProvider, LLMResponse
from ..utils.token_counter import TokenCounter
from ..mcp.tool_formatter import ToolFormatter


class GroqClient(LLMProvider):
    """Groq LLM Provider implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        # Check for nested config structure (e.g., config['llm']['groq'])
        if "llm" in config and "groq" in config["llm"]:
            groq_config = config["llm"]["groq"]
            api_key = groq_config.get("api_key")
            self.available_models = groq_config.get("models", ["llama3-70b-8192", "llama3-8b-8192", "mixtral-8x7b-32768"])
        elif "groq" in config:
            groq_config = config["groq"]
            api_key = groq_config.get("api_key")
            self.available_models = groq_config.get("models", ["llama3-70b-8192", "llama3-8b-8192", "mixtral-8x7b-32768"])
        else:
            api_key = config.get("GROQ_API_KEY")
            self.available_models = config.get("GROQ_MODELS", ["llama3-70b-8192", "llama3-8b-8192", "mixtral-8x7b-32768"])
        super().__init__(api_key)
        self.client = Groq(api_key=api_key)
        self.token_counter = TokenCounter()
        self.logger = logger.bind(provider="Groq")
    
    def send_message(
        self,
        messages: List[Dict[str, Any]],
        system_prompt: str,
        model_name: str,
        tools: Optional[List[Any]] = None,
        user_request_id: Optional[str] = None,
        action_id: Optional[str] = None
    ) -> LLMResponse:
        # Prepend system prompt as the first message if not already present
        groq_messages = messages.copy()
        if not groq_messages or groq_messages[0]["role"] != "system":
            groq_messages.insert(0, {"role": "system", "content": system_prompt})
        
        # LLM Debug Logging - Log the request being sent to Groq
        llm_request_data = {
            "provider": "groq",
            "model": model_name,
            "messages": groq_messages,
            "tools": tools
        }
        self.logger.bind(interface="llm", direction="request", data=llm_request_data).debug("Calling Groq LLM")
        
        try:
            # Prepare the request parameters
            request_params = {
                "model": model_name,
                "messages": groq_messages,
            }
            
            # Add tools if provided
            if tools:
                request_params["tools"] = tools
                request_params["tool_choice"] = "auto"
            
            response = self.client.chat.completions.create(**request_params)
            response_message = response.choices[0].message
            response_content = response_message.content
            response_tool_calls = response_message.tool_calls
            
            # Format tool calls if present
            if response_tool_calls:
                response_tool_calls = [
                    {
                        "id": tc.id,
                        "type": tc.type,
                        "function": {"name": tc.function.name, "arguments": tc.function.arguments}
                    } for tc in response_tool_calls
                ]
            
            # Get token counts
            token_count_in = getattr(response.usage, 'prompt_tokens', 0)
            token_count_out = getattr(response.usage, 'completion_tokens', 0)
            
            # LLM Debug Logging - Log the response received from Groq
            llm_response_data = {
                "content_length": len(response_content) if response_content else 0,
                "tool_calls_count": len(response_tool_calls) if response_tool_calls else 0,
                "token_count_in": token_count_in,
                "token_count_out": token_count_out,
                "error": None,
                "full_response": response_content
            }
            self.logger.bind(interface="llm", direction="response", data=llm_response_data).debug("llm-response")
            
            return LLMResponse(
                content=response_content,
                tool_calls=response_tool_calls,
                token_count_in=token_count_in,
                token_count_out=token_count_out
            )
        except Exception as e:
            self.logger.error(f"Groq API error: {e}")
            # LLM Debug Logging - Log the error response
            llm_error_data = {
                "error": str(e),
                "token_count_in": 0,
                "token_count_out": 0
            }
            self.logger.bind(interface="llm", direction="response", data=llm_error_data).debug("Received error from Groq LLM")
            raise
    
    def format_tools(self, tools: List[Dict[str, Any]]) -> Any:
        return ToolFormatter.format_tools_for_provider(tools, "groq")
    
    def is_configured(self) -> bool:
        return bool(self.api_key)
    
    def get_available_models(self) -> List[str]:
        return self.available_models 
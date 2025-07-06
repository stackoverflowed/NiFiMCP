"""
Perplexity LLM Provider implementation.

This module implements the LLMProvider interface for Perplexity using the OpenAI-compatible API.
"""

from typing import List, Dict, Any, Optional
from openai import OpenAI
from loguru import logger
from ..base import LLMProvider, LLMResponse
from ..utils.token_counter import TokenCounter
from ..mcp.tool_formatter import ToolFormatter


class PerplexityClient(LLMProvider):
    """Perplexity LLM Provider implementation (OpenAI-compatible)."""
    
    def __init__(self, config: Dict[str, Any]):
        # Handle both nested and flat config structures
        if "perplexity" in config:
            perplexity_config = config["perplexity"]
            api_key = perplexity_config.get("api_key")
            self.available_models = perplexity_config.get("models", ["pplx-70b-chat", "pplx-7b-chat"])
        else:
            api_key = config.get("PERPLEXITY_API_KEY")
            self.available_models = config.get("PERPLEXITY_MODELS", ["pplx-70b-chat", "pplx-7b-chat"])
        
        model_name = self.available_models[0] if self.available_models else "pplx-70b-chat"
        super().__init__(api_key, model_name)
        self.client = OpenAI(api_key=api_key, base_url="https://api.perplexity.ai")
        self.token_counter = TokenCounter()
        self.logger = logger.bind(provider="Perplexity")
    
    def send_message(
        self,
        messages: List[Dict[str, Any]],
        system_prompt: str,
        tools: Optional[List[Any]] = None,
        user_request_id: Optional[str] = None,
        action_id: Optional[str] = None
    ) -> LLMResponse:
        # Perplexity doesn't support function calling/tools
        if tools:
            self.logger.warning("Perplexity models don't support function calling. Tools will be ignored.")
        
        # Always add a clear reminder about Perplexity's limitations
        limitation_note = """

IMPORTANT: You are using a Perplexity model which has limitations compared to other LLM providers.

🔧 **Tool Support**: Perplexity models cannot execute NiFi tools or perform direct operations on your NiFi instance.

✅ **What I CAN do**:
- Answer questions about NiFi concepts and best practices
- Help troubleshoot issues and provide guidance
- Explain NiFi components, processors, and configurations
- Suggest solutions and approaches

❌ **What I CANNOT do**:
- Execute NiFi operations directly
- Create, modify, or delete NiFi objects
- Access your NiFi instance to perform actions
- Call any tools or functions

Please keep this in mind when asking questions. I'll provide helpful guidance and explanations, but you'll need to use a different provider (like OpenAI or Anthropic) if you want to execute NiFi operations directly."""
        
        system_prompt += limitation_note
        
        # Prepend system prompt as the first message if not already present
        perplexity_messages = messages.copy()
        if not perplexity_messages or perplexity_messages[0]["role"] != "system":
            perplexity_messages.insert(0, {"role": "system", "content": system_prompt})
        
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=perplexity_messages,
                # Don't send tools to Perplexity
            )
            response_message = response.choices[0].message
            response_content = response_message.content
            response_tool_calls = response_message.tool_calls
            if response_tool_calls:
                response_tool_calls = [
                    {
                        "id": tc.id,
                        "type": tc.type,
                        "function": {"name": tc.function.name, "arguments": tc.function.arguments}
                    } for tc in response_tool_calls
                ]
            token_count_in = getattr(response.usage, 'prompt_tokens', 0)
            token_count_out = getattr(response.usage, 'completion_tokens', 0)
            return LLMResponse(
                content=response_content,
                tool_calls=response_tool_calls,
                token_count_in=token_count_in,
                token_count_out=token_count_out
            )
        except Exception as e:
            self.logger.error(f"Perplexity API error: {e}")
            raise
    
    def format_tools(self, tools: List[Dict[str, Any]]) -> Any:
        return ToolFormatter.format_tools_for_provider(tools, "perplexity")
    
    def is_configured(self) -> bool:
        return bool(self.api_key)
    
    def get_available_models(self) -> List[str]:
        return self.available_models
    
    def supports_tools(self) -> bool:
        """Perplexity models don't support function calling/tools."""
        return False 
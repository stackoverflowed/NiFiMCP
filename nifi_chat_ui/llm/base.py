"""
Base classes and interfaces for LLM providers.

This module defines the abstract base classes and data structures that all LLM providers
must implement, ensuring consistent interfaces across different providers.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from loguru import logger


@dataclass
class LLMResponse:
    """Standardized response format for all LLM providers."""
    content: Optional[str]
    tool_calls: Optional[List[Dict[str, Any]]]
    token_count_in: int
    token_count_out: int
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert response to dictionary format for compatibility."""
        return {
            "content": self.content,
            "tool_calls": self.tool_calls,
            "token_count_in": self.token_count_in,
            "token_count_out": self.token_count_out,
            "error": self.error
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LLMResponse":
        """Create response from dictionary format."""
        return cls(
            content=data.get("content"),
            tool_calls=data.get("tool_calls"),
            token_count_in=data.get("token_count_in", 0),
            token_count_out=data.get("token_count_out", 0),
            error=data.get("error")
        )


class LLMProvider(ABC):
    """Abstract base class for all LLM providers."""
    
    def __init__(self, api_key: str, model_name: str):
        self.api_key = api_key
        self.model_name = model_name
        self.logger = logger.bind(provider=self.__class__.__name__)
    
    @abstractmethod
    def send_message(
        self, 
        messages: List[Dict[str, Any]], 
        system_prompt: str, 
        tools: Optional[List[Any]] = None,
        user_request_id: Optional[str] = None,
        action_id: Optional[str] = None
    ) -> LLMResponse:
        """
        Send a message to the LLM provider and get a response.
        
        Args:
            messages: List of message dictionaries in OpenAI format
            system_prompt: System prompt to use
            tools: Optional list of tools to make available
            user_request_id: Optional user request ID for logging
            action_id: Optional action ID for logging
            
        Returns:
            LLMResponse object with standardized format
        """
        pass
    
    @abstractmethod
    def format_tools(self, tools: List[Dict[str, Any]]) -> Any:
        """
        Format tools for this specific provider.
        
        Args:
            tools: List of tool definitions in MCP format
            
        Returns:
            Provider-specific tool format
        """
        pass
    
    @abstractmethod
    def is_configured(self) -> bool:
        """Check if the provider is properly configured."""
        pass
    
    @abstractmethod
    def get_available_models(self) -> List[str]:
        """Get list of available models for this provider."""
        pass
    
    def handle_error(self, exception: Exception) -> str:
        """
        Handle provider-specific errors and return user-friendly message.
        
        Args:
            exception: The caught exception
            
        Returns:
            User-friendly error message
        """
        # Default error handling - can be overridden by providers
        return f"{self.__class__.__name__} API error: {str(exception)}"
    
    def validate_model(self, model_name: str) -> bool:
        """
        Validate that the specified model is available.
        
        Args:
            model_name: Name of the model to validate
            
        Returns:
            True if model is available, False otherwise
        """
        available_models = self.get_available_models()
        return model_name in available_models
    
    def supports_tools(self) -> bool:
        """
        Check if this provider supports function calling/tools.
        
        Returns:
            True if provider supports tools, False otherwise
        """
        # Default to True - providers can override this
        return True 
"""
Compatibility wrapper for the new modular ChatManager.

This module provides the same function signatures as the old chat_manager.py
but uses the new modular LLM architecture internally.
"""

import os
import sys
from typing import List, Dict, Any, Optional
from loguru import logger

# Add the llm module to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from llm.chat_manager import ChatManager
from llm.utils.token_counter import TokenCounter
from llm.mcp.client import MCPClient

# Global instance of the new ChatManager
_new_chat_manager = None

def _get_chat_manager() -> ChatManager:
    """Get or create the new ChatManager instance."""
    global _new_chat_manager
    
    if _new_chat_manager is None:
        # Import config
        try:
            # Add parent directory to Python path so we can import config
            parent_dir = os.path.dirname(current_dir)
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
            
            from config import settings as config
            
            # Use the existing config system - the API keys are already loaded
            config_dict = {
                'openai': {
                    'api_key': config.OPENAI_API_KEY,
                    'models': config.OPENAI_MODELS
                },
                'gemini': {
                    'api_key': config.GOOGLE_API_KEY,
                    'models': config.GEMINI_MODELS
                },
                'anthropic': {
                    'api_key': config.ANTHROPIC_API_KEY,
                    'models': config.ANTHROPIC_MODELS
                },
                'perplexity': {
                    'api_key': config.PERPLEXITY_API_KEY,
                    'models': config.PERPLEXITY_MODELS
                }
            }
            
            _new_chat_manager = ChatManager(config_dict)
            logger.info("Initialized new modular ChatManager")
            
        except ImportError as e:
            logger.error(f"Failed to import config: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize ChatManager: {e}")
            raise
    
    return _new_chat_manager

# Compatibility functions that match the old chat_manager.py interface

def get_llm_response(
    messages: List[Dict[str, Any]],
    system_prompt: str,
    tools: Optional[List[Dict[str, Any]]] = None,
    provider: str = "openai",
    model_name: str = "gpt-4",
    user_request_id: Optional[str] = None,
    action_id: Optional[str] = None,
    selected_nifi_server_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get LLM response using the new modular architecture.
    
    This function maintains the same signature as the old chat_manager.py
    but uses the new modular ChatManager internally.
    """
    try:
        chat_manager = _get_chat_manager()
        return chat_manager.get_llm_response(
            messages=messages,
            system_prompt=system_prompt,
            provider=provider,
            model_name=model_name,
            user_request_id=user_request_id,
            action_id=action_id,
            selected_nifi_server_id=selected_nifi_server_id
        )
    except Exception as e:
        logger.error(f"Error in get_llm_response: {e}")
        return {"error": str(e)}

def get_formatted_tool_definitions(provider: str = "openai", raw_tools: Optional[List[Dict[str, Any]]] = None, user_request_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Get formatted tool definitions for the specified provider.
    
    This function maintains the same signature as the old chat_manager.py
    but uses the new modular MCP client internally.
    
    Args:
        provider: LLM provider name
        raw_tools: Optional raw tools list (ignored, tools come from MCP client)
        user_request_id: Optional user request ID for logging
    """
    try:
        chat_manager = _get_chat_manager()
        return chat_manager.get_tools(provider, user_request_id)
    except Exception as e:
        logger.error(f"Error in get_formatted_tool_definitions: {e}")
        return []

def calculate_input_tokens(
    messages: List[Dict[str, Any]], 
    provider: str = "openai", 
    model_name: str = "gpt-4",
    tools: Optional[List[Any]] = None
) -> int:
    """
    Calculate input tokens for the given messages.
    
    This function maintains the same signature as the old chat_manager.py
    but uses the new modular TokenCounter internally.
    """
    try:
        token_counter = TokenCounter()
        return token_counter.calculate_input_tokens(messages, provider, model_name, tools)
    except Exception as e:
        logger.error(f"Error in calculate_input_tokens: {e}")
        return 0

def configure_llms():
    """
    Configure LLM providers.
    
    This function maintains the same signature as the old chat_manager.py
    but uses the new modular ChatManager internally.
    """
    try:
        _get_chat_manager()  # This will initialize all providers
        logger.info("LLM providers configured successfully")
    except Exception as e:
        logger.error(f"Error configuring LLMs: {e}")
        raise

def is_initialized() -> bool:
    """
    Check if LLM providers are initialized.
    
    This function maintains the same signature as the old chat_manager.py
    but uses the new modular ChatManager internally.
    """
    try:
        chat_manager = _get_chat_manager()
        return len(chat_manager.get_available_providers()) > 0
    except Exception as e:
        logger.error(f"Error checking initialization: {e}")
        return False

# Additional utility functions that might be needed

def get_available_providers() -> List[str]:
    """Get list of available LLM providers."""
    try:
        chat_manager = _get_chat_manager()
        return chat_manager.get_available_providers()
    except Exception as e:
        logger.error(f"Error getting available providers: {e}")
        return []

def get_available_models(provider: str) -> List[str]:
    """Get list of available models for a provider."""
    try:
        chat_manager = _get_chat_manager()
        return chat_manager.get_available_models(provider)
    except Exception as e:
        logger.error(f"Error getting available models: {e}")
        return []

def is_provider_configured(provider: str) -> bool:
    """Check if a provider is properly configured."""
    try:
        chat_manager = _get_chat_manager()
        return chat_manager.is_provider_configured(provider)
    except Exception as e:
        logger.error(f"Error checking provider configuration: {e}")
        return False 
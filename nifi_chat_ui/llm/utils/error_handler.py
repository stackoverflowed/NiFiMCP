"""
Unified error handling for all LLM providers.

This module provides centralized error handling that can extract user-friendly
error messages from various LLM API exceptions.
"""

import json
from typing import Dict, Any
from loguru import logger


class LLMErrorHandler:
    """Centralized error handling for all LLM providers."""
    
    @staticmethod
    def handle_error(exception: Exception, provider: str) -> str:
        """
        Extract user-friendly error messages from LLM API exceptions.
        
        Args:
            exception: The caught exception
            provider: The LLM provider name for context-specific handling
            
        Returns:
            A user-friendly error message
        """
        error_str = str(exception).lower()
        
        # Common error patterns across providers
        if 'rate limit' in error_str or 'rate_limit' in error_str:
            return "Rate limit exceeded. Please try again later."
        elif 'authentication' in error_str or 'api key' in error_str:
            return "Authentication failed. Please check your API key."
        elif 'model not found' in error_str or 'not found' in error_str:
            return "Model not found. Please check the model name."
        elif 'quota' in error_str:
            return "Quota exceeded. Please check your account billing."
        elif 'timeout' in error_str:
            return "Request timed out. Please try again."
        elif 'network' in error_str or 'connection' in error_str:
            return "Network error. Please check your connection and try again."
        else:
            return f"{provider.title()} API error: {str(exception)}"
    
    @staticmethod
    def extract_openai_error(exception: Exception) -> str:
        """Extract user-friendly error from OpenAI API exceptions."""
        error_str = str(exception)
        
        # Try to extract structured error information
        if hasattr(exception, 'response'):
            try:
                response_content = exception.response.text
                response_json = json.loads(response_content)
                if isinstance(response_json, dict) and 'error' in response_json:
                    error_info = response_json['error']
                    if isinstance(error_info, dict):
                        error_type = error_info.get('type', 'unknown_error')
                        error_message = error_info.get('message', 'Unknown error')
                        
                        # Map common OpenAI error types
                        if error_type == 'rate_limit_exceeded':
                            return "Rate limit exceeded. Please try again later."
                        elif error_type == 'model_not_found':
                            return f"Model not found: {error_message}"
                        elif error_type == 'invalid_api_key':
                            return "Invalid API key. Please check your OpenAI API key."
                        elif error_type == 'insufficient_quota':
                            return "Insufficient quota. Please check your OpenAI account billing."
                        else:
                            return f"OpenAI API error ({error_type}): {error_message}"
            except Exception:
                pass
        
        return LLMErrorHandler.handle_error(exception, "OpenAI")
    
    @staticmethod
    def extract_anthropic_error(exception: Exception) -> str:
        """Extract user-friendly error from Anthropic API exceptions."""
        error_str = str(exception)
        
        # Anthropic has structured error information in e.body
        if hasattr(exception, 'body') and isinstance(exception.body, dict):
            try:
                if 'error' in exception.body and isinstance(exception.body['error'], dict):
                    error_info = exception.body['error']
                    error_type = error_info.get('type', 'unknown_error')
                    error_message = error_info.get('message', 'Unknown error')
                    
                    # Map common Anthropic error types
                    if error_type == 'rate_limit_error':
                        return "Rate limit exceeded. Please try again later or reduce the prompt length."
                    elif error_type == 'not_found_error':
                        if 'model:' in error_message:
                            model_name = error_message.split('model:')[-1].strip()
                            return f"Model '{model_name}' not found. Please check the model name or try a different model."
                        else:
                            return f"Resource not found: {error_message}"
                    elif error_type == 'authentication_error':
                        return "Authentication failed. Please check your API key."
                    elif error_type == 'permission_error':
                        return "Permission denied. Please check your API key permissions."
                    elif error_type == 'invalid_request_error':
                        return f"Invalid request: {error_message}"
                    elif error_type == 'server_error':
                        return "Anthropic server error. Please try again later."
                    else:
                        return f"Anthropic API error ({error_type}): {error_message}"
            except Exception:
                pass
        
        return LLMErrorHandler.handle_error(exception, "Anthropic")
    
    @staticmethod
    def extract_gemini_error(exception: Exception) -> str:
        """Extract user-friendly error from Gemini API exceptions with enhanced diagnostics."""
        error_str = str(exception)
        
        # Check for Gemini-specific error patterns with more detailed messages
        if 'MALFORMED_FUNCTION_CALL' in error_str:
            return ("Function call schema error: Gemini detected malformed function call arguments. "
                   "This usually indicates an issue with tool parameter schemas. Check the logs for "
                   "detailed schema analysis that identifies the problematic tool and specific issues.")
        elif 'SAFETY' in error_str:
            return ("Safety filter triggered: Gemini blocked the response due to safety concerns. "
                   "Try rephrasing your request to avoid triggering content filters.")
        elif 'MAX_TOKENS' in error_str:
            return ("Token limit exceeded: Gemini response was truncated due to reaching the maximum "
                   "token limit. Try using a shorter prompt or breaking the request into smaller parts.")
        elif 'finish_reason' in error_str:
            # Handle Gemini finish reasons with more context
            if 'MALFORMED_FUNCTION_CALL' in error_str:
                return ("Function call schema error: Gemini could not parse the function call arguments. "
                       "Check the logs for detailed schema validation that identifies specific issues "
                       "with tool definitions (missing properties, incorrect types, etc.).")
            elif 'SAFETY' in error_str:
                return ("Safety filter triggered: Content was blocked by Gemini's safety filters. "
                       "Rephrase your request to avoid triggering safety mechanisms.")
            elif 'MAX_TOKENS' in error_str:
                return ("Token limit exceeded: Response was truncated. Consider reducing prompt length "
                       "or simplifying the request.")
        
        # Check for API-level errors
        if 'rate limit' in error_str.lower() or 'quota' in error_str.lower():
            return ("Rate limit or quota exceeded: Too many requests to Gemini API. "
                   "Please wait a moment and try again.")
        elif 'api key' in error_str.lower() or 'authentication' in error_str.lower():
            return ("Authentication failed: Please check your Google API key configuration.")
        elif 'model not found' in error_str.lower():
            return ("Model not found: The specified Gemini model is not available. "
                   "Please check the model name or try a different model.")
        elif 'network' in error_str.lower() or 'connection' in error_str.lower():
            return ("Network error: Failed to connect to Gemini API. Check your internet connection.")
        
        return LLMErrorHandler.handle_error(exception, "Gemini")
    
    @staticmethod
    def extract_perplexity_error(exception: Exception) -> str:
        """Extract user-friendly error from Perplexity API exceptions."""
        error_str = str(exception)
        
        # Perplexity uses OpenAI-compatible error format
        if hasattr(exception, 'body') and exception.body:
            try:
                body = exception.body
                if isinstance(body, dict) and 'error' in body:
                    error_info = body['error']
                    if isinstance(error_info, dict):
                        error_type = error_info.get('type', 'unknown_error')
                        error_message = error_info.get('message', 'Unknown error')
                        
                        # Map common Perplexity error types
                        if error_type == 'rate_limit_exceeded':
                            return "Rate limit exceeded. Please try again later."
                        elif error_type == 'model_not_found':
                            return f"Model not found: {error_message}"
                        else:
                            return f"Perplexity API error ({error_type}): {error_message}"
            except Exception:
                pass
        
        return LLMErrorHandler.handle_error(exception, "Perplexity") 
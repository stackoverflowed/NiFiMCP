"""
Tool formatting utilities for different LLM providers.

This module handles formatting of MCP tools for different LLM providers.
"""

from typing import List, Dict, Any, Optional
from loguru import logger


class ToolFormatter:
    """Format tools for different LLM providers."""
    
    @staticmethod
    def format_tools_for_provider(tools: List[Dict[str, Any]], provider: str) -> Any:
        """
        Format tools for a specific LLM provider.
        
        Args:
            tools: List of tool definitions from MCP server
            provider: LLM provider name
            
        Returns:
            Provider-specific tool format
        """
        provider_lower = provider.lower()
        
        if provider_lower == "openai":
            return ToolFormatter._format_for_openai(tools)
        elif provider_lower == "perplexity":
            return ToolFormatter._format_for_perplexity(tools)
        elif provider_lower == "anthropic":
            return ToolFormatter._format_for_anthropic(tools)
        elif provider_lower == "gemini":
            return ToolFormatter._format_for_gemini(tools)
        else:
            logger.warning(f"Unknown provider '{provider}', using OpenAI format")
            return ToolFormatter._format_for_openai(tools)
    
    @staticmethod
    def _format_for_openai(tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format tools for OpenAI (OpenAI-compatible format)."""
        # OpenAI format matches our API format directly, but clean up the schema
        cleaned_tools = []
        for tool in tools:
            if tool.get("type") == "function" and isinstance(tool.get("function"), dict):
                function_def = tool["function"]
                
                # Clean up parameters if present
                if "parameters" in function_def and isinstance(function_def["parameters"], dict):
                    params = function_def["parameters"]
                    # Remove top-level additionalProperties field which might cause issues
                    params.pop("additionalProperties", None)
                    
                    # Clean individual properties
                    if "properties" in params and isinstance(params["properties"], dict):
                        props = params["properties"]
                        for prop_name, prop_value in props.items():
                            # Ensure prop_value is a dict before cleaning
                            if isinstance(prop_value, dict):
                                prop_value.pop("additionalProperties", None)
                            # If prop_value is not a dict or becomes empty after cleaning, set default type
                            if not isinstance(prop_value, dict) or not prop_value:
                                props[prop_name] = {"type": "string"}
                
                cleaned_tools.append(tool)
        
        return cleaned_tools
    
    @staticmethod
    def _format_for_perplexity(tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format tools for Perplexity (OpenAI-compatible format)."""
        # Perplexity uses the same format as OpenAI
        return ToolFormatter._format_for_openai(tools)
    
    @staticmethod
    def _format_for_anthropic(tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format tools for Anthropic."""
        # Convert OpenAI format to Anthropic format
        anthropic_tools = []
        
        for tool_data in tools:
            func_details = tool_data.get("function", {})
            name = func_details.get("name")
            description = func_details.get("description")
            parameters_schema = func_details.get("parameters") 
            
            if not name or not description:
                continue
                
            # Anthropic expects tools WITHOUT any "type" field
            # Just the direct tool definition with name, description, and input_schema
            anthropic_tool = {
                "name": name,
                "description": description,
                "input_schema": parameters_schema or {"type": "object", "properties": {}}
            }
            
            anthropic_tools.append(anthropic_tool)
        
        return anthropic_tools
    
    @staticmethod
    def _format_for_gemini(tools: List[Dict[str, Any]]) -> List[Any]:
        """Format tools for Gemini (FunctionDeclaration objects)."""
        try:
            import google.generativeai.types as genai_types
        except ImportError:
            logger.error("Google Generative AI types not available for Gemini tool formatting")
            return []
        
        gemini_tools = []
        
        for tool_data in tools:
            func_details = tool_data.get("function", {})
            name = func_details.get("name")
            description = func_details.get("description")
            parameters_schema = func_details.get("parameters")
            
            if not name or not description:
                logger.warning(f"Skipping tool without name or description: {tool_data}")
                continue
            
            try:
                # Clean up the schema for Gemini - remove additionalProperties
                cleaned_schema = ToolFormatter._clean_schema_for_gemini(parameters_schema)
                
                # Create Gemini FunctionDeclaration object
                function_declaration = genai_types.FunctionDeclaration(
                    name=name,
                    description=description,
                    parameters=cleaned_schema
                )
                gemini_tools.append(function_declaration)
                logger.debug(f"Created Gemini FunctionDeclaration for: {name}")
                
            except Exception as e:
                logger.error(f"Failed to create FunctionDeclaration for {name}: {e}")
                continue
        
        logger.info(f"Formatted {len(gemini_tools)} tools for Gemini")
        return gemini_tools
    
    @staticmethod
    def _clean_schema_for_gemini(schema: Dict[str, Any]) -> Dict[str, Any]:
        """Clean up schema for Gemini by removing unsupported fields."""
        if not isinstance(schema, dict):
            return {"type": "object", "properties": {}}
        
        # Make a deep copy to avoid modifying the original
        import copy
        cleaned = copy.deepcopy(schema)
        
        # Remove additionalProperties at all levels
        def remove_additional_properties(obj):
            if isinstance(obj, dict):
                obj.pop("additionalProperties", None)
                for key, value in obj.items():
                    remove_additional_properties(value)
            elif isinstance(obj, list):
                for item in obj:
                    remove_additional_properties(item)
        
        remove_additional_properties(cleaned)
        
        # Ensure we have a valid schema structure
        if not cleaned.get("type"):
            cleaned["type"] = "object"
        if not cleaned.get("properties"):
            cleaned["properties"] = {}
        
        return cleaned 
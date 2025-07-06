"""
MCP Schema validation and correction utilities.

This module validates and corrects MCP server schema inconsistencies before
sending tools to LLM providers.
"""

from typing import Dict, Any, List
from loguru import logger
import copy


class MCPSchemaValidator:
    """Validates and corrects MCP server schema inconsistencies."""
    
    @staticmethod
    def validate_and_correct_schema(schema: Dict[str, Any], provider: str) -> Dict[str, Any]:
        """
        Fix common MCP schema issues before sending to LLM providers.
        
        Args:
            schema: The schema to validate and correct
            provider: The LLM provider name for provider-specific corrections
            
        Returns:
            Corrected schema
        """
        corrected = copy.deepcopy(schema)
        
        # Provider-specific corrections
        if provider.lower() == "gemini":
            # Gemini-specific corrections (Protocol Buffer format)
            corrected = MCPSchemaValidator._correct_for_gemini(corrected)
        else:
            # Standard JSON Schema corrections for OpenAI, Anthropic, Perplexity
            corrected = MCPSchemaValidator._correct_for_json_schema(corrected)
        
        return corrected
    
    @staticmethod
    def _correct_for_gemini(schema: Dict[str, Any]) -> Dict[str, Any]:
        """Apply Gemini-specific schema corrections."""
        # Convert JSON Schema types to Gemini Protocol Buffer types
        type_mapping = {
            "string": "STRING",
            "number": "NUMBER", 
            "integer": "INTEGER",
            "boolean": "BOOLEAN",
            "object": "OBJECT",
            "array": "ARRAY"
        }
        
        # Fix common MCP server inconsistencies for Gemini
        field_corrections = {
            "timeout_seconds": "NUMBER",
            "port": "INTEGER",
            "enabled": "BOOLEAN",
            "properties": "OBJECT",
            "relationships": "ARRAY",
            "object_id": "STRING",
            "process_group_id": "STRING",
            "processor_id": "STRING",
            "connection_id": "STRING",
            "target_id": "STRING",
            "source_id": "STRING",
            "service_id": "STRING",
            "port_id": "STRING",
            "controller_service_id": "STRING",
            "parent_process_group_id": "STRING",
            "starting_processor_id": "STRING",
            "url": "STRING",
            "name": "STRING",
            "bundle_artifact_filter": "STRING",
            "processor_name": "STRING",
            "service_name": "STRING",
            "question": "STRING",
            "query": "STRING",
            "filter_process_group_id": "STRING",
            "bulletin_limit": "INTEGER",
            "max_content_bytes": "INTEGER",
            "event_id": "INTEGER",
            "position_x": "INTEGER",
            "position_y": "INTEGER",
            "width": "INTEGER",
            "height": "INTEGER",
            "include_bulletins": "BOOLEAN",
            "include_suggestions": "BOOLEAN",
            "recursive": "BOOLEAN",
            "disabled": "BOOLEAN",
            "active": "BOOLEAN",
            "required": "BOOLEAN",
            "optional": "BOOLEAN",
            "operations": "ARRAY",
            "objects": "ARRAY",
            "updates": "ARRAY",
            "processors": "ARRAY",
            "ports": "ARRAY",
            "connections": "ARRAY",
            "controller_services": "ARRAY",
            "nifi_objects": "ARRAY",
            "relationships": "ARRAY",
            "auto_terminated_relationships": "ARRAY",
            "property_names_to_delete": "ARRAY",
        }
        
        return MCPSchemaValidator._apply_corrections(schema, field_corrections, type_mapping)
    
    @staticmethod
    def _correct_for_json_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
        """Apply standard JSON Schema corrections."""
        # Fix common MCP server inconsistencies for JSON Schema providers
        field_corrections = {
            "timeout_seconds": "number",
            "port": "integer",
            "enabled": "boolean",
            "properties": "object",
            "relationships": "array",
            "object_id": "string",
            "process_group_id": "string",
            "processor_id": "string",
            "connection_id": "string",
            "target_id": "string",
            "source_id": "string",
            "service_id": "string",
            "port_id": "string",
            "controller_service_id": "string",
            "parent_process_group_id": "string",
            "starting_processor_id": "string",
            "url": "string",
            "name": "string",
            "bundle_artifact_filter": "string",
            "processor_name": "string",
            "service_name": "string",
            "question": "string",
            "query": "string",
            "filter_process_group_id": "string",
            "bulletin_limit": "integer",
            "max_content_bytes": "integer",
            "event_id": "integer",
            "position_x": "integer",
            "position_y": "integer",
            "width": "integer",
            "height": "integer",
            "include_bulletins": "boolean",
            "include_suggestions": "boolean",
            "recursive": "boolean",
            "disabled": "boolean",
            "active": "boolean",
            "required": "boolean",
            "optional": "boolean",
            "operations": "array",
            "objects": "array",
            "updates": "array",
            "processors": "array",
            "ports": "array",
            "connections": "array",
            "controller_services": "array",
            "nifi_objects": "array",
            "relationships": "array",
            "auto_terminated_relationships": "array",
            "property_names_to_delete": "array",
        }
        
        return MCPSchemaValidator._apply_corrections(schema, field_corrections, {})
    
    @staticmethod
    def _apply_corrections(schema: Dict[str, Any], field_corrections: Dict[str, str], type_mapping: Dict[str, str]) -> Dict[str, Any]:
        """
        Apply field-specific corrections to schema.
        
        Args:
            schema: The schema to correct
            field_corrections: Mapping of field names to expected types
            type_mapping: Mapping of JSON Schema types to provider-specific types
            
        Returns:
            Corrected schema
        """
        if "properties" in schema:
            for field, expected_type in field_corrections.items():
                if field in schema["properties"]:
                    prop = schema["properties"][field]
                    current_type = prop.get("type", "").lower()
                    expected_type_lower = expected_type.lower()
                    
                    if current_type != expected_type_lower:
                        # Apply type mapping if provided (for Gemini)
                        corrected_type = type_mapping.get(expected_type_lower, expected_type_lower)
                        prop["type"] = corrected_type
                        logger.info(f"Corrected {field} type: {current_type} -> {corrected_type}")
                        
                        # Add items field for ARRAY types if missing
                        if corrected_type.lower() == "array" and "items" not in prop:
                            if field in ["operations", "objects", "updates", "processors", "ports", "connections", "controller_services", "nifi_objects"]:
                                prop["items"] = {"type": "object"}
                            elif field in ["relationships", "auto_terminated_relationships", "property_names_to_delete"]:
                                prop["items"] = {"type": "string"}
                            else:
                                prop["items"] = {"type": "object"}
                            logger.info(f"Added items field for '{field}': {prop['items']}")
        
        return schema
    
    @staticmethod
    def validate_tool_schema(tool: Dict[str, Any], provider: str) -> Dict[str, Any]:
        """
        Validate and correct a single tool's schema.
        
        Args:
            tool: Tool definition from MCP server
            provider: LLM provider name
            
        Returns:
            Corrected tool definition
        """
        corrected_tool = tool.copy()
        
        if "function" in tool and "parameters" in tool["function"]:
            corrected_params = MCPSchemaValidator.validate_and_correct_schema(
                tool["function"]["parameters"], 
                provider
            )
            corrected_tool["function"]["parameters"] = corrected_params
        
        return corrected_tool
    
    @staticmethod
    def validate_tools_list(tools: List[Dict[str, Any]], provider: str) -> List[Dict[str, Any]]:
        """
        Validate and correct a list of tools.
        
        Args:
            tools: List of tool definitions from MCP server
            provider: LLM provider name
            
        Returns:
            List of corrected tool definitions
        """
        corrected_tools = []
        
        for tool in tools:
            corrected_tool = MCPSchemaValidator.validate_tool_schema(tool, provider)
            corrected_tools.append(corrected_tool)
        
        # Apply provider-specific tool formatting after schema validation
        from .tool_formatter import ToolFormatter
        formatted_tools = ToolFormatter.format_tools_for_provider(corrected_tools, provider)
        
        return formatted_tools 
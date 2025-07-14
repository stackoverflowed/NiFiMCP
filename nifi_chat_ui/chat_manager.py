from __future__ import annotations # Add this import for forward references

# Placeholder for LLM interaction logic 

import google.generativeai as genai
from openai import OpenAI # Import OpenAI
import uuid # Added missing import
import sys # Add missing sys import
import os
# Import config from new location
try:
    # Add parent directory to Python path so we can import config
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    
    from config import settings as config 
except ImportError:
    # Need logger here, but setup might not have run. Use basic print.
    print("ERROR [chat_manager.py]: Failed to import config.settings.", file=sys.stderr)
    # Re-raise or handle gracefully depending on whether this module can run without config
    raise 

import streamlit as st # Import streamlit for UI error messages
import json
from typing import List, Dict, Any, Optional
# Import mcp_handler carefully, assuming it's in the same directory or PYTHONPATH
try:
    from .mcp_handler import get_available_tools, execute_mcp_tool
except ImportError:
    # Fallback if run as script or structure changes
    from mcp_handler import get_available_tools, execute_mcp_tool 
import sys
from loguru import logger # Import Loguru logger

# Import Anthropic SDK
try:
    import anthropic
    anthropic_available = True
except ImportError:
    anthropic_available = False
    logger.warning("anthropic package not available, Anthropic models will not be available.")

# Import tiktoken for accurate OpenAI token counting
try:
    import tiktoken
    tiktoken_available = True
except ImportError:
    tiktoken_available = False
    logger.warning("tiktoken not available, OpenAI token counting will use approximation.") # Use logger

# Import Gemini types for correct tool formatting
try:
    # Try importing the modules first
    import google.generativeai.types as genai_types 
    # Then access the specific classes
    Tool = genai_types.Tool
    FunctionDeclaration = genai_types.FunctionDeclaration
    gemini_types_imported = True
except ImportError as e:
    # Use logger for internal errors
    logger.error(f"Failed to import google.generativeai.types: {e}", exc_info=True)
    gemini_types_imported = False
    Tool = None
    FunctionDeclaration = None
except AttributeError as e:
    logger.error(f"Failed to access attributes within google.generativeai.types: {e}", exc_info=True)
    gemini_types_imported = False
    Tool = None
    FunctionDeclaration = None
except Exception as e:
    # Catch any other unexpected import errors
    logger.error(f"An unexpected error occurred during google.generativeai.types import: {e}", exc_info=True)
    gemini_types_imported = False
    Tool = None
    FunctionDeclaration = None

# --- Client Initialization --- #
# gemini_model = None # Store the initialized model - Removed, will instantiate per request
openai_client = None
perplexity_client = None
anthropic_client = None
is_initialized = False # Flag to track whether LLM clients have been initialized 

def configure_llms():
    """Configures LLM clients based on available keys and models."""
    # global gemini_model, openai_client, is_initialized # Removed gemini_model
    global openai_client, perplexity_client, anthropic_client, is_initialized
    
    # Configure Gemini (only API key setup here)
    if config.GOOGLE_API_KEY:
        try:
            genai.configure(api_key=config.GOOGLE_API_KEY)
            # Don't instantiate a specific model here
            # We need the model name which comes per request
            logger.info(f"Gemini API key configured. Available models: {config.GEMINI_MODELS}")
        except Exception as e:
            # Log internal error, avoid UI warning during import
            logger.error(f"Failed to configure Gemini API key: {e}", exc_info=True)
            # We don't nullify anything here, maybe the key is valid but listing models failed?

    # Configure OpenAI client
    # Always reinitialize when this function is called
    if config.OPENAI_API_KEY:
        try:
            # First set to None to force a clean initialization
            openai_client = None
            
            # Ensure we have a fresh API key value
            api_key = str(config.OPENAI_API_KEY).strip()
            if not api_key:
                logger.error("OpenAI API key is empty or whitespace")
                openai_client = None
                # Need to handle this failure state better - perhaps return False?
                is_initialized = False # Mark as not initialized if key is bad
                return # Don't proceed if key is bad
            
            # Create a new client with explicit parameters
            logger.debug(f"Creating new OpenAI client with API key (length: {len(api_key)})")
            openai_client = OpenAI(
                api_key=api_key,
                timeout=60.0,  # Explicit timeout
                max_retries=2  # Limit retries to avoid hanging
            )
            
            # Test the client with a minimal API call
            logger.debug("Testing OpenAI client with models.list()...")
            _ = openai_client.models.list() # Use list instead of retrieve which needs a model name
            
            logger.info(f"OpenAI client configured. Available models: {config.OPENAI_MODELS}")
        except Exception as e:
            # Log internal error, avoid UI warning during import
            logger.error(f"Failed to initialize OpenAI client: {e}", exc_info=True)
            openai_client = None
            # is_initialized = False # Mark as not initialized if setup fails - Handled below

    # Configure Perplexity client (OpenAI-compatible)
    if config.PERPLEXITY_API_KEY:
        try:
            # First set to None to force a clean initialization
            perplexity_client = None
            
            # Ensure we have a fresh API key value
            api_key = str(config.PERPLEXITY_API_KEY).strip()
            if not api_key:
                logger.error("Perplexity API key is empty or whitespace")
                perplexity_client = None
                return # Don't proceed if key is bad
            
            # Create a new client with Perplexity's base URL
            logger.debug(f"Creating new Perplexity client with API key (length: {len(api_key)})")
            perplexity_client = OpenAI(
                api_key=api_key,
                base_url="https://api.perplexity.ai",
                timeout=60.0,  # Explicit timeout
                max_retries=2  # Limit retries to avoid hanging
            )
            
            logger.info(f"Perplexity client configured. Available models: {config.PERPLEXITY_MODELS}")
        except Exception as e:
            # Log internal error, avoid UI warning during import
            logger.error(f"Failed to initialize Perplexity client: {e}", exc_info=True)
            perplexity_client = None

    # Configure Anthropic client
    if config.ANTHROPIC_API_KEY and anthropic_available:
        try:
            # First set to None to force a clean initialization
            anthropic_client = None
            
            # Ensure we have a fresh API key value
            api_key = str(config.ANTHROPIC_API_KEY).strip()
            if not api_key:
                logger.error("Anthropic API key is empty or whitespace")
                anthropic_client = None
                return # Don't proceed if key is bad
            
            # Create a new Anthropic client
            logger.debug(f"Creating new Anthropic client with API key (length: {len(api_key)})")
            anthropic_client = anthropic.Anthropic(
                api_key=api_key,
                timeout=60.0,  # Explicit timeout
                max_retries=2  # Limit retries to avoid hanging
            )
            
            logger.info(f"Anthropic client configured. Available models: {config.ANTHROPIC_MODELS}")
        except Exception as e:
            # Log internal error, avoid UI warning during import
            logger.error(f"Failed to initialize Anthropic client: {e}", exc_info=True)
            anthropic_client = None

    # Update initialization status based on whether *at least one* client/key is ready
    # And if *at least one* model list is non-empty for the configured keys
    gemini_ready = bool(config.GOOGLE_API_KEY and config.GEMINI_MODELS)
    openai_ready = bool(openai_client and config.OPENAI_MODELS) # Check client and models
    perplexity_ready = bool(perplexity_client and config.PERPLEXITY_MODELS) # Check client and models
    anthropic_ready = bool(anthropic_client and config.ANTHROPIC_MODELS) # Check client and models
    
    is_initialized = gemini_ready or openai_ready or perplexity_ready or anthropic_ready
    
    if not is_initialized:
         logger.warning("LLM configuration incomplete: No valid API key and corresponding model list found.")
    else:
         logger.info(f"LLM configuration status: Gemini Ready={gemini_ready}, OpenAI Ready={openai_ready}, Perplexity Ready={perplexity_ready}, Anthropic Ready={anthropic_ready}")


# Do NOT call configure_llms() during module import as it can cause
# Streamlit errors with set_page_config(). Instead, we'll call this
# function from app.py after setting the page config.

# --- Tool Management --- #

# @st.cache_data(ttl=3600) # Consider caching if MCP calls are slow
def get_formatted_tool_definitions(
    provider: str,
    raw_tools: list[dict] | None, # Accept raw tools as input
    user_request_id: str | None = None
) -> List[Dict[str, Any]] | List["genai_types.FunctionDeclaration"] | None:
    """Formats a given list of raw tool definitions for the specified LLM provider."""
    bound_logger = logger.bind(user_request_id=user_request_id) # Bind context
    global FunctionDeclaration  # Make FunctionDeclaration accessible in function scope
    
    # Convert provider to lowercase for case-insensitive comparison
    provider = provider.lower()
    bound_logger.debug(f"Formatting tools for provider: {provider}")
    
    # Use the provided raw_tools list instead of fetching
    # tools = get_available_tools(user_request_id=user_request_id) # REMOVED fetch
    tools = raw_tools # Use the passed-in list
    
    if not tools:
        bound_logger.warning("No raw tools provided for formatting.")
        # st.warning("Failed to retrieve tools from MCP handler.") # Don't show UI warning here
        return None # Return None if fetching failed

    if provider in ["openai", "perplexity"]:
        # Both OpenAI and Perplexity use the same format (OpenAI-compatible)
        # OpenAI format matches our API format directly, but let's clean up the schema
        cleaned_tools = []
        for tool in tools:
            if tool.get("type") == "function" and isinstance(tool.get("function"), dict):
                function_def = tool["function"]
                tool_name = function_def.get("name", "") # Get tool name for specific fixes
                
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
                                bound_logger.debug(f"Setting default type 'string' for empty/invalid property '{prop_name}' in tool '{tool_name}'")
                                props[prop_name] = {"type": "string"} # Default to string type
                                prop_value = props[prop_name] # Update prop_value for subsequent checks
                            
                            # --- SPECIFIC FIX for update_nifi_processor_config.update_data --- 
                            if tool_name == "update_nifi_processor_config" and prop_name == "update_data":
                                bound_logger.debug(f"Applying specific schema fix for {tool_name}.{prop_name}")
                                props[prop_name] = {
                                    # Keep original description if available
                                    "description": prop_value.get("description", "A dictionary (for 'properties') or list (for 'relationships') representing the update."),
                                    # Use anyOf to represent Union[Dict, List]
                                    "anyOf": [
                                        { "type": "object" },
                                        { 
                                            "type": "array",
                                            "items": { "type": "string" } # Specify items are strings
                                        }
                                    ]
                                }
                            # --- END SPECIFIC FIX --- 
                
                cleaned_tools.append(tool)
        
        bound_logger.debug(f"Formatted {len(cleaned_tools)} tools for {provider.title()}.")
        return cleaned_tools
    elif provider == "anthropic":
        if not anthropic_available:
            bound_logger.error("anthropic package not available, cannot format tools for Anthropic.")
            st.error("anthropic package not available. Please install with: pip install anthropic")
            return None
        
        # Convert OpenAI format to Anthropic format
        anthropic_tools = []
        
        for tool_data in tools:
            func_details = tool_data.get("function", {})
            name = func_details.get("name")
            description = func_details.get("description")
            parameters_schema = func_details.get("parameters") 
            
            if not name or not description:
                continue
                
            # Convert OpenAI parameters format to Anthropic input_schema format
            # Anthropic uses the same JSON Schema structure but expects it under "input_schema"
            anthropic_tool = {
                "name": name,
                "description": description,
                "input_schema": parameters_schema or {"type": "object", "properties": {}}
            }
            
            anthropic_tools.append(anthropic_tool)
            
        if not anthropic_tools:
            bound_logger.warning("No valid tool definitions found after formatting for Anthropic.")
            st.warning("No valid tool definitions could be formatted for Anthropic.") # Keep UI warning
            return None
        
        bound_logger.debug(f"Formatted {len(anthropic_tools)} tools for Anthropic.")
        return anthropic_tools
    elif provider == "gemini":
        if not gemini_types_imported:
            bound_logger.error("Required google-generativeai types could not be imported. Cannot format tools for Gemini.")
            st.error("Required google-generativeai types could not be imported. Cannot format tools for Gemini.") # Keep UI error
            return None
            
        if not FunctionDeclaration:
            bound_logger.error("Gemini FunctionDeclaration class is not available. Cannot format tools.")
            st.error("Internal Error: Gemini FunctionDeclaration class is not available.") # Keep UI error
            return None
        
        # Convert OpenAI format list to a list containing Gemini FunctionDeclaration objects
        all_declarations = []
        
        for tool_data in tools:
            func_details = tool_data.get("function", {})
            name = func_details.get("name")
            description = func_details.get("description")
            parameters_schema = func_details.get("parameters") 
            
            if not name or not description:
                continue
                
            # Clean up parameters schema for Gemini
            bound_logger.debug(f"🔍 Starting schema cleaning for tool '{name}'")
            cleaned_schema = parameters_schema.copy() if parameters_schema else {}
            corrections_applied = []  # Track corrections for this tool
            if isinstance(cleaned_schema, dict):
                # Always set top-level type to OBJECT if it has properties
                if "properties" in cleaned_schema:
                    cleaned_schema["type"] = "OBJECT"
                
                # Helper function for intelligent type inference
                def _infer_property_type(prop_name, prop_schema):
                    """
                    Intelligently infer the property type based on name, description, and other hints.
                    
                    Args:
                        prop_name: The name of the property
                        prop_schema: The property schema dictionary
                        
                    Returns:
                        The inferred type as a string (uppercase for Gemini)
                    """
                    prop_name_lower = prop_name.lower()
                    description = prop_schema.get("description", "").lower()
                    
                    # Numeric types based on field names
                    if any(keyword in prop_name_lower for keyword in [
                        "timeout", "limit", "count", "size", "max", "min", "port", "seconds",
                        "minutes", "hours", "days", "bytes", "kb", "mb", "gb", "id", "num",
                        "number", "index", "position", "width", "height", "x", "y", "z"
                    ]):
                        # Check if description suggests integer or float
                        if any(keyword in description for keyword in ["integer", "whole", "count"]):
                            return "INTEGER"
                        else:
                            return "NUMBER"
                    
                    # Boolean types
                    if any(keyword in prop_name_lower for keyword in [
                        "is_", "has_", "can_", "should_", "enabled", "disabled", "active", 
                        "inactive", "include", "exclude", "allow", "deny", "required", "optional"
                    ]):
                        return "BOOLEAN"
                    
                    # Object types based on description
                    if any(keyword in description for keyword in [
                        "dictionary", "object", "map", "properties", "configuration", "config",
                        "settings", "options", "parameters", "attrs", "attributes"
                    ]):
                        return "OBJECT"
                    
                    # Array types based on description
                    if any(keyword in description for keyword in [
                        "list", "array", "collection", "items", "elements", "values", "names",
                        "ids", "uuids", "entries"
                    ]):
                        return "ARRAY"
                    
                    # Default to STRING for everything else
                    return "STRING"
                
                # Add debug logging wrapper
                def _infer_property_type_with_logging(prop_name, prop_schema):
                    inferred_type = _infer_property_type(prop_name, prop_schema)
                    bound_logger.debug(f"Inferred type for '{prop_name}': {inferred_type} (based on description: '{prop_schema.get('description', '')[:50]}...')")
                    return inferred_type
                
                # Force type correction for known problematic fields
                def _force_type_correction(prop_name, existing_type, prop_schema):
                    """
                    Force correct types for known problematic fields, overriding incorrect types from MCP server.
                    
                    Args:
                        prop_name: The property name
                        existing_type: The current type (lowercase)
                        prop_schema: The property schema
                        
                    Returns:
                        The corrected type (uppercase for Gemini)
                    """
                    prop_name_lower = prop_name.lower()
                    description = prop_schema.get("description", "").lower()
                    
                    # CRITICAL: Don't change types for fields that have enums - they must be STRING
                    if "enum" in prop_schema:
                        bound_logger.debug(f"Preserving STRING type for '{prop_name}' (has enum values)")
                        return "STRING"
                    
                    # Don't change types for common enum fields even if enum isn't explicit
                    enum_fields = ["object_type", "search_scope", "target_type", "filter_object_type", "service_type", "processor_type"]
                    if prop_name_lower in enum_fields:
                        bound_logger.debug(f"Preserving STRING type for '{prop_name}' (likely enum field)")
                        return "STRING"
                    
                    # SPECIFIC FIELD CORRECTIONS based on error logs:
                    # Fix specific problematic fields that are incorrectly typed by MCP server
                    specific_corrections = {
                        # Fields that should be STRING but are incorrectly typed as OBJECT/NUMBER
                        "object_id": "STRING",
                        "query": "STRING", 
                        "filter_process_group_id": "STRING",
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
                        
                        # Fields that should be NUMBER but might be incorrectly typed
                        "timeout_seconds": "NUMBER",
                        "polling_timeout": "NUMBER",
                        "bulletin_limit": "INTEGER",
                        "max_content_bytes": "INTEGER",
                        "event_id": "INTEGER",
                        "position_x": "INTEGER",
                        "position_y": "INTEGER",
                        "width": "INTEGER",
                        "height": "INTEGER",
                        
                        # Fields that should be BOOLEAN
                        "include_bulletins": "BOOLEAN",
                        "include_suggestions": "BOOLEAN",
                        "recursive": "BOOLEAN",
                        "enabled": "BOOLEAN",
                        "disabled": "BOOLEAN",
                        "active": "BOOLEAN",
                        "required": "BOOLEAN",
                        "optional": "BOOLEAN",
                        
                        # Fields that should be OBJECT (configuration/properties)
                        "properties": "OBJECT",
                        "config": "OBJECT",
                        "configuration": "OBJECT",
                        "settings": "OBJECT",
                        "options": "OBJECT",
                        "parameters": "OBJECT",
                        "headers": "OBJECT",
                        "payload": "OBJECT",
                        
                        # Fields that should be ARRAY
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
                    
                    # Check if this field needs specific correction
                    if prop_name_lower in specific_corrections:
                        corrected_type = specific_corrections[prop_name_lower]
                        bound_logger.debug(f"🔍 SPECIFIC MAPPING for '{prop_name}': should be '{corrected_type}', currently '{existing_type.upper()}'")
                        if corrected_type != existing_type.upper():
                            bound_logger.debug(f"🎯 SPECIFIC CORRECTION for '{prop_name}': {existing_type.upper()} → {corrected_type}")
                            # CRITICAL FIX: If we're setting to ARRAY, also add items field to the prop_schema
                            if corrected_type == "ARRAY" and "items" not in prop_schema:
                                if prop_name_lower in ["operations", "objects", "updates", "processors", "ports", "connections", "controller_services", "nifi_objects"]:
                                    prop_schema["items"] = {"type": "OBJECT"}
                                elif prop_name_lower in ["relationships", "auto_terminated_relationships", "property_names_to_delete"]:
                                    prop_schema["items"] = {"type": "STRING"}
                                else:
                                    prop_schema["items"] = {"type": "OBJECT"}
                                bound_logger.info(f"🔧 ARRAY ITEMS FIX: Added items field for '{prop_name}': {prop_schema['items']}")
                            return corrected_type
                        else:
                            # Type is already correct, preserve it
                            bound_logger.debug(f"✅ PRESERVING correct type for '{prop_name}': {existing_type.upper()}")
                            return existing_type.upper()
                    
                    # Fallback to pattern-based corrections (only if not in specific corrections)
                    # Force numeric types for known timeout/numeric fields that are incorrectly typed as string
                    if existing_type == "string" and any(keyword in prop_name_lower for keyword in [
                        "timeout", "limit", "count", "size", "max", "min", "seconds",
                        "minutes", "hours", "days", "bytes", "port", "position", "width", "height", "x", "y", "z"
                    ]):
                        # Check description for more specific type hints
                        if any(keyword in description for keyword in ["integer", "whole", "count"]):
                            return "INTEGER"
                        else:
                            return "NUMBER"
                    
                    # Force boolean types for known boolean fields incorrectly typed as string
                    if existing_type == "string" and any(keyword in prop_name_lower for keyword in [
                        "include", "exclude", "enabled", "disabled", "active", "required", "optional"
                    ]):
                        return "BOOLEAN"
                    
                    # Force object types for properties/config fields incorrectly typed as string
                    if existing_type == "string" and (
                        prop_name_lower in ["properties", "config", "configuration", "settings", "options", "parameters", "headers"] or
                        any(keyword in description for keyword in ["dictionary", "object", "map", "properties"])
                    ):
                        return "OBJECT"
                    
                    # Force array types for known list fields incorrectly typed as string
                    if existing_type == "string" and any(keyword in description for keyword in [
                        "list", "array", "collection", "items", "elements"
                    ]):
                        # CRITICAL FIX: If we're setting to ARRAY, also add items field to the prop_schema
                        if "items" not in prop_schema:
                            if prop_name_lower in ["operations", "objects", "updates", "processors", "ports", "connections", "controller_services", "nifi_objects"]:
                                prop_schema["items"] = {"type": "OBJECT"}
                            elif prop_name_lower in ["relationships", "auto_terminated_relationships", "property_names_to_delete"]:
                                prop_schema["items"] = {"type": "STRING"}
                            else:
                                prop_schema["items"] = {"type": "OBJECT"}
                            bound_logger.info(f"🔧 ARRAY ITEMS FIX: Added items field for '{prop_name}': {prop_schema['items']}")
                        return "ARRAY"
                    
                    # Return the existing type (uppercase) if no correction needed
                    return existing_type.upper()
                
                # Recursively clean properties and array items
                def clean_gemini_schema(schema_node):
                    if not isinstance(schema_node, dict):
                        return schema_node

                    # Debug: Check if _prop_name is present
                    prop_name = schema_node.get("_prop_name", "unknown")
                    bound_logger.debug(f"🔍 CLEAN SCHEMA: Processing schema for '{prop_name}', type: {schema_node.get('type', 'no-type')}")

                    # Remove problematic fields
                    schema_node.pop("additionalProperties", None)
                    
                    # Handle properties
                    if "properties" in schema_node:
                        # If we have properties, this must be an OBJECT type
                        schema_node["type"] = "OBJECT"
                        props = schema_node["properties"]
                        for prop_name, prop_value in list(props.items()): # Use list for safe iteration
                            if isinstance(prop_value, dict):
                                # Clean nested schema
                                prop_value_copy = prop_value.copy()
                                prop_value_copy["_prop_name"] = prop_name  # Pass property name for array items logic
                                bound_logger.debug(f"🔍 PROP NAME PASS: Passing '{prop_name}' to clean_gemini_schema")
                                props[prop_name] = clean_gemini_schema(prop_value_copy)
                                # Ensure type is specified with intelligent inference and correction
                                if "type" not in props[prop_name]:
                                    if "properties" in props[prop_name]:
                                        props[prop_name]["type"] = "OBJECT"
                                        bound_logger.debug(f"Set type to OBJECT for '{prop_name}' (has properties)")
                                    elif "items" in props[prop_name]:
                                        props[prop_name]["type"] = "ARRAY"
                                        bound_logger.debug(f"Set type to ARRAY for '{prop_name}' (has items)")
                                    else:
                                        # Smart type inference based on field name and description
                                        inferred_type = _infer_property_type_with_logging(prop_name, props[prop_name])
                                        props[prop_name]["type"] = inferred_type
                                else:
                                    # FORCE type correction for known problematic fields
                                    existing_type = props[prop_name]["type"].lower()
                                    bound_logger.debug(f"🔍 Checking correction for '{prop_name}': existing_type='{existing_type}', has_enum={'enum' in props[prop_name]}")
                                    corrected_type = _force_type_correction(prop_name, existing_type, props[prop_name])
                                    bound_logger.debug(f"🔍 Correction result for '{prop_name}': {existing_type.upper()} → {corrected_type}")
                                    
                                    if corrected_type != existing_type.upper():
                                        correction_msg = f"'{prop_name}': {existing_type.upper()} → {corrected_type}"
                                        corrections_applied.append(correction_msg)
                                        bound_logger.info(f"🔧 SCHEMA CORRECTION: {correction_msg}")
                                        props[prop_name]["type"] = corrected_type
                                    else:
                                        bound_logger.debug(f"✅ Type '{props[prop_name]['type']}' preserved for '{prop_name}' (no correction needed)")
                                # Convert type to uppercase for Gemini (final step)
                                if "type" in props[prop_name]:
                                    original_type = props[prop_name]["type"]
                                    props[prop_name]["type"] = props[prop_name]["type"].upper()
                                    if original_type != props[prop_name]["type"]:
                                        bound_logger.debug(f"📝 Final type formatting: '{original_type}' → '{props[prop_name]['type']}' for '{prop_name}'")
                            else:
                                # Non-dict properties default to STRING type
                                props[prop_name] = {"type": "STRING"}
                    
                    # Handle arrays
                    if "items" in schema_node:
                        schema_node["type"] = "ARRAY"
                        if isinstance(schema_node["items"], dict):
                            schema_node["items"] = clean_gemini_schema(schema_node["items"].copy())
                    
                    # CRITICAL FIX: Ensure ARRAY types have required items field
                    if schema_node.get("type") == "ARRAY" and "items" not in schema_node:
                        # Add default items specification based on field name
                        prop_name = schema_node.get("_prop_name", "unknown")
                        bound_logger.info(f"🔧 ARRAY ITEMS FIX: Found ARRAY field '{prop_name}' without items field")
                        if prop_name in ["operations", "objects", "updates", "processors", "ports", "connections", "controller_services", "nifi_objects"]:
                            schema_node["items"] = {"type": "OBJECT"}
                            bound_logger.info(f"🔧 ARRAY ITEMS FIX: Added OBJECT items for '{prop_name}'")
                        elif prop_name in ["relationships", "auto_terminated_relationships", "property_names_to_delete"]:
                            schema_node["items"] = {"type": "STRING"}
                            bound_logger.info(f"🔧 ARRAY ITEMS FIX: Added STRING items for '{prop_name}'")
                        else:
                            # Generic default
                            schema_node["items"] = {"type": "OBJECT"}
                            bound_logger.info(f"🔧 ARRAY ITEMS FIX: Added default OBJECT items for '{prop_name}'")
                        bound_logger.info(f"🔧 ARRAY ITEMS FIX: Final schema for '{prop_name}': {schema_node}")
                    elif schema_node.get("type") == "ARRAY" and "items" in schema_node:
                        prop_name = schema_node.get("_prop_name", "unknown")
                        bound_logger.debug(f"✅ ARRAY field '{prop_name}' already has items field: {schema_node['items']}")
                    elif schema_node.get("type") == "ARRAY":
                        bound_logger.warning(f"⚠️ ARRAY field found but no _prop_name available: {schema_node}")
                    
                    # Ensure type is uppercase for Gemini
                    if "type" in schema_node:
                        schema_node["type"] = schema_node["type"].upper()
                    elif not any(key in schema_node for key in ["properties", "items", "enum"]):
                        # If no type and no complex structure, default to STRING
                        schema_node["type"] = "STRING"
                    
                    # Clean up temporary fields
                    schema_node.pop("_prop_name", None)
                    
                    return schema_node

                cleaned_schema = clean_gemini_schema(cleaned_schema)
                # Use a different variable name to avoid confusion if cleaning returns None unexpectedly
                schema_to_use = cleaned_schema if cleaned_schema is not None else {"type": "OBJECT", "properties": {}}

            # Log summary of corrections applied to this tool
            if corrections_applied:
                bound_logger.info(f"📋 Tool '{name}' schema corrections summary: {len(corrections_applied)} corrections applied")
                for correction in corrections_applied:
                    bound_logger.info(f"   - {correction}")
            else:
                bound_logger.debug(f"✅ Tool '{name}' schema required no corrections")

            # Create FunctionDeclaration with the fully cleaned schema
            try:
                 declaration = FunctionDeclaration(
                     name=name,
                     description=description,
                     parameters=schema_to_use # Use the cleaned schema 
                 )
                 all_declarations.append(declaration)
            except Exception as decl_error:
                 # Log internal error, show UI warning
                 bound_logger.error(f"Error creating FunctionDeclaration for '{name}': {decl_error}. Schema: {json.dumps(schema_to_use, indent=2)}", exc_info=True)
                 # print(f"ERROR creating FunctionDeclaration for '{name}': {decl_error}", file=sys.stderr)
                 # print(f"Schema that caused error: {json.dumps(schema_to_use, indent=2)}", file=sys.stderr)
                 st.warning(f"Skipping tool '{name}' due to schema error during FunctionDeclaration creation.") # Simplified UI warning
                 continue # Skip to the next tool
            
        if not all_declarations:
            bound_logger.warning("No valid tool declarations found after formatting for Gemini.")
            st.warning("No valid tool declarations could be formatted for Gemini.") # Keep UI warning
            return None
        
        bound_logger.info(f"🎯 Successfully formatted {len(all_declarations)} tools for Gemini with schema corrections applied.")
        return all_declarations
    else:
        bound_logger.error(f"Unsupported LLM provider for tool formatting: {provider}")
        st.error(f"Unsupported LLM provider for tool formatting: {provider}") # Keep UI error
        return None

# --- Helper Functions --- #

def count_tokens_openai(text: str, model: str) -> int: # Require model name
    """Count tokens using OpenAI's tiktoken library, with fallback for specific models."""
    if not tiktoken_available:
        # Fallback to approximation if tiktoken isn't available
        return len(text.split())
    
    encoding = None
    try:
        # First, try the standard model mapping
        encoding = tiktoken.encoding_for_model(model)
        # logger.debug(f"Using standard tiktoken encoding for model: {model}") # Optional logging
    except KeyError:
        # If standard mapping fails, try the common encoding for GPT-4 and new models
        try:
            encoding = tiktoken.get_encoding("cl100k_base")
        except Exception as e:
            logger.error(f"Failed to get 'cl100k_base' encoding: {e}. Using approximation.")
            # Fall back to approximation if cl100k_base also fails
            return len(text.split())
    except Exception as e:
        # Catch any other unexpected errors during encoding_for_model
        logger.warning(f"Unexpected error getting tiktoken encoding for model {model}: {e}. Using approximation.")
        return len(text.split())

    # If we successfully got an encoding (either standard or fallback)
    if encoding:
        try:
            return len(encoding.encode(text))
        except Exception as e:
            logger.warning(f"Error encoding text with tiktoken for model {model} (encoding: {encoding.name}): {e}. Using approximation.")
            return len(text.split())
    else:
        # Should not be reached if logic above is correct, but as a final fallback
        logger.error(f"Failed to obtain any tiktoken encoding for model {model}. Using approximation.")
        return len(text.split())

def count_tokens_gemini(text: str) -> int:
    """Approximate token count for Gemini models (character-based estimate)"""
    # For Gemini, we'll use a character-based estimate since there's no official tokenizer
    # Roughly 4 characters per token is a common approximation
    return len(text) // 4

def count_tokens_perplexity(text: str, model: str) -> int:
    """Count tokens for Perplexity models using OpenAI's tiktoken (since it's OpenAI-compatible)"""
    # Perplexity uses OpenAI-compatible API, so we can use the same tokenizer approach
    return count_tokens_openai(text, model)

def count_tokens_anthropic(text: str) -> int:
    """Approximate token count for Anthropic models (character-based estimate)"""
    # For Anthropic, we'll use a character-based estimate since there's no official tokenizer
    # Roughly 4 characters per token is a common approximation
    return len(text) // 4

def calculate_input_tokens(
    messages: List[Dict],
    provider: str,
    model_name: str,
    tools: List[Dict[str, Any]] | List["genai_types.FunctionDeclaration"] | None = None # Added tools param
) -> int:
    """Calculate total input tokens based on message history, provider, and tools."""
    total_tokens = 0
    provider_lower = provider.lower()

    # Calculate tokens for messages
    for message in messages:
        content = message.get("content", "")
        if isinstance(content, str):
            if provider_lower == "openai":
                total_tokens += count_tokens_openai(content, model_name)
            elif provider_lower == "perplexity":
                total_tokens += count_tokens_perplexity(content, model_name)
            elif provider_lower == "anthropic":
                total_tokens += count_tokens_anthropic(content)
            else: # gemini or others
                total_tokens += count_tokens_gemini(content)
        elif message.get("role") == "tool":
            # Simple approximation for tool results
            total_tokens += len(str(content)) // 4 
        elif isinstance(message.get("tool_calls"), list):
            # Simple approximation for tool requests
            total_tokens += len(json.dumps(message["tool_calls"])) // 4
            
    # Calculate tokens for tool definitions
    if tools:
        tools_str = ""
        try:
            if provider_lower in ["openai", "perplexity"]:
                # OpenAI and Perplexity tools are already JSON-serializable dicts
                tools_str = json.dumps(tools)
            elif provider_lower == "anthropic":
                # Anthropic tools are dicts with input_schema, should be JSON-serializable
                tools_str = json.dumps(tools)
            elif provider_lower == "gemini":
                # Gemini tools are FunctionDeclaration objects, need safe serialization
                # Convert each declaration to a dict representation for token counting
                tool_dicts = []
                for declaration in tools:
                    # Basic dict representation - might not be perfectly accurate
                    # but better than nothing. Adjust as needed for accuracy vs complexity.
                    param_dict = {} # Placeholder for parameters if needed
                    # Example: Accessing parameters requires knowledge of FunctionDeclaration structure
                    # For now, just using name and description for estimation
                    tool_dicts.append({
                        "name": getattr(declaration, 'name', ''),
                        "description": getattr(declaration, 'description', ''),
                        # Add parameters serialization here if more accuracy is needed
                    })
                tools_str = json.dumps(tool_dicts)
            else:
                 logger.warning(f"Token calculation for tools not implemented for provider: {provider}")

            # Count tokens for the serialized tool string
            if tools_str:
                tool_tokens = 0
                if provider_lower == "openai":
                    tool_tokens = count_tokens_openai(tools_str, model_name)
                elif provider_lower == "perplexity":
                    tool_tokens = count_tokens_perplexity(tools_str, model_name)
                elif provider_lower == "anthropic":
                    tool_tokens = count_tokens_anthropic(tools_str)
                else: # Assume Gemini or other
                    tool_tokens = count_tokens_gemini(tools_str)
                
                # logger.debug(f"Calculated tool definition tokens: {tool_tokens}") # Optional logging
                total_tokens += tool_tokens
                
        except Exception as e:
            logger.warning(f"Error estimating token count for tool definitions: {e}")

    # logger.debug(f"Calculated total input tokens: {total_tokens} for {provider}")
    return total_tokens

# --- Core LLM Interaction Functions --- #

def _convert_mapcomposite_to_dict(value):
    """
    Recursively convert MapComposite objects and other Google Proto objects to serializable dictionaries.
    
    Args:
        value: The value to convert, which may be a MapComposite, list, dict, or primitive type
        
    Returns:
        A JSON-serializable representation of the value
    """
    # Handle MapComposite objects
    if hasattr(value, 'items') and callable(value.items):
        # This is a MapComposite or similar dict-like object
        result = {}
        for key, val in value.items():
            result[key] = _convert_mapcomposite_to_dict(val)  # Recursive conversion
        return result
    
    # Handle lists (including ListComposite)
    elif isinstance(value, list) or (hasattr(value, '__iter__') and not isinstance(value, (str, bytes))):
        try:
            return [_convert_mapcomposite_to_dict(item) for item in value]
        except TypeError:
            # If iteration fails, convert to string
            return str(value)
    
    # Handle dictionaries
    elif isinstance(value, dict):
        result = {}
        for key, val in value.items():
            result[key] = _convert_mapcomposite_to_dict(val)
        return result
    
    # Handle Google Proto objects and other complex objects
    elif hasattr(value, '__dict__') or hasattr(value, '_pb') or str(type(value)).startswith('<google'):
        # Try to convert to dict if it has dict-like behavior
        if hasattr(value, 'items') and callable(value.items):
            result = {}
            for key, val in value.items():
                result[key] = _convert_mapcomposite_to_dict(val)
            return result
        else:
            # Fall back to string representation
            return str(value)
    
    # Handle primitive types - test if JSON serializable
    else:
        try:
            json.dumps(value)
            return value
        except (TypeError, OverflowError):
            return str(value)

def get_gemini_response(
    messages: List[Dict[str, Any]], 
    system_prompt: str,
    tools: List["genai_types.FunctionDeclaration"] | None, # Gemini requires FunctionDeclaration
    model_name: str, # Added: Specific model to use
    user_request_id: str | None = None, # Added context ID
    action_id: str | None = None # Added: Specific action ID for this LLM call
) -> Dict[str, Any]:
    """Gets a response from the Gemini model, handling potential tool calls."""
    # Bind both user_request_id and action_id to the logger
    bound_logger = logger.bind(user_request_id=user_request_id, action_id=action_id)

    tool_id_to_name_map = {} # Initialize the map here

    if not config.GOOGLE_API_KEY: # Check if API key is configured
        bound_logger.error("Gemini API key not configured. Cannot get response.")
        # Use st.error in the calling function (app.py) if needed for UI feedback
        return {"error": "Gemini API key not configured."}
        
    if not model_name or model_name not in config.GEMINI_MODELS:
        bound_logger.error(f"Invalid or missing Gemini model specified: {model_name}. Available: {config.GEMINI_MODELS}")
        return {"error": f"Invalid Gemini model specified: {model_name}"}

    # --- Instantiate Gemini Model ---
    try:
        bound_logger.debug(f"Instantiating Gemini model: {model_name}")
        model_instance = genai.GenerativeModel(
            model_name,
            # system_instruction=system_prompt # Apply system prompt during generation if supported this way, else prepend
        )
        bound_logger.info(f"Using Gemini model: {model_name}")
    except Exception as e:
        bound_logger.error(f"Failed to instantiate Gemini model {model_name}: {e}", exc_info=True)
        return {"error": f"Failed to instantiate Gemini model {model_name}: {str(e)}"}
    # -----------------------------

    # Prepare messages: Prepend system prompt if necessary and convert format
    gemini_history = []
    system_instruction_applied = False
    
    # First pass: identify assistant tool calls and build ID-to-name mapping
    for msg in messages:
        if msg["role"] == "assistant" and "tool_calls" in msg:
            for tc in msg.get("tool_calls", []):
                if tc.get("id") and tc.get("function", {}).get("name"):
                    tool_id_to_name_map[tc["id"]] = tc["function"]["name"]
                    bound_logger.debug(f"Mapped tool ID {tc['id']} to function name {tc['function']['name']}")

    # Second pass: convert messages to Gemini format
    for msg in messages:
        role = msg["role"]
        content = msg.get("content")
        tool_calls = msg.get("tool_calls") # OpenAI format
        tool_call_id = msg.get("tool_call_id") # For tool role
        
        # Convert OpenAI roles to Gemini roles
        if role == "user":
            gemini_role = "user"
        elif role == "assistant":
            gemini_role = "model" 
        elif role == "tool":
            gemini_role = "function"
        else:
            bound_logger.warning(f"Unknown role in message: {role}, defaulting to user")
            gemini_role = "user"
        
        # For regular text messages
        if role in ["user", "assistant"] and content:
            gemini_history.append({"role": gemini_role, "parts": [content]})

        # Handle Assistant requesting tool calls (OpenAI format -> Gemini format)
        if role == "assistant" and tool_calls:
            # Build a message with both content and function call if available
            parts = []
            if content:
                parts.append(content)
            
            # Create function call parts
            for tc in tool_calls:
                function_call = tc.get("function")
                if function_call:
                    # Parse arguments as JSON
                    try:
                        args = json.loads(function_call.get("arguments", "{}"))
                    except json.JSONDecodeError:
                        args = {}
                        bound_logger.warning(f"Failed to parse arguments for function {function_call.get('name')}")
                    
                    # Create the function call dictionary
                    fc_dict = {
                        "function_call": {
                            "name": function_call.get("name"),
                            "args": args
                        }
                    }
                    parts.append(fc_dict)
            
            # Only add if we have parts
            if parts:
                gemini_history.append({"role": gemini_role, "parts": parts})
        
        # Handle Tool execution results using Gemini's function format
        elif role == "tool" and tool_call_id:
            # Look up the function name from our mapping
            function_name = tool_id_to_name_map.get(tool_call_id)
            
            if not function_name:
                bound_logger.warning(f"Could not find function name for tool_call_id: {tool_call_id}. Using generic name.")
                function_name = f"unknown_function_{tool_call_id[-6:]}"
            
            # Parse the content - should be JSON if possible
            try:
                if isinstance(content, str):
                    if content.strip().startswith(("{", "[")):
                        result_content = json.loads(content)
                    else:
                        # For non-JSON strings, wrap in a result object
                        result_content = {"result": content}
                else:
                    # For non-string content (should be rare)
                    result_content = {"result": str(content)}
            except json.JSONDecodeError:
                bound_logger.warning(f"Failed to parse tool result as JSON: {content[:100]}...")
                result_content = {"result": content}
            
            # Convert lists to a specific structure for Gemini that doesn't cause items() errors
            if isinstance(result_content, list):
                # Wrap the list in a dictionary with a 'results' key
                result_content = {"results": result_content}
                bound_logger.debug(f"Converted list result to dictionary with 'results' key for {function_name}")
            
            # Add the function result in Gemini format
            gemini_history.append({
                "role": "function",
                "parts": [{
                    "function_response": {
                        "name": function_name,
                        "response": result_content
                    }
                }]
            })
        
    bound_logger.debug(f"Prepared Gemini history with {len(gemini_history)} entries.")
    
    try:
        # Configure the model instance with the system prompt for this call
        model_instance = genai.GenerativeModel(
             model_name, 
             system_instruction=system_prompt
        )
        
        # --- Log LLM Request ---
        
        # Helper function to safely serialize tools for logging
        def _safe_serialize_tools(tools_list):
            if not tools_list:
                return None
                
            safe_tools = []
            for tool in tools_list:
                # Handle Schema objects safely to avoid serialization errors
                if hasattr(tool, 'parameters'):
                    # Convert Schema to string representation for logging
                    params_str = str(tool.parameters) if tool.parameters else None
                    safe_tool = {
                        'name': getattr(tool, 'name', None),
                        'description': getattr(tool, 'description', None),
                        'parameters_str': params_str  # Use string representation
                    }
                    safe_tools.append(safe_tool)
                else:
                    # If it's not the expected structure, convert whole object to string
                    safe_tools.append(str(tool))
            
            return safe_tools

        request_payload = {
            "model": model_name,
            "history": gemini_history, # Log prepared history
            # Use helper function to safely serialize tools for logging
            "tools": _safe_serialize_tools(tools), 
            "system_instruction": system_prompt,
            # Add relevant generation_config if needed
        }
        bound_logger.bind(
            interface="llm", 
            direction="request", 
            data=request_payload
        ).debug("Sending request to Gemini API")
        # -----------------------
        
        # Generate content
        bound_logger.info(f"Sending request to Gemini model: {model_name}") # Use model_name
        
        # Add more debug tracing before the API call
        bound_logger.debug(f"Gemini history structure: {json.dumps([{'role': item.get('role'), 'parts_count': len(item.get('parts', []))} for item in gemini_history], indent=2)}")
        
        try:
            response = model_instance.generate_content(
                gemini_history,
                tools=tools if tools else None,
                # tool_config=genai_types.ToolConfig(function_calling_config="AUTO") # Let Gemini decide
                generation_config=genai.types.GenerationConfig(
                    # candidate_count=1, # Defaults to 1
                    # stop_sequences=['...'],
                    # max_output_tokens=2048,
                    temperature=0.3, # Adjust creativity/predictability - REDUCED from 0.7
                )
            )
            bound_logger.info("Received response from Gemini model.")
        except Exception as api_error:
            # Catch specific API call failures
            bound_logger.error(f"API call to Gemini failed: {api_error}", exc_info=True)
            return {"error": f"API call failed: {str(api_error)}"}

        # --- Log LLM Response --- 
        try:
            # Define helper functions for safe serialization
            def _safe_part_to_dict(part):
                if not part: 
                    return None
                data = {}
                
                # Handle text
                if hasattr(part, 'text') and part.text:
                    data['text'] = part.text
                
                # Handle function_call
                if hasattr(part, 'function_call') and part.function_call:
                    fc = part.function_call
                    
                    # More robust args handling for MapComposite and similar objects
                    if hasattr(fc, 'args'):
                        try:
                            # Convert MapComposite to plain dict recursively
                            args_dict = {}
                            if hasattr(fc.args, 'items') and callable(fc.args.items):
                                for key, value in fc.args.items():
                                    # Convert any complex objects to serializable types
                                    if hasattr(value, '__dict__') or hasattr(value, '_pb') or str(type(value)).startswith('<google'):
                                        args_dict[key] = str(value)
                                    elif hasattr(value, 'items') and callable(value.items):
                                        # Handle nested MapComposite objects
                                        args_dict[key] = dict(value.items())
                                    else:
                                        # Test if the value is JSON serializable
                                        try:
                                            json.dumps(value)
                                            args_dict[key] = value
                                        except (TypeError, OverflowError):
                                            args_dict[key] = str(value)
                            elif isinstance(fc.args, dict):
                                # Already a dict, but check each value for serializability
                                for key, value in fc.args.items():
                                    try:
                                        json.dumps(value)
                                        args_dict[key] = value
                                    except (TypeError, OverflowError):
                                        args_dict[key] = str(value)
                            else:
                                args_dict = {"raw_args": str(fc.args)}
                        except Exception as e:
                            args_dict = {"serialization_error": str(e), "raw_args": str(fc.args)}
                    else:
                        args_dict = {}
                    
                    data['function_call'] = {'name': fc.name, 'args': args_dict}
                
                # Handle function_response
                if hasattr(part, 'function_response') and part.function_response:
                    fr = part.function_response
                    # Handle response data safely
                    response_data = fr.response
                    try:
                        # If dict-like, convert to dict
                        if isinstance(response_data, dict) or (hasattr(response_data, 'items') and callable(response_data.items)):
                            response_data = dict(response_data)
                        # Lists should be preserved as lists
                        elif isinstance(response_data, list):
                            # No conversion needed for lists, already JSON serializable
                            pass
                        # Basic check if serializable, otherwise convert to string
                        json.dumps(response_data)
                    except (TypeError, OverflowError):
                        response_data = str(response_data) # Fallback to string
                    
                    data['function_response'] = {'name': fr.name, 'response': response_data}
                
                return data
            
            def _safe_candidate_to_dict(candidate):
                if not candidate: 
                    return None
                    
                # Extract parts safely
                parts = []
                if hasattr(candidate, 'content') and candidate.content and hasattr(candidate.content, 'parts'):
                    for part in candidate.content.parts:
                        parts.append(_safe_part_to_dict(part))
                
                return {'parts': parts}
                
            # Safely serialize the response for logging
            response_data = {
                'candidates': [_safe_candidate_to_dict(c) for c in response.candidates] if hasattr(response, 'candidates') else None,
                'prompt_feedback': None # Add prompt feedback serialization if needed
            }
            # Log the response data
            bound_logger.bind(
                interface="llm", 
                direction="response", 
                data=response_data
            ).debug("Received response from Gemini API")
        except Exception as logging_error:
            bound_logger.error(f"Error logging Gemini response (logging error only): {logging_error}", exc_info=True)
            # Continue processing response despite logging error
        # -----------------------

        # Check response structure before processing
        if not hasattr(response, 'parts'):
            bound_logger.error(f"Unexpected Gemini response structure - missing 'parts' attribute. Response type: {type(response)}")
            
            # Debug the response structure
            response_attrs = dir(response)
            bound_logger.debug(f"Response attributes: {response_attrs}")
            
            # Check for candidates
            if hasattr(response, 'candidates'):
                bound_logger.debug(f"Response has {len(response.candidates)} candidates")
                
                # Extract parts from first candidate if available
                if response.candidates and hasattr(response.candidates[0], 'content'):
                    candidate_content = response.candidates[0].content
                    if hasattr(candidate_content, 'parts'):
                        bound_logger.debug(f"Using parts from first candidate instead")
                        # Use the first candidate's parts
                        response_parts = candidate_content.parts
                    else:
                        bound_logger.error("Candidate content doesn't have parts either")
                        response_parts = []
                else:
                    bound_logger.error("No usable content in candidates")
                    response_parts = []
            else:
                bound_logger.error("No candidates available in response")
                response_parts = []
        else:
            response_parts = response.parts
        
        # Parse the response into a standardized format matching our API
        response_content = None
        response_tool_calls = []
        
        # Calculate token counts (needed for both success and error cases)
        token_count_in = calculate_input_tokens(messages, "Gemini", model_name, tools)
        
        # Check for function calls in the response parts with better error handling
        if response_parts:
            bound_logger.debug(f"Processing {len(response_parts)} parts from response")
            for i, part in enumerate(response_parts):
                try:
                    # Debug the part structure
                    part_attrs = dir(part)
                    bound_logger.debug(f"Part {i} attributes: {part_attrs}")
                    
                    if hasattr(part, 'text') and part.text:
                        response_content = part.text
                        bound_logger.debug(f"Found text content in part {i}")
                    
                    if hasattr(part, 'function_call') and part.function_call:
                        bound_logger.debug(f"Found function_call in part {i}")
                        # Convert Gemini FunctionCall back to OpenAI format ToolCall
                        fc = part.function_call
                        
                        # Debug the function call structure
                        fc_attrs = dir(fc)
                        bound_logger.debug(f"Function call attributes: {fc_attrs}")
                        
                        # More careful args handling with improved MapComposite support
                        if hasattr(fc, 'args'):
                            try:
                                # Debug: log the raw fc.args before conversion
                                bound_logger.debug(f"Function call args type: {type(fc.args)}, value: {fc.args}")
                                
                                # Handle MapComposite and similar objects more robustly
                                if hasattr(fc.args, 'items') and callable(fc.args.items):
                                    # Convert MapComposite to plain dict with safe serialization
                                    safe_args = {}
                                    for key, value in fc.args.items():
                                        # FIXED: Properly handle MapComposite objects recursively
                                        safe_args[key] = _convert_mapcomposite_to_dict(value)
                                    args_str = json.dumps(safe_args)
                                    bound_logger.debug(f"Converted MapComposite args: {args_str}")
                                elif isinstance(fc.args, dict):
                                    # Already a dict, but check each value for serializability
                                    safe_args = {}
                                    for key, value in fc.args.items():
                                        safe_args[key] = _convert_mapcomposite_to_dict(value)
                                    args_str = json.dumps(safe_args)
                                    bound_logger.debug(f"Converted dict args: {args_str}")
                                else:
                                    # Not a dict or dict-like, convert to string
                                    args_str = json.dumps({"raw_args": str(fc.args)})
                                    bound_logger.debug(f"Converted non-dict args: {args_str}")
                            except Exception as args_error:
                                # If all else fails, create a safe fallback
                                args_str = json.dumps({"serialization_error": str(args_error), "raw_args": str(fc.args)})
                                bound_logger.error(f"Error converting function call args: {args_error}")
                        else:
                            # FIXED: fc.args doesn't exist, set empty args
                            args_str = "{}"
                            bound_logger.debug("Function call has no args attribute, using empty object")
                        
                        response_tool_calls.append({
                            "id": str(uuid.uuid4()), # Generate shorter ID (just UUID)
                            "type": "function",
                            "function": {
                                "name": fc.name,
                                "arguments": args_str
                            }
                        })
                        bound_logger.debug(f"Added tool call for function: {fc.name}")
                except Exception as part_error:
                    bound_logger.error(f"Error processing response part {i}: {part_error}", exc_info=True)
                    # Continue with other parts
        else:
            # ENHANCED ERROR DETECTION - No parts found is a critical error
            bound_logger.error("CRITICAL: No parts found in Gemini response - investigating cause...")
            
            # Check for finish_reason indicating why response failed
            finish_reasons = []
            malformed_function_call_detected = False
            
            if hasattr(response, 'candidates') and response.candidates:
                for i, candidate in enumerate(response.candidates):
                    if hasattr(candidate, 'finish_reason'):
                        finish_reason = str(candidate.finish_reason)
                        finish_reasons.append(f"Candidate {i}: {finish_reason}")
                        
                        if 'MALFORMED_FUNCTION_CALL' in finish_reason:
                            malformed_function_call_detected = True
                            bound_logger.error(f"MALFORMED_FUNCTION_CALL detected in candidate {i}")
                        elif 'SAFETY' in finish_reason:
                            bound_logger.error(f"Safety filter triggered in candidate {i}")
                        elif 'MAX_TOKENS' in finish_reason:
                            bound_logger.error(f"Max tokens exceeded in candidate {i}")
                        elif 'STOP' in finish_reason:
                            bound_logger.debug(f"Normal stop condition in candidate {i}")
                        else:
                            bound_logger.error(f"Unknown finish reason: {finish_reason}")
            
            # Log comprehensive diagnostic information
            diagnostic_info = {
                "response_type": str(type(response)),
                "response_attributes": dir(response),
                "finish_reasons": finish_reasons,
                "malformed_function_call": malformed_function_call_detected,
                "has_candidates": hasattr(response, 'candidates'),
                "candidate_count": len(response.candidates) if hasattr(response, 'candidates') and response.candidates else 0,
                "tools_provided": len(tools) if tools else 0,
                "tools_names": [getattr(t, 'name', 'unknown') for t in tools] if tools else [],
            }
            
            # Add candidate details if available
            if hasattr(response, 'candidates') and response.candidates:
                diagnostic_info["candidate_details"] = []
                for i, candidate in enumerate(response.candidates):
                    candidate_info = {
                        "index": i,
                        "finish_reason": str(getattr(candidate, 'finish_reason', 'unknown')),
                        "has_content": hasattr(candidate, 'content'),
                        "content_parts_count": len(candidate.content.parts) if hasattr(candidate, 'content') and candidate.content and hasattr(candidate.content, 'parts') else 0
                    }
                    
                    # Check safety ratings
                    if hasattr(candidate, 'safety_ratings') and candidate.safety_ratings:
                        candidate_info["safety_ratings"] = []
                        for rating in candidate.safety_ratings:
                            rating_info = {
                                "category": str(getattr(rating, 'category', 'unknown')),
                                "probability": str(getattr(rating, 'probability', 'unknown'))
                            }
                            candidate_info["safety_ratings"].append(rating_info)
                    
                    diagnostic_info["candidate_details"].append(candidate_info)
            
            # Log the comprehensive diagnostic info
            bound_logger.bind(
                diagnostic_data=diagnostic_info
            ).error("Gemini response diagnostic information")
            
            # Specific error messages based on detected issues
            if malformed_function_call_detected:
                error_msg = "MALFORMED_FUNCTION_CALL: Gemini could not parse the function call arguments. This likely indicates a tool schema issue."
                bound_logger.error(error_msg)
                if tools:
                    bound_logger.error(f"Problem likely with one of these tools: {[getattr(t, 'name', 'unknown') for t in tools]}")
                    # Log the specific tool schemas for debugging
                    for i, tool in enumerate(tools):
                        tool_name = getattr(tool, 'name', 'unknown')
                        tool_params = getattr(tool, 'parameters', None)
                        bound_logger.error(f"Tool {i+1}/{len(tools)} '{tool_name}' schema: {str(tool_params)[:500]}...")
                        
                        # Check for common Gemini schema issues
                        if tool_params:
                            schema_issues = []
                            # Check for empty or missing required fields
                            if not tool_params.get('properties'):
                                schema_issues.append("No properties defined")
                            
                            # Check for complex nested objects that Gemini might struggle with
                            properties = tool_params.get('properties', {})
                            for prop_name, prop_def in properties.items():
                                if isinstance(prop_def, dict):
                                    prop_type = prop_def.get('type_')
                                    if prop_type == 'OBJECT' and not prop_def.get('properties'):
                                        schema_issues.append(f"Property '{prop_name}' is OBJECT type but lacks properties definition")
                                    elif prop_type == 'ARRAY':
                                        items = prop_def.get('items', {})
                                        if isinstance(items, dict) and items.get('type_') == 'OBJECT' and not items.get('properties'):
                                            schema_issues.append(f"Property '{prop_name}' array items lack properties definition")
                            
                            if schema_issues:
                                bound_logger.error(f"Potential schema issues in '{tool_name}': {', '.join(schema_issues)}")
                        else:
                            bound_logger.error(f"Tool '{tool_name}' has no parameters schema!")
                
                # Return an error response for MALFORMED_FUNCTION_CALL
                return {
                    "error": "Function call schema error: Gemini detected malformed function call arguments. Please check tool parameter schemas.",
                    "error_type": "MALFORMED_FUNCTION_CALL",
                    "content": None,
                    "tool_calls": [],
                    "token_count_in": token_count_in,
                    "token_count_out": 0,
                    "diagnostic_info": diagnostic_info
                }
            
            elif finish_reasons and any('SAFETY' in reason for reason in finish_reasons):
                error_msg = "SAFETY: Gemini's safety filters blocked the response."
                bound_logger.error(error_msg)
                return {
                    "error": "Safety filter triggered: Gemini blocked the response due to safety concerns.",
                    "error_type": "SAFETY_FILTER",
                    "content": None,
                    "tool_calls": [],
                    "token_count_in": token_count_in,
                    "token_count_out": 0,
                    "diagnostic_info": diagnostic_info
                }
            
            elif finish_reasons and any('MAX_TOKENS' in reason for reason in finish_reasons):
                error_msg = "MAX_TOKENS: Gemini response was truncated due to token limit."
                bound_logger.error(error_msg)
                return {
                    "error": "Token limit exceeded: Gemini response was truncated.",
                    "error_type": "MAX_TOKENS",
                    "content": None,
                    "tool_calls": [],
                    "token_count_in": token_count_in,
                    "token_count_out": 0,
                    "diagnostic_info": diagnostic_info
                }
            
            else:
                error_msg = f"UNKNOWN: Gemini returned empty response for unknown reasons. Finish reasons: {finish_reasons}"
                bound_logger.error(error_msg)
                return {
                    "error": f"Empty response from Gemini: {error_msg}",
                    "error_type": "EMPTY_RESPONSE",
                    "content": None,
                    "tool_calls": [],
                    "token_count_in": token_count_in,
                    "token_count_out": 0,
                    "diagnostic_info": diagnostic_info
                }
        
        # Calculate output token counts for successful responses
        token_count_out = count_tokens_gemini(response_content if response_content else "") if response_content else 0
        bound_logger.debug(f"Token counts - In: {token_count_in}, Out: {token_count_out}")

        # Log the response details
        bound_logger.debug(f"Gemini Response: Content={response_content is not None}, ToolCalls={len(response_tool_calls)}")

        return {
            "content": response_content, 
            "tool_calls": response_tool_calls,
            "token_count_in": token_count_in,
            "token_count_out": token_count_out
        }

    except Exception as e:
        bound_logger.error(f"Error during Gemini API call: {e}", exc_info=True)
        st.error(f"An error occurred while communicating with the Gemini API: {e}")
        return {"error": str(e)}

def get_openai_response(
    messages: List[Dict[str, Any]], 
    system_prompt: str, 
    tools: List[Dict[str, Any]] | None, # OpenAI uses dict format
    model_name: str, # Added: Specific model to use
    user_request_id: str | None = None, # Added context ID
    action_id: str | None = None # Added: Specific action ID for this LLM call
) -> Dict[str, Any]:
    """Gets a response from the OpenAI model, handling potential tool calls."""
    # Bind both user_request_id and action_id to the logger
    bound_logger = logger.bind(user_request_id=user_request_id, action_id=action_id)

    if not openai_client: # Check if client was configured
        bound_logger.error("OpenAI client is not configured. Cannot get response.")
        # UI error handled by calling function
        return {"error": "OpenAI not configured or configuration failed."}
        
    if not model_name or model_name not in config.OPENAI_MODELS:
        bound_logger.error(f"Invalid or missing OpenAI model specified: {model_name}. Available: {config.OPENAI_MODELS}")
        return {"error": f"Invalid OpenAI model specified: {model_name}"}

    # Prepend system prompt as the first message if not already present
    openai_messages = messages.copy()
    if not openai_messages or openai_messages[0]["role"] != "system":
        openai_messages.insert(0, {"role": "system", "content": system_prompt})
    
    bound_logger.debug(f"Prepared OpenAI messages with {len(openai_messages)} entries.")

    try:
        bound_logger.info(f"Sending request to OpenAI model: {model_name}") # Use model_name
        
        # Double-check that client is properly initialized before making API call
        if openai_client is None:
            bound_logger.error("OpenAI client is not initialized or was incorrectly initialized.")
            st.error("OpenAI client not initialized. Please check your API key and try again.")
            return {"error": "OpenAI client not initialized. Please check your API key and try again."}
        
        # --- Log LLM Request ---
        request_payload = {
            "model": model_name, # Use model_name
            "messages": openai_messages,
            "tools": tools,
            "tool_choice": "auto" if tools else None,
            #"temperature": 0.7,
        }
        bound_logger.bind(
            interface="llm", 
            direction="request", 
            data=request_payload
        ).debug("Sending request to OpenAI API")
        # -----------------------
        
        # Try a small API call first to verify the client is working
        try:
            bound_logger.debug("Verifying OpenAI client is working...")
            # Simply check if we can retrieve the model info without errors
            try:
                _ = openai_client.models.retrieve(model_name) # Use model_name
                bound_logger.debug("OpenAI client verification successful.")
            except Exception as model_error:
                # Try models.list() as a fallback
                bound_logger.warning(f"Could not retrieve specific model: {model_error}. Trying models.list() instead.")
                models = openai_client.models.list()
                if not models or not hasattr(models, 'data') or not models.data:
                    raise ValueError("Models list is empty or invalid")
                bound_logger.debug(f"OpenAI client verification with models.list successful. Found {len(models.data)} models.")
        except Exception as verify_error:
            bound_logger.error(f"OpenAI client verification failed: {verify_error}", exc_info=True)
            st.error(f"OpenAI client verification failed: {str(verify_error)}")
            return {"error": f"OpenAI client verification failed: {str(verify_error)}"}
        
        # Now attempt the actual completions API call
        bound_logger.debug(f"Sending messages to OpenAI: {json.dumps(openai_messages, indent=2)}") # Log the exact payload
        response = openai_client.chat.completions.create(
            model=model_name, # Use model_name
            messages=openai_messages,
            tools=tools if tools else None,
            tool_choice="auto" if tools else None,
            #temperature=0.7,
        )
        bound_logger.info("Received response from OpenAI model.")
        
        # --- Log LLM Response --- 
        try:
            # Safely convert response object to dict for logging
            # Use a safer approach than model_dump() which might raise errors
            response_dict = {}
            
            # Extract only the fields we need
            if hasattr(response, 'id'):
                response_dict['id'] = response.id
                
            if hasattr(response, 'choices') and response.choices:
                # Safe extraction of choices
                choices = []
                for choice in response.choices:
                    choice_dict = {'index': getattr(choice, 'index', 0)}
                    
                    # Extract message safely
                    if hasattr(choice, 'message'):
                        message = choice.message
                        message_dict = {
                            'role': getattr(message, 'role', None),
                            'content': getattr(message, 'content', None)
                        }
                        
                        # Handle tool calls safely
                        if hasattr(message, 'tool_calls') and message.tool_calls:
                            tool_calls = []
                            for tc in message.tool_calls:
                                tc_dict = {
                                    'id': getattr(tc, 'id', None),
                                    'type': getattr(tc, 'type', None),
                                }
                                
                                # Extract function data safely
                                if hasattr(tc, 'function'):
                                    tc_dict['function'] = {
                                        'name': getattr(tc.function, 'name', None),
                                        # Arguments might be JSON string - keep as is
                                        'arguments': getattr(tc.function, 'arguments', None)
                                    }
                                
                                tool_calls.append(tc_dict)
                            
                            message_dict['tool_calls'] = tool_calls
                        
                        choice_dict['message'] = message_dict
                    
                    # Add finish reason
                    if hasattr(choice, 'finish_reason'):
                        choice_dict['finish_reason'] = choice.finish_reason
                        
                    choices.append(choice_dict)
                
                response_dict['choices'] = choices
            
            # Add model and usage info if available
            if hasattr(response, 'model'):
                response_dict['model'] = response.model
                
            if hasattr(response, 'usage'):
                usage = response.usage
                response_dict['usage'] = {
                    'prompt_tokens': getattr(usage, 'prompt_tokens', 0),
                    'completion_tokens': getattr(usage, 'completion_tokens', 0),
                    'total_tokens': getattr(usage, 'total_tokens', 0)
                }
                
            response_data_for_log = response_dict
        except Exception as log_e:
            response_data_for_log = {"error": f"Failed to serialize OpenAI response for logging: {log_e}"}
            bound_logger.warning(f"Could not serialize OpenAI response for logging: {log_e}")
            
        bound_logger.bind(
            interface="llm", 
            direction="response", 
            data=response_data_for_log
        ).debug("Received response from OpenAI API")
        # ------------------------

        response_message = response.choices[0].message

        # Extract content and tool calls
        response_content = response_message.content
        response_tool_calls = response_message.tool_calls
        
        # Convert ToolCall objects to dictionaries if needed by app.py
        if response_tool_calls:
            response_tool_calls = [
                {
                    "id": tc.id,
                    "type": tc.type,
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments}
                } for tc in response_tool_calls
            ]
        
        # Calculate token counts - use OpenAI's reported usage when available
        token_count_in = getattr(response.usage, 'prompt_tokens', 0) if hasattr(response, 'usage') else calculate_input_tokens(messages, "OpenAI", model_name, tools)
        token_count_out = getattr(response.usage, 'completion_tokens', 0) if hasattr(response, 'usage') else 0

        bound_logger.debug(f"Token counts - In: {token_count_in}, Out: {token_count_out}")
        bound_logger.debug(f"OpenAI Response: Content={response_content is not None}, ToolCalls={len(response_tool_calls) if response_tool_calls else 0}")

        return {
            "content": response_content, 
            "tool_calls": response_tool_calls,
            "token_count_in": token_count_in,
            "token_count_out": token_count_out
        }

    except Exception as e:
        # Log the type of the exception for better debugging
        bound_logger.error(f"Caught exception of type: {type(e).__name__}, message: {str(e)}")
        
        # Try to extract more details if it's an OpenAI API error
        error_details = str(e)
        if hasattr(e, 'response'): # Check if it might be an httpx.HTTPStatusError or similar
            try:
                response_content = e.response.text
                error_details = f"{str(e)} - Response: {response_content[:500]}" # Limit response length
            except Exception as inner_e:
                bound_logger.warning(f"Could not extract response details from exception: {inner_e}")
        elif hasattr(e, 'body'): # Check for OpenAI specific error body
             error_details = f"{str(e)} - Body: {getattr(e, 'body', None)}" 
        
        # Log the potentially more detailed error
        bound_logger.error(f"Error during OpenAI API call: {error_details}", exc_info=True)
        st.error(f"An error occurred while communicating with the OpenAI API: {error_details}")
        # Return the more detailed error string if available
        return {"error": error_details}

def candidate_to_dict(candidate):
    # Helper to convert Candidate object safely for logging
    if not candidate: return None
    return {
        "content": {"parts": [part_to_dict(p) for p in candidate.content.parts] if candidate.content else None},
        "finish_reason": str(candidate.finish_reason) if hasattr(candidate, 'finish_reason') else None,
        "safety_ratings": [rating_to_dict(r) for r in candidate.safety_ratings] if hasattr(candidate, 'safety_ratings') else None,
        # Add other fields like token_count if available/needed
    }

def part_to_dict(part):
    # Helper to convert Part object safely for logging
    if not part: return None
    data = {}
    if hasattr(part, 'text') and part.text:
        data['text'] = part.text
    if hasattr(part, 'function_call') and part.function_call:
        fc = part.function_call
        # Convert MapComposite args to dict *before* storing for logging
        args_dict = dict(fc.args) if hasattr(fc, 'args') else {}
        data['function_call'] = {'name': fc.name, 'args': args_dict}
    if hasattr(part, 'function_response') and part.function_response:
        fr = part.function_response
        # Handle response data safely
        response_data = fr.response
        try:
            # If dict-like, convert to dict
            if isinstance(response_data, dict) or (hasattr(response_data, 'items') and callable(response_data.items)):
                response_data = dict(response_data)
            # Lists should be preserved as lists
            elif isinstance(response_data, list):
                # No conversion needed for lists, already JSON serializable
                pass
            # Basic check if serializable, otherwise convert to string
            json.dumps(response_data)
        except (TypeError, OverflowError):
            response_data = str(response_data) # Fallback to string
        data['function_response'] = {'name': fr.name, 'response': response_data}
    # Add other part types if needed (e.g., inline_data)
    return data

def prompt_feedback_to_dict(feedback):
    # Helper to convert PromptFeedback object safely for logging
    if not feedback: return None
    return {
        "block_reason": str(feedback.block_reason) if hasattr(feedback, 'block_reason') else None,
        "safety_ratings": [rating_to_dict(r) for r in feedback.safety_ratings] if hasattr(feedback, 'safety_ratings') else None,
    }

def rating_to_dict(rating):
    # Helper to convert SafetyRating object safely for logging
    if not rating: return None
    return {
        "category": str(rating.category) if hasattr(rating, 'category') else None,
        "probability": str(rating.probability) if hasattr(rating, 'probability') else None,
        "blocked": rating.blocked if hasattr(rating, 'blocked') else None,
    }

def get_perplexity_response(
    messages: List[Dict[str, Any]], 
    system_prompt: str, 
    tools: List[Dict[str, Any]] | None, # Perplexity uses OpenAI-compatible dict format
    model_name: str, # Added: Specific model to use
    user_request_id: str | None = None, # Added context ID
    action_id: str | None = None # Added: Specific action ID for this LLM call
) -> Dict[str, Any]:
    """Gets a response from the Perplexity model, handling potential tool calls."""
    # Bind both user_request_id and action_id to the logger
    bound_logger = logger.bind(user_request_id=user_request_id, action_id=action_id)

    if not perplexity_client: # Check if client was configured
        bound_logger.error("Perplexity client is not configured. Cannot get response.")
        # UI error handled by calling function
        return {"error": "Perplexity not configured or configuration failed."}
        
    if not model_name or model_name not in config.PERPLEXITY_MODELS:
        bound_logger.error(f"Invalid or missing Perplexity model specified: {model_name}. Available: {config.PERPLEXITY_MODELS}")
        return {"error": f"Invalid Perplexity model specified: {model_name}"}

    # Prepend system prompt as the first message if not already present
    perplexity_messages = messages.copy()
    if not perplexity_messages or perplexity_messages[0]["role"] != "system":
        perplexity_messages.insert(0, {"role": "system", "content": system_prompt})
    
    bound_logger.debug(f"Prepared Perplexity messages with {len(perplexity_messages)} entries.")

    try:
        bound_logger.info(f"Sending request to Perplexity model: {model_name}")
        
        # Double-check that client is properly initialized before making API call
        if perplexity_client is None:
            bound_logger.error("Perplexity client is not initialized or was incorrectly initialized.")
            st.error("Perplexity client not initialized. Please check your API key and try again.")
            return {"error": "Perplexity client not initialized. Please check your API key and try again."}
        
        # --- Log LLM Request ---
        request_payload = {
            "model": model_name,
            "messages": perplexity_messages,
            "tools": tools,
            "tool_choice": "auto" if tools else None,
            #"temperature": 0.7,
        }
        bound_logger.bind(
            interface="llm", 
            direction="request", 
            data=request_payload
        ).debug("Sending request to Perplexity API")
        # -----------------------
        
        # Now attempt the actual completions API call
        bound_logger.debug(f"Sending messages to Perplexity: {json.dumps(perplexity_messages, indent=2)}") # Log the exact payload
        response = perplexity_client.chat.completions.create(
            model=model_name,
            messages=perplexity_messages,
            tools=tools if tools else None,
            tool_choice="auto" if tools else None,
            #temperature=0.7,
        )
        bound_logger.info("Received response from Perplexity model.")
        
        # --- Log LLM Response --- 
        try:
            # Safely convert response object to dict for logging
            # Use a safer approach than model_dump() which might raise errors
            response_dict = {}
            
            # Extract only the fields we need
            if hasattr(response, 'id'):
                response_dict['id'] = response.id
                
            if hasattr(response, 'choices') and response.choices:
                # Safe extraction of choices
                choices = []
                for choice in response.choices:
                    choice_dict = {'index': getattr(choice, 'index', 0)}
                    
                    # Extract message safely
                    if hasattr(choice, 'message'):
                        message = choice.message
                        message_dict = {
                            'role': getattr(message, 'role', None),
                            'content': getattr(message, 'content', None)
                        }
                        
                        # Handle tool calls safely
                        if hasattr(message, 'tool_calls') and message.tool_calls:
                            tool_calls = []
                            for tc in message.tool_calls:
                                tc_dict = {
                                    'id': getattr(tc, 'id', None),
                                    'type': getattr(tc, 'type', None),
                                }
                                
                                # Extract function data safely
                                if hasattr(tc, 'function'):
                                    tc_dict['function'] = {
                                        'name': getattr(tc.function, 'name', None),
                                        # Arguments might be JSON string - keep as is
                                        'arguments': getattr(tc.function, 'arguments', None)
                                    }
                                
                                tool_calls.append(tc_dict)
                            
                            message_dict['tool_calls'] = tool_calls
                        
                        choice_dict['message'] = message_dict
                    
                    # Add finish reason
                    if hasattr(choice, 'finish_reason'):
                        choice_dict['finish_reason'] = choice.finish_reason
                        
                    choices.append(choice_dict)
                
                response_dict['choices'] = choices
            
            # Add model and usage info if available
            if hasattr(response, 'model'):
                response_dict['model'] = response.model
                
            if hasattr(response, 'usage'):
                usage = response.usage
                response_dict['usage'] = {
                    'prompt_tokens': getattr(usage, 'prompt_tokens', 0),
                    'completion_tokens': getattr(usage, 'completion_tokens', 0),
                    'total_tokens': getattr(usage, 'total_tokens', 0)
                }
                
            response_data_for_log = response_dict
        except Exception as log_e:
            response_data_for_log = {"error": f"Failed to serialize Perplexity response for logging: {log_e}"}
            bound_logger.warning(f"Could not serialize Perplexity response for logging: {log_e}")
            
        bound_logger.bind(
            interface="llm", 
            direction="response", 
            data=response_data_for_log
        ).debug("Received response from Perplexity API")
        # ------------------------

        response_message = response.choices[0].message

        # Extract content and tool calls
        response_content = response_message.content
        response_tool_calls = response_message.tool_calls
        
        # Convert ToolCall objects to dictionaries if needed by app.py
        if response_tool_calls:
            response_tool_calls = [
                {
                    "id": tc.id,
                    "type": tc.type,
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments}
                } for tc in response_tool_calls
            ]
        
        # Calculate token counts - use Perplexity's reported usage when available
        token_count_in = getattr(response.usage, 'prompt_tokens', 0) if hasattr(response, 'usage') else calculate_input_tokens(messages, "Perplexity", model_name, tools)
        token_count_out = getattr(response.usage, 'completion_tokens', 0) if hasattr(response, 'usage') else 0

        bound_logger.debug(f"Token counts - In: {token_count_in}, Out: {token_count_out}")
        bound_logger.debug(f"Perplexity Response: Content={response_content is not None}, ToolCalls={len(response_tool_calls) if response_tool_calls else 0}")

        return {
            "content": response_content, 
            "tool_calls": response_tool_calls,
            "token_count_in": token_count_in,
            "token_count_out": token_count_out
        }

    except Exception as e:
        # Log the type of the exception for better debugging
        bound_logger.error(f"Caught exception of type: {type(e).__name__}, message: {str(e)}")
        
        # Enhanced error message extraction for Perplexity API (OpenAI-compatible)
        error_details = str(e)
        user_friendly_message = error_details
        
        # Try to extract meaningful error message
        if hasattr(e, 'message') and e.message:
            user_friendly_message = e.message
        elif hasattr(e, 'body') and e.body:
            try:
                # Try to parse error body for meaningful message
                body = e.body
                if isinstance(body, dict) and 'message' in body:
                    user_friendly_message = body['message']
                elif isinstance(body, str):
                    try:
                        body_dict = json.loads(body)
                        if 'error' in body_dict and isinstance(body_dict['error'], dict):
                            user_friendly_message = body_dict['error'].get('message', user_friendly_message)
                        elif 'message' in body_dict:
                            user_friendly_message = body_dict['message']
                    except:
                        pass
            except Exception as body_parse_e:
                bound_logger.warning(f"Could not parse Perplexity error body: {body_parse_e}")
        elif hasattr(e, 'response'):
            try:
                response_content = e.response.text
                error_details = f"{str(e)} - Response: {response_content[:500]}"
                # Try to parse response for better error message
                try:
                    response_json = json.loads(response_content)
                    if isinstance(response_json, dict) and 'error' in response_json:
                        error_info = response_json['error']
                        if isinstance(error_info, dict) and 'message' in error_info:
                            user_friendly_message = error_info['message']
                        elif isinstance(error_info, str):
                            user_friendly_message = error_info
                except:
                    pass
            except Exception as inner_e:
                bound_logger.warning(f"Could not extract response details from exception: {inner_e}")
        
        # Log the full technical error details for debugging
        bound_logger.error(f"Error during Perplexity API call: {error_details}", exc_info=True)
        
        # Show user-friendly message in UI
        st.error(f"Perplexity API Error: {user_friendly_message}")
        
        # Return the user-friendly error message
        return {"error": user_friendly_message}

def get_anthropic_response(
    messages: List[Dict[str, Any]], 
    system_prompt: str, 
    tools: List[Dict[str, Any]] | None, # Anthropic tools have different format
    model_name: str, # Added: Specific model to use
    user_request_id: str | None = None, # Added context ID
    action_id: str | None = None # Added: Specific action ID for this LLM call
) -> Dict[str, Any]:
    """Gets a response from the Anthropic model, handling format conversion and tool calls."""
    # Bind both user_request_id and action_id to the logger
    bound_logger = logger.bind(user_request_id=user_request_id, action_id=action_id)

    if not anthropic_client: # Check if client was configured
        bound_logger.error("Anthropic client is not configured. Cannot get response.")
        # UI error handled by calling function
        return {"error": "Anthropic not configured or configuration failed."}
        
    if not model_name or model_name not in config.ANTHROPIC_MODELS:
        bound_logger.error(f"Invalid or missing Anthropic model specified: {model_name}. Available: {config.ANTHROPIC_MODELS}")
        return {"error": f"Invalid Anthropic model specified: {model_name}"}

    # Convert messages from OpenAI format to Anthropic format
    # Anthropic expects different structure:
    # - System message is passed separately
    # - Messages should alternate user/assistant
    # - Tool calls and tool results are handled differently
    
    anthropic_messages = []
    
    for msg in messages:
        role = msg.get("role")
        content = msg.get("content")
        
        if role == "system":
            # Skip system messages here - we'll use the system_prompt parameter
            continue
        elif role == "user":
            anthropic_messages.append({
                "role": "user",
                "content": content or ""
            })
        elif role == "assistant":
            # Assistant messages may have content and/or tool_calls
            assistant_content = []
            
            # Add text content if present
            if content:
                assistant_content.append({
                    "type": "text",
                    "text": content
                })
            
            # Convert tool calls to Anthropic format
            tool_calls = msg.get("tool_calls")
            if tool_calls:
                for tc in tool_calls:
                    function = tc.get("function", {})
                    # Parse arguments string to dict if needed
                    arguments = function.get("arguments", "{}")
                    if isinstance(arguments, str):
                        try:
                            arguments = json.loads(arguments)
                        except json.JSONDecodeError:
                            bound_logger.warning(f"Failed to parse tool call arguments: {arguments}")
                            arguments = {}
                    
                    assistant_content.append({
                        "type": "tool_use",
                        "id": tc.get("id", str(uuid.uuid4())),
                        "name": function.get("name", ""),
                        "input": arguments
                    })
            
            if assistant_content:
                anthropic_messages.append({
                    "role": "assistant",
                    "content": assistant_content
                })
        elif role == "tool":
            # Tool results become user messages in Anthropic format
            tool_call_id = msg.get("tool_call_id")
            anthropic_messages.append({
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_call_id,
                        "content": content or ""
                    }
                ]
            })
    
    bound_logger.debug(f"Converted {len(messages)} messages to {len(anthropic_messages)} Anthropic format messages.")

    try:
        bound_logger.info(f"Sending request to Anthropic model: {model_name}")
        
        # Double-check that client is properly initialized before making API call
        if anthropic_client is None:
            bound_logger.error("Anthropic client is not initialized or was incorrectly initialized.")
            st.error("Anthropic client not initialized. Please check your API key and try again.")
            return {"error": "Anthropic client not initialized. Please check your API key and try again."}
        
        # --- Log LLM Request ---
        request_payload = {
            "model": model_name,
            "max_tokens": 4096,  # Anthropic requires max_tokens
            "system": system_prompt,
            "messages": anthropic_messages,
            "tools": tools,
        }
        bound_logger.bind(
            interface="llm", 
            direction="request", 
            data=request_payload
        ).debug("Sending request to Anthropic API")
        # -----------------------
        
        # Now attempt the actual messages API call
        bound_logger.debug(f"Sending messages to Anthropic: {json.dumps(anthropic_messages, indent=2)}") # Log the exact payload
        response = anthropic_client.messages.create(
            model=model_name,
            max_tokens=4096,  # Anthropic requires max_tokens
            system=system_prompt,
            messages=anthropic_messages,
            tools=tools if tools else None,
        )
        bound_logger.info("Received response from Anthropic model.")
        
        # --- Log LLM Response --- 
        try:
            # Safely convert response object to dict for logging
            response_dict = {}
            
            # Extract basic fields
            for field in ['id', 'model', 'role', 'type']:
                if hasattr(response, field):
                    response_dict[field] = getattr(response, field)
            
            # Extract content
            if hasattr(response, 'content') and response.content:
                content_list = []
                for content_block in response.content:
                    content_dict = {'type': getattr(content_block, 'type', 'unknown')}
                    
                    if hasattr(content_block, 'text'):
                        content_dict['text'] = content_block.text
                    if hasattr(content_block, 'name'):
                        content_dict['name'] = content_block.name
                    if hasattr(content_block, 'id'):
                        content_dict['id'] = content_block.id
                    if hasattr(content_block, 'input'):
                        content_dict['input'] = content_block.input
                    
                    content_list.append(content_dict)
                
                response_dict['content'] = content_list
            
            # Extract usage info if available
            if hasattr(response, 'usage'):
                usage = response.usage
                response_dict['usage'] = {
                    'input_tokens': getattr(usage, 'input_tokens', 0),
                    'output_tokens': getattr(usage, 'output_tokens', 0)
                }
                
            response_data_for_log = response_dict
        except Exception as log_e:
            response_data_for_log = {"error": f"Failed to serialize Anthropic response for logging: {log_e}"}
            bound_logger.warning(f"Could not serialize Anthropic response for logging: {log_e}")
            
        bound_logger.bind(
            interface="llm", 
            direction="response", 
            data=response_data_for_log
        ).debug("Received response from Anthropic API")
        # ------------------------

        # Convert Anthropic response back to OpenAI-compatible format
        response_content = ""
        response_tool_calls = []
        
        for content_block in response.content:
            if hasattr(content_block, 'type'):
                if content_block.type == "text":
                    response_content += getattr(content_block, 'text', '')
                elif content_block.type == "tool_use":
                    # Convert to OpenAI format
                    tool_call = {
                        "id": getattr(content_block, 'id', str(uuid.uuid4())),
                        "type": "function",
                        "function": {
                            "name": getattr(content_block, 'name', ''),
                            "arguments": json.dumps(getattr(content_block, 'input', {}))
                        }
                    }
                    response_tool_calls.append(tool_call)
        
        # Calculate token counts - use Anthropic's reported usage when available
        token_count_in = getattr(response.usage, 'input_tokens', 0) if hasattr(response, 'usage') else calculate_input_tokens(messages, "Anthropic", model_name, tools)
        token_count_out = getattr(response.usage, 'output_tokens', 0) if hasattr(response, 'usage') else 0

        bound_logger.debug(f"Token counts - In: {token_count_in}, Out: {token_count_out}")
        bound_logger.debug(f"Anthropic Response: Content length={len(response_content)}, ToolCalls={len(response_tool_calls)}")

        return {
            "content": response_content or None, 
            "tool_calls": response_tool_calls if response_tool_calls else None,
            "token_count_in": token_count_in,
            "token_count_out": token_count_out
        }

    except Exception as e:
        # Log the type of the exception for better debugging
        bound_logger.error(f"Caught exception of type: {type(e).__name__}, message: {str(e)}")
        
        # Simple error logging
        bound_logger.error(f"Anthropic API error: {str(e)}")
        
        # Use the centralized error handling function
        user_friendly_message = extract_user_friendly_error(e, "anthropic")
        
        # Log the error details for debugging
        bound_logger.error(f"Error during Anthropic API call: {user_friendly_message}", exc_info=True)
        
        # Return the user-friendly error message
        return {"error": user_friendly_message}

def extract_user_friendly_error(exception: Exception, provider: str) -> str:
    """
    Extract user-friendly error messages from LLM API exceptions.
    
    Args:
        exception: The caught exception
        provider: The LLM provider name for context-specific handling
        
    Returns:
        A user-friendly error message
    """
    error_str = str(exception)
    
    # Provider-specific error handling
    if provider.lower() == "anthropic":
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
    
    elif provider.lower() == "openai":
        # OpenAI error handling
        if hasattr(exception, 'response'):
            try:
                response_content = exception.response.text
                # Try to parse OpenAI error response
                try:
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
                except:
                    pass
            except Exception:
                pass
    
    elif provider.lower() == "perplexity":
        # Perplexity error handling (OpenAI-compatible)
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
    
    # Generic error handling for all providers
    error_lower = error_str.lower()
    if 'rate limit' in error_lower or 'rate_limit' in error_lower:
        return "Rate limit exceeded. Please try again later or reduce the prompt length."
    elif 'not found' in error_lower or 'model not found' in error_lower:
        return "Model not found. Please check the model name or try a different model."
    elif 'authentication' in error_lower or 'api key' in error_lower:
        return "Authentication failed. Please check your API key."
    elif 'permission' in error_lower:
        return "Permission denied. Please check your API key permissions."
    elif 'quota' in error_lower:
        return "Quota exceeded. Please check your account billing."
    else:
        return f"{provider.title()} API error: {error_str}"

def get_llm_response(
    messages: List[Dict[str, Any]], 
    system_prompt: str, 
    tools: List[Dict[str, Any]] | None, 
    provider: str, 
    model_name: str,
    user_request_id: str | None = None,
    action_id: str | None = None
) -> Dict[str, Any]:
    """Generic LLM response dispatcher that routes to the appropriate provider."""
    # Use provided action ID or generate a new one for this specific LLM call
    if action_id is None:
        action_id = str(uuid.uuid4())
    bound_logger = logger.bind(user_request_id=user_request_id, action_id=action_id)
    
    # Normalize provider name
    provider_normalized = provider.lower().strip()
    
    bound_logger.info(f"Routing LLM request to provider: {provider_normalized}, model: {model_name}")
    
    # Ensure LLM clients are configured - this handles cases where the workflow
    # runs in a different context where configure_llms() wasn't called
    global openai_client, perplexity_client, anthropic_client, is_initialized
    if not is_initialized or (provider_normalized == "openai" and not openai_client) or \
       (provider_normalized == "perplexity" and not perplexity_client) or \
       (provider_normalized == "anthropic" and not anthropic_client):
        bound_logger.debug("LLM clients not initialized or missing, configuring now...")
        configure_llms()
    
    # Validate provider/model combination
    if provider_normalized == "openai":
        if not openai_client:
            return {"error": "OpenAI not configured. Please check your API key."}
        if model_name not in config.OPENAI_MODELS:
            return {"error": f"Invalid OpenAI model: {model_name}. Available: {config.OPENAI_MODELS}"}
        return get_openai_response(messages, system_prompt, tools, model_name, user_request_id, action_id)
    
    elif provider_normalized == "perplexity":
        if not perplexity_client:
            return {"error": "Perplexity not configured. Please check your API key."}
        if model_name not in config.PERPLEXITY_MODELS:
            return {"error": f"Invalid Perplexity model: {model_name}. Available: {config.PERPLEXITY_MODELS}"}
        return get_perplexity_response(messages, system_prompt, tools, model_name, user_request_id, action_id)
    
    elif provider_normalized == "anthropic":
        if not anthropic_client:
            return {"error": "Anthropic not configured. Please check your API key."}
        if model_name not in config.ANTHROPIC_MODELS:
            return {"error": f"Invalid Anthropic model: {model_name}. Available: {config.ANTHROPIC_MODELS}"}
        return get_anthropic_response(messages, system_prompt, tools, model_name, user_request_id, action_id)
    
    elif provider_normalized == "gemini":
        if not config.GOOGLE_API_KEY:
            return {"error": "Gemini not configured. Please check your API key."}
        if model_name not in config.GEMINI_MODELS:
            return {"error": f"Invalid Gemini model: {model_name}. Available: {config.GEMINI_MODELS}"}
        return get_gemini_response(messages, system_prompt, tools, model_name, user_request_id, action_id)
    
    else:
        bound_logger.error(f"Unsupported LLM provider: {provider}")
        return {"error": f"Unsupported LLM provider: {provider}. Supported: OpenAI, Perplexity, Anthropic, Gemini"}

# --- (Potentially other helper functions) --- 
from __future__ import annotations # Add this import for forward references

# Placeholder for LLM interaction logic 

import google.generativeai as genai
from openai import OpenAI # Import OpenAI
import config
import streamlit as st # Import streamlit for error messages
import json
from typing import List, Dict, Any, Optional
from mcp_handler import get_available_tools, execute_mcp_tool
import sys

# Import tiktoken for accurate OpenAI token counting
try:
    import tiktoken
    tiktoken_available = True
except ImportError:
    tiktoken_available = False
    print("WARNING: tiktoken not available, token counting will be less accurate")

# Import Gemini types for correct tool formatting
try:
    # Try importing the modules first
    import google.generativeai.types as genai_types 
    # Then access the specific classes
    Tool = genai_types.Tool
    FunctionDeclaration = genai_types.FunctionDeclaration
    gemini_types_imported = True
except ImportError as e:
    print(f"ERROR [chat_manager.py]: Failed to import google.generativeai.types: {e}", file=sys.stderr)
    gemini_types_imported = False
    Tool = None
    FunctionDeclaration = None
except AttributeError as e:
    print(f"ERROR [chat_manager.py]: Failed to access attributes within google.generativeai.types: {e}", file=sys.stderr)
    gemini_types_imported = False
    Tool = None
    FunctionDeclaration = None
except Exception as e:
    # Catch any other unexpected import errors
    print(f"ERROR [chat_manager.py]: An unexpected error occurred during google.generativeai.types import: {e}", file=sys.stderr)
    gemini_types_imported = False
    Tool = None
    FunctionDeclaration = None

# --- Client Initialization --- #
gemini_configured = False
openai_client = None

def configure_llms():
    """Configures both LLM clients based on available keys."""
    global gemini_configured, openai_client
    if config.GOOGLE_API_KEY and not gemini_configured:
        try:
            genai.configure(api_key=config.GOOGLE_API_KEY)
            gemini_configured = True
        except Exception as e:
            st.warning(f"Failed to configure Gemini: {e}")
            gemini_configured = False

    if config.OPENAI_API_KEY and openai_client is None:
        try:
            openai_client = OpenAI(api_key=config.OPENAI_API_KEY)
        except Exception as e:
            st.warning(f"Failed to initialize OpenAI client: {e}")
            openai_client = None

# Call configure_llms() once when the module is loaded
# Streamlit reruns the script, so state needs careful handling,
# but basic configuration can happen here.

# --- Tool Management --- #

# @st.cache_data(ttl=3600)  # Cache tool definitions for 1 hour
def get_formatted_tool_definitions(provider: str) -> List[Dict[str, Any]] | List["genai_types.FunctionDeclaration"] | None:
    """Gets tool definitions from the MCP server and formats them for the specified LLM provider."""
    global FunctionDeclaration  # Make FunctionDeclaration accessible in function scope
    
    # Convert provider to lowercase for case-insensitive comparison
    provider = provider.lower()
    
    tools = get_available_tools() # This returns list of OpenAI format dicts
    
    if not tools:
        st.warning("Failed to retrieve tools from MCP handler.")
        return None # Return None if fetching failed

    if provider == "openai":
        # OpenAI format matches our API format directly, but let's clean up the schema
        cleaned_tools = []
        for tool in tools:
            if tool.get("type") == "function" and isinstance(tool.get("function"), dict):
                function_def = tool["function"]
                # Clean up parameters if present
                if "parameters" in function_def and isinstance(function_def["parameters"], dict):
                    # Remove additionalProperties field which might cause issues
                    if "additionalProperties" in function_def["parameters"]:
                        del function_def["parameters"]["additionalProperties"]
                    
                    # Also clean properties if present
                    if "properties" in function_def["parameters"] and isinstance(function_def["parameters"]["properties"], dict):
                        for prop_name, prop_value in function_def["parameters"]["properties"].items():
                            if isinstance(prop_value, dict) and "additionalProperties" in prop_value:
                                del prop_value["additionalProperties"]
                
                cleaned_tools.append(tool)
        
        return cleaned_tools
    elif provider == "gemini":
        if not gemini_types_imported:
            st.error("Required google-generativeai types (Tool, FunctionDeclaration) could not be imported. Cannot format tools for Gemini.")
            return None
            
        if not FunctionDeclaration:
            st.error("FunctionDeclaration is not available. Cannot format tools for Gemini.")
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
            cleaned_schema = parameters_schema.copy() if parameters_schema else {}
            if isinstance(cleaned_schema, dict):
                # Always set top-level type to OBJECT if it has properties
                if "properties" in cleaned_schema:
                    cleaned_schema["type"] = "OBJECT"
                
                # Recursively clean properties and array items
                def clean_gemini_schema(schema_node):
                    if not isinstance(schema_node, dict):
                        return schema_node

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
                                props[prop_name] = clean_gemini_schema(prop_value.copy())
                                # Ensure type is specified
                                if "type" not in props[prop_name]:
                                    if "properties" in props[prop_name]:
                                        props[prop_name]["type"] = "OBJECT"
                                    elif "items" in props[prop_name]:
                                        props[prop_name]["type"] = "ARRAY"
                                    else:
                                        props[prop_name]["type"] = "STRING"
                                # Convert type to uppercase for Gemini
                                if "type" in props[prop_name]:
                                    props[prop_name]["type"] = props[prop_name]["type"].upper()
                            else:
                                # Non-dict properties default to STRING type
                                props[prop_name] = {"type": "STRING"}
                    
                    # Handle arrays
                    if "items" in schema_node:
                        schema_node["type"] = "ARRAY"
                        if isinstance(schema_node["items"], dict):
                            schema_node["items"] = clean_gemini_schema(schema_node["items"].copy())
                    
                    # Ensure type is uppercase for Gemini
                    if "type" in schema_node:
                        schema_node["type"] = schema_node["type"].upper()
                    elif not any(key in schema_node for key in ["properties", "items", "enum"]):
                        # If no type and no complex structure, default to STRING
                        schema_node["type"] = "STRING"
                    
                    return schema_node

                cleaned_schema = clean_gemini_schema(cleaned_schema)
                # Use a different variable name to avoid confusion if cleaning returns None unexpectedly
                schema_to_use = cleaned_schema if cleaned_schema is not None else {"type": "OBJECT", "properties": {}}

            # Create FunctionDeclaration with the fully cleaned schema
            try:
                 declaration = FunctionDeclaration(
                     name=name,
                     description=description,
                     parameters=schema_to_use # Use the cleaned schema 
                 )
                 all_declarations.append(declaration)
            except Exception as decl_error:
                 print(f"ERROR creating FunctionDeclaration for '{name}': {decl_error}", file=sys.stderr)
                 print(f"Schema that caused error: {json.dumps(schema_to_use, indent=2)}", file=sys.stderr)
                 st.warning(f"Skipping tool '{name}' due to schema error during FunctionDeclaration creation: {decl_error}")
                 continue # Skip to the next tool
            
        if not all_declarations:
            st.warning("No valid tool declarations found after formatting for Gemini.")
            return None
        
        return all_declarations
    else:
        st.error(f"Unsupported LLM provider for tool formatting: {provider}")
        return None

def handle_tool_call(tool_name: str, parameters: Dict[str, Any], display_in_ui: bool = False) -> Dict[str, Any]:
    """Executes a tool call and returns the result.
    
    Args:
        tool_name: The name of the tool to execute
        parameters: The parameters to pass to the tool
        display_in_ui: Whether to display the tool call in the UI (should be False when called from within LLM functions)
    """
    print(f"Executing tool: {tool_name} with parameters: {parameters}")
    
    # Only show the tool call to the user if explicitly requested
    # This prevents nested chat messages in Streamlit
    if display_in_ui:
        with st.chat_message("assistant"):
            st.markdown(f"🔧 Using tool: `{tool_name}`")
            st.json(parameters)
    
    result = execute_mcp_tool(tool_name, parameters)
    
    # If result is a string, it's probably an error message
    if isinstance(result, str):
        return {"error": result}
    
    return result

# --- Helper Functions --- #

def count_tokens_openai(text: str, model: str = "gpt-3.5-turbo") -> int:
    """Count tokens using OpenAI's tiktoken library"""
    if not tiktoken_available:
        # Fallback to approximation if tiktoken isn't available
        return len(text.split())
    
    try:
        encoding = tiktoken.encoding_for_model(model)
        return len(encoding.encode(text))
    except Exception as e:
        print(f"Error counting tokens with tiktoken: {e}")
        return len(text.split())

def count_tokens_gemini(text: str) -> int:
    """Approximate token count for Gemini models (character-based estimate)"""
    # For Gemini, we'll use a character-based estimate since there's no official tokenizer
    # Roughly 4 characters per token is a common approximation
    return len(text) // 4

def calculate_input_tokens(messages: List[Dict], provider: str) -> int:
    """Calculates the token count for the input messages."""
    total_tokens = 0
    if provider == "openai":
        # Ensure tiktoken is available
        if not tiktoken_available:
            print("Warning: tiktoken not available for precise OpenAI token counting.")
            return 0 # Or fallback to estimation
            
        # Select the appropriate model, default to gpt-3.5-turbo if unknown
        # This might need adjustment based on the actual model used in get_openai_response
        model = "gpt-3.5-turbo" # Make this dynamic if possible later
        try:
            encoding = tiktoken.encoding_for_model(model)
            for message in messages:
                # Add tokens for role and content, plus overhead per message
                # Ref: https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
                total_tokens += 4 # Every message follows <|im_start|>{role/name}\n{content}<|im_end|>

                for key, value in message.items():
                    if value: # Ensure value is not None
                         total_tokens += len(encoding.encode(str(value))) # Encode content/role/etc.
                    if key == "name": # If there's a name, the role is omitted
                        total_tokens -= 1 # Role is always required and always 1 token
            total_tokens += 2 # Every reply is primed with <|im_start|>assistant -> Adjust if needed
            return total_tokens
        except Exception as e:
            print(f"Error calculating OpenAI tokens: {e}")
            return 0 # Return 0 on error
            
    elif provider == "gemini":
        # Gemini token counting is simpler via API, but requires making a call.
        # For a rough estimate, we can count characters or use a basic word count.
        # Let's use a character count heuristic for now.
        # A more accurate way would be to call model.count_tokens(messages) if needed later.
        total_chars = sum(len(str(msg.get("content", ""))) for msg in messages)
        return total_chars // 4 # Very rough estimate: 1 token ~ 4 chars
    else:
        return 0 # Unknown provider

# --- LLM Response Handling --- #

def get_gemini_response(messages: List[Dict[str, Any]], system_prompt: str, tools: List["genai_types.FunctionDeclaration"] | None) -> Dict[str, Any]:
    """Gets a response from the Gemini model, handling potential tool calls.

    Args:
        messages: The chat history (list of dicts with 'role' and 'content').
        system_prompt: The system prompt string.
        tools: Formatted tool definitions for Gemini (List[FunctionDeclaration] or None).

    Returns:
        A dictionary containing:
            - 'content': The textual response (str) or None.
            - 'tool_calls': A list of requested tool calls ([{'name':..., 'arguments':...}]) or None.
            - 'token_count_in': Estimated input tokens.
            - 'token_count_out': Estimated output tokens.
    """
    configure_llms() # Ensure client is configured
    if not gemini_configured:
        return {"content": "Error: Gemini client not configured. Check API key.", "tool_calls": None, "token_count_in": 0, "token_count_out": 0}

    # Use configured model from config.py
    model_name = config.GEMINI_MODEL

    # Convert message history to Gemini format
    gemini_history = []
    for msg in messages:
        role = msg.get("role")
        content = msg.get("content", "") # Ensure content is a string
        
        # Attempt to handle potential FunctionCall/FunctionResponse formats if they appear
        # This might need refinement based on exactly how results are stored in messages
        if isinstance(content, dict) and 'function_call' in content: 
             # This was likely an older format, adapt if needed.
             # Current loop expects results formatted as simple assistant messages.
             continue
             
        # Map roles: user -> user, assistant -> model
        mapped_role = "user" if role == "user" else "model"
        
        # Basic structure: {"role": ..., "parts": [{"text": ...}]}
        # Handle potential tool results embedded in content (simple string format expected from app.py)
        gemini_history.append({"role": mapped_role, "parts": [{"text": str(content)}]})

    # Ensure history is not empty
    if not gemini_history:
         return {"content": "Error: Cannot generate response from empty or invalid history.", "tool_calls": None, "token_count_in": 0, "token_count_out": 0}

    try:
        # Initialize the model with system prompt
        model = genai.GenerativeModel(
            model_name=model_name,
            system_instruction=system_prompt 
        )

        # --- Generate Content ---
        response = model.generate_content(
            gemini_history,
            tools=tools, # Pass the formatted tools
        )

        # --- Process Response ---
        response_content = None
        response_tool_calls = []

        # Check for function calls in the response parts
        # Use response.candidates[0].content.parts
        if response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
            for part in response.candidates[0].content.parts:
                if part.function_call:
                    fc = part.function_call
                    # Convert arguments Struct to dict
                    args_dict = {key: value for key, value in fc.args.items()}
                    response_tool_calls.append({
                        "name": fc.name,
                        "arguments": args_dict
                    })
                elif part.text:
                    response_content = part.text

        # Calculate token counts (using heuristics for now)
        input_tokens = calculate_input_tokens(messages, "gemini")
        output_tokens = count_tokens_gemini(response_content or "") 
        # Add heuristic token count for tool calls if any
        if response_tool_calls:
             output_tokens += count_tokens_gemini(json.dumps(response_tool_calls)) # Rough estimate

        final_response = {
            "content": response_content,
            "tool_calls": response_tool_calls if response_tool_calls else None,
            "token_count_in": input_tokens,
            "token_count_out": output_tokens
        }
        return final_response

    except Exception as e:
        st.error(f"Error calling Gemini API: {e}")
        print(f"ERROR [get_gemini_response]: Gemini API call failed: {e}", file=sys.stderr)
        # Print details if available (e.g., response parts if error occurred during processing)
        try:
             # Ensure 'response' exists before accessing attributes
             if 'response' in locals() and response and response.prompt_feedback:
                  print(f"Gemini Safety Feedback: {response.prompt_feedback}", file=sys.stderr)
        except Exception as report_err:
             print(f"ERROR reporting Gemini error details: {report_err}", file=sys.stderr)
        return {"content": f"Error communicating with Gemini: {e}", "tool_calls": None, "token_count_in": 0, "token_count_out": 0}

def get_openai_response(messages: List[Dict[str, Any]], system_prompt: str, tools: List[Dict[str, Any]] | None) -> Dict[str, Any]:
    """Gets a response from the OpenAI model, handling potential tool calls.

    Args:
        messages: The chat history (list of dicts with 'role' and 'content', potentially including tool results).
        system_prompt: The system prompt string.
        tools: Formatted tool definitions for OpenAI (List[Dict] or None).

    Returns:
        A dictionary containing:
            - 'content': The textual response (str) or None.
            - 'tool_calls': A list of requested tool calls ([{'name':..., 'arguments':...}]) or None.
            - 'token_count_in': Input tokens (prompt_tokens).
            - 'token_count_out': Output tokens (completion_tokens).
    """
    configure_llms()  # Ensure client is configured
    if not openai_client:
        return {"content": "Error: OpenAI client not configured. Check API key.", "tool_calls": None, "token_count_in": 0, "token_count_out": 0}

    # Use configured model from config.py
    model = config.OPENAI_MODEL

    # --- Prepare messages for OpenAI API --- 
    openai_messages = []
    if system_prompt:
        openai_messages.append({"role": "system", "content": system_prompt})

    for msg in messages:
        role = msg.get("role")
        content = msg.get("content")

        # Check if the message is a tool result string added by app.py
        is_tool_result_message = (
            role == "assistant"
            and isinstance(content, str)
            and content.startswith("Tool call")
            and "Result:" in content
        )

        if is_tool_result_message:
            # Attempt to parse the tool result and format for OpenAI 'tool' role
            try:
                # Basic parsing: Extract tool name and JSON result
                lines = content.split('\n', 2) # Split into max 3 parts: tool line, Result line, rest
                tool_name_line = lines[0]
                tool_name = tool_name_line.split("'")[1] # Extract name between single quotes
                
                result_part = lines[2] # The part after "Result:\n"
                
                # Extract JSON string, assuming it's within ```json ... ``` markers
                json_start_marker = "```json"
                json_end_marker = "```"
                start_index = result_part.find(json_start_marker)
                end_index = result_part.rfind(json_end_marker)
                
                if start_index != -1 and end_index != -1 and start_index < end_index:
                    result_json_str = result_part[start_index + len(json_start_marker):end_index].strip()
                else:
                     # Fallback if markers aren't found - try finding json between {} 
                     start_index = result_part.find('{') 
                     end_index = result_part.rfind('}') 
                     if start_index != -1 and end_index != -1: 
                          result_json_str = result_part[start_index : end_index + 1] 
                     else: 
                          raise ValueError("Could not extract JSON result from tool message: " + result_part)

                parsed_result = json.loads(result_json_str)

                # Add message with 'tool' role for OpenAI
                # WARNING: tool_call_id is missing. OpenAI might struggle without it.
                openai_messages.append({
                    "role": "tool",
                    "tool_call_id": "PLACEHOLDER_ID", # Needs real ID from the request
                    "name": tool_name,
                    "content": json.dumps(parsed_result), # Tool result content must be a JSON string
                })
            except Exception as parse_error:
                print(f"ERROR [get_openai_response]: Failed to parse/convert tool result message: {parse_error}. Appending as plain assistant text.")
                # Fallback: Append the original string content as assistant message
                openai_messages.append({"role": "assistant", "content": str(content)})
        
        elif role == "assistant" and msg.get("tool_calls"):
            # Handle messages that already contain OpenAI-formatted tool *requests*
            openai_messages.append(msg) # Add the whole message dict
        
        elif role in ["user", "assistant"]:
            # Regular user/assistant text message
            openai_messages.append({"role": role, "content": str(content)})
        
        # Ignore other roles or message formats

    # --- Make API Call --- 
    try:
        api_response = openai_client.chat.completions.create(
            model=model,
            messages=openai_messages,
            tools=tools if tools else None,
            tool_choice="auto" if tools else None,
        )

        response_message = api_response.choices[0].message
        response_content = response_message.content
        openai_tool_calls = response_message.tool_calls # Original OpenAI tool calls list (or None)

        # Token counts
        token_count_in = api_response.usage.prompt_tokens if api_response.usage else 0
        token_count_out = api_response.usage.completion_tokens if api_response.usage else 0

        # --- Prepare Return Value for app.py (simplified tool_calls structure) --- 
        processed_tool_calls = None
        if openai_tool_calls:
            processed_tool_calls = []
            for tc in openai_tool_calls:
                # Extract structure expected by app.py: {"name": ..., "arguments": ...}
                # WARNING: Losing tool_call.id here!
                print(f"WARNING [get_openai_response]: OpenAI tool call ID '{tc.id}' is not being explicitly passed back to app.py's loop logic.")
                try:
                    arguments = json.loads(tc.function.arguments)
                except json.JSONDecodeError:
                    print(f"Warning: Could not JSON decode arguments for tool call {tc.function.name}: {tc.function.arguments}")
                    arguments = {"_raw_arguments": tc.function.arguments} # Pass raw string if decode fails
                
                processed_tool_calls.append({
                    "name": tc.function.name,
                    "arguments": arguments
                })

        final_response = {
            "content": response_content,
            "tool_calls": processed_tool_calls, # Simplified list for app.py loop
            "token_count_in": token_count_in,
            "token_count_out": token_count_out,
        }
        return final_response

    except Exception as e:
        st.error(f"Error calling OpenAI API: {e}")
        print(f"ERROR [get_openai_response]: OpenAI API call failed: {e}", file=sys.stderr)
        if hasattr(e, 'response') and e.response:
            try:
                print(f"OpenAI API Error Details: {e.response.text}", file=sys.stderr)
            except Exception:
                pass
        # Ensure all expected keys are present even in error case
        return {"content": f"Error communicating with OpenAI: {e}", "tool_calls": None, "token_count_in": 0, "token_count_out": 0}

# --- (Potentially other helper functions) --- 
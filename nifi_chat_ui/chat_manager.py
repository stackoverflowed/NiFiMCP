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
    print("DEBUG [chat_manager.py]: Attempting to import google.generativeai.types...")
    # Try importing the modules first
    import google.generativeai.types as genai_types 
    print(f"DEBUG [chat_manager.py]: Imported google.generativeai.types as genai_types: {dir(genai_types)}")
    # Then access the specific classes
    Tool = genai_types.Tool
    FunctionDeclaration = genai_types.FunctionDeclaration
    print("DEBUG [chat_manager.py]: Successfully accessed Tool, FunctionDeclaration.")
    gemini_types_imported = True
except ImportError as e:
    print(f"ERROR [chat_manager.py]: Failed to import google.generativeai.types: {e}", file=sys.stderr)
    gemini_types_imported = False
except AttributeError as e:
    print(f"ERROR [chat_manager.py]: Failed to access attributes within google.generativeai.types: {e}", file=sys.stderr)
    gemini_types_imported = False
except Exception as e:
    # Catch any other unexpected import errors
    print(f"ERROR [chat_manager.py]: An unexpected error occurred during google.generativeai.types import: {e}", file=sys.stderr)
    gemini_types_imported = False

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
            print("DEBUG [chat_manager.py]: Gemini configured.") # Add debug print
        except Exception as e:
            st.warning(f"Failed to configure Gemini: {e}")
            gemini_configured = False

    if config.OPENAI_API_KEY and openai_client is None:
        try:
            openai_client = OpenAI(api_key=config.OPENAI_API_KEY)
            print("DEBUG [chat_manager.py]: OpenAI client initialized.") # Add debug print
        except Exception as e:
            st.warning(f"Failed to initialize OpenAI client: {e}")
            openai_client = None

# Call configure_llms() once when the module is loaded
# Streamlit reruns the script, so state needs careful handling,
# but basic configuration can happen here.

# --- Tool Management --- #

@st.cache_data(ttl=3600)  # Cache tool definitions for 1 hour
def get_formatted_tool_definitions(provider: str) -> List[Dict[str, Any]] | List['Tool'] | None:
    """Gets tool definitions from the MCP server and formats them for the specified LLM provider."""
    tools = get_available_tools() # This returns list of OpenAI format dicts
    if not tools:
        st.warning("Failed to retrieve tools from MCP handler.")
        return None # Return None if fetching failed

    # Debug: Print raw tools received
    print(f"DEBUG [get_formatted_tool_definitions]: Raw tools from server: {json.dumps(tools, indent=2)}")

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
        
        print(f"DEBUG [get_formatted_tool_definitions]: Cleaned tools for OpenAI: {json.dumps(cleaned_tools, indent=2)}")
        return cleaned_tools
    elif provider == "gemini":
        if not gemini_types_imported:
            st.error("Required google-generativeai types (Tool, FunctionDeclaration) could not be imported. Cannot format tools for Gemini.")
            return None
        
        # Convert OpenAI format list to a list containing ONE Gemini Tool object
        all_declarations = []
        for tool_data in tools:
            # Basic validation
            if not isinstance(tool_data, dict) or tool_data.get("type") != "function" or not isinstance(tool_data.get("function"), dict):
                st.warning(f"Skipping tool with unexpected format: {tool_data}")
                continue
                
            func_details = tool_data.get("function", {})
            name = func_details.get("name")
            description = func_details.get("description")
            # Parameters should be a dict matching JSON Schema requirements
            parameters_schema = func_details.get("parameters") 
            
            if not name or not description:
                st.warning(f"Skipping tool with missing name or description: {func_details}")
                continue

            # Clean up parameters schema for Gemini
            if parameters_schema and isinstance(parameters_schema, dict):
                # Remove additionalProperties field which causes issues with Gemini
                if "additionalProperties" in parameters_schema:
                    del parameters_schema["additionalProperties"]
                
                # Also clean properties if present
                if "properties" in parameters_schema and isinstance(parameters_schema["properties"], dict):
                    for prop_name, prop_value in parameters_schema["properties"].items():
                        if isinstance(prop_value, dict):
                            # Remove additionalProperties if present
                            if "additionalProperties" in prop_value:
                                del prop_value["additionalProperties"]
                            
                            # Add default type "string" if no type is specified
                            if "type" not in prop_value or not prop_value["type"]:
                                prop_value["type"] = "string"
                                print(f"DEBUG: Added default type 'string' to property '{prop_name}'")
                        else:
                            # If property is empty or not a dict, replace with a basic string type
                            parameters_schema["properties"][prop_name] = {"type": "string"}
                            print(f"DEBUG: Replaced empty property '{prop_name}' with string type")

            # Create FunctionDeclaration with cleaned schema
            declaration = FunctionDeclaration(
                name=name,
                description=description,
                parameters=parameters_schema 
            )
            all_declarations.append(declaration)
            
        if not all_declarations:
            st.warning("No valid tool declarations found after formatting for Gemini.")
            return None
        
        # Debug: Print what we're creating for Gemini     
        print(f"DEBUG [get_formatted_tool_definitions]: Created {len(all_declarations)} FunctionDeclarations for Gemini")
        # For debugging, print the original schema dict instead of the FunctionDeclaration object
        if len(all_declarations) > 0 and parameters_schema:
            print(f"DEBUG: Sample cleaned parameters schema: {json.dumps(parameters_schema, indent=2)}")
        
        # Return a list containing one Tool object
        return [Tool(function_declarations=all_declarations)]
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
    """Calculate input tokens more accurately based on all messages"""
    if not messages:
        return 0
    
    # Serialize the entire message array to count all content
    serialized = json.dumps(messages)
    
    if provider.lower() == "openai":
        return count_tokens_openai(serialized)
    else:  # gemini
        return count_tokens_gemini(serialized)

# --- LLM Response Handling --- #

def get_gemini_response(chat_history: List[Dict[str, str]]) -> Dict[str, Any]:
    """Gets a response from the Gemini model based on the chat history."""
    global gemini_configured
    if not gemini_configured:
        # Attempt to reconfigure if key was added after startup
        configure_llms()
        if not gemini_configured:
            return {"error": "Error: Google API Key not configured or configuration failed."}

    try:
        model = genai.GenerativeModel('gemini-1.5-pro')
        
        # Get tool definitions in Gemini format (now returns List[Tool] or None)
        gemini_tool_config = get_formatted_tool_definitions("gemini")
        if gemini_tool_config is None:
            return {"error": "Error: Failed to get or format tool definitions for Gemini."}
        
        # Convert chat history to Gemini format
        gemini_history = []
        for msg in chat_history:
            role = "model" if msg["role"] == "assistant" else msg["role"]
            if role in ["user", "model"]:
                gemini_history.append({"role": role, "parts": [msg["content"]]})

        # Debug: Print the structure being sent
        print(f"DEBUG [get_gemini_response]: Sending History: {json.dumps(gemini_history, indent=2)}")
        print(f"DEBUG [get_gemini_response]: Sending Tools (count): {len(gemini_tool_config)}")
        
        # Make the API call with the correctly formatted tools
        response = model.generate_content(
            gemini_history,
            tools=gemini_tool_config, # Pass the list[Tool]
            # tool_config={'function_calling_config': 'AUTO'} # Alternate way for Gemini? Check docs if needed
        )

        # --- IMPORTANT: Adapt response parsing for Gemini --- 
        # The way tool calls and results are handled might differ slightly
        response_content = None
        function_call_part = None
        
        # Safely access candidate and parts
        try:
            candidate = response.candidates[0]
            if candidate.content and candidate.content.parts:
                 # Check for function call *first*
                 for part in candidate.content.parts:
                     if hasattr(part, 'function_call') and part.function_call:
                          function_call_part = part
                          break # Found the function call part
                 # If no function call, assume text content
                 if not function_call_part:
                      response_content = candidate.text # Use .text for safe text extraction
        except (IndexError, AttributeError) as e:
             print(f"DEBUG: Error parsing Gemini response structure: {e}")
             # Fallback or default response?
             response_content = response.text # Attempt to get text anyway

        # --- Handle Function Call if present ---
        if function_call_part:
            function_call = function_call_part.function_call
            tool_name = function_call.name
            # Gemini often puts args directly in function_call.args, might not need json.loads
            parameters = dict(function_call.args) 
            
            print(f"DEBUG [get_gemini_response]: Gemini requested tool '{tool_name}' with args: {parameters}")
            
            # Execute tool via handler (which uses REST API), don't display in UI from here
            tool_result_dict = handle_tool_call(tool_name, parameters, display_in_ui=False) 
            
            # Convert tool_result_dict to a simple JSON string
            tool_result_json = json.dumps(tool_result_dict)
            
            # Create a NEW history instead of appending to existing one to prevent nesting
            new_history = gemini_history.copy()
            
            # Add a simplified version of the model's response with the function call
            new_history.append({
                "role": "model", 
                "parts": [{"text": f"I'll use the tool {tool_name}({json.dumps(parameters)})"}]
            })
            
            # Add our function response as a simple text string
            # IMPORTANT: Use "user" role instead of "function" - Gemini only supports "user" and "model" roles
            new_history.append({
                "role": "user",  # Changed from "function" to "user"
                "parts": [{"text": f"Result of {tool_name}: {tool_result_json}"}]
            })

            # Debug: Print history before second call
            # Safer JSON serialization by avoiding complex objects
            history_for_debug = []
            for msg in new_history:
                serializable_msg = dict(msg)
                history_for_debug.append(serializable_msg)
            print(f"DEBUG [get_gemini_response]: Sending History (Post-Tool Call): {json.dumps(history_for_debug, indent=2)}")

            # Get final response from Gemini after providing tool result
            final_response = model.generate_content(new_history)

            # After processing the response
            tools_called = []  # Collect tools called
            
            # More accurate token counting
            # Input tokens - include all history plus the tool declarations
            gemini_history_str = json.dumps(gemini_history)
            tool_declaration_str = json.dumps(str(gemini_tool_config))  # Convert tool config to string for counting
            token_count_in = count_tokens_gemini(gemini_history_str + tool_declaration_str)
            
            # Output tokens - include both the initial response and final response
            initial_request_str = str(function_call_part) if function_call_part else ""
            tool_result_str = tool_result_json if 'tool_result_json' in locals() else ""
            final_response_str = final_response.text if hasattr(final_response, 'text') else ""
            token_count_out = count_tokens_gemini(initial_request_str + tool_result_str + final_response_str)

            # If a tool call was made, add it to tools_called
            if function_call_part:
                tools_called.append(function_call_part.function_call.name)

            return {
                "content": final_response.text or "No response content.",
                "tools_called": tools_called,
                "token_count_in": token_count_in,
                "token_count_out": token_count_out
            }
            
        # If no tool call, return the initial text content
        elif response_content is not None:
            # After processing the response
            tools_called = []  # Collect tools called
            
            # More accurate token counting
            # Input tokens - include all history plus the tool declarations
            gemini_history_str = json.dumps(gemini_history)
            tool_declaration_str = json.dumps(str(gemini_tool_config))  # Convert tool config to string for counting
            token_count_in = count_tokens_gemini(gemini_history_str + tool_declaration_str)
            
            # Output tokens - the response content
            token_count_out = count_tokens_gemini(response_content)

            # If a tool call was made, add it to tools_called
            if function_call_part:
                tools_called.append(function_call_part.function_call.name)

            return {
                "content": response_content or "No response content.",
                "tools_called": tools_called,
                "token_count_in": token_count_in,
                "token_count_out": token_count_out
            }
        else:
             # Handle cases where response is empty or unexpected
             print(f"DEBUG: Unexpected Gemini response structure: {response}")
             st.error("Received an unexpected or empty response from Gemini.")
             return {"error": "Sorry, I received an unexpected response from Gemini."}

    except Exception as e:
        import traceback
        traceback.print_exc()  # Print full traceback for better debugging
        st.error(f"An error occurred while contacting the Gemini API: {e}")
        return {"error": f"Sorry, I encountered an error while trying to get a response from Gemini: {e}"}

def get_openai_response(chat_history: List[Dict[str, str]]) -> Dict[str, Any]:
    """Gets a response from the OpenAI model."""
    global openai_client
    if openai_client is None:
        # Attempt to reconfigure if key was added after startup
        configure_llms()
        if openai_client is None:
            return {"error": "Error: OpenAI API Key not configured or client initialization failed."}

    try:
        # Get tool definitions in OpenAI format
        tools = get_formatted_tool_definitions("openai")
        if not tools:
            return {"error": "Error: Failed to get or format tool definitions for OpenAI."}
        
        # Prepare messages for OpenAI - just use the basic history
        messages_for_openai = [
            {"role": msg["role"], "content": msg["content"]}
            for msg in chat_history
        ]

        print(f"DEBUG [get_openai_response]: Initial call with {len(messages_for_openai)} messages")
        print(f"DEBUG [get_openai_response]: Messages: {json.dumps(messages_for_openai, indent=2)}")
        print(f"DEBUG [get_openai_response]: Tools: {json.dumps(tools, indent=2)}")
        
        # Initial API call with tools
        completion = openai_client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=messages_for_openai,
            tools=tools,
            tool_choice="auto"
        )
        
        # Get response message 
        response_message = completion.choices[0].message
        
        # If no tool calls, return the content directly
        if not hasattr(response_message, 'tool_calls') or not response_message.tool_calls:
            return {
                "content": response_message.content,
                "tools_called": [],
                "token_count_in": len(" ".join([msg["content"] for msg in chat_history])),
                "token_count_out": len(response_message.content.split())
            }
            
        print(f"DEBUG [get_openai_response]: Assistant requested tool(s): {len(response_message.tool_calls)}")
        
        # We have tool calls - handle them
        # Simplified: Just process the first tool call for now
        # In a real app, you might want to handle multiple tool calls
        if len(response_message.tool_calls) > 0:
            # Extract just what we need from the first tool call
            tool_call = response_message.tool_calls[0]
            tool_name = tool_call.function.name
            try:
                parameters = json.loads(tool_call.function.arguments)
            except json.JSONDecodeError as e:
                print(f"ERROR: Could not parse tool arguments: {e}")
                return {"error": f"I tried to use a tool but encountered an error parsing the arguments: {e}"}
                
            tool_call_id = tool_call.id
            
            # Execute the tool (don't display in UI from here)
            tool_result = handle_tool_call(tool_name, parameters, display_in_ui=False)
            
            # Create a completely new message array with original history
            second_messages = messages_for_openai.copy()
            
            # Add the assistant's request with the tool call
            # Make sure to use a valid format that won't cause nesting issues
            second_messages.append({
                "role": "assistant",
                "content": None,  # Must be None when tool_calls is present
                "tool_calls": [{
                    "id": tool_call_id,
                    "type": "function",
                    "function": {
                        "name": tool_name,
                        "arguments": json.dumps(parameters)  # Use parameter dict and convert to JSON
                    }
                }]
            })
            
            # Add the tool response
            second_messages.append({
                "role": "tool",
                "tool_call_id": tool_call_id,
                "content": json.dumps(tool_result)  # Convert result to json string
            })
            
            print(f"DEBUG [get_openai_response]: Second call with {len(second_messages)} messages including tool result")
            print(f"DEBUG [get_openai_response]: Second call messages: {json.dumps(second_messages, indent=2)}")
            
            # Make second API call with tool results
            try:
                final_completion = openai_client.chat.completions.create(
                    model="gpt-3.5-turbo-0125",
                    messages=second_messages
                )
                response_content = final_completion.choices[0].message.content
            except Exception as e:
                print(f"ERROR in second API call: {e}")
                st.error(f"Error in follow-up API call: {e}")
                # Return a fallback response or the original content
                response_content = response_message.content or "I encountered an error processing the tool results."

            # After processing the response
            tools_called = []  # Collect tools called
            
            # Calculate token counts more accurately
            # Get token counts from the API response if available
            if hasattr(final_completion, 'usage'):
                token_count_in = final_completion.usage.prompt_tokens
                token_count_out = final_completion.usage.completion_tokens
            else:
                # Fallback to manual counting
                # Input tokens - include all messages and tool definitions
                all_messages_str = json.dumps(second_messages)
                tools_str = json.dumps(tools)
                token_count_in = count_tokens_openai(all_messages_str + tools_str)
                # Output tokens - the final response
                token_count_out = count_tokens_openai(response_content)

            # If a tool call was made, add it to tools_called
            if hasattr(response_message, 'tool_calls') and response_message.tool_calls:
                for tool_call in response_message.tool_calls:
                    tools_called.append(tool_call.function.name)

            return {
                "content": response_content,
                "tools_called": tools_called,
                "token_count_in": token_count_in,
                "token_count_out": token_count_out
            }
                
        # Fallback - return original content if no valid tool calls processed
        # Calculate token counts more accurately
        if hasattr(completion, 'usage'):
            token_count_in = completion.usage.prompt_tokens
            token_count_out = completion.usage.completion_tokens
        else:
            # Fallback to manual counting
            all_messages_str = json.dumps(messages_for_openai)
            tools_str = json.dumps(tools)
            token_count_in = count_tokens_openai(all_messages_str + tools_str)
            token_count_out = count_tokens_openai(response_message.content)
            
        return {
            "content": response_message.content,
            "tools_called": [],
            "token_count_in": token_count_in,
            "token_count_out": token_count_out
        }

    except Exception as e:
        st.error(f"An error occurred while contacting the OpenAI API: {e}")
        return {"error": f"Sorry, I encountered an error while trying to get a response from OpenAI: {e}"} 
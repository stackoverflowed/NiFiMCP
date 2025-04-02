# Placeholder for LLM interaction logic 

import google.generativeai as genai
from openai import OpenAI # Import OpenAI
import config
import streamlit as st # Import streamlit for error messages
import json
from typing import List, Dict, Any, Optional
from mcp_handler import get_available_tools, execute_mcp_tool

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
def get_formatted_tool_definitions(provider: str = "openai") -> List[Dict[str, Any]]:
    """Gets tool definitions from the MCP server and formats them for the specified LLM provider."""
    tools = get_available_tools()
    
    if provider == "openai":
        # OpenAI already uses the same format as our MCP server
        return tools
    elif provider == "gemini":
        # Convert OpenAI format to Gemini format
        gemini_tools = []
        for tool in tools:
            gemini_tool = {
                "function_declarations": [{
                    "name": tool["name"],
                    "description": tool["description"],
                    "parameters": tool["parameters"]
                }]
            }
            gemini_tools.append(gemini_tool)
        return gemini_tools
    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")

def handle_tool_call(tool_name: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Executes a tool call and returns the result."""
    print(f"Executing tool: {tool_name} with parameters: {parameters}")
    
    # Show the tool call to the user
    with st.chat_message("assistant"):
        st.markdown(f"🔧 Using tool: `{tool_name}`")
        st.json(parameters)
    
    result = execute_mcp_tool(tool_name, parameters)
    
    # If result is a string, it's probably an error message
    if isinstance(result, str):
        return {"error": result}
    
    return result

# --- LLM Response Handling --- #

def get_gemini_response(chat_history: List[Dict[str, str]]) -> str:
    """Gets a response from the Gemini model based on the chat history."""
    global gemini_configured
    if not gemini_configured:
        # Attempt to reconfigure if key was added after startup
        configure_llms()
        if not gemini_configured:
            return "Error: Google API Key not configured or configuration failed."

    try:
        model = genai.GenerativeModel('gemini-1.5-pro')  # Using pro for function calling support
        
        # Get tool definitions in Gemini format
        tools = get_formatted_tool_definitions("gemini")
        
        # Convert chat history to Gemini format
        gemini_history = []
        for msg in chat_history:
            role = "model" if msg["role"] == "assistant" else msg["role"]
            if role in ["user", "model"]:
                gemini_history.append({"role": role, "parts": [msg["content"]]})

        # Make the API call with tools
        response = model.generate_content(
            gemini_history,
            tools=tools,
            tool_choice="auto"
        )

        # Check if the response includes a tool call
        if hasattr(response, 'candidates') and response.candidates:
            for candidate in response.candidates:
                if hasattr(candidate, 'content') and candidate.content.parts:
                    for part in candidate.content.parts:
                        if hasattr(part, 'function_call'):
                            # Execute the tool call
                            tool_name = part.function_call.name
                            parameters = json.loads(part.function_call.args)
                            tool_result = handle_tool_call(tool_name, parameters)
                            
                            # Send tool result back to Gemini
                            gemini_history.append({
                                "role": "function",
                                "parts": [json.dumps(tool_result)],
                                "name": tool_name
                            })
                            
                            # Get final response
                            final_response = model.generate_content(gemini_history)
                            return final_response.text

        # If no tool call, return the text directly
        return response.text

    except Exception as e:
        st.error(f"An error occurred while contacting the Gemini API: {e}")
        return f"Sorry, I encountered an error while trying to get a response from Gemini: {e}"

def get_openai_response(chat_history: List[Dict[str, str]]) -> str:
    """Gets a response from the OpenAI model."""
    global openai_client
    if openai_client is None:
        # Attempt to reconfigure if key was added after startup
        configure_llms()
        if openai_client is None:
            return "Error: OpenAI API Key not configured or client initialization failed."

    try:
        # Get tool definitions in OpenAI format
        tools = get_formatted_tool_definitions("openai")
        
        # Prepare messages for OpenAI
        messages_for_openai = [
            {"role": msg["role"], "content": msg["content"]}
            for msg in chat_history
        ]

        # Make the API call with tools
        completion = openai_client.chat.completions.create(
            model="gpt-3.5-turbo-0125",  # Using a version that supports tools
            messages=messages_for_openai,
            tools=tools,
            tool_choice="auto"
        )
        
        # Check if the response includes a tool call
        response_message = completion.choices[0].message
        if response_message.tool_calls:
            # Handle each tool call in sequence
            for tool_call in response_message.tool_calls:
                tool_name = tool_call.function.name
                parameters = json.loads(tool_call.function.arguments)
                
                # Execute the tool
                tool_result = handle_tool_call(tool_name, parameters)
                
                # Add the tool call and result to the conversation
                messages_for_openai.append({
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": tool_call.id,
                            "function": {"name": tool_name, "arguments": json.dumps(parameters)}
                        }
                    ]
                })
                messages_for_openai.append({
                    "role": "tool",
                    "content": json.dumps(tool_result),
                    "tool_call_id": tool_call.id
                })
            
            # Get final response after tool calls
            final_completion = openai_client.chat.completions.create(
                model="gpt-3.5-turbo-0125",
                messages=messages_for_openai
            )
            return final_completion.choices[0].message.content
        
        # If no tool call, return the content directly
        return response_message.content

    except Exception as e:
        st.error(f"An error occurred while contacting the OpenAI API: {e}")
        return f"Sorry, I encountered an error while trying to get a response from OpenAI: {e}" 
import streamlit as st
import json # Import json for formatting tool results
# Restore imports
from chat_manager import get_gemini_response, get_openai_response, get_formatted_tool_definitions
from mcp_handler import get_available_tools, execute_mcp_tool
import config

st.set_page_config(page_title="NiFi Chat UI", layout="wide")

# --- Constants ---
MAX_LOOP_ITERATIONS = 10 # Safety break for the execution loop
SYSTEM_PROMPT_FILE = "nifi_chat_ui/system_prompt.md"

# --- Load System Prompt --- 
def load_system_prompt():
    try:
        with open(SYSTEM_PROMPT_FILE, "r") as f:
            return f.read()
    except FileNotFoundError:
        st.error(f"Error: System prompt file not found at {SYSTEM_PROMPT_FILE}")
        return "You are a helpful NiFi assistant." # Fallback prompt
    except Exception as e:
        st.error(f"Error reading system prompt file: {e}")
        return "You are a helpful NiFi assistant."

system_prompt = load_system_prompt()

# Initialize session state for chat history if it doesn't exist
if "messages" not in st.session_state:
    # Prepend the system prompt conceptually (actual sending depends on LLM function)
    st.session_state.messages = [] 
    # Optionally add a system message if the LLM API uses it explicitly
    # st.session_state.messages = [{"role": "system", "content": system_prompt}]

# Sidebar for settings
with st.sidebar:
    st.title("Settings")
    
    # LLM Provider Selection
    available_providers = []
    if config.GOOGLE_API_KEY:
        available_providers.append("Gemini")
    if config.OPENAI_API_KEY:
        available_providers.append("OpenAI")
    
    if not available_providers:
        st.error("No API keys configured. Please set GOOGLE_API_KEY and/or OPENAI_API_KEY.")
        provider = None
    else:
        default_provider = "Gemini" if "Gemini" in available_providers else available_providers[0]
        provider = st.selectbox("Select LLM Provider:", available_providers, index=available_providers.index(default_provider))
    
    # Display available MCP tools
    st.markdown("---")
    st.subheader("Available MCP Tools")
    # Get raw tools (OpenAI format expected from API) - Keep this for display
    raw_tools_list = get_available_tools() 
    
    # --- Display raw tool details in sidebar (uses raw list) ---
    if raw_tools_list:
         # Display logic using raw_tools_list (remains the same)
        for tool_data in raw_tools_list: 
            # ... (existing display logic using raw_tools_list) ...
            if not isinstance(tool_data, dict) or tool_data.get("type") != "function" or not isinstance(tool_data.get("function"), dict):
                st.warning(f"Skipping unexpected tool data format: {tool_data}")
                continue
            function_details = tool_data.get("function", {})
            tool_name = function_details.get('name', 'Unnamed Tool')
            tool_description = function_details.get('description', 'No description')
            parameters = function_details.get('parameters', {}) # parameters schema
            
            # Ensure parameters is a dict before accessing properties
            properties = parameters.get('properties', {}) if isinstance(parameters, dict) else {}
            required_params = parameters.get('required', []) if isinstance(parameters, dict) else []
            
            with st.expander(f"🔧 {tool_name}", expanded=False):
                st.markdown(f"**Description:** {tool_description}")
                if properties:
                    st.markdown("**Parameters:**")
                    for param_name, param_info in properties.items():
                        required = "✳️ " if param_name in required_params else ""
                        # param_info might not be a dict if schema is unusual
                        param_desc = param_info.get('description', 'No description') if isinstance(param_info, dict) else 'Invalid parameter info'
                        st.markdown(f"- {required}`{param_name}`: {param_desc}")
                else:
                    st.markdown("_(No parameters specified)_")
    else:
         st.warning("No MCP tools available or failed to retrieve.")

# Main chat interface
st.title("NiFi Chat UI")

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Accept user input
if prompt := st.chat_input("What would you like to do with NiFi?"):
    if not provider:
        st.error("Please configure an API key and select a provider.")
    else:
        # Add user message to chat history and display it
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user") :
             st.markdown(prompt)

        # --- Start Execution Loop --- 
        loop_count = 0
        while loop_count < MAX_LOOP_ITERATIONS:
            loop_count += 1
            
            # --- Format Tools Just Before LLM Call --- 
            formatted_tools = None
            print(f"DEBUG [app.py Loop]: Provider value before formatting = '{provider}'") 
            if raw_tools_list: # Check if we have raw tools to format
                 try:
                      # Use the current provider value for this specific interaction
                      formatted_tools = get_formatted_tool_definitions(provider=provider)
                      if formatted_tools:
                           print(f"DEBUG [app.py Loop]: Tools successfully formatted for {provider}.")
                      else:
                           # Warning printed inside the function, maybe add context here?
                           print(f"DEBUG [app.py Loop]: get_formatted_tool_definitions returned None/empty for {provider}.")
                 except Exception as fmt_e:
                      st.error(f"Error formatting tools for {provider} in loop: {fmt_e}")
                      formatted_tools = None # Ensure it's None on error
            else:
                 print("DEBUG [app.py Loop]: No raw tools available to format.")

            # --- Prepare arguments for LLM call --- 
            llm_args = {
                 "messages": st.session_state.messages,
                 "system_prompt": system_prompt, 
                 "tools": formatted_tools # Pass the tools formatted in *this* loop iteration
            }

            response_data = None
            with st.spinner(f"Thinking... (Step {loop_count})"):
                try:
                    if provider == "Gemini":
                        # **ASSUMPTION**: get_gemini_response returns {"content": str|None, "tool_calls": list|None, ...}
                        response_data = get_gemini_response(**llm_args)
                    elif provider == "OpenAI":
                        # **ASSUMPTION**: get_openai_response returns {"content": str|None, "tool_calls": list|None, ...}
                        response_data = get_openai_response(**llm_args)
                    else:
                        st.error("Invalid provider selected.")
                        break # Exit loop on provider error

                    if response_data is None:
                         st.error("LLM response was empty.")
                         break

                except Exception as e:
                    st.error(f"Error calling LLM API: {e}")
                    break # Exit loop on LLM API error

            # --- Process LLM Response --- 
            llm_content = response_data.get("content")
            tool_calls = response_data.get("tool_calls") # Expecting list like [{"name": ..., "arguments": ...}] 
            
            # Append assistant's thought/response message to history (even if calling tools)
            if llm_content:
                st.session_state.messages.append({"role": "assistant", "content": llm_content})
            
            # --- Handle Tool Calls --- 
            if tool_calls:
                # Display that tools are being called
                with st.chat_message("assistant"):
                    tool_names = [tc.get('name', 'unknown') for tc in tool_calls]
                    st.markdown(f"⚙️ Calling tool(s): `{', '.join(tool_names)}`...") # Indicate tool call
                
                # Execute tools and collect results
                tool_results_messages = []
                for tool_call in tool_calls:
                    tool_name = tool_call.get("name")
                    tool_args = tool_call.get("arguments")
                    if not tool_name or tool_args is None:
                        st.error(f"LLM requested invalid tool call format: {tool_call}")
                        # Add an error message for the LLM
                        tool_results_messages.append({
                            "role": "assistant", # Or "tool" if LLM expects that
                            "content": f"Error: Invalid tool call format received from LLM: {tool_call}"
                        })
                        continue # Skip this invalid call
                        
                    # Execute the tool using the mcp_handler function
                    try:
                        with st.spinner(f"Executing tool: `{tool_name}`..."):
                             # result can be dict (success) or str (error)
                             result = execute_mcp_tool(tool_name=tool_name, params=tool_args) 
                        
                        # Format result for the next LLM call
                        # Use JSON for dicts to ensure structure is maintained
                        result_content = json.dumps(result) if isinstance(result, dict) else str(result)
                        
                        # Append result message to history for the LLM
                        tool_results_messages.append({
                             "role": "assistant", # Using assistant role to feed back result
                             "content": f"Tool call '{tool_name}' executed.\nResult:\n```json\n{result_content}\n```"
                        })
                        
                    except Exception as e:
                        st.error(f"Unexpected error executing tool '{tool_name}': {e}")
                        # Append error message for the LLM
                        tool_results_messages.append({
                             "role": "assistant",
                             "content": f"Error: Failed to execute tool '{tool_name}'. Exception: {e}"
                        })
                        # Decide if we should break the loop on tool execution error? Maybe allow LLM to react?
                        # For now, continue the loop, feeding the error back to the LLM.
                
                # Add all tool results to the main message history
                st.session_state.messages.extend(tool_results_messages)
                
                # --- Continue the loop --- 
                # (Don't display anything yet, let the loop run again) 
            
            # --- Handle Final Response (No Tool Calls or Task Complete) --- 
            else:
                final_content = llm_content if llm_content else "Assistant finished without a textual response."
                
                # Check for completion signal
                task_complete_signal = "TASK COMPLETE"
                is_task_complete = task_complete_signal in final_content
                if is_task_complete:
                    final_content = final_content.replace(task_complete_signal, "").strip()

                # Display the final assistant response
                with st.chat_message("assistant"):
                    st.markdown(final_content)
                    # Optionally display token counts etc. from response_data if available
                    token_count_in = response_data.get("token_count_in", 0)
                    token_count_out = response_data.get("token_count_out", 0)
                    if token_count_in or token_count_out:
                         st.caption(f"Tokens: In={token_count_in}, Out={token_count_out}")

                # --- Break the loop --- 
                break 
        # --- End Execution Loop --- 

        # Handle loop timeout
        if loop_count >= MAX_LOOP_ITERATIONS:
            st.error(f"Assistant reached maximum calculation steps ({MAX_LOOP_ITERATIONS}). Task may be incomplete.")
            # Append a message to history indicating the timeout
            st.session_state.messages.append({
                 "role": "assistant",
                 "content": f"(System Note: Reached maximum iteration limit of {MAX_LOOP_ITERATIONS}.)"
            })

# (Ensure any code previously below the input handling is either removed or placed appropriately)
# For example, the old way of adding assistant response is now handled within the loop. 
import streamlit as st
import json # Import json for formatting tool results
import uuid # Added for context IDs
from loguru import logger # Import logger directly

# Set page config MUST be the first Streamlit call
st.set_page_config(page_title="NiFi Chat UI", layout="wide")

# --- Setup Logging --- 
try:
    # Use session state to ensure logging is only set up once per session, not on every script re-run
    if "logging_initialized" not in st.session_state:
        from config.logging_setup import setup_logging
        setup_logging() 
        st.session_state.logging_initialized = True
        logger.info("Logging initialized for new session")
    else:
        logger.debug("Logging already initialized for this session")
except ImportError:
    print("Warning: Logging setup failed. Check config/logging_setup.py")
# ---------------------

# Restore imports
from chat_manager import get_gemini_response, get_openai_response, get_formatted_tool_definitions
from chat_manager import configure_llms, is_initialized
from mcp_handler import get_available_tools, execute_mcp_tool
# Import config from the new location
try:
    from config import settings as config # Updated import
except ImportError:
    logger.error("Failed to import config.settings. Ensure config/__init__.py exists if needed, or check PYTHONPATH.")
    st.error("Configuration loading failed. Application cannot start.")
    st.stop()

# Initialize LLM clients after page config
if not is_initialized:
    logger.info("Initializing LLM clients...")
    configure_llms()

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
        # Track provider changes to re-initialize clients if needed
        previous_provider = st.session_state.get("previous_provider", None)
        default_provider = "Gemini" if "Gemini" in available_providers else available_providers[0]
        provider = st.selectbox("Select LLM Provider:", available_providers, 
                              index=available_providers.index(default_provider) if default_provider in available_providers else 0)
        
        # If provider changed, reconfigure LLM clients
        if provider != previous_provider:
            try:
                # Now configure with clean clients
                configure_llms()
                st.session_state["previous_provider"] = provider
                logger.info(f"Provider changed from {previous_provider} to {provider}, re-configured LLM clients")
            except Exception as e:
                logger.error(f"Error reconfiguring clients after provider change: {e}", exc_info=True)
                # Show error but don't prevent UI from loading
                st.sidebar.warning(f"Error initializing {provider} client. Some features may not work properly.")
                st.sidebar.write(f"Error details: {str(e)}")
    
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
    # Skip displaying "tool" role messages - they're results that should only be processed by the LLM
    if message.get("role") == "tool":
        continue
        
    with st.chat_message(message["role"]):
        # Safely handle messages without content (like tool call messages)
        if "content" in message:
            st.markdown(message["content"])
        elif "tool_calls" in message:
            # Display tool calls summary if no content
            tool_names = [tc.get('function', {}).get('name', 'unknown') for tc in message.get("tool_calls", [])]
            if tool_names:
                st.markdown(f"⚙️ Calling tool(s): `{', '.join(tool_names)}`...")
            else:
                st.markdown("_(Tool call with no details)_")
                
        # Display token counts if available in message metadata
        token_count_in = message.get("token_count_in", 0)
        token_count_out = message.get("token_count_out", 0)
        if token_count_in or token_count_out:
            st.caption(f"Tokens: In={token_count_in}, Out={token_count_out}")

# Accept user input
if prompt := st.chat_input("What would you like to do with NiFi?"):
    if not provider:
        st.error("Please configure an API key and select a provider.")
        logger.warning("User submitted prompt but no provider was configured/selected.") # Added log
    else:
        # --- Generate User Request ID --- 
        user_request_id = str(uuid.uuid4())
        bound_logger = logger.bind(user_request_id=user_request_id) # Bind user_request_id
        bound_logger.info(f"Received new user prompt (Provider: {provider})") # Log with context
        # ---------------------------------

        # Add user message to chat history and display it
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user") :
             st.markdown(prompt)

        # --- Start Execution Loop --- 
        loop_count = 0
        while loop_count < MAX_LOOP_ITERATIONS:
            loop_count += 1
            current_loop_logger = bound_logger.bind(loop=loop_count) # Add loop count to context
            current_loop_logger.info(f"Starting execution loop iteration {loop_count}")
            
            # --- Format Tools Just Before LLM Call --- 
            formatted_tools = None
            current_loop_logger.debug(f"Formatting tools for provider: {provider}") # Log instead of print
            if raw_tools_list: # Check if we have raw tools to format
                 try:
                      # Pass user_request_id
                      formatted_tools = get_formatted_tool_definitions(provider=provider, user_request_id=user_request_id) 
                      if formatted_tools:
                           current_loop_logger.debug(f"Tools successfully formatted for {provider}.") # Log
                      else:
                           current_loop_logger.warning(f"get_formatted_tool_definitions returned None/empty for {provider}.") # Log warning
                 except Exception as fmt_e:
                      st.error(f"Error formatting tools for {provider} in loop: {fmt_e}")
                      current_loop_logger.error(f"Error formatting tools for {provider}: {fmt_e}", exc_info=True) # Log error
                      formatted_tools = None # Ensure it's None on error
            else:
                 current_loop_logger.info("No raw tools available to format.") # Log info

            # --- Prepare arguments for LLM call --- 
            llm_args = {
                 "messages": st.session_state.messages,
                 "system_prompt": system_prompt, 
                 "tools": formatted_tools # Pass the tools formatted in *this* loop iteration
            }

            response_data = None
            with st.spinner(f"Thinking... (Step {loop_count})"):
                try:
                    current_loop_logger.info(f"Calling LLM ({provider})...") # Log LLM call start
                    if provider == "Gemini":
                        # Pass user_request_id
                        response_data = get_gemini_response(**llm_args, user_request_id=user_request_id)
                    elif provider == "OpenAI":
                        # Pass user_request_id
                        response_data = get_openai_response(**llm_args, user_request_id=user_request_id)
                    else:
                        st.error("Invalid provider selected.")
                        current_loop_logger.error(f"Invalid provider selected: {provider}") # Log error
                        break # Exit loop on provider error

                    if response_data is None:
                         st.error("LLM response was empty.")
                         current_loop_logger.error("LLM response data was None.") # Log error
                         break
                    else:
                        current_loop_logger.info(f"Received response from LLM ({provider}).") # Log LLM call success

                except Exception as e:
                    st.error(f"Error calling LLM API: {e}")
                    current_loop_logger.error(f"Error calling LLM API ({provider}): {e}", exc_info=True) # Log error
                    break # Exit loop on LLM API error

            # --- Process LLM Response --- 
            llm_content = response_data.get("content")
            tool_calls = response_data.get("tool_calls") # Expecting list like [{"id": ..., "type": "function", "function": {"name": ..., "arguments": ...}}]
            error_message = response_data.get("error")

            # Check for critical errors from the LLM that should terminate the loop
            if error_message:
                current_loop_logger.error(f"LLM returned an error: {error_message}")
                               
                # Add the error to the chat history as an assistant message
                with st.chat_message("assistant"):
                    st.markdown(f"Sorry, I encountered an error: {error_message}")
                
                # Add to history
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"Sorry, I encountered an error: {error_message}"
                })            
                # Break out of the loop on errors
                break

            # --- Append the Assistant's Full Response to History --- 
            # The message MUST include tool_calls if the LLM generated them
            assistant_message_to_add = {"role": "assistant"}
            if llm_content:
                assistant_message_to_add["content"] = llm_content
            if tool_calls: # If the LLM wants to call tools, include the tool_calls structure
                # Ensure the structure added matches what the API expects back.
                # Assuming get_openai_response returns the correct structure.
                assistant_message_to_add["tool_calls"] = tool_calls 
            
            # Add token counts to the message metadata (won't affect API calls but preserves it for UI)
            token_count_in = response_data.get("token_count_in", 0)
            token_count_out = response_data.get("token_count_out", 0)
            if token_count_in or token_count_out:
                assistant_message_to_add["token_count_in"] = token_count_in
                assistant_message_to_add["token_count_out"] = token_count_out
            
            # Only add if there's content or tool calls
            if llm_content or tool_calls:
                # Log the assistant's response (potentially large, consider truncation or DEBUG level)
                current_loop_logger.debug(f"Assistant response: Content={llm_content is not None}, ToolCalls={tool_calls is not None}") 
                st.session_state.messages.append(assistant_message_to_add)
            else:
                current_loop_logger.warning("LLM response had neither content nor tool calls.")

            # --- Handle Tool Calls (if any) --- 
            if tool_calls:
                # Display indication that tools are being called 
                # We add the full message (content + tool calls) to history ABOVE, 
                # but only display the tool call indicator here to avoid duplicate text display.
                with st.chat_message("assistant"):
                    # display_content = llm_content if llm_content else "" # REMOVED: Don't display text content here
                    tool_names = [tc.get('function', {}).get('name', 'unknown') for tc in tool_calls]
                    # ONLY display the tool indicator
                    st.markdown(f"⚙️ Calling tool(s): `{', '.join(tool_names)}`...") 
                    
                    # Display token counts
                    token_count_in = response_data.get("token_count_in", 0)

                # --- Execute Tools and Append Results --- 
                for tool_call in tool_calls:
                    # --- Generate Action ID ---
                    action_id = str(uuid.uuid4())
                    tool_logger = current_loop_logger.bind(action_id=action_id) # Bind action_id
                    # --------------------------

                    tool_call_id = tool_call.get("id")
                    function_info = tool_call.get("function", {})
                    tool_name = function_info.get("name")
                    tool_args_str = function_info.get("arguments")

                    tool_logger.info(f"Processing tool call: ID={tool_call_id}, Name={tool_name}") # Log tool call

                    if not tool_call_id or not tool_name or tool_args_str is None:
                        st.error(f"LLM requested invalid tool call format: {tool_call}")
                        tool_logger.error(f"Invalid tool call format received from LLM: {tool_call}") # Log error
                        # Append an error message as the tool result for THIS tool_call_id
                        st.session_state.messages.append({
                            "role": "tool",
                            "tool_call_id": tool_call_id if tool_call_id else "unknown_id", # Provide ID if possible
                            "content": f"Error: Invalid tool call format received from LLM: {tool_call}"
                        })
                        continue # Skip this invalid call

                    # Parse arguments
                    try:
                        tool_args = json.loads(tool_args_str)
                        tool_logger.debug(f"Parsed arguments for tool '{tool_name}': {tool_args}")
                    except json.JSONDecodeError:
                        st.error(f"Failed to parse JSON arguments for tool '{tool_name}': {tool_args_str}")
                        tool_logger.error(f"Failed to parse JSON arguments for tool '{tool_name}': {tool_args_str}", exc_info=True) # Log error
                        st.session_state.messages.append({
                            "role": "tool",
                            "tool_call_id": tool_call_id,
                            "content": f"Error: Failed to parse JSON arguments: {tool_args_str}"
                        })
                        continue

                    # Execute the tool
                    try:
                        with st.spinner(f"Executing tool: `{tool_name}`..."):
                             tool_logger.info(f"Executing tool: {tool_name} with args: {tool_args}") # Log execution start
                             # Pass context IDs (user_request_id, action_id) 
                             result = execute_mcp_tool(
                                 tool_name=tool_name, 
                                 params=tool_args,
                                 user_request_id=user_request_id,
                                 action_id=action_id
                             )
                             tool_logger.info(f"Tool '{tool_name}' executed successfully.") # Log execution end
                        
                        # Format result as JSON string if it's a dict/list, else string
                        if isinstance(result, (dict, list)):
                            result_content = json.dumps(result)
                        else:
                            result_content = str(result)

                        # Append the valid tool result message to history
                        tool_logger.debug(f"Appending tool result for '{tool_name}' (ID: {tool_call_id}) to history.")
                        st.session_state.messages.append({
                             "role": "tool", 
                             "tool_call_id": tool_call_id,
                             "content": result_content
                        })
                        
                    except Exception as e:
                        st.error(f"Unexpected error executing tool '{tool_name}': {e}")
                        tool_logger.error(f"Unexpected error executing tool '{tool_name}': {e}", exc_info=True) # Log error
                        # Append error message as the tool result for this ID
                        st.session_state.messages.append({
                             "role": "tool",
                             "tool_call_id": tool_call_id,
                             "content": f"Error: Failed to execute tool '{tool_name}'. Exception: {e}"
                        })
                        # Execution stops here due to rerun, loop effectively processes one tool per run

                # The loop implicitly continues on the next Streamlit run triggered by st.rerun()
                # No need for the 'Finished processing...' log or explicit continue here anymore.

            # --- Handle Final Response (No Tool Calls in this iteration) --- 
            elif llm_content: # Only display if there was content and NO tool calls
                final_content = llm_content
                
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
                current_loop_logger.info("LLM provided final response. Exiting loop.")
                break 
        # --- End of Loop --- 
        if loop_count >= MAX_LOOP_ITERATIONS:
            st.warning(f"Reached maximum loop iterations ({MAX_LOOP_ITERATIONS}). Stopping execution.")
            bound_logger.warning(f"Reached maximum loop iterations ({MAX_LOOP_ITERATIONS}).") # Log loop limit reached

# Add a way to clear history - outside the main input block
if st.sidebar.button("Clear Chat History"):
    st.session_state.messages = []
    logger.info("Chat history cleared by user.") # Log history clear
    st.rerun()

# (Ensure any code previously below the input handling is either removed or placed appropriately)
# For example, the old way of adding assistant response is now handled within the loop. 
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
HISTORY_CLEAR_MARKER = "--- Max iterations reached. History below this point was cleared for the AI's context. ---"

# --- Session State Initialization ---
# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = [] 

# Initialize objective
if "current_objective" not in st.session_state:
    st.session_state.current_objective = ""
    
# Initialize recovery flags
if "run_recovery_loop" not in st.session_state:
    st.session_state.run_recovery_loop = False
if "history_cleared_for_next_llm_call" not in st.session_state:
    st.session_state.history_cleared_for_next_llm_call = False

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

# Store the base prompt loaded from the file
base_system_prompt = load_system_prompt()

# --- Sidebar --- 
with st.sidebar:
    st.title("Settings")
    
    # --- Provider Selection --- 
    available_providers = []
    if config.GOOGLE_API_KEY:
        available_providers.append("Gemini")
    if config.OPENAI_API_KEY:
        available_providers.append("OpenAI")
    
    if not available_providers:
        st.error("No API keys configured. Please set GOOGLE_API_KEY and/or OPENAI_API_KEY.")
        provider = None
    else:
        previous_provider = st.session_state.get("previous_provider", None)
        default_provider = "Gemini" if "Gemini" in available_providers else available_providers[0]
        provider = st.selectbox("Select LLM Provider:", available_providers, 
                              index=available_providers.index(default_provider) if default_provider in available_providers else 0,
                              key="provider_select") # Added key
        
        if provider != previous_provider:
            try:
                configure_llms()
                st.session_state["previous_provider"] = provider
                logger.info(f"Provider changed from {previous_provider} to {provider}, re-configured LLM clients")
            except Exception as e:
                logger.error(f"Error reconfiguring clients after provider change: {e}", exc_info=True)
                st.sidebar.warning(f"Error initializing {provider} client. Some features may not work properly.")
                st.sidebar.write(f"Error details: {str(e)}")
                
    # --- Tool Display --- 
    st.markdown("---")
    st.subheader("Available MCP Tools")
    # Fetch tools based on the *current* value of the selectbox for sidebar display
    current_sidebar_phase = st.session_state.get("selected_phase", "All") # Read current value via key
    raw_tools_list = get_available_tools(phase=current_sidebar_phase)
    
    if raw_tools_list:
        for tool_data in raw_tools_list: 
            # No filtering needed here anymore, backend does it.
            if not isinstance(tool_data, dict) or tool_data.get("type") != "function" or not isinstance(tool_data.get("function"), dict):
                st.warning(f"Skipping unexpected tool data format: {tool_data}")
                continue
            function_details = tool_data.get("function", {})
            tool_name = function_details.get('name', 'Unnamed Tool')
            tool_description = function_details.get('description', 'No description')
            parameters = function_details.get('parameters', {}) # parameters schema
            properties = parameters.get('properties', {}) if isinstance(parameters, dict) else {}
            required_params = parameters.get('required', []) if isinstance(parameters, dict) else []
            
            with st.expander(f"🔧 {tool_name}", expanded=False):
                st.markdown(f"**Description:** {tool_description}")
                if properties:
                    st.markdown("**Parameters:**")
                    for param_name, param_info in properties.items():
                        required = "✳️ " if param_name in required_params else ""
                        desc_lines = param_info.get('description', 'No description').split('\n')
                        first_line = desc_lines[0]
                        other_lines = desc_lines[1:]
                        st.markdown(f"- {required}`{param_name}`: {first_line}")
                        if other_lines:
                            col1, col2 = st.columns([0.05, 0.95])
                            with col2:
                                for line in other_lines:
                                    st.markdown(f"{line.strip()}")
                else:
                    st.markdown("_(No parameters specified)_")
    else:
         st.warning("No MCP tools available or failed to retrieve.")
         
    # --- History Clear --- 
    if st.sidebar.button("Clear Chat History"):
        st.session_state.messages = []
        # Clear objective and flags as well
        st.session_state.current_objective = ""
        st.session_state.run_recovery_loop = False
        st.session_state.history_cleared_for_next_llm_call = False
        # Reset phase to default
        st.session_state.selected_phase = "All" # Reset phase on clear
        logger.info("Chat history and objective cleared by user.") 
        st.rerun()

# --- Main Chat Interface --- 
st.title("NiFi Chat UI")

# --- Objective Input Panel --- 
st.text_area(
    "Define the overall objective for this session:", 
    key="objective_input",
    value=st.session_state.current_objective, 
    height=100,
    on_change=lambda: st.session_state.update(current_objective=st.session_state.objective_input) 
)
st.markdown("---")

# --- Display Chat History --- 
for message in st.session_state.messages:
    if message.get("role") == "tool":
        continue
    elif message.get("role") == "system" and message.get("content") == HISTORY_CLEAR_MARKER:
        st.info(HISTORY_CLEAR_MARKER)
        continue
        
    with st.chat_message(message["role"]):
        if message["role"] == "assistant" and "tool_calls" in message:
            tool_names = [tc.get('function', {}).get('name', 'unknown') for tc in message.get("tool_calls", [])]
            if tool_names:
                st.markdown(f"⚙️ Calling tool(s): `{', '.join(tool_names)}`...")
            else:
                st.markdown("_(Tool call with no details)_")
        elif "content" in message:
            st.markdown(message["content"])
        elif message["role"] == "assistant" and "tool_calls" in message and "content" not in message:
             tool_names = [tc.get('function', {}).get('name', 'unknown') for tc in message.get("tool_calls", [])]
             if tool_names:
                 st.markdown(f"⚙️ Calling tool(s): `{', '.join(tool_names)}`...")
             else:
                 st.markdown("_(Tool call with no details)_")
                
        token_count_in = message.get("token_count_in", 0)
        token_count_out = message.get("token_count_out", 0)
        if token_count_in or token_count_out:
            st.caption(f"Tokens: In={token_count_in}, Out={token_count_out}")

# --- Refactored Execution Loop Function --- 
def run_execution_loop(provider: str, base_sys_prompt: str, user_req_id: str):
    """Runs the main LLM interaction loop."""
    bound_logger = logger.bind(user_request_id=user_req_id)
    loop_count = 0
    while loop_count < MAX_LOOP_ITERATIONS:
        loop_count += 1
        current_loop_logger = bound_logger.bind(loop=loop_count)
        current_loop_logger.info(f"Starting execution loop iteration {loop_count}")

        # --- Prepare context for LLM call (History and System Prompt) ---
        messages_for_llm = None
        system_prompt_for_llm = None
        
        if st.session_state.get("history_cleared_for_next_llm_call", False):
            current_loop_logger.info("History clear flag set. Preparing recovery context.")
            messages_for_llm = [st.session_state.messages[-1]] if st.session_state.messages else []
            system_prompt_for_llm = base_sys_prompt
            st.session_state.history_cleared_for_next_llm_call = False
        else:
            current_loop_logger.debug("Using full history and potentially dynamic system prompt.")
            messages_for_llm = st.session_state.messages
            current_objective = st.session_state.get("current_objective", "")
            if current_objective and current_objective.strip():
                system_prompt_for_llm = f"{base_sys_prompt}\n\n## Current Objective\n{current_objective.strip()}"
                current_loop_logger.debug("Objective found, appending to system prompt.")
            else:
                system_prompt_for_llm = base_sys_prompt
                current_loop_logger.debug("No objective set, using base system prompt.")
        # -----------------------------------------------------------------

        # Get the selected phase
        current_phase = st.session_state.get("selected_phase", "All")
        
        # Fetch the potentially filtered tools based on the selected phase
        current_loop_logger.debug(f"Fetching tools for phase: {current_phase}")
        filtered_raw_tools_list = get_available_tools(phase=current_phase, user_request_id=user_req_id)
        
        # Format the filtered tools for the current provider
        formatted_tools = None
        current_loop_logger.debug(f"Formatting tools for provider: {provider}")
        if filtered_raw_tools_list:
             try:
                  # Pass the *filtered* list to the formatter
                  formatted_tools = get_formatted_tool_definitions(provider=provider, raw_tools=filtered_raw_tools_list, user_request_id=user_req_id) 
                  if formatted_tools:
                       current_loop_logger.debug(f"Tools successfully formatted for {provider} ({len(formatted_tools)} tools).")
                  else:
                       current_loop_logger.warning(f"get_formatted_tool_definitions returned None/empty for {provider} with filtered list.")
             except Exception as fmt_e:
                  st.error(f"Error formatting tools for {provider} in loop: {fmt_e}")
                  current_loop_logger.error(f"Error formatting tools for {provider}: {fmt_e}", exc_info=True)
                  formatted_tools = None
        else:
             current_loop_logger.info(f"No tools available for the current phase '{current_phase}'.")

        # Prepare arguments for LLM call
        llm_args = {
             "messages": messages_for_llm,
             "system_prompt": system_prompt_for_llm,
             "tools": formatted_tools # Pass the phase-filtered and formatted tools
        }

        # --- Call LLM --- 
        response_data = None
        with st.spinner(f"Thinking... (Step {loop_count})"):
            try:
                current_loop_logger.info(f"Calling LLM ({provider})...")
                if provider == "Gemini":
                    response_data = get_gemini_response(**llm_args, user_request_id=user_req_id)
                elif provider == "OpenAI":
                    response_data = get_openai_response(**llm_args, user_request_id=user_req_id)
                else:
                    st.error("Invalid provider selected.")
                    current_loop_logger.error(f"Invalid provider selected: {provider}")
                    break
                if response_data is None:
                     st.error("LLM response was empty.")
                     current_loop_logger.error("LLM response data was None.")
                     break
                else:
                    current_loop_logger.info(f"Received response from LLM ({provider}).")
            except Exception as e:
                st.error(f"Error calling LLM API: {e}")
                current_loop_logger.error(f"Error calling LLM API ({provider}): {e}", exc_info=True)
                break
        # ---------------
        
        # --- Process LLM Response --- 
        llm_content = response_data.get("content")
        tool_calls = response_data.get("tool_calls")
        error_message = response_data.get("error")
        if error_message:
            current_loop_logger.error(f"LLM returned an error: {error_message}")
            with st.chat_message("assistant"):
                st.markdown(f"Sorry, I encountered an error: {error_message}")
            st.session_state.messages.append({"role": "assistant", "content": f"Sorry, I encountered an error: {error_message}"})
            break
        assistant_message_to_add = {"role": "assistant"}
        if llm_content: assistant_message_to_add["content"] = llm_content
        if tool_calls: assistant_message_to_add["tool_calls"] = tool_calls
        token_count_in = response_data.get("token_count_in", 0)
        token_count_out = response_data.get("token_count_out", 0)
        if token_count_in or token_count_out:
            assistant_message_to_add["token_count_in"] = token_count_in
            assistant_message_to_add["token_count_out"] = token_count_out
        if llm_content or tool_calls:
            current_loop_logger.debug(f"Assistant response: Content={llm_content is not None}, ToolCalls={tool_calls is not None}")
            st.session_state.messages.append(assistant_message_to_add)
        else:
            current_loop_logger.warning("LLM response had neither content nor tool calls.")

        # --- Handle Tool Calls (if any) --- 
        if tool_calls:
            with st.chat_message("assistant"):
                tool_names = [tc.get('function', {}).get('name', 'unknown') for tc in tool_calls]
                st.markdown(f"⚙️ Calling tool(s): `{', '.join(tool_names)}`...")
            for tool_call in tool_calls:
                action_id = str(uuid.uuid4())
                tool_logger = current_loop_logger.bind(action_id=action_id)
                tool_call_id = tool_call.get("id")
                function_info = tool_call.get("function", {})
                tool_name = function_info.get("name")
                tool_args_str = function_info.get("arguments")
                tool_logger.info(f"Processing tool call: ID={tool_call_id}, Name={tool_name}")
                if not tool_call_id or not tool_name or tool_args_str is None:
                    st.error(f"LLM requested invalid tool call format: {tool_call}")
                    tool_logger.error(f"Invalid tool call format received from LLM: {tool_call}")
                    st.session_state.messages.append({"role": "tool", "tool_call_id": tool_call_id if tool_call_id else "unknown_id", "content": f"Error: Invalid tool call format received from LLM: {tool_call}"})
                    continue
                try:
                    tool_args = json.loads(tool_args_str)
                    tool_logger.debug(f"Parsed arguments for tool '{tool_name}': {tool_args}")
                except json.JSONDecodeError:
                    st.error(f"Failed to parse JSON arguments for tool '{tool_name}': {tool_args_str}")
                    tool_logger.error(f"Failed to parse JSON arguments for tool '{tool_name}': {tool_args_str}", exc_info=True)
                    st.session_state.messages.append({"role": "tool", "tool_call_id": tool_call_id, "content": f"Error: Failed to parse JSON arguments: {tool_args_str}"})
                    continue
                try:
                    with st.spinner(f"Executing tool: `{tool_name}`..."):
                         tool_logger.info(f"Executing tool: {tool_name} with args: {tool_args}")
                         result = execute_mcp_tool(tool_name=tool_name, params=tool_args, user_request_id=user_req_id, action_id=action_id)
                         tool_logger.info(f"Tool '{tool_name}' executed successfully.")
                    if isinstance(result, (dict, list)):
                        result_content = json.dumps(result)
                    else:
                        result_content = str(result)
                    tool_logger.debug(f"Appending tool result for '{tool_name}' (ID: {tool_call_id}) to history.")
                    st.session_state.messages.append({"role": "tool", "tool_call_id": tool_call_id, "content": result_content})
                except Exception as e:
                    st.error(f"Unexpected error executing tool '{tool_name}': {e}")
                    tool_logger.error(f"Unexpected error executing tool '{tool_name}': {e}", exc_info=True)
                    st.session_state.messages.append({"role": "tool", "tool_call_id": tool_call_id, "content": f"Error: Failed to execute tool '{tool_name}'. Exception: {e}"})
            continue 
            
        # --- Handle Final Response (No Tool Calls) --- 
        elif llm_content:
            final_content = llm_content
            task_complete_signal = "TASK COMPLETE"
            is_task_complete = task_complete_signal in final_content
            if is_task_complete:
                final_content = final_content.replace(task_complete_signal, "").strip()
            with st.chat_message("assistant"):
                st.markdown(final_content)
                token_count_in = response_data.get("token_count_in", 0)
                token_count_out = response_data.get("token_count_out", 0)
                if token_count_in or token_count_out:
                     st.caption(f"Tokens: In={token_count_in}, Out={token_count_out}")
            current_loop_logger.info("LLM provided final response. Exiting loop.")
            break
        
        # --- Handle case where LLM returns neither content nor tool calls ---
        else: 
            current_loop_logger.warning("LLM response had neither content nor tool calls in this loop iteration. Breaking loop to avoid infinite cycling.")
            st.warning("Assistant did not provide a response or further actions. Please try rephrasing your request.")
            break
            
    # --- End of Loop --- 
    
    # --- Max Iteration Handling --- 
    if loop_count >= MAX_LOOP_ITERATIONS:
        warning_msg = f"Reached maximum loop iterations ({MAX_LOOP_ITERATIONS}). Task may be incomplete."
        st.warning(warning_msg)
        bound_logger.warning(warning_msg) 
        st.session_state.messages.append({"role": "system", "content": HISTORY_CLEAR_MARKER})
        st.session_state.history_cleared_for_next_llm_call = True
        current_objective = st.session_state.get("current_objective", "").strip()
        recovery_prompt = (
            "The previous attempt reached the maximum iterations.\n\n"
            f"**Original Objective:** {current_objective if current_objective else 'Not specified.'}\n\n"
            "Please assess the current state of the NiFi flow using available tools (e.g., `list_nifi_objects`) "
            "and report the **next steps** needed to achieve the objective or confirm if it's already met. "
            "**Do not execute any further actions now, only report your findings and proposed next steps.**"
        )
        st.session_state.messages.append({"role": "user", "content": recovery_prompt})
        bound_logger.info("Max iterations reached. History marker added. Recovery prompt added.")
        st.session_state.run_recovery_loop = True 
        st.rerun()

# --- Phase Selection and Chat Input Area --- 
phase_options = ["All", "Review", "Build", "Modify", "Operate"]
# Initialize session state for selected_phase if it doesn't exist
if "selected_phase" not in st.session_state:
    st.session_state.selected_phase = "All"

col1, col2 = st.columns([1, 4]) # Adjust ratio as needed
with col1:
    # Use st.session_state.selected_phase directly with the selectbox
    st.selectbox(
        "Phase:",
        phase_options,
        key="selected_phase", # Bind directly to session state key
        label_visibility="collapsed"
    )
with col2:
    prompt = st.chat_input("What would you like to do with NiFi?")

# --- Process User Input --- 
if prompt:
    if not provider:
        st.error("Please configure an API key and select a provider.")
        logger.warning("User submitted prompt but no provider was configured/selected.")
    else:
        user_request_id = str(uuid.uuid4())
        bound_logger = logger.bind(user_request_id=user_request_id)
        bound_logger.info(f"Received new user prompt (Provider: {provider})")

        st.session_state.messages.append({"role": "user", "content": prompt})
        # Displaying the prompt happens naturally on rerun after appending
        # Call the refactored execution loop
        run_execution_loop(provider, base_system_prompt, user_request_id)

# --- Process Recovery Trigger --- 
if st.session_state.get("run_recovery_loop", False):
    st.session_state.run_recovery_loop = False # Reset flag immediately
    logger.info("Recovery loop triggered.")
    # Need a user_request_id for the recovery attempt
    recovery_request_id = str(uuid.uuid4())
    # Ensure provider is available before calling loop
    if provider:
        run_execution_loop(provider, base_system_prompt, recovery_request_id)
    else:
        st.error("Cannot run recovery: No LLM provider selected.") 
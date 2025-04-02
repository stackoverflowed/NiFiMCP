import streamlit as st
# Restore imports
from chat_manager import get_gemini_response, get_openai_response
from mcp_handler import get_available_tools
import config

st.set_page_config(page_title="NiFi Chat UI", layout="wide")

# Initialize session state for chat history if it doesn't exist
if "messages" not in st.session_state:
    st.session_state.messages = []

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
        provider = st.selectbox("Select LLM Provider:", available_providers)
    
    # Display available MCP tools - Restore tool display
    st.markdown("---")  # Add a visual separator
    st.subheader("Available MCP Tools")
    # Restore get_available_tools call
    tools_list = get_available_tools()
    if not tools_list:
        st.warning("No MCP tools available or failed to retrieve.") # Updated message
    else:
        # Iterate through the list of tools (OpenAI format)
        for tool_data in tools_list:
            # Ensure the structure is as expected
            if not isinstance(tool_data, dict) or tool_data.get("type") != "function" or not isinstance(tool_data.get("function"), dict):
                st.warning(f"Skipping unexpected tool data format: {tool_data}")
                continue
            
            # Access the nested function dictionary
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


# Main chat interface
st.title("NiFi Chat UI")

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Accept user input
if prompt := st.chat_input("What would you like to do with NiFi?"):
    if not provider:
        st.error("Please configure at least one API key and select a provider to use the chat interface.")
    else:
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
            
        # Get bot response based on selected provider - Restore functionality
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                if provider == "Gemini":
                    response = get_gemini_response(st.session_state.messages)
                elif provider == "OpenAI":
                    response = get_openai_response(st.session_state.messages)
                else:
                    # This case shouldn't be reachable if selectbox is populated
                    response = "Error: Invalid provider selected."
                st.markdown(response)
        
        # Add assistant response to chat history
        st.session_state.messages.append({"role": "assistant", "content": response}) 
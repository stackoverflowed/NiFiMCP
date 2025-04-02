# NiFi Chat UI Implementation Plan

This document outlines the steps to create a Streamlit-based chat interface that interacts with an LLM and the NiFi MCP server.

## 1. Goal

To build an interactive chat application where users can issue natural language instructions to query or modify an Apache NiFi instance. The application will leverage an LLM (supporting OpenAI and Gemini) to interpret user requests and utilize the tools defined in our `nifi_mcp_server` via the Model Context Protocol (MCP).

## 2. Architecture

*   **UI Framework:** Streamlit
*   **LLM Interaction:** Python clients for OpenAI (`openai`) and Google Gemini (`google-generativeai`).
*   **MCP Interaction:** `mcp` client library to communicate with the `nifi_mcp_server` (running as a separate process).
*   **Configuration:** API keys and potentially the path to the MCP server executable managed via environment variables or Streamlit secrets.

## 3. Implementation Phases

### Phase 1: Basic Streamlit Chat Interface & LLM Connection

*   **Tasks:**
    1.  **Setup Project Structure:** Create the `nifi_chat_ui/` directory with initial files (`app.py`, `requirements.txt`, `config.py`, `chat_manager.py`).
    2.  **Install Dependencies:** Add `streamlit`, `openai`, `google-generativeai` to `requirements.txt` and install them.
    3.  **Basic UI:** In `app.py`, implement the core Streamlit chat elements:
        *   Page title (`st.title`).
        *   Chat message display area (using `st.chat_message` within a loop over history).
        *   User input box (`st.chat_input`).
    4.  **Chat History Management:** Use `st.session_state` to store and manage the conversation history (list of messages with roles like "user" and "assistant").
    5.  **Configuration:** Set up `config.py` to load API keys (e.g., `OPENAI_API_KEY`, `GOOGLE_API_KEY`) from environment variables or Streamlit secrets (`st.secrets`).
    6.  **LLM Manager:** Create `chat_manager.py`. Implement initial functions to:
        *   Initialize the client for one LLM provider (e.g., Gemini).
        *   Accept chat history and user input.
        *   Send the conversation context to the LLM API.
        *   Receive and return the LLM's text response.
    7.  **Connect UI & LLM:** Modify `app.py` so that when a user enters input:
        *   The message is added to `st.session_state.messages`.
        *   The relevant history is passed to the `chat_manager`.
        *   The LLM's response is received.
        *   The response is added to `st.session_state.messages` with the "assistant" role.
        *   The UI automatically reruns to display the new messages.
*   **Goal:** A functional chat interface that talks to a single LLM, responding to user text inputs without any tool usage.

### Phase 2: Supporting Multiple LLM Providers

*   **Tasks:**
    1.  **Provider Selection:** Add a UI element in `app.py` (e.g., `st.selectbox` or `st.radio` in the sidebar `st.sidebar`) to allow the user to choose between "Gemini" and "OpenAI". Store the selection in `st.session_state`.
    2.  **Update Chat Manager:** Modify `chat_manager.py` to:
        *   Initialize clients for both OpenAI and Gemini (if keys are available).
        *   Select the appropriate client and API call based on the user's selection passed from `app.py`.
    3.  **API Key Handling:** Ensure `config.py` handles keys for both services gracefully (e.g., only initialize clients for which keys exist).
*   **Goal:** Allow the user to seamlessly switch between supported LLM providers.

### Phase 3: Integrating NiFi MCP Tools

*   **Tasks:**
    1.  **Add MCP Dependency:** Add the `mcp` library to `nifi_chat_ui/requirements.txt`.
    2.  **MCP Handler:** Create `nifi_chat_ui/mcp_handler.py`. Implement:
        *   A way to locate and start the `nifi_mcp_server.py` script as a subprocess (e.g., using `subprocess.Popen`). Need to configure the path to the server script.
        *   Logic using the `mcp` client library to connect to the subprocess's `stdio`.
        *   A function `execute_mcp_tool(tool_name: str, params: dict) -> dict` that sends a tool execution request to the connected MCP server and returns the result.
        *   Proper management of the MCP server subprocess lifecycle.
    3.  **Tool Definitions:** Determine how the chat UI will know about the available NiFi tools (e.g., hardcode definitions initially, later potentially load from a shared file or query the server if the MCP standard supports it).
    4.  **Update Chat Manager:** Modify the LLM interaction logic in `chat_manager.py`:
        *   When calling the LLM, provide the list of available NiFi MCP tool definitions in the format expected by the LLM API (e.g., OpenAI's `tools` parameter, Gemini's `tools` parameter).
        *   Check the LLM response for tool call requests (`tool_calls` in OpenAI/Gemini responses).
        *   If a tool call is requested:
            *   Extract the `tool_name` and `parameters`.
            *   **(Inform User):** Add a message to the chat like "Assistant wants to use tool: `nifi.createProcessor(...)`".
            *   Call `mcp_handler.execute_mcp_tool`.
            *   Send the tool's result back to the LLM in a subsequent API call (as a message with `role="tool"`).
            *   The LLM will then generate a final text response based on the tool result.
    5.  **Update UI:** Adjust `app.py` to handle the display:
        *   Show the intermediate "Assistant wants to use tool..." message.
        *   Display the final text response from the LLM after the tool call completes.
*   **Goal:** The LLM can request execution of NiFi MCP tools, the UI facilitates this interaction with the `nifi_mcp_server`, and the LLM uses the results to inform its final response.

### Phase 4: Error Handling and Refinements

*   **Tasks:**
    1.  **Error Handling:** Implement `try...except` blocks around LLM API calls and MCP tool executions. Catch potential exceptions (API errors, connection issues, tool execution errors from NiFi).
    2.  **User Feedback:** Display errors clearly in the chat interface (e.g., using `st.error`).
    3.  **Refine UX:** Improve loading states, potentially add indicators when the LLM or a tool is working.
    4.  **Testing:** Add manual and potentially automated tests for key interaction flows.
    5.  **Code Quality:** Refactor and clean up code. Add documentation (docstrings, comments).
*   **Goal:** A robust, user-friendly application that handles common errors gracefully.

## 4. Configuration Requirements

*   `OPENAI_API_KEY`: Required if using OpenAI.
*   `GOOGLE_API_KEY`: Required if using Gemini.
*   `NIFI_MCP_SERVER_PATH`: Path to the `nifi_mcp_server.py` script (or executable) for Phase 3.
*   NiFi connection details (`NIFI_API_URL`, `NIFI_USERNAME`, `NIFI_PASSWORD`, `NIFI_TLS_VERIFY`) will be needed by the `nifi_mcp_server` itself, configured as per its own plan. 
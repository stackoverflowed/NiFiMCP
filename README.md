## NiFi MCP (Model Context Protocol)

This repository contains the NiFi MCP project. It provides a chat interface allowing users to interact with Apache NiFi instances using natural language queries. The system uses a Large Language Model (LLM) integrated with custom tools (MCP Tools) that can communicate with the NiFi API to retrieve information, document flows, and create and perform actions on NiFi components.

It has been tested with Nifi versions 1.23 and 1.28, but could work on other versions assuming the Nifi REST Api stays consistent.  While it is quite functional, you will find that the type of LLM model you use will have a big effect on how well it follows your instructions.  I have found o4-mini and gpt-4.1 to be good.

It's ability to read and document existing flows is very good.  It's ability to create new and modify existing flows is OK, but often takes several iterations and some user help. My future plans are to refine the tools to improve this aspect.  I may also introduce some tools to help it debug flows itself.

## Some other usage notes
The top text box "Define the overall objective for this session" is optional, if you add something here it will be appended to the system prompt for every llm request.  It's useful to include the process group id that you want the llm to work within, or for keeping the objective front and center for the llm.

The token count after each llm response is worth keeping an eye on to see how large it is getting because the history of the prior conversations will be included, it will always grow in size and you may want to reset the conversation history (button in side panel) from time to time.

LLM responses can be copied to the clipboard with the white icons under the responses, and in the side panel there is a button to copy the complete conversation.  

The Tool Phase filter dropdown is to allow you to expose different tools to the LLM at the different phases you are working on, for example if you just want it to document flows, you can set the dropdown to "Review" which only gives it read tools and not write ones.

## Setup Instructions

To set up the development environment and install all dependencies, follow these steps:

1. **Clone the Repository:**
   ```bash
   git clone git@github.com:ms82119/NiFiMCP.git
   cd NiFiMCP
   ```

2. **Set Up a Virtual Environment:**
   It's recommended to use a virtual environment to manage dependencies. You can create one using `venv`:
   ```bash
   python3 -m venv .venv
   ```

3. **Activate the Virtual Environment:**
   - On macOS and Linux:
     ```bash
     source .venv/bin/activate
     ```
   - On Windows:
     ```bash
     .venv\Scripts\activate
     ```

4. **Install Dependencies:**
   Use `uv` to install dependencies based on `pyproject.toml` and `uv.lock`:
   ```bash
   uv sync
   ```

5. **Update the config.yaml file with your Nifi details and LLM API keys**
   Use the config.example.yaml as your guide for format and structure

6. **Run the MCP Server:**
   Start the MCP server with:
   ```bash
   uvicorn nifi_mcp_server.server:app --reload --port 8000
   ```

7. **Run the Streamlit Client:**
   Start the Streamlit client with:
   ```bash
   python -m streamlit run nifi_chat_ui/app.py
   ```

## Running Automated Tests

A test script is included to verify the core functionality of the NiFi MCP tools via the REST API.

1.  **Ensure the MCP Server is Running:**
    Follow step 6 in the Setup Instructions to start the server:
    ```bash
    uvicorn nifi_mcp_server.server:app --reload --port 8000 
    ```

2.  **Set the Target NiFi Instance ID:**
    The test script needs to know which configured NiFi server instance to target. Set the `NIFI_TEST_SERVER_ID` environment variable to the `id` of the desired server from your `config.yaml`.
    - On macOS/Linux:
      ```bash
      export NIFI_TEST_SERVER_ID="your_nifi_server_id_here"
      ```
    - On Windows (Command Prompt):
      ```bash
      set NIFI_TEST_SERVER_ID="your_nifi_server_id_here"
      ```
    - On Windows (PowerShell):
      ```bash
      $env:NIFI_TEST_SERVER_ID="your_nifi_server_id_here"
      ```
    *Replace `"your_nifi_server_id_here"` with the actual ID.*

3.  **Run the Test Script:**
    Execute the script from the project root directory:
    ```bash
    python -m tests.test_nifi_tools
    ```
    The script will output logs indicating the progress and success or failure of each step. It will create and then clean up a process group named `mcp-auto-test-pg-...` on the target NiFi instance.


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

5. **Update the .env file with Nifi and LLM API keys**

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


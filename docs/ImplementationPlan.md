# NiFi MCP Server Implementation Plan

## 1. Introduction

**Goal:** To create a Model Context Protocol (MCP) server that acts as an intermediary between an MCP client (like a chat interface or development tool) and an Apache NiFi instance. This will allow users to interact with NiFi via natural language or structured commands to build, inspect, and modify data flows.

**Target NiFi Version:** Apache NiFi 1.28.0

**Initial Scope:** The initial implementation will focus on a core set of functionalities:
*   Listing basic components within a process group.
*   Creating simple processors (e.g., GenerateFlowFile, LogAttribute).
*   Connecting processors.
*   Providing basic status information.

**MCP Standard:** The server and a dedicated test client will adhere to the public Anthropic Model Context Protocol specification and utilize their Python SDKs where applicable.

## 2. Architecture

**MCP Server:**
*   **Language/Framework:** Python 3.10+ using the `mcp-server` package (specifically `FastMCP` for tool definition, based on the Anthropic Quickstart).
*   **Dependencies:** `mcp-server`, `httpx` (for NiFi API calls).

**NiFi Interaction:**
*   **API:** NiFi REST API v1.28.0
*   **NiFi Instance (Development):** `https://localhost:8443/nifi/`
*   **NiFi API Base:** Likely `https://localhost:8443/nifi-api` (to be confirmed).
*   **Authentication (Initial):** Username/Password authentication via the `/access/token` endpoint to obtain a JWT for subsequent API calls. Will require secure handling of credentials.
*   **HTTPS/TLS:** The client making requests to the local NiFi instance will need to handle the self-signed certificate (e.g., disable verification for local dev *only* or explicitly trust the certificate).

**Test Client:**
*   **Language/Framework:** Python 3.10+ using the `mcp` client package and `anthropic` (based on the Anthropic Quickstart).
*   **Purpose:** To directly interact with and test the NiFi MCP server independently of other MCP hosts (like Cursor or Claude Desktop).

**High-Level Flow:**

+-------------+       +-------------------+       +---------------+       +-----------------+
| User        | ----> | MCP Client        | ----> | NiFi MCP      | ----> | NiFi 1.28 REST  |
| (via Chat)  |       | (Test Client/Tool)|       | Server (Python)|       | API             |
+-------------+       +-------------------+       +---------------+       +-----------------+
                             |                                |
                             +--------------------------------+
                                (MCP Protocol - JSON/RPC)

## 3. NiFi API Interaction

*   **API Reference:** [NiFi 1.28.0 REST API Documentation](https://nifi.apache.org/docs/nifi-docs/rest-api/index.html)
*   **Client Library:** A custom wrapper module will be built using `httpx` to handle:
    *   Authentication (fetching and using JWT tokens).
    *   Making requests to specific NiFi REST endpoints.
    *   Parsing JSON responses.
    *   Handling NiFi API errors (e.g., 4xx, 5xx status codes) and translating them into meaningful errors for the MCP layer.
    *   Configuring TLS verification for different environments (disable for localhost dev, enable for production).
*   **Key Endpoints (Initial):**
    *   `/access/token` (POST): For authentication.
    *   `/process-groups/{id}/processors` (POST): To create processors.
    *   `/process-groups/{id}/connections` (POST): To create connections.
    *   `/process-groups/{id}/processors` (GET): To list processors.
    *   `/processors/{id}` (GET, PUT): To get/update processor details and configuration.
    *   `/connections/{id}` (GET, PUT): To get/update connection details.
    *   `/flow/process-groups/{id}` (GET): To get process group details (needed for coordinates, etc.).

## 4. MCP Server Implementation

*   **Framework:** `mcp-server` (Python), likely using `FastMCP`.
*   **Transport:** Standard I/O (`stdio`) for initial testing, potentially WebSocket later if needed.
*   **Server Entrypoint:** A Python script (e.g., `nifi_mcp_server.py`) that initializes `FastMCP` and defines tools.
*   **Defined MCP Tools (Initial):**
    *   `nifi.getApiToken(username: str, password: str) -> str`: Logs into NiFi, returns status/token (token likely stored server-side).
    *   `nifi.listProcessors(processGroupId: str = "root") -> list[dict]`: Lists processors in a group.
    *   `nifi.createProcessor(processGroupId: str = "root", type: str, name: str, config: dict, position: dict) -> dict`: Creates a new processor.
    *   `nifi.createConnection(processGroupId: str = "root", sourceId: str, sourceRelationship: str, targetId: str) -> dict`: Creates a connection between two components.
    *   *Further tools to be defined as needed (e.g., `getProcessorDetails`, `updateProcessorConfig`, `startProcessor`, `getFlowSnippet`).*
*   **Tool Implementation:** Each tool function will use the NiFi API client library (from section 3) to perform the corresponding action on the NiFi instance. Return values should conform to MCP expectations (often JSON serializable data).

## 5. Initial Supported Actions/Patterns

*   **Goal 1:** Authenticate with the NiFi instance.
    *   *MCP Call:* `nifi.getApiToken(username="user", password="pwd")`
*   **Goal 2:** Create a simple "GenerateFlowFile -> LogAttribute" flow on the root canvas.
    *   *MCP Calls:*
        1.  `nifi.createProcessor(type="org.apache.nifi.processors.standard.GenerateFlowFile", name="Generate Data", config={...}, position={"x": 100, "y": 100})` -> returns processor details including ID (e.g., `gen_id`)
        2.  `nifi.createProcessor(type="org.apache.nifi.processors.standard.LogAttribute", name="Log Data", config={...}, position={"x": 100, "y": 300})` -> returns processor details including ID (e.g., `log_id`)
        3.  `nifi.createConnection(sourceId=gen_id, sourceRelationship="success", targetId=log_id)` -> returns connection details.

## 6. Test Client

*   **Framework:** `mcp` client library (Python).
*   **Purpose:** Provide a command-line or simple interactive interface to:
    *   Connect to the running NiFi MCP Server (via `stdio`).
    *   Send specific MCP tool requests (e.g., `client.execute_tool('nifi.createProcessor', {...})`).
    *   Display the results received from the MCP server.
    *   Facilitate rapid testing and debugging of the MCP server tools.

## 7. Testing Strategy

*   **Unit Tests:** Use `pytest` to test the NiFi API client library functions in isolation (potentially mocking `httpx` responses).
*   **Integration Tests:** Use the Test Client to execute sequences of MCP tool calls against a running NiFi MCP Server connected to a live (local) NiFi instance. Verify that the expected changes occur in the NiFi UI/API.
*   **Manual Testing:** Use the Test Client and inspect the NiFi UI manually for more complex scenarios.

## 8. Deployment & Configuration

*   **Local Development:**
    *   Run NiFi 1.28 locally.
    *   Run the NiFi MCP Server Python script.
    *   Run the Test Client Python script, connecting it to the server's `stdio`.
*   **Configuration:**
    *   MCP Server will need:
        *   NiFi Base URL (`NIFI_API_URL`).
        *   Mechanism to get NiFi credentials (e.g., environment variables `NIFI_USERNAME`, `NIFI_PASSWORD`, or prompt).
        *   Flag to control TLS verification (`NIFI_TLS_VERIFY`).
    *   Test Client will need:
        *   Command/path to launch the MCP Server.

---

## 9. Use Cases & LLM Interaction

This section outlines potential end-user scenarios demonstrating how the NiFi MCP server, combined with an LLM-powered chat client, can enhance interaction with Apache NiFi.

**Assumptions:**
*   The MCP client integrates with an LLM (e.g., Claude, Gemini, GPT models).
*   The LLM is provided with the definitions (name, description, parameters) of the MCP tools exposed by our server.

**Use Case 1: Flow Documentation and Exploration**

*   **Scenario:** A user needs to understand a complex NiFi flow they didn't create.
*   **User Interaction:** "Can you explain the 'Customer Data Ingest' flow in the main process group? What does the 'LookupCustomer' processor do and how is it configured? Show me the expression used in the 'RouteOnAttribute' processor."
*   **LLM / MCP Interaction:**
    1.  The LLM receives the user query.
    2.  It identifies the need to inspect NiFi components.
    3.  It calls MCP tools like `nifi.listProcessors(processGroupId="...")`, `nifi.getProcessorDetails(processorId="...")`, `nifi.listConnections(...)` etc., to retrieve information about the specified flow/processors.
    4.  The MCP server interacts with the NiFi API to fetch the data.
    5.  The LLM receives the structured data (JSON) from the MCP server.
    6.  The LLM synthesizes this information into a natural language summary, potentially including specific configuration details or NiFi Expression Language snippets as requested.
    7.  The LLM presents the explanation to the user via the chat client.

**Use Case 2: Flow Generation from Requirements**

*   **Scenario:** A user needs to build a new NiFi flow for a specific task.
*   **User Interaction:** "Create a new flow that listens for incoming JSON data on HTTP port 8081. For each request, add an attribute 'received_time' with the current timestamp. Then, log the attributes and send the original FlowFile content to Kafka topic 'raw_events'."
*   **LLM / MCP Interaction:**
    1.  The LLM analyzes the user's requirements.
    2.  It decomposes the request into NiFi components: ListenHTTP -> UpdateAttribute -> LogAttribute -> PublishKafka.
    3.  It maps these components and their configuration needs to the available MCP tools.
    4.  It makes a sequence of MCP calls:
        *   `nifi.createProcessor(type="org.apache.nifi.processors.standard.ListenHTTP", name="Listen HTTP 8081", config={"Listening Port": "8081", ...}, position={...})` -> get ID `listen_id`
        *   `nifi.createProcessor(type="org.apache.nifi.processors.standard.UpdateAttribute", name="Add Timestamp", config={"received_time": "${now()}"}, position={...})` -> get ID `update_id`
        *   `nifi.createProcessor(type="org.apache.nifi.processors.standard.LogAttribute", name="Log Attributes", config={...}, position={...})` -> get ID `log_id`
        *   `nifi.createProcessor(type="org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6", name="Publish to Kafka", config={"Kafka Topic": "raw_events", ...}, position={...})` -> get ID `kafka_id`
        *   `nifi.createConnection(sourceId=listen_id, sourceRelationship="success", targetId=update_id)`
        *   `nifi.createConnection(sourceId=update_id, sourceRelationship="success", targetId=log_id)`
        *   `nifi.createConnection(sourceId=log_id, sourceRelationship="success", targetId=kafka_id)`
    5.  The MCP server executes these requests via the NiFi API.
    6.  The LLM potentially confirms the successful creation or reports any errors back to the user.
    7.  *(Advanced)* The LLM might even suggest starting the processors (`nifi.startProcessor`) or running a simple test if tools for that exist.

**Implications for Development:**
*   MCP tool design should include clear descriptions and parameter names to aid the LLM's understanding.
*   The NiFi API client needs robust error handling to report issues back through the MCP server to the LLM/user.
*   Initial focus should be on providing comprehensive read/query tools (for documentation) and core creation/connection tools (for generation).
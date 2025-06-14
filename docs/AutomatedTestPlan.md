# NiFi MCP Tools - Automated Test Plan

This document outlines the steps for an automated test script that validates the core functionality of the NiFi MCP tools via their REST API endpoints.

## Goal

To create a repeatable, self-contained test that exercises each major NiFi MCP tool category (creation, operation, modification, deletion) within a dedicated, temporary process group. The test should ensure basic functionality and proper cleanup.

## Prerequisites

1.  The NiFi MCP REST API server (`nifi_mcp_server/server.py`) must be running.
2.  The server must be configured with at least one valid NiFi instance in its `config.yaml`.
3.  The test script needs the base URL of the running server (e.g., `http://localhost:8000`) and the `id` of the target NiFi instance from the server's configuration.

## Test Script Approach

*   Use a standard Python HTTP client library (e.g., `httpx`).
*   Target the REST endpoints exposed by `nifi_mcp_server/server.py`.
*   Include the `X-Nifi-Server-Id` header in all requests to the `/tools/*` endpoints, specifying the target NiFi instance.
*   Implement robust cleanup using a `try...finally` block to ensure test components are deleted even if errors occur mid-test.
*   Perform basic assertions on HTTP status codes (expect 200 OK for successful tool calls) and the `"status"` field within the JSON response body (expect `"success"` or potentially `"warning"` where appropriate).

## Test Flow Details

Let `BASE_URL` be the server's base URL and `NIFI_SERVER_ID` be the target NiFi instance ID. Headers should include `{"X-Nifi-Server-Id": NIFI_SERVER_ID, "Content-Type": "application/json"}` for POST requests. Generate unique names/IDs (e.g., using timestamps or UUIDs) where necessary to avoid collisions if tests are run concurrently or cleanup fails.

**Variables to Store:**

*   `test_pg_id`: ID of the main test process group.
*   `generate_proc_id`: ID of the GenerateFlowFile processor.
*   `log_proc_id`: ID of the LogAttribute processor.
*   `connection_id`: ID of the connection between the processors.

---

### 1. Setup Phase

*   **Action:** Define `BASE_URL` and `NIFI_SERVER_ID`.
*   **(Optional) Action:** Verify Server Connectivity and Target ID.
    *   **API Call:** `GET {BASE_URL}/config/nifi-servers`
    *   **Check:** Response status 200 OK. Check if the `NIFI_SERVER_ID` exists in the returned list of server IDs.

### 2. Test Execution Phase (within `try` block)

*   **Action:** Create Test Process Group.
    *   **API Call:** `POST {BASE_URL}/tools/create_nifi_process_group`
    *   **Headers:** `{"X-Nifi-Server-Id": NIFI_SERVER_ID, "Content-Type": "application/json"}`
    *   **Body:**
        ```json
        {
          "arguments": {
            "name": "mcp-automated-test-pg-[timestamp]",
            "position_x": 0,
            "position_y": 0
          }
        }
        ```
    *   **Check:** Response status 200 OK. Check JSON body for `"status": "success"`.
    *   **Store:** Extract `entity.id` into `test_pg_id`.

*   **Action:** Create GenerateFlowFile Processor within the Test PG.
    *   **API Call:** `POST {BASE_URL}/tools/create_nifi_processors`
    *   **Headers:** `{"X-Nifi-Server-Id": NIFI_SERVER_ID, "Content-Type": "application/json"}`
    *   **Body:**
        ```json
        {
          "arguments": {
            "process_group_id": test_pg_id, // Use the stored PG ID
            "processor_type": "org.apache.nifi.processors.standard.GenerateFlowFile",
            "name": "mcp-test-generate",
            "position_x": 0,
            "position_y": 0
          }
        }
        ```
    *   **Check:** Response status 200 OK. Check JSON body for `"status": "success"` or `"warning"` (as it might need configuration).
    *   **Store:** Extract `entity.id` into `generate_proc_id`.

*   **Action:** Create LogAttribute Processor within the Test PG.
    *   **API Call:** `POST {BASE_URL}/tools/create_nifi_processors`
    *   **Headers:** `{"X-Nifi-Server-Id": NIFI_SERVER_ID, "Content-Type": "application/json"}`
    *   **Body:**
        ```json
        {
          "arguments": {
            "process_group_id": test_pg_id, // Use the stored PG ID
            "processor_type": "org.apache.nifi.processors.standard.LogAttribute",
            "name": "mcp-test-log",
            "position_x": 0,
            "position_y": 200
          }
        }
        ```
    *   **Check:** Response status 200 OK. Check JSON body for `"status": "success"` or `"warning"`.
    *   **Store:** Extract `entity.id` into `log_proc_id`.

*   **Action:** Connect GenerateFlowFile to LogAttribute.
    *   **API Call:** `POST {BASE_URL}/tools/create_nifi_connections`
    *   **Headers:** `{"X-Nifi-Server-Id": NIFI_SERVER_ID, "Content-Type": "application/json"}`
    *   **Body:**
        ```json
        {
          "arguments": {
            "source_id": generate_proc_id,
            "relationships": ["success"], // GenerateFlowFile only has 'success'
            "target_id": log_proc_id
          }
        }
        ```
    *   **Check:** Response status 200 OK. Check JSON body for `"status": "success"`.
    *   **Store:** Extract `entity.id` into `connection_id`.

*   **Action:** Update LogAttribute Properties (Example: Log Level).
    *   **API Call:** `POST {BASE_URL}/tools/update_nifi_processor_properties`
    *   **Headers:** `{"X-Nifi-Server-Id": NIFI_SERVER_ID, "Content-Type": "application/json"}`
    *   **Body:**
        ```json
        {
          "arguments": {
            "processor_id": log_proc_id,
            "processor_config_properties": {
              "Log Level": "WARN", // Change from default INFO
              "Attributes to Log": "", // Ensure required properties have values if needed
              "Attributes to Ignore": "",
              "Log prefix": "mcp-test"
            }
          }
        }
        ```
    *   **Check:** Response status 200 OK. Check JSON body for `"status": "success"` or `"warning"`. *Note: This tool requires the processor to be stopped.*

*   **Action:** Start the Process Group (starts components inside).
    *   **API Call:** `POST {BASE_URL}/tools/operate_nifi_object`
    *   **Headers:** `{"X-Nifi-Server-Id": NIFI_SERVER_ID, "Content-Type": "application/json"}`
    *   **Body:**
        ```json
        {
          "arguments": {
            "object_type": "process_group",
            "object_id": test_pg_id,
            "operation_type": "start"
          }
        }
        ```
    *   **Check:** Response status 200 OK. Check JSON body for `"status": "success"`.

*   **(Optional) Action:** Pause/Wait briefly to allow flowfile generation/processing.

*   **(Optional) Action:** Check Processor Status (Example: Get LogAttribute details).
    *   **API Call:** `POST {BASE_URL}/tools/get_nifi_processor_details`
    *   **Headers:** `{"X-Nifi-Server-Id": NIFI_SERVER_ID, "Content-Type": "application/json"}`
    *   **Body:**
        ```json
        {
          "arguments": { "processor_id": log_proc_id }
        }
        ```
    *   **Check:** Response status 200 OK. Check JSON body for `component.state == "RUNNING"`. Check `component.config.properties['Log Level'] == "WARN"`.

*   **Action:** Stop the Process Group (stops components inside).
    *   **API Call:** `POST {BASE_URL}/tools/operate_nifi_object`
    *   **Headers:** `{"X-Nifi-Server-Id": NIFI_SERVER_ID, "Content-Type": "application/json"}`
    *   **Body:**
        ```json
        {
          "arguments": {
            "object_type": "process_group",
            "object_id": test_pg_id,
            "operation_type": "stop"
          }
        }
        ```
    *   **Check:** Response status 200 OK. Check JSON body for `"status": "success"`.

*   **(Optional) Action:** Pause/Wait briefly to allow components to stop.

---

### 3. Teardown Phase (within `finally` block)

*   **Goal:** Delete all created components in reverse order (connections, processors, process group), ensuring deletion attempts happen even if earlier steps failed or components weren't created. Check if variables (`connection_id`, `generate_proc_id`, etc.) have values before attempting deletion.

*   **Action:** Delete Connection (if `connection_id` exists).
    *   **API Call:** `POST {BASE_URL}/tools/delete_nifi_object`
    *   **Headers:** `{"X-Nifi-Server-Id": NIFI_SERVER_ID, "Content-Type": "application/json"}`
    *   **Body:**
        ```json
        {
          "arguments": {
            "object_type": "connection",
            "object_id": connection_id
          }
        }
        ```
    *   **Check:** Response status 200 OK. Check JSON body for `"status": "success"`. Log warnings on errors but continue cleanup.

*   **Action:** Delete LogAttribute Processor (if `log_proc_id` exists).
    *   **API Call:** `POST {BASE_URL}/tools/delete_nifi_object`
    *   **Headers:** `{"X-Nifi-Server-Id": NIFI_SERVER_ID, "Content-Type": "application/json"}`
    *   **Body:**
        ```json
        {
          "arguments": {
            "object_type": "processor",
            "object_id": log_proc_id
          }
        }
        ```
    *   **Check:** Response status 200 OK. Check JSON body for `"status": "success"`. Log warnings on errors but continue cleanup.

*   **Action:** Delete GenerateFlowFile Processor (if `generate_proc_id` exists).
    *   **API Call:** `POST {BASE_URL}/tools/delete_nifi_object`
    *   **Headers:** `{"X-Nifi-Server-Id": NIFI_SERVER_ID, "Content-Type": "application/json"}`
    *   **Body:**
        ```json
        {
          "arguments": {
            "object_type": "processor",
            "object_id": generate_proc_id
          }
        }
        ```
    *   **Check:** Response status 200 OK. Check JSON body for `"status": "success"`. Log warnings on errors but continue cleanup.

*   **Action:** Delete Test Process Group (if `test_pg_id` exists).
    *   **API Call:** `POST {BASE_URL}/tools/delete_nifi_object`
    *   **Headers:** `{"X-Nifi-Server-Id": NIFI_SERVER_ID, "Content-Type": "application/json"}`
    *   **Body:**
        ```json
        {
          "arguments": {
            "object_type": "process_group",
            "object_id": test_pg_id
          }
        }
        ```
    *   **Check:** Response status 200 OK. Check JSON body for `"status": "success"`. Log warnings on errors.

---

## Future Enhancements

*   Add tests for port creation/deletion/operation.
*   Test error conditions (e.g., deleting a non-existent processor, creating a connection between different groups).
*   Integrate with `pytest` for better test structure, assertions, and reporting.
*   Add checks for FlowFile queue counts or provenance data if relevant tools are developed.
*   Test updating connection properties.
*   Test deleting processor properties.

# NiFi MCP Enhanced Error Handling: Auto-Stop, Auto-Delete, Auto-Purge

This document outlines the implementation plan and testing strategy for three new features aimed at improving the robustness and autonomy of the NiFi MCP server when interacting with the NiFi API. These features are: Auto-stop, Auto-delete, and Auto-purge.

## 1. Feature Overview

These features will be configurable (e.g., toggled on/off via server configuration, accessible through a side panel in a UI if applicable). When enabled, they will allow the MCP server to automatically attempt to resolve common operational errors encountered with the NiFi API before returning a failure to the requesting LLM.

*   **Auto-stop:** If an action (e.g., update, delete) on a NiFi component fails because the component is running, the MCP server will attempt to stop the component and retry the original action.
*   **Auto-delete:** If deleting a component (e.g., a processor) fails due to existing connections, the MCP server will attempt to identify and delete these dependent connections before retrying the original deletion.
*   **Auto-purge:** If deleting a connection fails because it has an active queue, the MCP server will attempt to purge the queue and then retry deleting the connection.

## 2. Implementation Plan

The core of these features will reside in the MCP server's logic that handles NiFi API calls. We'll need to intercept specific error responses, trigger remedial actions, and then retry the original request.

### 2.1. General Approach

1.  **Configuration:**
    *   Implement a server-side configuration mechanism to enable/disable each feature individually. This will be managed in the main `config.yaml` file, with settings loaded and accessible via `config/settings.py`. Example `config.yaml` section:
        ```yaml
        mcp_features:
          auto_stop_enabled: true
          auto_delete_enabled: true
          auto_purge_enabled: true
        ```
    *   For testing and potential runtime overrides, the server should also respect HTTP headers like `X-Mcp-Auto-Stop-Enabled: true/false`, `X-Mcp-Auto-Delete-Enabled: true/false`, `X-Mcp-Auto-Purge-Enabled: true/false`. These headers would take precedence over the `config.yaml` settings for a given request.
    *   The application (e.g., side panel) will read/write these settings if UI control is implemented later. For now, `config.yaml` is the primary control.
2.  **Error Detection:**
    *   Analyze NiFi API error responses (HTTP status codes and response bodies) to accurately identify the conditions for each auto-feature. Common error messages include:
        *   "Cannot delete Processor because it is currently running"
        *   "Cannot delete Processor because it has active connections"
        *   "Cannot delete Connection because it has an active queue"
    *   The NiFi API often provides IDs of involved components in error messages or requires a follow-up lookup.
3.  **Retry Logic:**
    *   Implement a wrapper around relevant NiFi API calls (e.g., delete processor, update processor, delete connection).
    *   This wrapper will check for specific errors and, if an auto-feature is enabled, execute the remediation steps.
    *   A limited number of retries (e.g., 1 retry after remediation) should be implemented to avoid infinite loops.
4.  **Centralized Retry Logic:**
    *   The core error handling, remediation, and retry logic will be centralized, likely in a new Python module (e.g., `nifi_mcp_server/core/nifi_error_handler.py`) or as a decorator/wrapper function.
    *   This centralized handler will:
        *   Execute the primary NiFi API call.
        *   Catch specific, relevant exceptions from the `NiFiClient`.
        *   Check if the corresponding auto-feature is enabled (via config settings, overridden by headers if present).
        *   If enabled and a known trigger error occurs, invoke the appropriate remediation action (e.g., call `operate_nifi_object` to stop, call a new purge tool).
        *   Retry the original API call once after successful remediation.
        *   Return a consolidated response, indicating if auto-actions were attempted and their outcome.
    *   Existing tool implementations (in `nifi_mcp_server/api_tools/`) will be refactored to use this centralized handler.
5.  **Feedback to LLM:**
    *   If remediation and retry are successful, the original command's success is reported.
    *   If remediation fails or the retry still fails, a more informative message should be returned to the LLM, indicating the attempted auto-actions (e.g., "Failed to delete processor. Auto-stop was attempted but the processor could not be stopped, or deletion failed even after stopping.").

### 2.2. Feature-Specific Implementation Details

#### 2.2.1. Auto-Stop

*   **Triggering Error:** Typically an HTTP 409 Conflict error. The response message usually indicates that a component (processor, process group) is running, preventing an update or deletion.
    *   Example message: `"Cannot perform DELETE on Processor with ID '...' because it is currently RUNNING"` or similar for other operations.
*   **Remediation Strategy:**
    1.  Identify the component (processor, connection, etc.) that the original operation was targeting and its parent Process Group.
    2.  If the error indicates a "running" state conflict:
        *   **Primary Approach:** Issue a "stop" command to the *entire parent Process Group* of the target component using the existing `operate_nifi_object` tool. This is a broader approach aiming to ensure all relevant activity ceases.
        *   **Alternative (if primary is problematic):** Issue a "stop" command only to the specific component identified in the error message (if the error clearly isolates a single running component as the blocker).
    3.  Wait for a short period (e.g., configurable, a few seconds) to allow components to stop.
    4.  Retry the original command (e.g., delete, update).
*   **Challenges:**
    *   Determining the parent Process Group ID if only the component ID is available (may require an extra lookup).
    *   Stopping a large Process Group might take time; the wait period needs to be sufficient but not excessive.
    *   Handling cases where stopping the Process Group (or specific component) also fails or times out.

#### 2.2.2. Auto-Delete (Dependent Connections)

*   **Triggering Error:** Typically an HTTP 409 Conflict error when trying to delete a component (e.g., processor) that has active connections.
    *   Example message: `"Cannot delete Processor with ID '...' because it has XX active connections."`
*   **Remediation:**
    1.  Extract the processor ID from the error or original request.
    2.  Use `get_nifi_object_details` for the processor (or query connections within its parent Process Group and filter by source/destination ID) to list all its incoming and outgoing connections.
    3.  For each identified connection:
        *   If Auto-purge is *also* enabled and the connection deletion fails due to a queue, Auto-purge logic should ideally be triggered for that connection first.
        *   Attempt to delete the connection (`delete_nifi_object` for connection).
    4.  After attempting to delete all dependent connections, retry the original component deletion.
*   **Challenges:**
    *   Reliably identifying all incoming and outgoing connections to a component. The NiFi API might not provide a direct "list connections for processor X" endpoint; it might require fetching all connections in the parent PG and filtering.
    *   Handling cascading dependencies (e.g., deleting a connection might be blocked by its own queue, requiring Auto-purge). The order of operations and interaction between Auto-delete and Auto-purge needs careful consideration.

#### 2.2.3. Auto-Purge (Connection Queues)

*   **Triggering Error:** Typically an HTTP 409 Conflict error when trying to delete a connection.
    *   Example message: `"Cannot delete Connection with ID '...' because it has an active FlowFile queue. You must empty the queue first."`
*   **Remediation:**
    1.  Extract the connection ID from the error or original request.
    2.  A new tool/internal function (e.g., `purge_nifi_connection_queue` in `operation.py` or a direct `NiFiClient` method) will be implemented. This will issue a "drop" or "empty queue" command for the connection (e.g., creating a "drop request" to the `/flowfile-queues/{connection_id}/drop-requests` endpoint).
    3.  Poll the drop request status until it's completed or wait a fixed short period.
    4.  Retry the original connection deletion command.
*   **Challenges:**
    *   The NiFi API for emptying queues is asynchronous (create a drop request, then check its status). Polling will be necessary.
    *   Ensuring the queue is confirmed empty before retrying deletion.

### 2.3. Affected MCP Server Components

*   **NiFi API Client Service:** This service within the MCP server will house the core logic for error parsing, remediation, and retries.
*   **Tool Implementations:** Existing tools like `delete_nifi_object`, `update_nifi_processor_properties`, `create_nifi_connections` (if it can be updated to replace an existing one) might need to be modified to use the enhanced API client service.
*   **Configuration Management:** Handled by `config.yaml` and `config/settings.py`, with HTTP header overrides.
*   **New Centralized Error Handler Module:** A new module (e.g., `nifi_mcp_server/core/nifi_error_handler.py`) will be created for the retry and remediation logic.

## 3. Testing Plan (`tests/test_nifi_tools.py`)

New tests will be added to `tests/test_nifi_tools.py` to simulate scenarios where these features would be triggered. The tests should verify:
1.  The feature correctly identifies the specific error.
2.  The feature performs the correct remediation steps.
3.  The original action succeeds after remediation.
4.  When the feature is disabled, the original error is propagated as expected.
5.  If remediation fails, an appropriate error message is returned.

### 3.1. General Test Setup

*   Leverage the existing `httpx.AsyncClient` and `call_tool` helper.
*   Ensure the `TARGET_NIFI_SERVER_ID` is correctly configured.
*   Introduce a mechanism to toggle these features ON/OFF for specific test cases, perhaps by:
    *   Making temporary modifications to the server's configuration if the tests run against a live server.
    *   Mocking the configuration service if tests are more unit-focused for the client logic.
    *   Passing feature flags through headers if the MCP server is designed to support this for testing. (e.g., `X-Mcp-Auto-Stop-Enabled: true`). This will be the primary method for controlling features on a per-test basis.

### 3.2. Test Scenarios

#### 3.2.1. Auto-Stop Tests

*   **Scenario 1: Delete Running Processor (Blocked by itself)**
    1.  Create a Process Group (PG_AutoStop1).
    2.  Create a processor P1 (e.g., GenerateFlowFile) inside PG_AutoStop1.
    3.  Start P1.
    4.  **With Auto-stop enabled (via header):** Attempt to delete P1.
        *   **Expected:** The centralized handler detects P1 is running. It stops P1 (or its parent PG_AutoStop1 as per the chosen strategy). P1 is then deleted successfully.
    5.  Recreate and start P1.
    6.  **With Auto-stop disabled (via header):** Attempt to delete P1.
        *   **Expected:** Deletion fails with the "processor is running" error.
    7.  Cleanup PG_AutoStop1.

*   **Scenario 2: Update Processor Config (Blocked by itself running)**
    1.  Create PG_AutoStop2 and processor P2.
    2.  Start P2.
    3.  **With Auto-stop enabled (via header):** Attempt to update a property of P2 that requires it to be stopped (e.g., scheduling strategy).
        *   **Expected:** P2 (or PG_AutoStop2) is stopped. Property updated successfully. Test verifies property change. (Consider if P2 should be restarted automatically by the tool or left for LLM to decide).
    4.  Revert property and restart P2 if needed.
    5.  **With Auto-stop disabled (via header):** Attempt the same update.
        *   **Expected:** Update fails with "processor is running" error.
    6.  Cleanup.

*   **Scenario 3: Delete Connection (Blocked by connected Processor running)**
    1.  Create PG_AutoStop3, P_Gen, P_Log. Connect P_Gen to P_Log with C1.
    2.  Start P_Gen and P_Log.
    3.  **With Auto-stop enabled (via header):** Attempt to delete C1. (Assume NiFi API might error if connected processors are running for some operations related to connections - this needs verification. If not, this test case might be more about deleting a PG that has running components).
        *   **Expected:** If NiFi prevents this, PG_AutoStop3 (or specific P_Gen/P_Log) is stopped. C1 is deleted.
    4.  Cleanup.

#### 3.2.2. Auto-Delete Tests

*   **Scenario 1: Delete Processor with Connections**
    1.  Create a Process Group (PG_AutoDelete).
    2.  Create two processors (P1_Source, P2_Sink) in PG_AutoDelete.
    3.  Create a connection C1 from P1_Source to P2_Sink.
    4.  **With Auto-delete enabled:** Attempt to delete P1_Source.
        *   **Expected:** Connection C1 is deleted, then P1_Source is deleted. P2_Sink remains.
    5.  Recreate P1_Source and C1.
    6.  **With Auto-delete disabled:** Attempt to delete P1_Source.
        *   **Expected:** Deletion of P1_Source fails with "has active connections" error. C1 and P2_Sink remain.
    7.  Cleanup PG_AutoDelete (including P2_Sink, C1, P1_Source if not already deleted).

*   **Scenario 2: Interaction with Auto-Purge (Advanced)**
    *   This scenario assumes Auto-purge is also implemented.
    1.  Create PG, P1, P2, and connection C1 from P1 to P2.
    2.  Start P1 to generate FlowFiles and queue them in C1. Stop P1.
    3.  **With Auto-delete AND Auto-purge enabled:** Attempt to delete P1.
        *   **Expected:** Auto-delete identifies C1. Deleting C1 would trigger Auto-purge. C1's queue is purged, C1 is deleted, then P1 is deleted.
    4.  Cleanup.

#### 3.2.3. Auto-Purge Tests

*   **Scenario 1: Delete Connection with Queued Data**
    1.  Create a Process Group (PG_AutoPurge).
    2.  Create P1 (GenerateFlowFile) and P2 (LogAttribute) in PG_AutoPurge.
    3.  Connect P1 to P2 with connection C_Queue.
    4.  Start P1 briefly to generate data into C_Queue, then stop P1. (Verify queue has data using `get_process_group_status` or connection status).
    5.  **With Auto-purge enabled:** Attempt to delete C_Queue.
        *   **Expected:** Queue in C_Queue is purged, then C_Queue is deleted successfully.
    6.  Recreate C_Queue and regenerate data.
    7.  **With Auto-purge disabled:** Attempt to delete C_Queue.
        *   **Expected:** Deletion fails with "queue is not empty" error.
    8.  Cleanup PG_AutoPurge.

### 3.3. Considerations for Tests

*   **Atomicity & Idempotency:** Test cleanup (in `finally` blocks) is crucial.
*   **NiFi API Version:** Ensure awareness of potential differences in API behavior or error messages across NiFi versions, though we target a specific API interaction pattern.
*   **Timing:** Introduce appropriate `asyncio.sleep()` calls to allow NiFi to process actions, especially for start/stop operations and queue purging.
*   **Assertions:** Make detailed assertions on the state of NiFi components and the responses from the MCP server tools.
*   **Test Setup Efficiency:** Test scenarios will be designed with minimal NiFi component setups to ensure clarity and efficiency. A common pattern will involve a 'Generator Processor' (e.g., `org.apache.nifi.processors.standard.GenerateFlowFile`) connected to a 'Sink/Terminating Processor' (e.g., `org.apache.nifi.processors.standard.LogAttribute`), allowing for flow file generation, queue population, and observation of component states. This aligns with existing patterns in `tests/test_nifi_tools.py` and facilitates testing features like Auto-Purge. We will also verify specific NiFi API behaviors, such as whether deleting a connection is blocked by its connected processors being in a running state, and adjust test scenarios accordingly if needed.

## 4. Code Structure and Reusability

*   Review existing NiFi client interaction patterns in the MCP codebase and specific tools like `operate_nifi_object` (found in `nifi_mcp_server/api_tools/operation.py`).
*   The new error handling and retry logic should be centralized in a new module (e.g., `nifi_mcp_server/core/nifi_error_handler.py`) or class. This new component will be utilized by existing tool functions in `nifi_mcp_server/api_tools/` to avoid duplicating logic.
*   Look for opportunities to reuse existing functions for getting component details (e.g., `get_nifi_object_details`), stopping/starting components (e.g., `operate_nifi_object`), and deleting components. A new function/tool will be needed for queue purging.

This plan provides a solid foundation. We will refine it as we delve into the implementation and encounter specific NiFi API behaviors.
The `config.example.yaml` will be updated with a new `mcp_features` section, and `config/settings.py` will be augmented to load and provide these settings.

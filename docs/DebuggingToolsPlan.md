# NiFi MCP Debugging Tools Implementation Plan

This document outlines the plan for implementing new MCP tools focused on debugging and operating NiFi flows.

---

## 1. `operate_nifi_object` (Extended)

*   **File:** `nifi_mcp_server/api_tools/operation.py` (Modify existing tool)
*   **Purpose:** Starts, stops, or enables/disables a specific NiFi processor, port, or *all* components within a process group.
*   **Parameters:**
    *   `object_type`: `Literal["processor", "port", "process_group"]` - The type of object to operate on. (*Extended*)
    *   `object_id`: `str` - The UUID of the object (processor, port, or process group).
    *   `operation_type`: `Literal["start", "stop"]` - The operation to perform. (Note: For 'process_group', this applies to all contained components).
*   **Returns:** `Dict[str, Any]`
    *   A dictionary indicating the status. Structure may vary slightly based on object_type:
        *   For processor/port:
            ```json
            {
              "status": "success", // or "warning", "error"
              "message": "Processor/Port '...' started/stopped successfully.",
              "entity": { ... } // Filtered entity details
            }
            ```
        *   For process_group:
            ```json
            {
              "status": "success", // or "warning", "error"
              "message": "Successfully started/stopped all components in process group XYZ.",
              "process_group_id": "...",
              "operation_type": "start/stop",
              "component_statuses": { ... } // Optional summary as before
            }
            ```
*   **Implementation Notes:**
    *   **Existing Logic (Processor/Port):** Keep the current logic using `get_..._details` for revision and `PUT /.../{id}/run-status`.
    *   **New Logic (Process Group):**
        *   Add an `elif object_type == "process_group":` block.
        *   Map `operation_type` ('start'/'stop') to the target state (`RUNNING`/`STOPPED`).
        *   Use NiFi API endpoint `PUT /flow/process-groups/{object_id}`.
        *   Payload required: `{"id": object_id, "state": "RUNNING" | "STOPPED"}`.
        *   Requires a new `nifi_client` method: `update_process_group_state(pg_id, state)`.
        *   Handle potential errors (e.g., group not found, permissions).
        *   Return a status dictionary appropriate for the group operation.
    *   Update the tool's docstring to reflect the added `process_group` capability.
    *   Ensure pre-checks for starting processors/ports remain in their respective logic paths.

---

## 2. `run_processor_once`

*   **File:** `nifi_mcp_server/api_tools/operation.py`
*   **Purpose:** Attempts to run a single execution cycle for a specified processor. It first ensures the processor is stopped, then requests a single run. Useful for step-by-step debugging.
*   **Parameters:**
    *   `processor_id`: `str` - The ID of the target processor.
*   **Returns:** `Dict[str, Any]`
    *   A dictionary indicating the status:
        ```json
        {
          "status": "success", // or "warning", "error"
          "message": "Processor XYZ successfully triggered for one run.",
          "processor_id": "...",
          "final_state": "STOPPED" // Or potentially RUNNING if it didn't stop automatically
        }
        ```
*   **Implementation Notes:**
    *   **Step 1: Get Processor Details:** Use `nifi_client.get_processor_details(processor_id)` to get the current state and revision.
    *   **Step 2: Stop if Necessary:** If `state` is `RUNNING` or `DISABLED`, call `nifi_client.update_processor_state(processor_id, "STOPPED")`. Get the *new* revision after stopping.
    *   **Step 3: Trigger Run Once:** Use NiFi API endpoint `PUT /processors/{id}/run-status` with payload `{"revision": {...}, "state": "RUN_ONCE", "disconnectedNodeAcknowledged": false}`. Use the latest revision obtained.
    *   **Step 4: Check Final State (Optional but recommended):** After a short delay (e.g., `asyncio.sleep(1)`), call `nifi_client.get_processor_details(processor_id)` again to report the processor's state after the `RUN_ONCE` attempt. Document that the final state might vary based on processor type/scheduling.
    *   Handle errors at each step (not found, conflict, etc.).
    *   Use request logger context.

---

## 3. `get_process_group_status`

*   **File:** `nifi_mcp_server/api_tools/review.py`
*   **Purpose:** Provides a consolidated status overview of a process group, including component states, validation issues, queue sizes, and bulletins. A quick snapshot for debugging.
*   **Parameters:**
    *   `process_group_id`: `str | None = None` - The ID of the target process group. Defaults to root if None.
    *   `include_bulletins`: `bool = True` - Whether to include bulletins specific to this process group.
*   **Returns:** `Dict[str, Any]`
    *   A dictionary summarizing the status:
        ```json
        {
          "process_group_id": "...",
          "process_group_name": "...",
          "component_summary": {
            "processors": {"total": 10, "running": 5, "stopped": 3, "invalid": 1, "disabled": 1},
            "input_ports": {"total": 2, "running": 1, "stopped": 1, "invalid": 0, "disabled": 0},
            "output_ports": {"total": 1, "running": 0, "stopped": 1, "invalid": 0, "disabled": 0}
          },
          "invalid_components": [ // List of components with validation errors
            {"id": "...", "name": "...", "type": "processor", "validation_errors": [...]},
            ...
          ],
          "queue_summary": {
             "total_queued_count": 150,
             "total_queued_size": "1.5 MB",
             "connections_with_data": [
                {"id": "...", "name": "...", "sourceName": "...", "destName": "...", "queued_count": 100, "queued_size": "1 MB"},
                ...
             ]
          },
          "bulletins": [ // Only if include_bulletins is True
             {"id": ..., "nodeAddress": "...", "severity": "ERROR", "timestamp": "...", "message": "..."},
             ...
          ]
        }
        ```
*   **Implementation Notes:**
    *   **Step 1: Get Components:**
        *   Call `nifi_client.list_processors(pg_id)`.
        *   Call `nifi_client.list_connections(pg_id)`.
        *   Call `nifi_client.get_input_ports(pg_id)`.
        *   Call `nifi_client.get_output_ports(pg_id)`.
    *   **Step 2: Process Components:**
        *   Iterate through processors/ports, tally counts by state (`component.state`).
        *   Identify components where `component.validationStatus` is not `VALID` and collect details. Reuse `filter_processor_data` / `filter_port_data` from `utils.py`.
    *   **Step 3: Get Queue Status:**
        *   For each connection ID from Step 1, call `nifi_client.get_connection_status(connection_id)` (New `nifi_client` method needed for `GET /connections/{id}/status`).
        *   Aggregate total counts/size. Identify connections with queued data (`aggregateSnapshot.queuedCount > 0`). Reuse `filter_connection_data` from `utils.py` for names.
    *   **Step 4: Get Bulletins (if requested):**
        *   Call `nifi_client.get_bulletin_board(group_id=pg_id)` (New `nifi_client` method needed for `GET /flow/bulletin-board?groupId={pg_id}`). Filter/limit results if necessary.
    *   **Step 5: Format Output:** Combine all gathered information into the return structure.
    *   Handle resolving `process_group_id` to root if None. Use helper `_get_process_group_name` from `review.py`.
    *   Use request logger context.

---

## 4. `list_flowfiles`

*   **File:** `nifi_mcp_server/api_tools/review.py`
*   **Purpose:** Lists FlowFile summaries either currently queued in a connection or recently processed by a processor (via provenance).
*   **Parameters:**
    *   `target_id`: `str` - The ID of the connection or processor.
    *   `target_type`: `Literal["connection", "processor"]` - Specifies whether the `target_id` refers to a connection or a processor.
    *   `max_results`: `int = 100` - The maximum number of FlowFile summaries to return.
*   **Returns:** `Dict[str, Any]`
    *   A dictionary containing the list of FlowFiles and metadata:
        ```json
        {
           "target_id": "...",
           "target_type": "connection", // or "processor"
           "listing_source": "queue" // or "provenance"
           "flowfile_summaries": [
              {
                "uuid": "...",
                "filename": "...",
                "size": 1024, // bytes
                "queued_duration": 5000, // ms (if from queue)
                "event_time": "...", // ISO timestamp (if from provenance)
                "attributes": {"attr1": "value1", ...}
              },
              ...
           ]
        }
        ```
*   **Implementation Notes:**
    *   **If `target_type == "connection"`:**
        *   Implement the 3-step queue listing process using `nifi_client`:
            1.  `POST /flowfile-queues/{connection-id}/listing-requests` -> Get request ID. (New `nifi_client` method: `create_flowfile_listing_request`)
            2.  Loop `GET /flowfile-queues/{connection-id}/listing-requests/{request-id}` until `finished` is true. (New `nifi_client` method: `get_flowfile_listing_request`)
            3.  `GET /flowfile-queues/{connection-id}/listing-requests/{request-id}/flowfiles` (or included in step 2 result) to get `flowFileSummaries`.
            4.  `DELETE /flowfile-queues/{connection-id}/listing-requests/{request-id}`. (New `nifi_client` method: `delete_flowfile_listing_request`)
        *   Format the `flowFileSummaries` for the response.
    *   **If `target_type == "processor"`:**
        *   Use the Provenance API:
            1.  `POST /provenance` with a query like: `{"searchTerms": {"componentId": target_id}, "maxResults": max_results}`. (New `nifi_client` method: `submit_provenance_query`)
            2.  Loop `GET /provenance/{query-id}` until `finished` is true. (New `nifi_client` method: `get_provenance_query`)
            3.  `GET /provenance/{query-id}/results` to get events. (New `nifi_client` method: `get_provenance_results`)
            4.  `DELETE /provenance/{query-id}`. (New `nifi_client` method: `delete_provenance_query`)
        *   Extract relevant FlowFile summary info (UUID, attributes, timestamp) from the provenance events. Note that multiple events might relate to the same FlowFile UUID within the results. Decide how to present this (e.g., latest event per UUID).
    *   Use request logger context.

---

## 5. `get_flowfile_details`

*   **File:** `nifi_mcp_server/api_tools/review.py`
*   **Purpose:** Retrieves detailed attributes and content (potentially sampled) for a specific FlowFile, identified by its UUID or by locating the most recent one associated with a connection/processor.
*   **Parameters:**
    *   `flowfile_uuid`: `str | None = None` - The specific UUID of the FlowFile to retrieve.
    *   `target_id`: `str | None = None` - The ID of a connection or processor to find the *most recent* FlowFile from. Used if `flowfile_uuid` is None.
    *   `target_type`: `Literal["connection", "processor"] | None = None` - Specifies the type of `target_id`. Required if `target_id` is used.
    *   `max_content_bytes`: `int = 4096` - Maximum bytes of content to return (-1 for unlimited, subject to system limits).
*   **Returns:** `Dict[str, Any]`
    *   A dictionary containing the FlowFile details:
        ```json
        {
          "status": "success", // or "error"
          "message": "Details retrieved for FlowFile ...",
          "flowfile_uuid": "...",
          "attributes": { ... },
          "content_available": true, // boolean
          "content_truncated": false, // boolean
          "content_bytes": 4000, // actual bytes returned
          "content": "..." // String content (or base64 if binary?) - TBD
        }
        ```
*   **Implementation Notes:**
    *   **Step 1: Determine FlowFile UUID:**
        *   If `flowfile_uuid` is provided, use it.
        *   If `target_id` and `target_type` are provided:
            *   Call the logic from `list_flowfiles` (using either queue listing or provenance search based on `target_type`) to get the summary of the *most recent* FlowFile (e.g., first in queue listing result, or latest event time in provenance).
            *   Extract the UUID from the summary.
        *   If no UUID can be determined, return an error.
    *   **Step 2: Get Attributes via Provenance:**
        *   Submit a provenance query (`POST /provenance`) searching specifically for the determined `flowfile_uuid`. Use `maxResults=1` and potentially sort by timestamp descending if needed, or search for a specific event type (e.g., ATTRIBUTES_MODIFIED, CONTENT_MODIFIED, RECEIVE).
        *   Get the results and extract the `attributes` map from a relevant event. (Requires new `nifi_client` methods as per `list_flowfiles`).
    *   **Step 3: Get Content via Provenance:**
        *   From the provenance event found in Step 2, get the `eventId`.
        *   Check if content claim is available (`contentClaimSection`, `contentClaimOffset`, etc.).
        *   Call `nifi_client.get_provenance_event_content(event_id, direction)` where `direction` is 'input' or 'output'. (New `nifi_client` method needed for `GET /provenance/events/{eventId}/content/{input|output}`).
        *   Read the response content stream. Apply `max_content_bytes` limit. Note if content was truncated. Determine how to handle binary vs text content (e.g., attempt decode, fallback to base64).
    *   Handle cases where content is not available or provenance event is not found.
    *   Use request logger context.

---

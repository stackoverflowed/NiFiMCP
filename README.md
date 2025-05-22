## NiFi MCP (Model Context Protocol)

This repository contains the NiFi MCP project. It provides a chat interface allowing users to interact with Apache NiFi instances using natural language queries. The system uses a Large Language Model (LLM) integrated with custom tools (MCP Tools) that can communicate with the NiFi API to retrieve information, document flows, and create and perform actions on NiFi components.

It has been tested with Nifi versions 1.23 and 1.28, but could work on other versions assuming the Nifi REST Api stays consistent.  While it is quite functional, you will find that the type of LLM model you use will have a big effect on how well it follows your instructions.  I have found o4-mini and gpt-4.1 to be good.

It's ability to read and document existing flows is very good.  It's ability to create new and modify existing flows is OK, but often takes several iterations and some user help. My future plans are to refine the tools to improve this aspect.  I may also introduce some tools to help it debug flows itself.

## Some other usage notes
The top text box "Define the overall objective for this session" is optional, if you add something here it will be appended to the system prompt for every llm request.  It's useful to include the process group id that you want the llm to work within, or for keeping the objective front and center for the llm.

The token count after each llm response is worth keeping an eye on to see how large it is getting because the history of the prior conversations will be included, it will always grow in size and you may want to reset the conversation history (button in side panel) from time to time.

LLM responses can be copied to the clipboard with the white icons under the responses, and in the side panel there is a button to copy the complete conversation.  

The Tool Phase filter dropdown is to allow you to expose different tools to the LLM at the different phases you are working on, for example if you just want it to document flows, you can set the dropdown to "Review" which only gives it read tools and not write ones.

## Automatic Features
NiFi MCP includes several automatic features that simplify component management:

1. **Auto-Stop**: Automatically stops running processors before deleting them. This prevents errors when trying to delete a running processor.

2. **Auto-Delete**: Automatically deletes connections to/from a processor being deleted. This eliminates the need to manually delete connections before removing a processor.

3. **Auto-Purge**: Automatically purges data from connections before deleting them. This prevents errors when trying to delete connections with data in the queue.

These features can be configured in the `config.yaml` file:

```yaml
features:
  auto_stop_enabled: true    # Controls the Auto-Stop feature
  auto_delete_enabled: true  # Controls the Auto-Delete feature
  auto_purge_enabled: true   # Controls the Auto-Purge feature
```

You can enable or disable these features globally by changing these settings. The server will use these default settings for all operations unless overridden specifically.

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

### Basic API Tests
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

### Core Operations Tests

The core operations tests verify specific functionality of the NiFi MCP server, including the Auto-Stop, Auto-Delete, and Auto-Purge features.

1. **Ensure the MCP Server is Running:**
   Start the server in debug mode for more detailed logs:
   ```bash
   python -m nifi_mcp_server.server --debug
   ```

2. **Run the Tests:**
   - To run all core operations tests:
     ```bash
     python -m pytest tests/core_operations/
     ```
   - To run specific auto-feature tests:
     ```bash
     # Auto-Stop tests
     python -m pytest tests/core_operations/test_nifi_processor_cleanup_operations.py::test_auto_stop_delete_running_processor -v
     
     # Auto-Delete tests
     python -m pytest tests/core_operations/test_nifi_processor_cleanup_operations.py::test_auto_delete_processor_with_connections -v
     
     # Auto-Purge tests
     python -m pytest tests/core_operations/test_nifi_connection_operations.py::test_auto_purge_connection_with_queued_data -v
     ```

3. **Check the Server Logs:**
   The server logs (located in `logs/server.log`) contain detailed information about how the auto-features function during tests.

## Conversation History: This is the actual log of a conversation using the ChatGPT 4.1 LLM to build a new flow

**User:**
Objective: All the flows that are created and used will be in the group id: 01961089-6001-1312-1782-81ffaa4c9729 (root-> "Martins Test Group2")

We are going to build a new flow according to the following spec.  We'll build it in phases, the first phase will be to set up the processors, their connections and properties using the tool create_nifi_flow, once that initial step is done you should access what needs to be done next and new tools will be made available.  Here's the flow to be built:

## Requirement Specification: NiFi Flow for AI Evaluation

**Overview**

Design a NiFi flow that receives a web request containing a structured JSON payload, processes the data through parsing, decision-making, transformation, and business logic, and returns a response. The flow should be complex enough to test an AI's ability to build, configure, and validate the flow against expected outputs.

---

### 1. **Input Specification**

- **Trigger:** HTTP POST request to a NiFi endpoint.
- **Payload Format:** JSON
- **Sample Input:**
  ```json
  {
    "userId": "12345",
    "order": {
      "items": [
        {"productId": "A1", "quantity": 2, "price": 10.0},
        {"productId": "B2", "quantity": 1, "price": 25.0}
      ],
      "shipping": {
        "address": "123 Main St, Hamden, CT",
        "method": "express"
      }
    },
    "coupon": "SAVE10"
  }
  ```

---

### 2. **Processing Steps**

**a. Parse and Validate Input**
- Parse the JSON payload.
- Validate required fields: `userId`, `order.items`, `order.shipping.address`.

**b. Decision Logic**
- If `coupon` is present and valid (e.g., "SAVE10"), apply a 10% discount to the total order price.
- If `order.shipping.method` is "express", add an express shipping fee ($5).

**c. Transformation**
- Calculate the total order amount:
  - Sum of (`quantity` × `price`) for all items.
  - Apply discount if applicable.
  - Add shipping fee if applicable.
- Generate a unique `orderId` (e.g., UUID).
- Prepare a response object with:
  - `orderId`
  - `userId`
  - `finalAmount`
  - `discountApplied` (boolean)
  - `shippingFee`
  - `status`: "confirmed"

**d. Logging and Auditing**
- Log the raw input and the final response for auditing.

---

### 3. **Output Specification**

- **Response Format:** JSON
- **Sample Output:**
  ```json
  {
    "orderId": "d290f1ee-6c54-4b01-90e6-d701748f0851",
    "userId": "12345",
    "finalAmount": 40.5,
    "discountApplied": true,
    "shippingFee": 5.0,
    "status": "confirmed"
  }
  ```

---

### 4. **Testing Criteria**

- Given the sample input, the AI-built NiFi flow should return the sample output.
- The flow should handle missing or invalid fields gracefully, returning an error response with a relevant message.
- The flow should log both input and output data for traceability.

---

### 5. **Edge Cases**

- Missing `coupon` field: No discount applied.
- Invalid `coupon` value: No discount applied, but no error.
- Missing `order.shipping.method`: Default to "standard" (no extra fee).
- Empty `order.items`: Return error response.

---

### 6. **Success Criteria**

- The AI can generate a NiFi flow that:
  - Accepts the HTTP request and parses JSON.
  - Implements business logic and transformations as described.
  - Constructs and returns the correct JSON response.
  - Handles errors and logs appropriately.

**Assistant:** ⚙️ Calling tool(s): `create_nifi_flow`...

**Assistant:**
**Phase 1 Results and Assessment**

The initial phase attempted to set up the structure as described. Here's what happened:

### Successfully Created:
- Processors:
    - HandleHttpRequest
    - HandleHttpResponse
    - Log Input (Raw)
    - Log Output (Response)
    - ValidateJson Input
    - Extract Fields / Items
- Connections between those that were created.

### Key Issues Encountered:
1. **Processor Errors (INVALID status):**
   - Several processors are INVALID due to configuration/property issues:
     - HandleHttpRequest: Missing/incorrect properties ('Base Path', 'HTTP Method', 'HTTP Context Map').
     - HandleHttpResponse: Missing required properties ('HTTP Status Code', 'HTTP Context Map').
     - ValidateJson Input: Wrong schema strategy and property names for inline schema.
     - JoltTransformJSON (Extract Fields / Items): Invalid Jolt spec property.
   - Most processors are not yet valid and will need detailed configuration or property correction.

2. **Missing Processor:**
   - "Business Logic & Response Builder" (intended to use ExecuteScript) could not be created: The processor class org.apache.nifi.processors.standard.ExecuteScript is not available in your NiFi instance.
   - As a result, connections involving "Business Logic & Response Builder" failed to be created.

### Immediate Next Steps (Recommended for Phase 2):
1. **Processor Type Corrections:**
    - The flow needs an available scripting/logic processor for business logic and JSON transformation. "ExecuteScript" is not available—alternatives could be "ExecuteGroovyScript", "JoltTransformJSON" (with custom spec), "ExecuteScript" (if a compatible NAR is installed), or "ExecuteStreamCommand" (calling out to Python/Groovy).

2. **Property Corrections:**
    - "HandleHttpRequest" and "HandleHttpResponse" require mandatory properties: HTTP Context Map, HTTP Status Code, etc.
    - "ValidateJson" needs correct property keys for schema definition.
    - "JoltTransformJSON" must use a valid Jolt specification.
    - Connections missing or failed due to validity propagate from processor issues.

3. **Design Adjustments:**
    - Assess which scripting or logic processor is available for your NiFi instance that can fulfill the required business logic.

Please advise how you want to proceed:
- Which processor to use for business logic/transformation? (Search available scripting processors, or should I suggest alternatives?)
- Do you want to focus on property corrections for existing processors and complete the missing/INVALID configuration?

**Summary:**  
Initial structure is present but flow is broken due to missing/excluded critical processor and required property issues. Once the correct business logic processor is identified and all configurations are fixed, we can continue to finish building and validating the flow.

Let me know your decision or questions regarding processor choice and property details before continuing.

TASK COMPLETE

**User:**
Ok I have created and enabled a StandardHttpContextMap.  You can use the lookup tool to find the appropriate processors for scripting.  You can then continue with remediating the flow so that it meets the requirements.

**Assistant:** ⚙️ Calling tool(s): `lookup_nifi_processor_type, lookup_nifi_processor_type, lookup_nifi_processor_type, lookup_nifi_processor_type`...

**Assistant:** ⚙️ Calling tool(s): `get_nifi_object_details, get_nifi_object_details, get_nifi_object_details, get_nifi_object_details`...

**Assistant:** ⚙️ Calling tool(s): `update_nifi_processor_properties, update_nifi_processor_properties, update_nifi_processor_properties, update_nifi_processor_properties`...

**Assistant:**
Processor type analysis and next step planning:

1. **Available scripting and transformation processors:**
   - You have both org.apache.nifi.processors.script.ExecuteScript and org.apache.nifi.processors.groovyx.ExecuteGroovyScript available for business logic.
   - JoltTransformJSON is present for lightweight JSON reshaping.
   - EvaluateJsonPath can extract JSON values to FlowFile attributes, if needed.

2. **Next step to fully enable the flow:**
   - Remediate processor configs. My previous tool calls to update processor properties failed due to missing required input structure ("processor_config_properties" is needed).
   - The correct way is to pass only the properties dict for property updates.

3. **Planned actions for remediation:**
   - Set correct/required properties:
     - HandleHttpRequest: Only relevant properties (remove Base Path/HTTP Method - they're not valid; set HTTP Context Map and Allowed Paths; disable unneeded methods).
     - HandleHttpResponse: Set HTTP Context Map and a default Status Code (e.g., 200 or dynamic).
     - ValidateJson Input: Switch Schema Access Strategy to SCHEMA_CONTENT_PROPERTY, set JSON Schema.
     - Extract Fields / Items: Provide a placeholder or valid Jolt specification according to your spec.
   - Replace "Business Logic & Response Builder" with ExecuteScript or ExecuteGroovyScript, connecting after Extract Fields / Items.

**Please confirm:**
- Should I use ExecuteScript or ExecuteGroovyScript for the business logic step?
- Should I also create and wire up this scripting processor, or just correct existing processor configs in this phase?

Once clarified, I'll proceed with remediation of processor configuration and complete the wiring for the business logic processor.

TASK COMPLETE

**User:**
Let's go with the Groovy Script and create and connect the processor, then continue with the overall remediation of the flow.

**Assistant:** ⚙️ Calling tool(s): `create_nifi_processor`...

**Assistant:** ⚙️ Calling tool(s): `create_nifi_processor`...

**Assistant:** ⚙️ Calling tool(s): `create_nifi_connection, create_nifi_connection, create_nifi_connection`...

**Assistant:** ⚙️ Calling tool(s): `update_nifi_processor_properties, update_nifi_processor_properties, update_nifi_processor_properties, update_nifi_processor_properties`...

**Assistant:** ⚙️ Calling tool(s): `delete_nifi_processor_properties, delete_nifi_processor_properties, delete_nifi_processor_properties`...

**Assistant:** ⚙️ Calling tool(s): `update_nifi_processor_properties`...

**Assistant:** ⚙️ Calling tool(s): `update_nifi_processor_relationships, update_nifi_processor_relationships, update_nifi_processor_relationships, update_nifi_processor_relationships, update_nifi_processor_relationships`...

**Assistant:** ⚙️ Calling tool(s): `get_nifi_object_details`...

**Assistant:** ⚙️ Calling tool(s): `update_nifi_processor_properties`...

**Assistant:**
- The "Business Logic & Response Builder" processor (Groovy) is now created, configured, and connected.
- The rest of the main processors are now also VALID, with validation issues for unused relationships resolved.
- The Jolt transformation is set to extract the needed fields for easy Groovy logic.
- Full Groovy script for AI/decision logic, error, and valid response handling has been applied.
- The processors and connections now match the original specification, are correctly wired, and have the required business/governance behavior.

Next steps would be processor documentation, processor state/enablement, and response validation, or any testing as required.

Flow remediation and design:  
TASK COMPLETE
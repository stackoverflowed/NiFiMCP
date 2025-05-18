import pytest
import httpx
import os
import sys
import uuid
from loguru import logger
from typing import AsyncGenerator, Any
from config.settings import get_nifi_servers

# Enable anyio for async test support
pytest_plugins = ("anyio",)

# --- Base Configuration Fixtures ---

@pytest.fixture(scope="session")
def base_url() -> str:
    return os.environ.get("MCP_SERVER_URL", "http://localhost:8000")

@pytest.fixture(scope="session")
def target_nifi_server_id(global_logger: Any) -> str:
    server_id = os.environ.get("NIFI_TEST_SERVER_ID")
    if not server_id:
        global_logger.error("FATAL: NIFI_TEST_SERVER_ID environment variable is not set.")
        global_logger.error("Please set this variable to the ID of the target NiFi server from the MCP server's config.yaml.")
        pytest.exit("NIFI_TEST_SERVER_ID not set", returncode=1)
    return server_id

@pytest.fixture(scope="session")
def test_run_id() -> str:
    """Provides a unique ID for the current test session."""
    return str(uuid.uuid4())[:8]

# --- Logging Fixture ---

@pytest.fixture(scope="session")
def global_logger() -> Any:
    """Session-scoped logger for tests."""
    logger.remove()
    logger.add(sys.stderr, level=os.environ.get("MCP_TEST_LOG_LEVEL", "INFO").upper())
    return logger

# --- HTTP Client Fixture ---

@pytest.fixture(scope="module")
async def async_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    async with httpx.AsyncClient() as client:
        yield client

# --- Header Fixtures ---

@pytest.fixture(scope="session")
def base_headers() -> dict:
    """Basic headers for most API calls."""
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

@pytest.fixture(scope="function")
def mcp_headers(base_headers: dict, nifi_test_server_id: str) -> dict:
    """Headers required for MCP NiFi operations, including the target server ID."""
    return {
        **base_headers,
        "X-Nifi-Server-Id": nifi_test_server_id,
    }


@pytest.fixture(scope="module", autouse=True)
async def check_server_connectivity(base_url: str, nifi_test_server_id: str, async_client: httpx.AsyncClient, global_logger: Any):
    """Fixture to check MCP server connectivity and NiFi configuration at the start of the session."""
    global_logger.info(f"Checking server connectivity to {base_url} and NiFi config for {nifi_test_server_id}...")
    try:
        config_url = f"{base_url}/config/nifi-servers"
        config_resp = await async_client.get(config_url, timeout=10.0)
        config_resp.raise_for_status()
        servers = config_resp.json()
        server_ids = [s.get('id') for s in servers]
        if nifi_test_server_id not in server_ids:
            global_logger.error(f"FATAL: Target NiFi Server ID '{nifi_test_server_id}' not found in server config: {server_ids}")
            pytest.exit(f"Target NiFi Server ID '{nifi_test_server_id}' not found in server config.", returncode=1)
        global_logger.info(f"Server contacted successfully. Target NiFi ID '{nifi_test_server_id}' found in config.")
    except httpx.RequestError as e:
        global_logger.error(f"FATAL: Could not connect to MCP server at {base_url}: {e}")
        pytest.exit(f"Could not connect to MCP server at {base_url}", returncode=1)
    except httpx.HTTPStatusError as e:
        global_logger.error(f"FATAL: Error fetching config from {base_url}: {e.response.status_code} - {e.response.text}")
        pytest.exit(f"Error fetching config from {base_url}", returncode=1)
    except Exception as e:
        global_logger.error(f"FATAL: Unexpected error during server connectivity check: {e}")
        pytest.exit(f"Unexpected error during server connectivity check: {e}", returncode=1)

# Add more fixtures here as we refactor (e.g., for creating/cleaning up test PGs)

@pytest.fixture(scope="function")
async def test_pg(
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: dict,
    nifi_test_server_id: str,  # Changed from test_run_id
    global_logger: Any
):
    """Creates a test process group for a test function and ensures its deletion afterwards."""
    pg_name = f"mcp-test-pg-{nifi_test_server_id}"  # Use server ID instead of run ID
    global_logger.info(f"Fixture: Creating test process group: {pg_name}")
    create_pg_args = {"name": pg_name, "position_x": 0, "position_y": 0}
    
    from .utils.nifi_test_utils import call_tool as util_call_tool

    pg_result_list = await util_call_tool(
        client=async_client, 
        base_url=base_url, 
        tool_name="create_nifi_process_group", 
        arguments=create_pg_args, 
        headers=mcp_headers,
        custom_logger=global_logger
    )
    
    assert isinstance(pg_result_list, list) and len(pg_result_list) > 0 and isinstance(pg_result_list[0], dict), \
        "Unexpected response format for create_nifi_process_group in fixture"
    pg_result = pg_result_list[0]
    assert pg_result.get("status") == "success", \
        f"Fixture failed to create process group: {pg_result.get('message')}"
    pg_id = pg_result.get("entity", {}).get("id")
    assert pg_id, "Process group ID not found in fixture response."
    global_logger.info(f"Fixture: Successfully created Process Group ID: {pg_id}")

    # Get the process group details
    details_args = {"object_type": "process_group", "object_id": pg_id}
    details_result_list = await util_call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="get_nifi_object_details",
        arguments=details_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert isinstance(details_result_list, list) and details_result_list and isinstance(details_result_list[0], dict), \
        "Unexpected response format for get_nifi_object_details in fixture"
    details_result = details_result_list[0]
    assert details_result.get("status") != "error", \
        f"Failed to get details for process group {pg_id}: {details_result.get('message')}"
    
    pg_details = {
        "id": pg_id,
        "name": pg_name,
        "details": details_result.get("component", details_result)
    }

    yield pg_details # Provide the PG details to the test

    # Teardown: Delete the process group
    global_logger.info(f"Fixture: Cleaning up test process group: {pg_id}")
    try:
        delete_pg_args = {"object_type": "process_group", "object_id": pg_id, "kwargs": {}}
        delete_pg_result_list = await util_call_tool(
            client=async_client, 
            base_url=base_url,
            tool_name="delete_nifi_object", 
            arguments=delete_pg_args, 
            headers=mcp_headers,
            custom_logger=global_logger
        )
        assert isinstance(delete_pg_result_list, list) and len(delete_pg_result_list) > 0 and isinstance(delete_pg_result_list[0], dict), \
            "Unexpected response format for delete_nifi_object (process group) in fixture teardown"
        delete_pg_result = delete_pg_result_list[0]
        if delete_pg_result.get("status") == "success":
            global_logger.info(f"Fixture: Successfully deleted Test Process Group {pg_id}")
        else:
            global_logger.error(f"Fixture: Failed to delete Test Process Group {pg_id}: {delete_pg_result.get('message')}")
            # Optionally, raise an error here if cleanup failure should fail tests, 
            # but often for teardown, logging is preferred to not obscure the original test failure.
    except Exception as e_del_pg:
        global_logger.error(f"Fixture: Error during Test Process Group {pg_id} deletion: {e_del_pg}", exc_info=False)

@pytest.fixture(scope="function")
async def test_pg_with_processors(
    test_pg: dict, # Get the details from the test_pg fixture
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: dict,
    nifi_test_server_id: str, # Changed from test_run_id
    global_logger: Any
):
    """Creates a test PG and two standard processors (Generate & Log) within it."""
    pg_id = test_pg["id"]
    pg_name = test_pg["name"]
    global_logger.info(f"Fixture: test_pg_with_processors using PG ID: {pg_id}")

    from .utils.nifi_test_utils import call_tool as util_call_tool

    # 1. Create GenerateFlowFile Processor
    gen_proc_name = f"mcp-test-generate-{nifi_test_server_id}"  # Use server ID instead of run ID
    create_gen_args = {
        "process_group_id": pg_id,
        "processor_type": "org.apache.nifi.processors.standard.GenerateFlowFile",
        "name": gen_proc_name,
        "position_x": 0,
        "position_y": 0
    }
    global_logger.info(f"Fixture: Creating GenerateFlowFile processor: {gen_proc_name} in PG {pg_id}")
    gen_result_list = await util_call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="create_nifi_processor",
        arguments=create_gen_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert isinstance(gen_result_list, list) and len(gen_result_list) > 0 and isinstance(gen_result_list[0], dict), \
        "Unexpected response format for create_nifi_processor (Generate) in fixture"
    gen_result = gen_result_list[0]
    assert gen_result.get("status") in ["success", "warning"], \
        f"Fixture failed to create Generate processor: {gen_result.get('message')}"
    generate_proc_id = gen_result.get("entity", {}).get("id")
    assert generate_proc_id, "Generate processor ID not found in fixture response."
    global_logger.info(f"Fixture: Successfully created GenerateFlowFile Processor ID: {generate_proc_id}")

    # Get Generate processor details
    gen_details_args = {"object_type": "processor", "object_id": generate_proc_id}
    gen_details_list = await util_call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="get_nifi_object_details",
        arguments=gen_details_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert isinstance(gen_details_list, list) and gen_details_list and isinstance(gen_details_list[0], dict)
    gen_details = gen_details_list[0].get("component", gen_details_list[0])

    # 2. Create LogAttribute Processor
    log_proc_name = f"mcp-test-log-{nifi_test_server_id}"  # Use server ID instead of run ID
    create_log_args = {
        "process_group_id": pg_id,
        "processor_type": "org.apache.nifi.processors.standard.LogAttribute",
        "name": log_proc_name,
        "position_x": 0,
        "position_y": 200
    }
    global_logger.info(f"Fixture: Creating LogAttribute processor: {log_proc_name} in PG {pg_id}")
    log_result_list = await util_call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="create_nifi_processor",
        arguments=create_log_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert isinstance(log_result_list, list) and len(log_result_list) > 0 and isinstance(log_result_list[0], dict), \
        "Unexpected response format for create_nifi_processor (Log) in fixture"
    log_result = log_result_list[0]
    assert log_result.get("status") in ["success", "warning"], \
        f"Fixture failed to create Log processor: {log_result.get('message')}"
    log_proc_id = log_result.get("entity", {}).get("id")
    assert log_proc_id, "Log processor ID not found in fixture response."
    global_logger.info(f"Fixture: Successfully created LogAttribute Processor ID: {log_proc_id}")

    # Get Log processor details
    log_details_args = {"object_type": "processor", "object_id": log_proc_id}
    log_details_list = await util_call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="get_nifi_object_details",
        arguments=log_details_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert isinstance(log_details_list, list) and log_details_list and isinstance(log_details_list[0], dict)
    log_details = log_details_list[0].get("component", log_details_list[0])

    # Return the process group details and processors
    result = {
        "pg_details": test_pg["details"],
        "processors": [gen_details, log_details],
        "pg_id": pg_id,
        "generate_proc_id": generate_proc_id,
        "log_proc_id": log_proc_id
    }

    yield result

    # Teardown for processors is implicitly handled by the test_pg fixture deleting the parent PG.
    # If explicit processor deletion testing is needed, separate fixtures/tests would be better.
    global_logger.info(f"Fixture: test_pg_with_processors cleanup (delegated to test_pg for PG {pg_id})")

@pytest.fixture(scope="function")
async def test_connection(
    test_pg_with_processors: dict, # Get processor IDs from this fixture
    async_client: httpx.AsyncClient,
    base_url: str,
    mcp_headers: dict,
    global_logger: Any
):
    """Creates a connection between the Generate and Log processors from test_pg_with_processors."""
    generate_proc_id = test_pg_with_processors["generate_proc_id"]
    log_proc_id = test_pg_with_processors["log_proc_id"]

    global_logger.info(f"Fixture: Connecting {generate_proc_id} -> {log_proc_id}")
    from .utils.nifi_test_utils import call_tool as util_call_tool # Relative import

    connect_args = {
        "source_id": generate_proc_id,
        "relationships": ["success"], # Default relationship for GenerateFlowFile
        "target_id": log_proc_id
    }
    conn_result_list = await util_call_tool(
        client=async_client,
        base_url=base_url,
        tool_name="create_nifi_connection",
        arguments=connect_args,
        headers=mcp_headers,
        custom_logger=global_logger
    )
    assert isinstance(conn_result_list, list) and conn_result_list and isinstance(conn_result_list[0], dict), \
        "Unexpected response format for create_nifi_connection in fixture"
    conn_result = conn_result_list[0]
    assert conn_result.get("status") == "success", \
        f"Fixture failed to create connection: {conn_result.get('message')}"
    connection_id = conn_result.get("entity", {}).get("id")
    assert connection_id, "Connection ID not found in fixture response."
    global_logger.info(f"Fixture: Successfully created Connection ID: {connection_id}")

    yield connection_id

    # Teardown: Delete the connection
    global_logger.info(f"Fixture: Cleaning up test connection: {connection_id}")
    try:
        delete_conn_args = {"object_type": "connection", "object_id": connection_id, "kwargs": {}}
        delete_conn_result_list = await util_call_tool(
            client=async_client,
            base_url=base_url,
            tool_name="delete_nifi_object",
            arguments=delete_conn_args,
            headers=mcp_headers,
            custom_logger=global_logger
        )
        delete_conn_result = delete_conn_result_list[0]
        if delete_conn_result.get("status") == "success":
            global_logger.info(f"Fixture: Successfully deleted Connection {connection_id}")
        else:
            global_logger.warning(f"Fixture: Could not delete Connection {connection_id}: {delete_conn_result.get('message')}")
    except Exception as e_del_conn:
        global_logger.error(f"Fixture: Error during Connection {connection_id} deletion: {e_del_conn}", exc_info=False)

@pytest.fixture(scope="session")
def nifi_test_server_id():
    """Provides the NiFi server ID for testing from config."""
    servers = get_nifi_servers()
    if not servers:
        pytest.fail("No NiFi servers configured in config.yaml")
    # Use the first configured server for testing
    server_id = servers[0].get('id')
    if not server_id:
        pytest.fail("First NiFi server in config.yaml has no ID")
    return server_id 
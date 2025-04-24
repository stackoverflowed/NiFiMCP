from loguru import logger
# from dotenv import load_dotenv # Removed

from mcp.server import FastMCP
from nifi_mcp_server.nifi_client import NiFiClient, NiFiAuthenticationError

# --- Import Config Settings --- #
from config.settings import get_nifi_server_config, get_nifi_servers # Added

# Load .env file - REMOVED (Handled by config.settings)
# load_dotenv()

# Initialize FastMCP server
# Shared instance for the application
mcp = FastMCP(
    "nifi_controller",
    description="An MCP server to interact with Apache NiFi.",
    protocol_version="2024-09-01",  # Explicitly set protocol version
    type_validation_mode="compat",  # Use compatibility mode for type validation
)
logger.info("MCP instance initialized in core.")

# REMOVED single shared NiFiClient instantiation
# nifi_api_client = None 

# --- NiFi Client Factory --- #
# Simple cache for authenticated clients within a request scope? (Could use contextvars or pass around)
# For now, create per request/call.

async def get_nifi_client(server_id: str, bound_logger = logger) -> NiFiClient:
    """Gets or creates an authenticated NiFi client for the specified server ID."""
    bound_logger.info(f"Requesting NiFi client for server ID: {server_id}")
    server_conf = get_nifi_server_config(server_id)
    if not server_conf:
        bound_logger.error(f"Configuration for NiFi server ID '{server_id}' not found.")
        raise ValueError(f"NiFi server configuration not found for ID: {server_id}")

    client = NiFiClient(
        base_url=server_conf.get('url'),
        username=server_conf.get('username'),
        password=server_conf.get('password'),
        tls_verify=server_conf.get('tls_verify', True)
    )
    bound_logger.debug(f"Instantiated NiFiClient for {server_conf.get('url')}")

    try:
        # Ensure client is authenticated
        if not client.is_authenticated:
            bound_logger.info(f"Authenticating NiFi client for {server_conf.get('url')}")
            await client.authenticate()
            bound_logger.info(f"Authentication successful for {server_conf.get('url')}")
        else:
            bound_logger.debug(f"NiFi client for {server_conf.get('url')} is already authenticated (cached?)")
        return client
    except NiFiAuthenticationError as e:
        bound_logger.error(f"Authentication failed for NiFi server {server_id} ({server_conf.get('url')}): {e}")
        # Close the client if auth fails to release resources
        await client.close()
        raise # Re-raise the authentication error
    except Exception as e:
        bound_logger.error(f"Unexpected error getting/authenticating NiFi client for {server_id}: {e}", exc_info=True)
        await client.close()
        raise # Re-raise other exceptions


# Ensure at least one NiFi server is configured on startup (Optional check)
try:
    if not get_nifi_servers():
        logger.warning("No NiFi servers defined in config.yaml. NiFi tools will likely fail.")
    else:
        logger.info(f"Found {len(get_nifi_servers())} NiFi server configurations.")
except Exception as e:
    logger.error(f"Failed to read NiFi server configurations on startup: {e}") 
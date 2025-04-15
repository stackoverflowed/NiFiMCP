from loguru import logger
from dotenv import load_dotenv

from mcp.server import FastMCP
from nifi_mcp_server.nifi_client import NiFiClient

# Load .env file - this should be done early, before instantiation
load_dotenv()

# Initialize FastMCP server
# Shared instance for the application
mcp = FastMCP(
    "nifi_controller",
    description="An MCP server to interact with Apache NiFi.",
    protocol_version="2024-09-01",  # Explicitly set protocol version
    type_validation_mode="compat",  # Use compatibility mode for type validation
)
logger.info("MCP instance initialized in core.")

# Instantiate NiFi API client
# Shared instance for the application
try:
    nifi_api_client = NiFiClient()
    logger.info("NiFi API Client instantiated in core.")
except ValueError as e:
    logger.error(f"Failed to instantiate NiFiClient in core: {e}. Ensure NIFI_API_URL is set.")
    # Mark as unavailable if instantiation fails
    nifi_api_client = None 
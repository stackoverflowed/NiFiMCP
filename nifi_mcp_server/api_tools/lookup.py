import asyncio
from typing import List, Dict, Optional, Any, Union, Literal

# Import necessary components from parent/utils
from loguru import logger
# Import mcp ONLY
from ..core import mcp
# Removed nifi_api_client import
# Import context variables
from ..request_context import current_nifi_client, current_request_logger # Added

from .utils import (
    tool_phases,
    # ensure_authenticated # Removed
    # No other utils needed for this specific tool
)
from nifi_mcp_server.nifi_client import NiFiClient, NiFiAuthenticationError
from mcp.server.fastmcp.exceptions import ToolError


def _format_processor_type_summary(processor_type_data: Dict) -> Dict:
    """Formats the processor type data for the lookup tool response."""
    bundle = processor_type_data.get("bundle", {})
    return {
        "type": processor_type_data.get("type"),
        "bundle_group": bundle.get("group"),
        "bundle_artifact": bundle.get("artifact"),
        "bundle_version": bundle.get("version"),
        "description": processor_type_data.get("description"),
        "tags": processor_type_data.get("tags", []), # Ensure tags is a list
    }

@mcp.tool()
@tool_phases(["Build", "Modify"])
async def lookup_nifi_processor_type(
    processor_name: str,
    bundle_artifact_filter: str | None = None
) -> Union[List[Dict], Dict]:
    """
    Looks up available NiFi processor types by display name, returning key details including the full class name.

    Args:
        processor_name: The display name (e.g., 'GenerateFlowFile'). Case-insensitive.
        bundle_artifact_filter: Optional. Filters by bundle artifact (e.g., 'nifi-standard-nar'). Case-insensitive.

    Returns:
        - If one match: A dictionary with details.
        - If multiple matches: A list of matching dictionaries.
        - If no matches: An empty list.
    """
    # Get client and logger from context
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger
    if not nifi_client:
        raise ToolError("NiFi client context is not set. This tool requires the X-Nifi-Server-Id header.")
    if not local_logger:
         raise ToolError("Request logger context is not set.")
         
    # Authentication handled by factory
    local_logger = local_logger.bind(processor_name=processor_name, bundle_artifact_filter=bundle_artifact_filter)
    
    local_logger.info(f"Looking up processor type details for name: '{processor_name}'")
    try:
        nifi_req = {"operation": "get_processor_types"}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
        all_types = await nifi_client.get_processor_types()
        nifi_resp = {"processor_type_count": len(all_types)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp).debug("Received from NiFi API")

        matches = []
        search_name_lower = processor_name.lower()
        filter_artifact_lower = bundle_artifact_filter.lower() if bundle_artifact_filter else None

        for proc_type in all_types:
            # Extract relevant fields, ensuring they are strings and lowercased
            title = proc_type.get("title", "").lower()
            type_str = proc_type.get("type", "").lower()
            description = proc_type.get("description", "").lower()
            tags = [tag.lower() for tag in proc_type.get("tags", [])] # Lowercase all tags

            # Check if the search term is present in any relevant field
            name_match_found = (
                search_name_lower in title or
                search_name_lower in type_str or
                search_name_lower in description or
                any(search_name_lower in tag for tag in tags)
            )

            if name_match_found:
                # Apply bundle artifact filter if specified
                if filter_artifact_lower:
                    bundle = proc_type.get("bundle", {})
                    artifact = bundle.get("artifact", "").lower()
                    if artifact == filter_artifact_lower: # Check lowercase artifact
                        matches.append(_format_processor_type_summary(proc_type))
                else:
                    matches.append(_format_processor_type_summary(proc_type))

        local_logger.info(f"Found {len(matches)} match(es) for '{processor_name}'")
        
        if len(matches) == 1:
            return matches[0]
        else:
            return matches
        
    except (NiFiAuthenticationError, ConnectionError, ToolError) as e:
        local_logger.error(f"API/Tool error looking up processor types: {e}", exc_info=False)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"Failed to lookup NiFi processor types: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error looking up processor types: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
        raise ToolError(f"An unexpected error occurred: {e}")

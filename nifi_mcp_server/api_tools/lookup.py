import asyncio
from typing import List, Dict, Optional, Any, Union, Literal

# Import necessary components from parent/utils
from loguru import logger
# Import mcp and nifi_api_client from the new core module
from ..core import mcp, nifi_api_client
from .utils import (
    tool_phases,
    ensure_authenticated
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
        processor_name: The display name (often the 'title' in NiFi UI, not the full Java class) of the processor type to search for (e.g., 'GenerateFlowFile', 'RouteOnAttribute'). Case-insensitive search.
        bundle_artifact_filter: Optional. Filters results to only include types from a specific bundle artifact (e.g., 'nifi-standard-nar', 'nifi-update-attribute-nar'). Case-insensitive search.

    Returns:
        - If one match found: A dictionary with details ('type', 'bundle_*', 'description', 'tags').
        - If multiple matches found: A list of matching dictionaries.
        - If no matches found: An empty list.
    """
    local_logger = logger.bind(tool_name="lookup_nifi_processor_type", processor_name=processor_name, bundle_artifact_filter=bundle_artifact_filter)
    await ensure_authenticated(nifi_api_client, local_logger)
    
    local_logger.info(f"Looking up processor type details for name: '{processor_name}'")
    try:
        nifi_req = {"operation": "get_processor_types"}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
        all_types = await nifi_api_client.get_processor_types()
        nifi_resp = {"processor_type_count": len(all_types)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp).debug("Received from NiFi API")

        matches = []
        search_name_lower = processor_name.lower()
        filter_artifact_lower = bundle_artifact_filter.lower() if bundle_artifact_filter else None

        for proc_type in all_types:
            display_name = proc_type.get("title", proc_type.get("type", "").split('.')[-1]) # Fallback to class name part
            
            if display_name.lower() == search_name_lower:
                if filter_artifact_lower:
                    bundle = proc_type.get("bundle", {})
                    artifact = bundle.get("artifact", "")
                    if artifact.lower() == filter_artifact_lower:
                        matches.append(_format_processor_type_summary(proc_type))
                else:
                    matches.append(_format_processor_type_summary(proc_type))

        local_logger.info(f"Found {len(matches)} match(es) for processor name '{processor_name}' (Filter: {bundle_artifact_filter})")
        
        if len(matches) == 1:
            return matches[0]
        else:
            return matches
        
    except (NiFiAuthenticationError, ConnectionError) as e:
        local_logger.error(f"API error looking up processor types: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"Failed to lookup NiFi processor types: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error looking up processor types: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
        raise ToolError(f"An unexpected error occurred looking up processor types: {e}")

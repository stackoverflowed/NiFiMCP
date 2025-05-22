import asyncio
from typing import List, Dict, Optional, Any, Union, Literal
from loguru import logger as _logger # Use _logger to avoid potential conflict

# Import mcp from the new core module
from ..core import mcp # Removed nifi_api_client
# REMOVED from ..server import mcp, nifi_api_client

# Removed imports for NiFi types/exceptions previously needed by ensure_authenticated
# from nifi_mcp_server.nifi_client import NiFiClient, NiFiAuthenticationError
# from mcp.server.fastmcp.exceptions import ToolError

# --- Phase Tagging Decorator --- 
PHASE_TAGS_ATTR = "_tool_phases"
_tool_phase_registry = {} # Module-level registry

def tool_phases(phases: List[str]):
    """Decorator to tag MCP tools with applicable operational phases."""
    def decorator(func):
        # Attach attribute (might still be useful)
        setattr(func, PHASE_TAGS_ATTR, phases)
        # Add to registry
        _tool_phase_registry[func.__name__] = phases
        _logger.trace(f"Registered phases {phases} for tool {func.__name__}")
        return func
    return decorator
# -----------------------------

# Removed ensure_authenticated function
# --- Helper Function for Authentication --- 
# 
# async def ensure_authenticated(nifi_api_client: NiFiClient, logger): # Accept client and logger
#     """Helper to ensure the NiFi client is authenticated before tool use."""
#     if nifi_api_client is None:
#         raise ToolError("NiFi Client is not configured properly (check NIFI_API_URL).")
#     if not nifi_api_client.is_authenticated:
#         logger.info("NiFi client not authenticated. Attempting authentication...")
#         try:
#             await nifi_api_client.authenticate()
#             logger.info("Authentication successful via MCP tool request.")
#         except NiFiAuthenticationError as e:
#             logger.error(f"Authentication failed during tool execution: {e}")
#             # Raise ToolError, but indicate user action needed in the message
#             raise ToolError(
#                 f"NiFi authentication failed ({e}). Please ensure NIFI_USERNAME and NIFI_PASSWORD "
#                 "are correctly set in the server's environment/.env file."
#             ) from e
#         except Exception as e:
#             logger.error(f"Unexpected error during authentication: {e}", exc_info=True)
#             raise ToolError(f"An unexpected error occurred during NiFi authentication: {e}")
#     pass # Add pass to avoid syntax error if body is empty

# --- Formatting/Filtering Helpers --- 

def _format_processor_summary(processors_data):
    """Formats basic processor data from NiFi API list response."""
    formatted = []
    if processors_data: # Already a list from list_processors
        for proc in processors_data:
            # Use the existing filter function for consistency, can enhance later
            basic_info = filter_processor_data(proc) 
            formatted.append(basic_info)
    return formatted

def _format_connection_summary(connections_data):
    """Formats basic connection data from NiFi API list response."""
    formatted = []
    if connections_data: # Already a list from list_connections
        for conn in connections_data:
             # Use existing filter function
            basic_info = filter_connection_data(conn)
            formatted.append(basic_info)
    return formatted

def _format_port_summary(input_ports_data, output_ports_data):
    """Formats and combines input and output port data for summary list."""
    formatted = []
    # Process Input Ports
    if input_ports_data: # Already a list
        for port in input_ports_data:
            component = port.get('component', {})
            status = port.get('status', {})
            formatted.append({
                "id": port.get('id'),
                "name": component.get('name'),
                "type": "INPUT_PORT",
                "state": component.get('state'),
                "comments": component.get('comments'),
                "concurrent_tasks": component.get('concurrentlySchedulableTaskCount'),
                "validation_errors": component.get('validationErrors'),
                "active_thread_count": status.get('aggregateSnapshot', {}).get('activeThreadCount'), # Check nesting
                "queued_count": status.get('aggregateSnapshot', {}).get('flowFilesQueued'),
                "queued_size": status.get('aggregateSnapshot', {}).get('bytesQueued'),
            })
    # Process Output Ports
    if output_ports_data: # Already a list
        for port in output_ports_data:
            component = port.get('component', {})
            status = port.get('status', {})
            formatted.append({
                "id": port.get('id'),
                "name": component.get('name'),
                "type": "OUTPUT_PORT",
                "state": component.get('state'),
                "comments": component.get('comments'),
                "concurrent_tasks": component.get('concurrentlySchedulableTaskCount'),
                "validation_errors": component.get('validationErrors'),
                "active_thread_count": status.get('aggregateSnapshot', {}).get('activeThreadCount'), # Check nesting
                "queued_count": status.get('aggregateSnapshot', {}).get('flowFilesQueued'),
                "queued_size": status.get('aggregateSnapshot', {}).get('bytesQueued'),
            })
    return formatted

def filter_processor_data(processor):
    """Extract only the essential fields from a processor object"""
    component = processor.get("component", {}) # Added safety get
    status = processor.get("status", {}) # Added safety get
    config = component.get("config", {}) # Get config dict
    properties = config.get("properties", {}) # Get properties from config

    return {
        "id": processor.get("id"),
        "name": component.get("name"),
        "type": component.get("type"),
        "state": component.get("state"),
        "position": processor.get("position"),
        "runStatus": status.get("runStatus"), # Use the safer status get
        "validationStatus": component.get("validationStatus"),
        "validationErrors": component.get("validationErrors", []),
        "relationships": component.get("relationships", []), # Use safer component get
        "properties": properties, # Add properties here

    }

def filter_created_processor_data(processor_entity):
    """Extract only the essential fields from a newly created processor entity"""
    component = processor_entity.get("component", {})
    revision = processor_entity.get("revision", {})
    return {
        "id": processor_entity.get("id"),
        "name": component.get("name"),
        "type": component.get("type"),
        "position": processor_entity.get("position"), # Position is top-level in creation response
        "validationStatus": component.get("validationStatus"),
        "validationErrors": component.get("validationErrors"),
        "version": revision.get("version"), # Get version from revision dict
    }

def filter_connection_data(connection_entity):
    """Extract only the essential identification fields from a connection entity."""
    # Access nested component data safely
    component = connection_entity.get("component", {})
    source = component.get("source", {})
    destination = component.get("destination", {})

    return {
        "id": connection_entity.get("id"),
        "uri": connection_entity.get("uri"),
        "sourceId": source.get("id"),
        "sourceGroupId": source.get("groupId"),
        "sourceType": source.get("type"),
        "sourceName": source.get("name"),
        "destinationId": destination.get("id"),
        "destinationGroupId": destination.get("groupId"),
        "destinationType": destination.get("type"),
        "destinationName": destination.get("name"),
        "name": component.get("name"), # Get connection name safely
        "selectedRelationships": component.get("selectedRelationships"),
        "availableRelationships": component.get("availableRelationships"),
    }

def filter_port_data(port_entity):
    """Extract essential fields from a port entity (input or output)."""
    component = port_entity.get("component", {})
    revision = port_entity.get("revision", {})
    return {
        "id": port_entity.get("id"),
        "name": component.get("name"),
        "type": component.get("type"), # Will be INPUT_PORT or OUTPUT_PORT
        "state": component.get("state"),
        "position": port_entity.get("position"),
        "comments": component.get("comments"),
        "allowRemoteAccess": component.get("allowRemoteAccess"),
        "concurrentlySchedulableTaskCount": component.get("concurrentlySchedulableTaskCount"),
        "validationStatus": component.get("validationStatus"),
        "validationErrors": component.get("validationErrors"),
        "version": revision.get("version"),
    }

def filter_process_group_data(pg_entity):
    """Extract essential fields from a process group entity."""
    component = pg_entity.get("component", {})
    revision = pg_entity.get("revision", {})
    status = pg_entity.get("status", {}).get("aggregateSnapshot", {}) # Status is nested
    return {
        "id": pg_entity.get("id"),
        "name": component.get("name"),
        "position": pg_entity.get("position"),
        "comments": component.get("comments"),
        "parameterContext": component.get("parameterContext", {}).get("id"), # Just the ID
        "flowfileConcurrency": component.get("flowfileConcurrency"),
        "flowfileOutboundPolicy": component.get("flowfileOutboundPolicy"),
        # Basic status counts if available from creation response
        "runningCount": status.get("runningCount"), 
        "stoppedCount": status.get("stoppedCount"),
        "invalidCount": status.get("invalidCount"),
        "disabledCount": status.get("disabledCount"),
        "activeRemotePortCount": status.get("activeRemotePortCount"),
        "inactiveRemotePortCount": status.get("inactiveRemotePortCount"),
        "version": revision.get("version"),
    }

def filter_drop_request_data(drop_request: Dict[str, Any]) -> Dict[str, Any]:
    """Filters and formats drop request data for API responses."""
    if not drop_request:
        return {}
    
    return {
        "id": drop_request.get("id"),
        "uri": drop_request.get("uri"),
        "lastUpdated": drop_request.get("lastUpdated"),
        "finished": drop_request.get("finished", False),
        "failureReason": drop_request.get("failureReason"),
        "percentCompleted": drop_request.get("percentCompleted", 0),
        "currentCount": drop_request.get("currentCount", 0),
        "currentSize": drop_request.get("currentSize", 0),
        "current": drop_request.get("current", 0),
        "originalCount": drop_request.get("originalCount", 0),
        "originalSize": drop_request.get("originalSize", 0),
        "dropped": drop_request.get("dropped", 0),
        "state": drop_request.get("state", "UNKNOWN"),
        "queueSize": {
            "byteCount": drop_request.get("queueSize", {}).get("byteCount", 0),
            "objectCount": drop_request.get("queueSize", {}).get("objectCount", 0)
        }
    }

def format_drop_request_summary(drop_results: Dict[str, Any]) -> Dict[str, Any]:
    """Formats the summary of drop request results for API responses."""
    if not drop_results:
        return {
            "success": False,
            "message": "No drop request results provided",
            "summary": {}
        }

    total_connections = len(drop_results.get("results", []))
    successful_drops = sum(1 for r in drop_results.get("results", []) if r.get("success", False))
    total_dropped = sum(r.get("dropped_count", 0) for r in drop_results.get("results", []) if r.get("success", False))
    
    failed_connections = []
    for result in drop_results.get("results", []):
        if not result.get("success", False):
            failed_connections.append({
                "connection_id": result.get("connection_id"),
                "error": result.get("error", "Unknown error")
            })

    return {
        "success": drop_results.get("success", False),
        "message": drop_results.get("message", ""),
        "summary": {
            "total_connections": total_connections,
            "successful_drops": successful_drops,
            "failed_drops": total_connections - successful_drops,
            "total_flowfiles_dropped": total_dropped,
            "failed_connections": failed_connections if failed_connections else None
        }
    }

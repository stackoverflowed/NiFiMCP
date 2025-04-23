import re
# Remove standard logging import
# import logging
from loguru import logger # Import Loguru logger
from typing import Dict, List, Any, Set, Tuple, Optional

# Set up logging - REMOVED
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# Define important properties for common processor types - REMOVING THIS
# IMPORTANT_PROPERTIES = {
#     "org.apache.nifi.processors.standard.GetFile": [
#         "Directory", "File Filter", "Batch Size", "Recurse Subdirectories"
#     ],
#     "org.apache.nifi.processors.standard.PutFile": [
#         "Directory", "Conflict Resolution Strategy"
#     ],
#     "org.apache.nifi.processors.standard.RouteOnAttribute": [
#         "Routing Strategy"  # Plus dynamic properties defining routes
#     ],
#     "org.apache.nifi.processors.standard.ReplaceText": [
#         "Replacement Strategy", "Regular Expression", "Replacement Value",
#         "Character Set", "Evaluation Mode", "Line-by-Line Evaluation Mode"
#     ],
#     "org.apache.nifi.processors.standard.UpdateAttribute": [
#         # Dynamic properties are the key part here
#     ],
#     "org.apache.nifi.processors.standard.EvaluateJsonPath": [
#         "Destination", "Return Type"  # Plus dynamic properties defining json paths
#     ],
#     "org.apache.nifi.processors.standard.SplitJson": [
#         "JsonPath Expression"
#     ],
#     "org.apache.nifi.processors.standard.MergeContent": [
#         "Merge Strategy", "Delimiter Strategy", "Defragment Correlation Attribute"
#     ],
#     "org.apache.nifi.processors.standard.InvokeHTTP": [
#         "HTTP Method", "Remote URL", "SSL Context Service"
#     ],
#     # Default for any processor type
#     "default": ["Schedule Period", "Scheduling Strategy", "Run Duration"]
# }

def extract_important_properties(processor: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts all configuration properties and analyzes expressions."""
    # Get all properties directly
    all_properties = processor["component"].get("config", {}).get("properties", {})
    # Use logger instead of print
    logger.debug(f"Extracted all properties for processor {processor.get('id', 'unknown')}")

    # Analyze expressions within these properties
    expressions = analyze_expressions(all_properties)

    return {
        "all_properties": all_properties,
        "expressions": expressions
    }

def analyze_expressions(properties: Dict[str, str]) -> Dict[str, List[str]]:
    """Identify and analyze Expression Language in properties."""
    expression_pattern = r"\$\{([^}]+)\}"
    expressions = {}
    
    for prop_name, prop_value in properties.items():
        if isinstance(prop_value, str):
            matches = re.findall(expression_pattern, prop_value)
            if matches:
                expressions[prop_name] = matches
    
    return expressions

def build_graph_structure(processors: List[Dict[str, Any]], connections: List[Dict[str, Any]]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """Build graph adjacency lists for the flow."""
    # Create adjacency lists
    outgoing_connections = {}  # processor_id -> [connection_ids]
    incoming_connections = {}  # processor_id -> [connection_ids]
    
    for conn in connections:
        src_id = conn["sourceId"] if "sourceId" in conn else conn["source"]["id"]
        dest_id = conn["destinationId"] if "destinationId" in conn else conn["destination"]["id"]
        
        if src_id not in outgoing_connections:
            outgoing_connections[src_id] = []
        outgoing_connections[src_id].append(conn)
        
        if dest_id not in incoming_connections:
            incoming_connections[dest_id] = []
        incoming_connections[dest_id].append(conn)
    
    return {
        "outgoing": outgoing_connections,
        "incoming": incoming_connections
    }

def format_connection(conn: Dict[str, Any], processor_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Format connection details for documentation."""
    source_id = conn["sourceId"] if "sourceId" in conn else conn["source"]["id"]
    dest_id = conn["destinationId"] if "destinationId" in conn else conn["destination"]["id"]
    
    relationship = "unknown"
    if "selectedRelationships" in conn:
        relationships = conn["selectedRelationships"]
        relationship = relationships[0] if relationships else "unknown"
    elif "component" in conn and "selectedRelationships" in conn["component"]:
        relationships = conn["component"]["selectedRelationships"]
        relationship = relationships[0] if relationships else "unknown"
    
    source_name = "unknown"
    if source_id in processor_map:
        source_name = processor_map[source_id]["component"]["name"]
        
    dest_name = "unknown"
    if dest_id in processor_map:
        dest_name = processor_map[dest_id]["component"]["name"]
    
    return {
        "id": conn["id"],
        "source_id": source_id,
        "source_name": source_name,
        "destination_id": dest_id,
        "destination_name": dest_name,
        "relationship": relationship
    }

# Removing pathfinding functions as they are no longer used by document_nifi_flow
# def find_all_paths(...): ... 
# def find_source_to_sink_paths(...): ...

def find_decision_branches(processor_map: Dict[str, Dict[str, Any]], graph: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> List[Dict[str, Any]]:
    """Identify decision points where flow branches based on relationships."""
    outgoing_connections = graph["outgoing"]
    branches = []
    
    # Find processors with multiple outgoing relationships
    for proc_id, connections in outgoing_connections.items():
        # Group connections by relationship
        rel_groups = {}
        for conn in connections:
            rel = "unknown"
            if "selectedRelationships" in conn:
                relationships = conn["selectedRelationships"]
                rel = relationships[0] if relationships else "unknown"
            elif "component" in conn and "selectedRelationships" in conn["component"]:
                relationships = conn["component"]["selectedRelationships"]
                rel = relationships[0] if relationships else "unknown"
                
            if rel not in rel_groups:
                rel_groups[rel] = []
            rel_groups[rel].append(conn)
        
        # If multiple relationships, it's a decision point
        if len(rel_groups) > 1:
            branch_info = {
                "decision_point": proc_id,
                "processor_name": processor_map[proc_id]["component"]["name"],
                "branches": []
            }
            
            # For each relationship, follow a short path
            for rel, conns in rel_groups.items():
                for conn in conns:
                    dest_id = conn["destinationId"] if "destinationId" in conn else conn["destination"]["id"]
                    if dest_id in processor_map:
                        branch_info["branches"].append({
                            "relationship": rel,
                            "destination_id": dest_id,
                            "destination_name": processor_map[dest_id]["component"]["name"]
                        })
            
            branches.append(branch_info)
    
    return branches 
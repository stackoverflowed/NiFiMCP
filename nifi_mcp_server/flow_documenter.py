import re
import logging
from typing import Dict, List, Any, Set, Tuple, Optional

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define important properties for common processor types
IMPORTANT_PROPERTIES = {
    "org.apache.nifi.processors.standard.GetFile": [
        "Directory", "File Filter", "Batch Size", "Recurse Subdirectories"
    ],
    "org.apache.nifi.processors.standard.PutFile": [
        "Directory", "Conflict Resolution Strategy"
    ],
    "org.apache.nifi.processors.standard.RouteOnAttribute": [
        "Routing Strategy"  # Plus dynamic properties defining routes
    ],
    "org.apache.nifi.processors.standard.UpdateAttribute": [
        # Dynamic properties are the key part here
    ],
    "org.apache.nifi.processors.standard.EvaluateJsonPath": [
        "Destination", "Return Type"  # Plus dynamic properties defining json paths
    ],
    "org.apache.nifi.processors.standard.SplitJson": [
        "JsonPath Expression"
    ],
    "org.apache.nifi.processors.standard.MergeContent": [
        "Merge Strategy", "Delimiter Strategy", "Defragment Correlation Attribute"
    ],
    "org.apache.nifi.processors.standard.InvokeHTTP": [
        "HTTP Method", "Remote URL", "SSL Context Service"
    ],
    # Default for any processor type
    "default": ["Schedule Period", "Scheduling Strategy", "Run Duration"]
}

def extract_important_properties(processor: Dict[str, Any]) -> Dict[str, Any]:
    """Extract the most important properties for documentation purposes."""
    proc_type = processor["component"]["type"]
    properties = processor["component"]["config"]["properties"]
    
    # Get properties to highlight based on processor type
    key_property_names = IMPORTANT_PROPERTIES.get(
        proc_type, IMPORTANT_PROPERTIES["default"]
    )
    
    # Extract values for those properties
    key_properties = {}
    for prop_name in key_property_names:
        if prop_name in properties:
            key_properties[prop_name] = properties[prop_name]
    
    # Look for dynamic properties (often used for expressions)
    dynamic_props = {}
    if "descriptors" in processor["component"]["config"]:
        for prop_name, prop_value in properties.items():
            if prop_name not in processor["component"]["config"]["descriptors"]:
                dynamic_props[prop_name] = prop_value
    
    return {
        "key_properties": key_properties,
        "dynamic_properties": dynamic_props,
        "all_properties": properties  # Include everything if needed
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

def find_all_paths(
    source_id: str, 
    sinks: List[str], 
    outgoing_connections: Dict[str, List[Dict[str, Any]]], 
    max_depth: int = 10, 
    visited: Optional[Set[str]] = None, 
    current_path: Optional[List[str]] = None, 
    current_relationships: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """Find all paths from a source processor to any sink processor."""
    if visited is None:
        visited = set()
    if current_path is None:
        current_path = [source_id]
    if current_relationships is None:
        current_relationships = []
    
    # Base case: reached max depth
    if len(current_path) > max_depth:
        return []
    
    # Base case: current node is a sink
    paths = []
    if source_id in sinks:
        paths.append({
            "processors": current_path.copy(),
            "relationships": current_relationships.copy()
        })
    
    # Mark current node as visited
    visited.add(source_id)
    
    # Explore outgoing connections
    if source_id in outgoing_connections:
        for conn in outgoing_connections[source_id]:
            dest_id = conn["destinationId"] if "destinationId" in conn else conn["destination"]["id"]
            
            # Skip if already visited (avoid cycles)
            if dest_id in visited:
                continue
            
            # Get relationship
            relationship = "unknown"
            if "selectedRelationships" in conn:
                relationships = conn["selectedRelationships"]
                relationship = relationships[0] if relationships else "unknown"
            elif "component" in conn and "selectedRelationships" in conn["component"]:
                relationships = conn["component"]["selectedRelationships"]
                relationship = relationships[0] if relationships else "unknown"
            
            # Extend path and relationships
            current_path.append(dest_id)
            current_relationships.append(relationship)
            
            # Recursively find paths from destination
            sub_paths = find_all_paths(
                dest_id, 
                sinks, 
                outgoing_connections, 
                max_depth, 
                visited.copy(),
                current_path.copy(), 
                current_relationships.copy()
            )
            paths.extend(sub_paths)
            
            # Backtrack
            current_path.pop()
            current_relationships.pop()
    
    return paths

def find_source_to_sink_paths(processor_map: Dict[str, Dict[str, Any]], graph: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> List[Dict[str, Any]]:
    """Find common data flow paths from source to sink processors."""
    outgoing_connections = graph["outgoing"]
    incoming_connections = graph["incoming"]
    
    # Find source processors (no incoming connections)
    sources = [p_id for p_id in processor_map.keys() if p_id not in incoming_connections]
    
    # Find sink processors (no outgoing connections)
    sinks = [p_id for p_id in processor_map.keys() if p_id not in outgoing_connections]
    
    # For each source, find all paths to sinks
    all_paths = []
    
    for source in sources:
        paths = find_all_paths(source, sinks, outgoing_connections)
        all_paths.extend(paths)
    
    # Format paths with processor names and relationships
    formatted_paths = []
    for path in all_paths:
        processor_ids = path['processors']
        relationships = path['relationships']
        
        formatted_paths.append({
            "path_ids": processor_ids,
            "path_names": [processor_map[p_id]["component"]["name"] for p_id in processor_ids],
            "path_types": [processor_map[p_id]["component"]["type"].split(".")[-1] for p_id in processor_ids],
            "relationships": relationships
        })
    
    return formatted_paths

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
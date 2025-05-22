from typing import Dict, List, Any, Optional, Set
from loguru import logger

def extract_important_properties(processor_entity: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and analyze important properties from a processor entity."""
    component = processor_entity.get("component", {})
    config = component.get("config", {})
    properties = config.get("properties", {})
    
    # Analyze properties for expressions
    expressions = {}
    for prop_name, prop_value in properties.items():
        if prop_value and "${" in str(prop_value):
            # Extract expressions like ${filename} from the value
            import re
            expr_matches = re.findall(r'\$\{([^}]+)\}', str(prop_value))
            if expr_matches:
                expressions[prop_name] = expr_matches
    
    return {
        "all_properties": properties,
        "expressions": expressions
    }

def format_connection(connection_entity: Dict[str, Any], all_components: Dict[str, Any]) -> Dict[str, Any]:
    """Format a connection entity into a simplified structure."""
    component = connection_entity.get("component", {})
    source = component.get("source", {})
    destination = component.get("destination", {})
    
    # Get names from the components map if available
    source_id = source.get("id")
    dest_id = destination.get("id")
    source_name = source.get("name", "Unknown")
    dest_name = destination.get("name", "Unknown")
    
    if source_id in all_components:
        source_comp = all_components[source_id].get("component", {})
        source_name = source_comp.get("name", source_name)
    if dest_id in all_components:
        dest_comp = all_components[dest_id].get("component", {})
        dest_name = dest_comp.get("name", dest_name)
    
    return {
        "id": connection_entity.get("id"),
        "name": component.get("name", ""),
        "source_id": source_id,
        "source_name": source_name,
        "source_type": source.get("type", "UNKNOWN"),
        "destination_id": dest_id,
        "destination_name": dest_name,
        "destination_type": destination.get("type", "UNKNOWN"),
        "relationship": component.get("selectedRelationships", [""])[0],
        "prioritizers": component.get("prioritizers", []),
        "backpressure": {
            "object_threshold": component.get("backPressureObjectThreshold", ""),
            "data_size_threshold": component.get("backPressureDataSizeThreshold", "")
        }
    }

def build_graph_structure(
    processors: List[Dict[str, Any]],
    connections: List[Dict[str, Any]],
    input_ports: List[Dict[str, Any]] = None,
    output_ports: List[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Build a graph representation of the flow."""
    outgoing_graph = {}  # id -> List[connection_details]
    incoming_graph = {}  # id -> List[connection_details]
    
    # Build both graphs
    for conn in connections:
        conn_comp = conn.get("component", {})
        source = conn_comp.get("source", {})
        dest = conn_comp.get("destination", {})
        source_id = source.get("id")
        dest_id = dest.get("id")
        
        # Add connection to outgoing graph
        if source_id:
            if source_id not in outgoing_graph:
                outgoing_graph[source_id] = []
            outgoing_graph[source_id].append(conn)
            
        # Add connection to incoming graph
        if dest_id:
            if dest_id not in incoming_graph:
                incoming_graph[dest_id] = []
            incoming_graph[dest_id].append(conn)
    
    return {
        "outgoing": outgoing_graph,
        "incoming": incoming_graph
    }

def find_decision_branches(all_components: Dict[str, Any], graph_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Identify processors that represent decision points and their branches."""
    decision_points = []
    outgoing_graph = graph_data.get("outgoing", {})
    
    for comp_id, outgoing_conns in outgoing_graph.items():
        # Skip if not enough outgoing connections
        if len(outgoing_conns) <= 1:
            continue
            
        # Get component details
        comp_entity = all_components.get(comp_id, {})
        comp = comp_entity.get("component", {})
        
        # Collect unique relationships
        relationships = set()
        for conn in outgoing_conns:
            conn_comp = conn.get("component", {})
            rel = conn_comp.get("selectedRelationships", [""])[0]
            relationships.add(rel)
        
        # If multiple relationships, this is a decision point
        if len(relationships) > 1:
            branches = []
            for conn in outgoing_conns:
                conn_comp = conn.get("component", {})
                dest = conn_comp.get("destination", {})
                rel = conn_comp.get("selectedRelationships", [""])[0]
                
                branches.append({
                    "relationship": rel,
                    "destination_id": dest.get("id"),
                    "destination_name": dest.get("name", "Unknown"),
                    "destination_type": dest.get("type", "UNKNOWN")
                })
            
            decision_points.append({
                "processor_id": comp_id,
                "processor_name": comp.get("name", "Unknown"),
                "processor_type": comp.get("type", "UNKNOWN"),
                "branches": branches
            })
    
    return decision_points

def identify_flow_paths(
    components: Dict[str, Dict[str, Any]],
    graph_data: Dict[str, Any],
    source_components: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Identify and document flow paths starting from source components."""
    flows = []
    outgoing_graph = graph_data.get("outgoing", {})
    
    def traverse_path(current_id: str, current_path: List[Dict[str, Any]], visited_set: Set[str], source_info: Dict[str, Any]):
        """Recursively traverse a flow path."""
        if current_id in visited_set:
            # End this path if we've seen this component in this path
            flows.append({
                "source": source_info["name"],
                "source_type": source_info["type"],
                "path": current_path.copy()
            })
            return
            
        visited_set.add(current_id)
        current_path.append({
            "id": current_id,
            "type": components[current_id]["type"],
            "name": components[current_id]["name"]
        })
        
        # Get outgoing connections
        outgoing_conns = outgoing_graph.get(current_id, [])
        if not outgoing_conns:
            # End of path, add it to flows
            flows.append({
                "source": source_info["name"],
                "source_type": source_info["type"],
                "path": current_path.copy()
            })
            return
            
        # Group connections by relationship
        rel_groups = {}
        for conn in outgoing_conns:
            conn_comp = conn.get("component", {})
            rel = conn_comp.get("selectedRelationships", [""])[0]
            if rel not in rel_groups:
                rel_groups[rel] = []
            rel_groups[rel].append(conn)
        
        # For each relationship group, create a new path
        for rel, conns in rel_groups.items():
            for conn in conns:
                conn_comp = conn.get("component", {})
                dest = conn_comp.get("destination", {})
                dest_id = dest.get("id")
                
                if dest_id and dest_id in components:
                    # Create a new path for this branch with its own visited set
                    new_path = current_path.copy()
                    new_visited = visited_set.copy()
                    traverse_path(dest_id, new_path, new_visited, source_info)
    
    # Start from each source component with a fresh visited set
    for source in source_components:
        source_id = source["id"]
        if source_id in components:
            source_info = {
                "name": source["name"],
                "type": source["type"]
            }
            traverse_path(source_id, [], set(), source_info)
    
    return flows

def document_nifi_flow_improved(
    processors: List[Dict[str, Any]],
    connections: List[Dict[str, Any]],
    input_ports: List[Dict[str, Any]],
    output_ports: List[Dict[str, Any]],
    include_properties: bool = True,
    include_descriptions: bool = True
) -> Dict[str, Any]:
    """
    Document a NiFi flow with the improved structure.
    
    Args:
        processors: List of processor entities
        connections: List of connection entities
        input_ports: List of input port entities
        output_ports: List of output port entities
        include_properties: Whether to include processor properties
        include_descriptions: Whether to include descriptions
        
    Returns:
        Dict with the following structure:
        {
            "components": {
                "processors": {id: {details}},
                "connections": {id: {details}},
                "ports": {id: {details}}
            },
            "flows": [
                {
                    "source": "name",
                    "source_type": "type",
                    "path": [{component details}]
                }
            ],
            "decision_points": [
                {
                    "processor_id": "id",
                    "processor_name": "name",
                    "processor_type": "type",
                    "branches": [{branch details}]
                }
            ],
            "unconnected_components": [
                {
                    "id": "id",
                    "name": "name",
                    "type": "type"
                }
            ]
        }
    """
    # Initialize result structure
    result = {
        "components": {
            "processors": {},
            "connections": {},
            "ports": {}
        },
        "flows": [],
        "decision_points": [],
        "unconnected_components": []
    }
    
    # Build lookup of all components
    all_components = {}  # id -> entity
    components_by_type = {}  # id -> simplified component info
    
    # Process processors
    for proc in processors:
        proc_id = proc.get("id")
        if proc_id:
            all_components[proc_id] = proc
            
            # Extract basic info
            component = proc.get("component", {})
            proc_info = {
                "id": proc_id,
                "name": component.get("name", "Unknown"),
                "type": "PROCESSOR",
                "processor_type": component.get("type", "Unknown"),
                "is_source": False  # Will be updated later
            }
            
            # Add properties if requested
            if include_properties:
                prop_analysis = extract_important_properties(proc)
                proc_info["properties"] = prop_analysis["all_properties"]
                proc_info["expressions"] = prop_analysis["expressions"]
            
            # Add description if requested
            if include_descriptions:
                proc_info["description"] = component.get("comments", "")
            
            components_by_type[proc_id] = proc_info
            result["components"]["processors"][proc_id] = proc_info
    
    # Process ports
    for port in input_ports + output_ports:
        port_id = port.get("id")
        if port_id:
            all_components[port_id] = port
            
            component = port.get("component", {})
            port_info = {
                "id": port_id,
                "name": component.get("name", "Unknown"),
                "type": "PORT",
                "port_type": "INPUT_PORT" if port in input_ports else "OUTPUT_PORT",
                "is_source": port in input_ports  # Input ports are sources
            }
            
            if include_descriptions:
                port_info["description"] = component.get("comments", "")
            
            components_by_type[port_id] = port_info
            result["components"]["ports"][port_id] = port_info
    
    # Build graph structure
    graph_data = build_graph_structure(processors, connections, input_ports, output_ports)
    outgoing_graph = graph_data.get("outgoing", {})
    incoming_graph = graph_data.get("incoming", {})
    
    # Process connections
    for conn in connections:
        conn_id = conn.get("id")
        if conn_id:
            formatted_conn = format_connection(conn, all_components)
            result["components"]["connections"][conn_id] = formatted_conn
    
    # Identify source components (no incoming connections or input ports)
    source_components = []
    for comp_id, comp_info in components_by_type.items():
        if comp_info["type"] == "PORT" and comp_info["port_type"] == "INPUT_PORT":
            source_components.append(comp_info)
        elif comp_id not in incoming_graph:
            comp_info["is_source"] = True
            source_components.append(comp_info)
    
    # Find decision points
    result["decision_points"] = find_decision_branches(all_components, graph_data)
    
    # Document flow paths
    result["flows"] = identify_flow_paths(components_by_type, graph_data, source_components)
    
    # Identify unconnected components
    connected_ids = set()
    for comp_id in outgoing_graph.keys():
        connected_ids.add(comp_id)
    for comp_id in incoming_graph.keys():
        connected_ids.add(comp_id)
        
    for comp_id, comp_info in components_by_type.items():
        if comp_id not in connected_ids:
            result["unconnected_components"].append({
                "id": comp_id,
                "name": comp_info["name"],
                "type": comp_info["type"]
            })
    
    return result 
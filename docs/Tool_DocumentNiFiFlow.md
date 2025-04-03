# `document_nifi_flow` Tool Implementation Plan

## Overview

The `document_nifi_flow` tool will traverse a NiFi flow starting from a specified processor, following connections between processors, and generating a comprehensive documentation of the flow logic in both a structured format and natural language descriptions. This will allow LLMs to better understand and explain the flow's purpose and logic.

## Objectives

1. Traverse a NiFi flow graph by following connections between processors
2. Collect relevant details about each processor, including configurations and relationships
3. Document the flow of data through the system
4. Generate a representation that's optimized for LLM understanding
5. Support both targeted (starting from a specific processor) and comprehensive (entire process group) documentation

## API Design

```python
@mcp.tool()
async def document_nifi_flow(
    process_group_id: str | None = None,
    starting_processor_id: str | None = None,
    max_depth: int = 10,
    include_properties: bool = True,
    include_descriptions: bool = True
) -> Dict[str, Any]:
    """
    Documents a NiFi flow by traversing processors and their connections.

    Args:
        process_group_id: The UUID of the process group to document. Defaults to the root group if None.
        starting_processor_id: The UUID of the processor to start the traversal from.
            If None, documents all processors in the process group.
        max_depth: Maximum depth to traverse from the starting processor. Defaults to 10.
        include_properties: Whether to include processor properties in the documentation. Defaults to True.
        include_descriptions: Whether to include processor descriptions in the documentation. Defaults to True.

    Returns:
        A dictionary containing the flow documentation, including:
        - processors: A list of processors and their configurations
        - connections: A list of connections between processors
        - graph_structure: The graph structure for traversal
        - common_paths: Pre-identified paths through the flow
        - decision_points: Branching points in the flow
        - parameters: Parameter context information (if available)
    """
    # Implementation details to be filled in
```

## Implementation Strategy

### 1. Data Collection Phase

First, we need to collect all the necessary data from the NiFi API:

1. **Fetch Process Group**: Get the process group details if provided
2. **Fetch Processors**: Get all processors in the group
3. **Fetch Connections**: Get all connections between processors
4. **Build Graph**: Construct an in-memory graph representation of processors and connections

For Phase 1, we'll use the "Collect-All-Then-Connect" approach:
- Get all processors in the process group in a single API call
- Get all connections in the process group in a single API call
- Build an in-memory graph representation using processor IDs and connection data
- This approach is simpler, more robust, and requires fewer API calls

### 2. Processor and Connection Enrichment

For each processor, we need to extract relevant information:

1. **Basic Information**:
   - ID, name, type, state, position
   - Input/output relationships

2. **Properties and Expressions**:
   - Key properties based on processor type
   - Dynamic properties (often contain expressions)
   - Expression Language usage
   - Parameter references

3. **Connection Details**:
   - Source and destination processors
   - Relationships used
   - Connection name and conditions (if any)

### 3. Graph Structure and Path Analysis

Build a structured representation of the flow:

1. **Graph Structure**:
   - Create adjacency lists for incoming and outgoing connections
   - Map processors by ID for easy lookup

2. **Common Flow Paths**:
   - Source-to-Sink Paths: Trace paths from source processors (no incoming connections) to sink processors (no outgoing connections)
   - Include processor names and relationships along each path

3. **Decision Points**:
   - Identify processors with multiple outgoing relationships
   - Document the different paths data can take from these decision points

### 4. Helper Functions

Several helper functions will be needed:

1. `build_graph_structure(processors, connections)`: Creates adjacency lists for the graph
2. `extract_important_properties(processor)`: Extracts key properties based on processor type
3. `analyze_expressions(properties)`: Identifies Expression Language usage in properties
4. `find_source_to_sink_paths(processor_map, graph)`: Finds common flow paths
5. `find_decision_branches(processor_map, graph)`: Identifies decision points
6. `format_connection(connection, processor_map)`: Formats connection details for documentation

## Detailed Implementation Components

### Property Extraction

```python
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
    # Default for any processor type
    "default": ["Schedule Period", "Scheduling Strategy", "Run Duration"]
}

def extract_important_properties(processor):
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
    for prop_name, prop_value in properties.items():
        if prop_name not in processor["component"]["config"]["descriptors"]:
            dynamic_props[prop_name] = prop_value
    
    return {
        "key_properties": key_properties,
        "dynamic_properties": dynamic_props,
        "all_properties": properties  # Include everything if needed
    }
```

### Expression Analysis

```python
def analyze_expressions(properties):
    """Identify and analyze Expression Language in properties."""
    expression_pattern = r"\$\{([^}]+)\}"
    expressions = {}
    
    for prop_name, prop_value in properties.items():
        if isinstance(prop_value, str):
            matches = re.findall(expression_pattern, prop_value)
            if matches:
                expressions[prop_name] = matches
    
    return expressions
```

### Graph Structure

```python
def build_graph_structure(processors, connections):
    """Build graph adjacency lists for the flow."""
    # Create adjacency lists
    outgoing_connections = {}  # processor_id -> [connection_ids]
    incoming_connections = {}  # processor_id -> [connection_ids]
    
    for conn in connections:
        src_id = conn["source"]["id"]
        dest_id = conn["destination"]["id"]
        
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
```

### Common Flow Paths

```python
def find_source_to_sink_paths(processor_map, outgoing_connections):
    """Find common data flow paths from source to sink processors."""
    # Find source processors (no incoming connections)
    sources = [p_id for p_id in processor_map.keys() if p_id not in incoming_connections]
    
    # Find sink processors (no outgoing connections)
    sinks = [p_id for p_id in processor_map.keys() if p_id not in outgoing_connections]
    
    # For each source, find all paths to sinks
    all_paths = []
    
    for source in sources:
        paths = find_all_paths(source, sinks, outgoing_connections, max_depth=10)
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
```

### Decision Points

```python
def find_decision_branches(processor_map, outgoing_connections):
    """Identify decision points where flow branches based on relationships."""
    branches = []
    
    # Find processors with multiple outgoing relationships
    for proc_id, connections in outgoing_connections.items():
        # Group connections by relationship
        rel_groups = {}
        for conn in connections:
            rel = conn["component"]["selectedRelationships"][0]  # Simplification
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
                    dest_id = conn["destination"]["id"]
                    branch_info["branches"].append({
                        "relationship": rel,
                        "destination_id": dest_id,
                        "destination_name": processor_map[dest_id]["component"]["name"]
                    })
            
            branches.append(branch_info)
    
    return branches
```

## Handling Special Cases

1. **Cycles in the Flow**: Use traversal markers to detect cycles
2. **Multiple Starting Points**: Handle flows with multiple entry points
3. **Process Group Nesting**: Initially, focus on a single process group; consider nested groups in future versions
4. **Error Paths**: Document normal flow and error/failure paths
5. **Remote Process Groups**: Document remote connections with available information

## Implementation Phases

1. **Phase 1**: Basic implementation
   - Core data collection: Processors, connections, properties
   - Graph structure and path identification
   - API implementation returning structured data

2. **Phase 2**: Enhanced features
   - More advanced property analysis
   - Better path detection and cycle handling
   - Parameter context integration

3. **Phase 3 (Future)**: Advanced features
   - Support for nested process groups
   - Documentation of variable contexts
   - Performance characteristics
   - Data transformation tracking

## Complete Example Implementation

```python
async def document_nifi_flow(
    process_group_id=None,
    starting_processor_id=None,
    max_depth=10,
    include_properties=True,
    include_descriptions=True
):
    # Get data from NiFi
    group_id = process_group_id or await get_root_process_group_id()
    processors = await nifi_api_client.list_processors(group_id)
    connections = await nifi_api_client.list_connections(group_id)
    
    # Enrich processor data with important properties and expressions
    enriched_processors = []
    for processor in processors:
        proc_data = {
            "id": processor["id"],
            "name": processor["component"]["name"],
            "type": processor["component"]["type"],
            "state": processor["component"]["state"],
            "position": processor["position"],
            "relationships": [r["name"] for r in processor["component"]["relationships"]]
        }
        
        if include_properties:
            # Extract and analyze properties
            property_info = extract_important_properties(processor)
            proc_data["properties"] = property_info["key_properties"]
            proc_data["dynamic_properties"] = property_info["dynamic_properties"]
            
            # Analyze expressions
            proc_data["expressions"] = analyze_expressions(property_info["all_properties"])
        
        enriched_processors.append(proc_data)
    
    # Build graph structure
    processor_map = {p["id"]: p for p in processors}
    graph = build_graph_structure(processors, connections)
    
    # Find common paths
    paths = find_source_to_sink_paths(processor_map, graph["outgoing"])
    decision_points = find_decision_branches(processor_map, graph["outgoing"])
    
    # Assemble result
    result = {
        "processors": enriched_processors,
        "connections": [format_connection(c, processor_map) for c in connections],
        "graph_structure": graph,
        "common_paths": paths,
        "decision_points": decision_points
    }
    
    # Include parameter context if available
    if include_properties:
        parameters = await get_parameter_context(group_id)
        if parameters:
            result["parameters"] = parameters
    
    return result
```

## Example Response Structure

```json
{
  "processors": [
    {
      "id": "abc123",
      "name": "GetFile",
      "type": "org.apache.nifi.processors.standard.GetFile",
      "state": "RUNNING",
      "properties": {
        "Directory": "/data/input",
        "File Filter": "*.csv",
        "Batch Size": "10" 
      },
      "dynamic_properties": {},
      "expressions": {
        "Directory": ["${input.directory}"]
      },
      "relationships": ["success", "failure"],
      "position": {"x": 100, "y": 100}
    }
  ],
  "connections": [
    {
      "id": "conn1",
      "source_id": "abc123",
      "source_name": "GetFile",
      "destination_id": "def456",
      "destination_name": "SplitRecord",
      "relationship": "success"
    }
  ],
  "graph_structure": {
    "outgoing": {
      "abc123": ["conn1"],
      "def456": ["conn2"]
    },
    "incoming": {
      "def456": ["conn1"],
      "ghi789": ["conn2"]
    }
  },
  "common_paths": [
    {
      "path_ids": ["abc123", "def456", "ghi789"],
      "path_names": ["GetFile", "SplitRecord", "PutFile"],
      "path_types": ["GetFile", "SplitRecord", "PutFile"],
      "relationships": ["success", "success"]
    }
  ],
  "decision_points": [
    {
      "decision_point": "def456",
      "processor_name": "RouteOnAttribute",
      "branches": [
        {
          "relationship": "matched",
          "destination_id": "ghi789",
          "destination_name": "PutFile"
        },
        {
          "relationship": "unmatched",
          "destination_id": "jkl012",
          "destination_name": "LogAttribute"
        }
      ]
    }
  ],
  "parameters": [
    {
      "name": "input.directory",
      "value": "/data/input",
      "description": "Input directory for CSV files"
    }
  ]
}
```

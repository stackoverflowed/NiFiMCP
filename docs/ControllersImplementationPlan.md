# Controller Services Implementation Plan

## Overview
This document outlines the complete implementation plan for adding NiFi controller service support to the MCP server, following the same patterns established for processors while incorporating the user's specific requirements.

## Key Design Decisions

### 1. **Scope & Coverage**
- ✅ **Complete implementation** - All phases included
- ✅ **Type-agnostic** - Implementation independent of specific controller types
- ✅ **Consistent patterns** - Follow established processor tool patterns

### 2. **Auto-Features Integration**
- ✅ **Auto-disable/enable** - Automatically disable before updates, re-enable after if originally enabled
- ❌ **No feature flags** - Always enabled, no configuration needed
- ✅ **Enhanced return structure** - Similar to processor auto-restart with property_update and enable_status

### 3. **Dependency & Validation Handling**
- ❌ **No dependency resolution** - Keep implementation simple
- ✅ **Validation state reporting** - Report bad validation states to user
- ✅ **Clear error messages** - Help users understand validation issues

### 4. **Naming Conventions**
- ✅ **Shorter names** - `create_controller_services` vs `create_nifi_controller_services`
- ✅ **Plural object types** - `"controller_services"` for lists, `"controller_service"` for details
- ✅ **Consistent patterns** - Match processor tool naming

### 5. **Process Group Handling**
- ✅ **Required field** - `process_group_id` always required, no defaulting to root
- ✅ **Explicit scope** - Users must specify where controller services are created
- ✅ **Clear ownership** - Avoid confusion about controller service inheritance

### 6. **Type Discovery & Validation**
- ✅ **Available types listing** - LLM can discover valid controller types
- ✅ **Enhanced error handling** - Return valid options when invalid types are provided
- ✅ **Apply to processors too** - Enhance existing processor creation with type validation

---

## Implementation Phases

## Phase 1: NiFi Client Controller Service API Methods

Add the following methods to `nifi_mcp_server/nifi_client.py`:

```python
async def list_controller_services(self, process_group_id: str) -> List[Dict]:
    """Lists all controller services in a process group."""
    
async def get_controller_service_details(self, controller_service_id: str) -> Dict:
    """Gets detailed information about a specific controller service."""
    
async def create_controller_service(
    self, 
    process_group_id: str, 
    service_type: str, 
    name: str, 
    properties: Optional[Dict[str, Any]] = None
) -> Dict:
    """Creates a new controller service."""
    
async def update_controller_service_properties(
    self, 
    controller_service_id: str, 
    properties: Dict[str, Any]
) -> Dict:
    """Updates controller service properties."""
    
async def delete_controller_service(self, controller_service_id: str, version: int) -> bool:
    """Deletes a controller service."""
    
async def enable_controller_service(self, controller_service_id: str) -> Dict:
    """Enables a controller service."""
    
async def disable_controller_service(self, controller_service_id: str) -> Dict:
    """Disables a controller service."""
    
async def get_controller_service_types(self) -> List[Dict]:
    """Gets all available controller service types."""
```

### API Endpoints to Implement:
- `GET /process-groups/{id}/controller-services` - List controller services
- `GET /controller-services/{id}` - Get controller service details
- `POST /process-groups/{id}/controller-services` - Create controller service
- `PUT /controller-services/{id}` - Update controller service properties
- `DELETE /controller-services/{id}` - Delete controller service
- `PUT /controller-services/{id}/run-status` - Enable/disable controller service
- `GET /flow/controller-service-types` - Get available types

---

## Phase 2: Utility Functions

Add to `nifi_mcp_server/api_tools/utils.py`:

```python
def filter_controller_service_data(controller_service_entity: Dict) -> Dict:
    """Filters controller service data to essential fields."""
    
def _format_controller_service_summary(controller_service: Dict) -> str:
    """Creates a summary string for a controller service."""
```

---

## Phase 3: Extend Existing Tools

### 3A. Review Tools (`review.py`)

**Update `list_nifi_objects`:**
```python
@mcp.tool()
async def list_nifi_objects(
    object_type: Literal["processors", "connections", "ports", "process_groups", "controller_services"],  # Add controller_services
    process_group_id: str | None = None,
    search_scope: Literal["current_group", "recursive"] = "current_group",
    timeout_seconds: Optional[float] = None,
    continuation_token: Optional[str] = None,
) -> Union[List[Dict], Dict]:
```

**Update `get_nifi_object_details`:**
```python
@mcp.tool()
async def get_nifi_object_details(
    object_type: Literal["processor", "connection", "port", "process_group", "controller_service"],  # Add controller_service
    object_id: str,
) -> Dict:
```

### 3B. Deletion Tools (`modification.py`)

**Update `delete_nifi_objects`:**
```python
@mcp.tool()
async def delete_nifi_objects(
    deletion_requests: List[Dict[str, Any]]  # Add support for "controller_service" object_type
) -> List[Dict]:
```

### 3C. Operation Tools (`operation.py`)

**Update `operate_nifi_object`:**
```python
@mcp.tool()
async def operate_nifi_object(
    object_type: Literal["processor", "port", "process_group", "controller_service"],  # Add controller_service
    object_id: str,
    operation_type: Literal["start", "stop", "enable", "disable"]  # Add enable/disable for controller services
) -> Dict:
```

---

## Phase 4: New Controller Service Tools

### 4A. Creation Tool (`creation.py`)

```python
@mcp.tool()
@tool_phases(["Modify"])
async def create_controller_services(
    controller_services: List[Dict[str, Any]],
    process_group_id: str
) -> List[Dict]:
    """
    Creates one or more controller services in batch.
    
    Args:
        controller_services: List of controller service definitions, each containing:
            - service_type: The fully qualified Java class name (required)
            - name: The desired name for the controller service (required)
            - properties: Dict of configuration properties (optional)
        process_group_id: The UUID of the process group where services should be created (required)
    
    Returns:
        List of results, one per controller service creation attempt.
        Includes enhanced error handling with available types when invalid types are provided.
    """

async def _create_controller_service_single(
    service_type: str,
    name: str,
    process_group_id: str,
    properties: Optional[Dict[str, Any]] = None
) -> Dict:
    """Internal helper for single controller service creation with type validation."""
```

### 4B. Modification Tool (`modification.py`)

```python
@mcp.tool()
@tool_phases(["Modify"])
async def update_controller_service_properties(
    controller_service_id: str,
    properties: Dict[str, Any]
) -> Dict:
    """
    Updates a controller service's properties with auto-disable/enable support.
    
    Automatically disables enabled controller services, performs the update,
    then re-enables them if originally enabled and validation is successful.
    
    Args:
        controller_service_id: The UUID of the controller service to update
        properties: Complete dictionary of desired property values
    
    Returns:
        Enhanced status information including property_update and enable_status sections.
        Possible status values: "success", "partial_success", "warning", "error"
    """
```

### 4C. Type Discovery Tool (`review.py`)

```python
@mcp.tool()
@tool_phases(["Review", "Build"])
async def get_controller_service_types() -> Dict:
    """
    Gets all available controller service types that can be created.
    
    Returns:
        Dictionary containing available controller service types with descriptions.
    """
```

---

## Phase 5: Enhanced Type Validation

### 5A. Processor Creation Enhancement

**Update `create_nifi_processors` and related functions:**
- Add type validation before creation attempt
- Return available processor types when invalid type is provided
- Apply same pattern to `create_nifi_flow`

### 5B. Error Response Format

```python
# Enhanced error response when invalid type is provided:
{
    "status": "error",
    "message": "Invalid processor type 'InvalidType'. See available_types for valid options.",
    "error_code": "INVALID_TYPE",
    "invalid_type": "InvalidType",
    "available_types": [
        {
            "type": "org.apache.nifi.processors.standard.LogAttribute", 
            "description": "Logs FlowFile attributes at specified log level"
        },
        # ... more types
    ]
}
```

---

## Phase 6: Integration Testing

### 6A. Test Coverage
- Unit tests for all new controller service methods
- Integration tests for auto-disable/enable functionality
- Type validation testing
- Error handling verification

### 6B. Update Existing Tests
- Extend processor operation tests to cover controller services
- Add controller service scenarios to flow creation tests
- Test enhanced error handling for invalid types

---

## Implementation Order

1. **Phase 1** - NiFi Client API methods (foundation)
2. **Phase 2** - Utility functions (support functions)
3. **Phase 3A** - Review tool extensions (discovery & inspection)
4. **Phase 4C** - Type discovery tool (enables better UX)
5. **Phase 4A** - Creation tool (with type validation)
6. **Phase 4B** - Modification tool (with auto-disable/enable)
7. **Phase 3B & 3C** - Deletion and operation tool extensions
8. **Phase 5** - Enhanced type validation for processors
9. **Phase 6** - Testing and validation

---

## Example Usage Scenarios

### Creating a Controller Service
```python
# Discover available types
types_result = await get_controller_service_types()

# Create a database connection pool
result = await create_controller_services(
    controller_services=[{
        "service_type": "org.apache.nifi.dbcp.DBCPConnectionPool",
        "name": "MyDatabasePool",
        "properties": {
            "Database Connection URL": "jdbc:postgresql://localhost:5432/mydb",
            "Database Driver Class Name": "org.postgresql.Driver"
        }
    }],
    process_group_id="pg-uuid"
)
```

### Updating Controller Service Properties
```python
# Update properties with auto-disable/enable
result = await update_controller_service_properties(
    controller_service_id="cs-uuid",
    properties={
        "Maximum Pool Size": "20",
        "Connection Timeout": "30 seconds"
    }
)

# Response includes enable status
{
    "status": "success",
    "message": "Controller service 'MyDatabasePool' properties updated successfully. Service re-enabled and is now active.",
    "property_update": {"status": "success"},
    "enable_status": {"status": "success", "final_state": "ENABLED"},
    "entity": {...}
}
```

### Enhanced Error Handling
```python
# Invalid type returns available options
result = await create_controller_services(
    controller_services=[{
        "service_type": "InvalidControllerType",
        "name": "MyService"
    }],
    process_group_id="pg-uuid"
)

# Response:
{
    "status": "error",
    "error_code": "INVALID_TYPE",
    "available_types": [
        {"type": "org.apache.nifi.dbcp.DBCPConnectionPool", "description": "..."},
        {"type": "org.apache.nifi.ssl.StandardSSLContextService", "description": "..."},
        # ... more types
    ]
}
```

---

## Success Criteria

- ✅ Full CRUD operations for controller services
- ✅ Auto-disable/enable functionality working correctly
- ✅ Type validation and discovery for both controllers and processors
- ✅ Consistent API patterns with existing processor tools
- ✅ Comprehensive error handling with helpful responses
- ✅ All existing functionality remains unaffected
- ✅ Complete test coverage for new features

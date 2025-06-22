import pytest
from unittest.mock import AsyncMock
from nifi_mcp_server.api_tools.creation import (
    _validate_script_processor_properties,
    _validate_groovy_script,
    _validate_processor_properties_phase2b
)
from nifi_mcp_server.api_tools.operation import (
    _analyze_error_patterns,
    _generate_debugging_suggestions
)
from nifi_mcp_server.api_tools.modification import _requires_restart_for_property_change


class TestDebuggingEfficiencyImprovements:
    """Test efficiency improvements based on MCP log analysis."""

    def test_script_validation_empty_string_fix(self):
        """Test that empty strings are automatically converted to None."""
        properties = {
            "Script Engine": "Groovy",
            "Script File": "",  # Empty string that causes validation errors
            "Module Directory": "",  # Empty string that causes validation errors
            "Script Body": "def flowFile = session.get()"
        }
        
        warnings = _validate_script_processor_properties(
            "org.apache.nifi.processors.script.ExecuteScript",
            properties
        )
        
        # Verify empty strings converted to None
        assert properties["Script File"] is None
        assert properties["Module Directory"] is None
        
        # Verify warnings generated
        assert any("Script File" in warning for warning in warnings)
        assert any("Module Directory" in warning for warning in warnings)

    def test_groovy_script_flowfile_scope_detection(self):
        """Test detection of flowFile scope issues that caused MissingPropertyException."""
        # Script with flowFile scope issue (from the logs) - note: no session.get() call
        problematic_script = "session.read(flowFile, new InputStreamCallback() { })"
        
        warnings = _validate_groovy_script(problematic_script)
        
        # Should detect the flowFile scope issue
        assert any("session.get()" in warning for warning in warnings)
        assert any("MissingPropertyException" in warning for warning in warnings)

    def test_groovy_script_fixed_pattern_validation(self):
        """Test that the corrected script pattern passes validation."""
        # Fixed script (like the final working version)
        fixed_script = """
        import org.apache.nifi.flowfile.FlowFile
        
        final FlowFile ff = session.get()
        if (ff == null) return
        
        String content = ''
        session.read(ff, new InputStreamCallback() {
            // Properly uses ff instead of flowFile
        })
        """
        
        warnings = _validate_groovy_script(fixed_script)
        
        # Should not generate flowFile scope warnings
        assert not any("session.get()" in warning for warning in warnings)

    def test_error_pattern_analysis_groovy_flowfile(self):
        """Test error pattern analysis for Groovy flowFile issues."""
        # Error message similar to what was in the logs
        error_message = """
        ExecuteScript failed to process due to org.apache.nifi.processor.exception.ProcessException: 
        javax.script.ScriptException: groovy.lang.MissingPropertyException: 
        No such property: flowFile for class: Script960
        """
        
        patterns = _analyze_error_patterns(
            error_message,
            "org.apache.nifi.processors.script.ExecuteScript"
        )
        
        assert "groovy_flowfile_scope_issue" in patterns

    def test_debugging_suggestions_generation(self):
        """Test that debugging suggestions are generated for detected patterns."""
        patterns = ["groovy_flowfile_scope_issue", "property_validation_error"]
        
        suggestions = _generate_debugging_suggestions(
            patterns,
            "org.apache.nifi.processors.script.ExecuteScript"
        )
        
        # Should generate suggestions for both patterns
        assert len(suggestions) == 2
        
        # Verify FlowFile scope suggestion
        flowfile_suggestion = next(
            (s for s in suggestions if "FlowFile Scope" in s["issue"]), 
            None
        )
        assert flowfile_suggestion is not None
        assert "session.get()" in flowfile_suggestion["solution"]
        assert "import org.apache.nifi.flowfile.FlowFile" in flowfile_suggestion["example"]

    def test_smart_restart_logic_script_properties(self):
        """Test that script property changes require restart."""
        # Script body changes should require restart
        assert _requires_restart_for_property_change(
            "org.apache.nifi.processors.script.ExecuteScript",
            "Script Body",
            "old script",
            "new script"
        )
        
        # Non-critical properties should not require restart
        assert not _requires_restart_for_property_change(
            "org.apache.nifi.processors.script.ExecuteScript",
            "Log Level",
            "INFO",
            "DEBUG"
        )

    def test_smart_restart_logic_http_properties(self):
        """Test smart restart logic for HTTP processors."""
        # HTTP Status Code changes don't require restart
        assert not _requires_restart_for_property_change(
            "org.apache.nifi.processors.standard.HandleHttpResponse",
            "HTTP Status Code",
            "200",
            "404"
        )
        
        # But context map changes would require restart (default behavior)
        assert _requires_restart_for_property_change(
            "org.apache.nifi.processors.standard.HandleHttpResponse",
            "HTTP Context Map",
            "old-service-id",
            "new-service-id"
        )

    def test_enhanced_validation_integration(self):
        """Test that enhanced validation is integrated into Phase 2B."""
        properties = {
            "Script Engine": "Groovy",
            "Script File": "",  # This should be auto-fixed
            "Script Body": "session.read(flowFile, callback)"  # This should generate scope warning
        }
        
        # Test the script processor validation directly since Phase 2B is async
        warnings = _validate_script_processor_properties(
            "org.apache.nifi.processors.script.ExecuteScript",
            properties
        )
        
        # Verify empty string was fixed
        assert properties["Script File"] is None
        
        # Verify script validation warnings were included  
        assert any("Script File" in warning for warning in warnings)
        assert any("session.get()" in warning for warning in warnings)

    def test_efficiency_improvement_scenarios(self):
        """Test scenarios that would have caused the inefficiency in the logs."""
        
        # Scenario 1: Empty string properties (caused 3 failed attempts)
        properties_with_empty_strings = {
            "Script Engine": "Groovy",
            "Script File": "",
            "Module Directory": "",
            "Script Body": "some script"
        }
        
        warnings = _validate_script_processor_properties(
            "org.apache.nifi.processors.script.ExecuteScript",
            properties_with_empty_strings
        )
        
        # Should auto-fix and prevent failures
        assert properties_with_empty_strings["Script File"] is None
        assert properties_with_empty_strings["Module Directory"] is None
        assert len(warnings) >= 2  # At least warnings for the two fixes
        
        # Scenario 2: Script with flowFile scope issue (caused runtime errors)
        script_with_issue = "session.read(flowFile, callback)"  # Missing session.get()
        script_warnings = _validate_groovy_script(script_with_issue)
        
        # Should detect the issue proactively
        assert any("session.get()" in warning for warning in script_warnings)

    def test_error_analysis_comprehensive_patterns(self):
        """Test comprehensive error pattern analysis."""
        test_cases = [
            {
                "error": "MissingPropertyException: No such property: flowFile",
                "processor": "org.apache.nifi.processors.script.ExecuteScript",
                "expected_pattern": "groovy_flowfile_scope_issue"
            },
            {
                "error": "Property validation failed: Script File is invalid",
                "processor": "org.apache.nifi.processors.script.ExecuteScript", 
                "expected_pattern": "property_validation_error"
            },
            {
                "error": "HTTP Context Map service not configured",
                "processor": "org.apache.nifi.processors.standard.HandleHttpRequest",
                "expected_pattern": "http_context_map_missing"
            }
        ]
        
        for case in test_cases:
            patterns = _analyze_error_patterns(case["error"], case["processor"])
            assert case["expected_pattern"] in patterns

    def test_restart_optimization_coverage(self):
        """Test restart optimization covers common scenarios."""
        test_cases = [
            # Properties that should NOT require restart
            ("any.processor.Type", "Log Level", False),
            ("any.processor.Type", "Bulletin Level", False),
            ("any.processor.Type", "Yield Duration", False),
            ("org.apache.nifi.processors.standard.HandleHttpResponse", "HTTP Status Code", False),
            
            # Properties that SHOULD require restart
            ("org.apache.nifi.processors.script.ExecuteScript", "Script Body", True),
            ("org.apache.nifi.processors.script.ExecuteScript", "Script File", True),
            ("any.processor.Type", "SomeOtherProperty", True),  # Default case
        ]
        
        for processor_type, property_name, should_restart in test_cases:
            result = _requires_restart_for_property_change(
                processor_type, property_name, "old", "new"
            )
            assert result == should_restart, f"Failed for {processor_type}.{property_name}"


class TestAutoTerminationGaps:
    """Test the auto-termination gaps identified from the LLM logs."""
    
    def test_http_request_processor_auto_termination(self):
        """Test that HandleHttpRequest relationships are properly auto-terminated when unused."""
        from nifi_mcp_server.api_tools.creation import _should_auto_terminate_relationship
        
        # Scenario from logs: HandleHttpRequest with unused success relationship
        planned_connections = [
            # No connections using the success relationship from ReceiveHTTP
        ]
        
        # Should auto-terminate unused success relationship
        should_terminate = _should_auto_terminate_relationship(
            processor_type="org.apache.nifi.processors.standard.HandleHttpRequest",
            relationship_name="success", 
            processor_name="ReceiveHTTP",
            planned_connections=planned_connections
        )
        
        assert should_terminate, "HandleHttpRequest unused success relationship should be auto-terminated"
        
        # Should auto-terminate unused failure relationship too
        should_terminate_failure = _should_auto_terminate_relationship(
            processor_type="org.apache.nifi.processors.standard.HandleHttpRequest",
            relationship_name="failure",
            processor_name="ReceiveHTTP", 
            planned_connections=planned_connections
        )
        
        assert should_terminate_failure, "HandleHttpRequest unused failure relationship should be auto-terminated"
    
    def test_http_request_processor_with_connections(self):
        """Test that HandleHttpRequest relationships are NOT auto-terminated when used."""
        from nifi_mcp_server.api_tools.creation import _should_auto_terminate_relationship
        
        # Scenario: HandleHttpRequest with success relationship being used
        planned_connections = [
            {
                "source": "ReceiveHTTP",
                "target": "ProcessRequest", 
                "relationships": ["success"]
            }
        ]
        
        # Should NOT auto-terminate success relationship (it's being used)
        should_terminate = _should_auto_terminate_relationship(
            processor_type="org.apache.nifi.processors.standard.HandleHttpRequest",
            relationship_name="success",
            processor_name="ReceiveHTTP",
            planned_connections=planned_connections
        )
        
        assert not should_terminate, "HandleHttpRequest used success relationship should NOT be auto-terminated"
        
        # Should auto-terminate unused failure relationship  
        should_terminate_failure = _should_auto_terminate_relationship(
            processor_type="org.apache.nifi.processors.standard.HandleHttpRequest",
            relationship_name="failure",
            processor_name="ReceiveHTTP",
            planned_connections=planned_connections
        )
        
        assert should_terminate_failure, "HandleHttpRequest unused failure relationship should be auto-terminated"
    
    def test_http_response_processor_auto_termination(self):
        """Test that HandleHttpResponse relationships are auto-terminated.""" 
        from nifi_mcp_server.api_tools.creation import _should_auto_terminate_relationship
        
        # Scenario from logs: HandleHttpResponse with unused relationships
        planned_connections = []
        
        # Should auto-terminate both success and failure (from explicit rules)
        should_terminate_success = _should_auto_terminate_relationship(
            processor_type="org.apache.nifi.processors.standard.HandleHttpResponse",
            relationship_name="success",
            processor_name="SendResponse",
            planned_connections=planned_connections
        )
        
        should_terminate_failure = _should_auto_terminate_relationship(
            processor_type="org.apache.nifi.processors.standard.HandleHttpResponse", 
            relationship_name="failure",
            processor_name="SendResponse",
            planned_connections=planned_connections
        )
        
        assert should_terminate_success, "HandleHttpResponse success should be auto-terminated"
        assert should_terminate_failure, "HandleHttpResponse failure should be auto-terminated"
    
    def test_execute_script_processor_auto_termination(self):
        """Test that ExecuteScript relationships are properly auto-terminated."""
        from nifi_mcp_server.api_tools.creation import _should_auto_terminate_relationship
        
        # Scenario: ExecuteScript with both success and failure unused
        planned_connections = []
        
        should_terminate_success = _should_auto_terminate_relationship(
            processor_type="org.apache.nifi.processors.script.ExecuteScript",
            relationship_name="success",
            processor_name="ProcessRequest", 
            planned_connections=planned_connections
        )
        
        should_terminate_failure = _should_auto_terminate_relationship(
            processor_type="org.apache.nifi.processors.script.ExecuteScript",
            relationship_name="failure",
            processor_name="ProcessRequest",
            planned_connections=planned_connections
        )
        
        assert should_terminate_success, "ExecuteScript unused success should be auto-terminated"
        assert should_terminate_failure, "ExecuteScript unused failure should be auto-terminated"
    
    def test_conservative_default_for_critical_relationships(self):
        """Test that critical relationships are not auto-terminated without explicit rules."""
        from nifi_mcp_server.api_tools.creation import _should_auto_terminate_relationship
        
        # Test unknown processor with success relationship (critical)
        should_terminate = _should_auto_terminate_relationship(
            processor_type="org.apache.nifi.processors.unknown.SomeProcessor",
            relationship_name="success",
            processor_name="TestProcessor",
            planned_connections=[]
        )
        
        assert not should_terminate, "Success relationships should not be auto-terminated without explicit rules"
        
        # Test unknown processor with non-critical relationship  
        should_terminate_other = _should_auto_terminate_relationship(
            processor_type="org.apache.nifi.processors.unknown.SomeProcessor",
            relationship_name="other", 
            processor_name="TestProcessor",
            planned_connections=[]
        )
        
        assert should_terminate_other, "Non-critical relationships should be auto-terminated by default"
    
    def test_exact_llm_scenario_replication_auto_termination(self):
        """Test the exact auto-termination scenario from the LLM logs."""
        from nifi_mcp_server.api_tools.creation import _should_auto_terminate_relationship
        
        # Replicate the exact planned connections from LLM logs
        planned_connections = [
            {"source": "ReceiveHTTP", "target": "Log Raw Request", "relationships": ["success"]},
            {"source": "ReceiveHTTP", "target": "ProcessRequest", "relationships": ["success"]},
            {"source": "ProcessRequest", "target": "Log Response", "relationships": ["success"]},
            {"source": "Log Response", "target": "SendResponse", "relationships": ["success"]},
            {"source": "ProcessRequest", "target": "SendResponse", "relationships": ["failure"]}
        ]
        
        # ReceiveHTTP: success is used, failure should be auto-terminated
        assert not _should_auto_terminate_relationship(
            "org.apache.nifi.processors.standard.HandleHttpRequest", "success", 
            "ReceiveHTTP", planned_connections
        ), "ReceiveHTTP success should NOT be auto-terminated (used by connections)"
        
        # SendResponse: both success and failure unused, should be auto-terminated
        assert _should_auto_terminate_relationship(
            "org.apache.nifi.processors.standard.HandleHttpResponse", "success",
            "SendResponse", planned_connections
        ), "SendResponse success should be auto-terminated (unused)"
        
        assert _should_auto_terminate_relationship(
            "org.apache.nifi.processors.standard.HandleHttpResponse", "failure", 
            "SendResponse", planned_connections
        ), "SendResponse failure should be auto-terminated (unused)"
        
        # ProcessRequest: success and failure both used, none should be auto-terminated
        # (This would be tested with actual planned connection analysis)


class TestServiceResolutionGaps:
    """Test the gaps between our service resolution tests and real-world LLM usage."""
    
    def test_missing_required_service_reference_detection(self):
        """Test that missing required service references are detected and handled."""
        from nifi_mcp_server.api_tools.creation import _suggest_missing_service_reference
        
        # Test HTTP processors without service references (like in the LLM logs)
        suggestion = _suggest_missing_service_reference(
            "org.apache.nifi.processors.standard.HandleHttpRequest",
            "HTTP Context Map",
            "test-pg-id",
            {}  # Empty service map - no existing services
        )
        
        # Should suggest creating the service
        assert suggestion is not None
        assert suggestion["reference"] == "@HttpContextMap"
        assert "Create org.apache.nifi.http.StandardHttpContextMap" in suggestion["message"]
    
    def test_missing_service_with_existing_service_in_map(self):
        """Test service suggestion when a compatible service already exists."""
        from nifi_mcp_server.api_tools.creation import _suggest_missing_service_reference
        
        # Simulate existing service in service_map (like when LLM creates service first)
        service_map = {"HttpContextMap": "88a1e773-0197-1000-d97d-6f168dc293e0"}
        
        suggestion = _suggest_missing_service_reference(
            "org.apache.nifi.processors.standard.HandleHttpResponse",
            "HTTP Context Map", 
            "test-pg-id",
            service_map
        )
        
        # Should find existing service
        assert suggestion is not None
        assert suggestion["reference"] == "88a1e773-0197-1000-d97d-6f168dc293e0"
        assert "Found existing service" in suggestion["message"]
    
    @pytest.mark.anyio
    async def test_property_validation_adds_missing_service_references(self):
        """Test that property validation adds missing required service references."""
        from nifi_mcp_server.api_tools.creation import _validate_and_fix_processor_properties
        from loguru import logger
        from unittest.mock import AsyncMock
        
        # Mock NiFi client
        mock_client = AsyncMock()
        
        # Test properties WITHOUT HTTP Context Map (like in LLM logs)
        properties = {
            "Listening Port": "7070",
            "Allowed Paths": "/order"
            # Missing "HTTP Context Map" - this is the gap!
        }
        
        # Service map with existing service (like LLM created)
        service_map = {"HttpContextMap": "88a1e773-0197-1000-d97d-6f168dc293e0"}
        
        fixed_props, warnings, errors = await _validate_and_fix_processor_properties(
            "org.apache.nifi.processors.standard.HandleHttpRequest",
            properties,
            service_map,
            "test-pg-id",
            mock_client,
            logger,
            "test-user-id",
            "test-action-id"
        )
        
        # Should auto-add the missing service reference
        assert "HTTP Context Map" in fixed_props
        assert fixed_props["HTTP Context Map"] == "88a1e773-0197-1000-d97d-6f168dc293e0"
        assert any("Added missing required service reference" in warning for warning in warnings)
    
    @pytest.mark.anyio
    async def test_comprehensive_llm_scenario_replication(self):
        """Test the exact scenario from the LLM logs to ensure it would be fixed."""
        from nifi_mcp_server.api_tools.creation import _validate_and_fix_processor_properties
        from loguru import logger
        from unittest.mock import AsyncMock
        
        # Mock NiFi client
        mock_client = AsyncMock()
        
        # Exact LLM scenario: ReceiveHTTP processor without HTTP Context Map
        receivehttp_properties = {
            "Listening Port": "7070",
            "Base Path": "/order",  # Wrong property name (should be Allowed Paths)
            # Missing "HTTP Context Map" entirely
        }
        
        # SendResponse processor without HTTP Context Map
        sendresponse_properties = {
            "Response Code": "${http.status.code}",
            "Response Body": "${filename:toString():substringBeforeLast('\\n')}",  # Invalid property
            # Missing "HTTP Context Map" entirely
        }
        
        # Service map representing LLM creating the service separately 
        service_map = {"HttpContextMap": "88a1e773-0197-1000-d97d-6f168dc293e0"}
        
        # Test ReceiveHTTP processor
        fixed_receive, warnings_receive, errors_receive = await _validate_and_fix_processor_properties(
            "org.apache.nifi.processors.standard.HandleHttpRequest",
            receivehttp_properties,
            service_map,
            "test-pg-id", 
            mock_client,
            logger,
            "test-user-id",
            "test-action-id"
        )
        
        # Test SendResponse processor  
        fixed_send, warnings_send, errors_send = await _validate_and_fix_processor_properties(
            "org.apache.nifi.processors.standard.HandleHttpResponse",
            sendresponse_properties,
            service_map,
            "test-pg-id",
            mock_client,
            logger, 
            "test-user-id",
            "test-action-id"
        )
        
        # Verify fixes for ReceiveHTTP
        assert "HTTP Context Map" in fixed_receive
        assert fixed_receive["HTTP Context Map"] == "88a1e773-0197-1000-d97d-6f168dc293e0"
        
        # Verify fixes for SendResponse
        assert "HTTP Context Map" in fixed_send  
        assert fixed_send["HTTP Context Map"] == "88a1e773-0197-1000-d97d-6f168dc293e0"
        
        # Verify warnings were generated
        assert any("missing required service reference" in w.lower() for w in warnings_receive)
        assert any("missing required service reference" in w.lower() for w in warnings_send)


class TestEfficiencyMetrics:
    """Test that efficiency improvements provide measurable benefits."""
    
    def test_property_validation_prevents_multiple_calls(self):
        """Test that property validation prevents the multiple-call scenario from logs."""
        # Simulate the problematic properties from the logs
        problematic_properties = {
            "Script Engine": "Groovy",
            "Script File": "",  # This caused validation errors
            "Module Directory": "",  # This caused validation errors
            "Script Body": "session.read(flowFile, callback)"  # Had scope issues - no session.get()
        }
        
        # Before improvements: would require multiple API calls
        # After improvements: single call with auto-correction
        
        warnings = _validate_script_processor_properties(
            "org.apache.nifi.processors.script.ExecuteScript",
            problematic_properties
        )
        
        # Verify all issues are caught and fixed in one pass
        assert problematic_properties["Script File"] is None  # Auto-fixed
        assert problematic_properties["Module Directory"] is None  # Auto-fixed
        assert len(warnings) >= 2  # Warnings for fixes (Script File + Module Directory)
        
    def test_error_analysis_reduces_trial_and_error(self):
        """Test that error analysis provides direct solutions."""
        # Simulate error from the logs
        error_message = """
        ExecuteScript failed to process due to groovy.lang.MissingPropertyException: 
        No such property: flowFile for class: Script960
        """
        
        patterns = _analyze_error_patterns(
            error_message,
            "org.apache.nifi.processors.script.ExecuteScript"
        )
        
        suggestions = _generate_debugging_suggestions(
            patterns,
            "org.apache.nifi.processors.script.ExecuteScript"
        )
        
        # Should provide direct solution instead of trial-and-error
        flowfile_suggestion = next(
            (s for s in suggestions if "FlowFile Scope" in s["issue"]),
            None
        )
        
        assert flowfile_suggestion is not None
        assert "final FlowFile ff = session.get()" in flowfile_suggestion["example"]
        # This exact pattern was the fix that eventually worked in the logs 
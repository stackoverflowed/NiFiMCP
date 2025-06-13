#!/usr/bin/env python3
"""
Example: Using timeout and continuation features with list_nifi_objects

This example demonstrates how to handle large NiFi hierarchies that might timeout
using the new timeout and continuation token features.
"""

import asyncio
import json
from typing import Dict, Any, Optional

async def list_with_timeout_example():
    """
    Example of using list_nifi_objects with timeout support.
    
    This would be called via the MCP API in practice, but shows the pattern.
    """
    
    # Example 1: Basic timeout usage
    print("=== Example 1: Basic timeout usage ===")
    
    # This would be an actual API call in practice
    example_call_1 = {
        "tool_name": "list_nifi_objects",
        "arguments": {
            "object_type": "processors",
            "process_group_id": None,  # Root group
            "search_scope": "recursive",
            "timeout_seconds": 30.0  # 30 second timeout
        }
    }
    
    print(f"API Call: {json.dumps(example_call_1, indent=2)}")
    print("Expected response structure:")
    print("""
    {
        "results": [...],
        "completed": true/false,
        "continuation_token": "pg-id:depth" or null,
        "processed_count": 42,
        "timeout_occurred": true/false
    }
    """)
    
    # Example 2: Using continuation token
    print("\n=== Example 2: Using continuation token ===")
    
    # Simulate a response that timed out
    simulated_timeout_response = {
        "results": [
            {
                "process_group_id": "root-pg-id",
                "process_group_name": "Root Process Group",
                "objects": [
                    {"id": "proc-1", "name": "Processor 1", "type": "GenerateFlowFile"},
                    {"id": "proc-2", "name": "Processor 2", "type": "LogAttribute"}
                ]
            }
        ],
        "completed": False,
        "continuation_token": "child-pg-id:2",
        "processed_count": 15,
        "timeout_occurred": True
    }
    
    print("Previous response (timed out):")
    print(json.dumps(simulated_timeout_response, indent=2))
    
    # Continue from where we left off
    continuation_call = {
        "tool_name": "list_nifi_objects",
        "arguments": {
            "object_type": "processors",
            "process_group_id": None,
            "search_scope": "recursive",
            "timeout_seconds": 30.0,
            "continuation_token": simulated_timeout_response["continuation_token"]
        }
    }
    
    print(f"\nContinuation API Call: {json.dumps(continuation_call, indent=2)}")
    
    # Example 3: Using the new streaming tool
    print("\n=== Example 3: Using list_nifi_objects_with_streaming ===")
    
    streaming_call = {
        "tool_name": "list_nifi_objects_with_streaming",
        "arguments": {
            "object_type": "processors",
            "process_group_id": None,
            "timeout_seconds": 60.0,  # Longer timeout
            "max_depth": 15,
            "batch_size": 100
        }
    }
    
    print(f"Streaming API Call: {json.dumps(streaming_call, indent=2)}")
    print("Expected enhanced response structure:")
    print("""
    {
        "results": [...],
        "completed": true/false,
        "continuation_token": "pg-id:depth" or null,
        "processed_count": 156,
        "timeout_occurred": true/false,
        "total_time_seconds": 45.7,
        "progress_info": {
            "object_type": "processors",
            "start_pg_id": "root-pg-id",
            "max_depth": 15,
            "timeout_seconds": 60.0
        }
    }
    """)

def demonstrate_continuation_pattern():
    """
    Demonstrates the recommended pattern for handling large hierarchies.
    """
    print("\n=== Recommended Pattern for Large Hierarchies ===")
    
    pattern_code = '''
async def get_all_processors_with_continuation(api_client, pg_id=None, timeout_per_call=30.0):
    """
    Get all processors from a large hierarchy using continuation tokens.
    """
    all_results = []
    continuation_token = None
    total_processed = 0
    
    while True:
        # Make API call with current continuation token
        response = await api_client.call_tool("list_nifi_objects_with_streaming", {
            "object_type": "processors",
            "process_group_id": pg_id,
            "timeout_seconds": timeout_per_call,
            "continuation_token": continuation_token
        })
        
        # Collect results
        if "results" in response:
            if isinstance(response["results"], list):
                all_results.extend(response["results"])
            else:
                all_results.extend(response["results"].get("results", []))
        
        total_processed += response.get("processed_count", 0)
        
        # Check if we're done
        if response.get("completed", True):
            print(f"✅ Completed! Total processed: {total_processed} groups")
            break
        
        # Get continuation token for next iteration
        continuation_token = response.get("continuation_token")
        if not continuation_token:
            print("⚠️ No continuation token but not completed. Stopping.")
            break
            
        print(f"⏳ Continuing from token: {continuation_token}")
        print(f"   Processed so far: {total_processed} groups")
        
        # Optional: Add delay between calls to be nice to the server
        await asyncio.sleep(1.0)
    
    return all_results

# Usage:
# all_processors = await get_all_processors_with_continuation(api_client)
'''
    
    print(pattern_code)

def show_timeout_benefits():
    """
    Shows the benefits of the timeout approach.
    """
    print("\n=== Benefits of Timeout Approach ===")
    
    benefits = [
        "✅ No lost work - partial results are always returned",
        "✅ Predictable response times - never wait indefinitely", 
        "✅ Resumable operations - continue from where you left off",
        "✅ Progress tracking - know how much has been processed",
        "✅ Parallel processing - faster than sequential traversal",
        "✅ Memory efficient - doesn't load entire hierarchy at once",
        "✅ Client timeout friendly - works with HTTP client timeouts",
        "✅ Backward compatible - existing calls still work"
    ]
    
    for benefit in benefits:
        print(f"  {benefit}")
    
    print("\n=== Configuration Recommendations ===")
    
    recommendations = [
        "🔧 For small hierarchies (< 50 groups): Use original list_nifi_objects without timeout",
        "🔧 For medium hierarchies (50-200 groups): Use timeout_seconds=30-60",
        "🔧 For large hierarchies (200+ groups): Use list_nifi_objects_with_streaming",
        "🔧 For very deep hierarchies: Increase max_depth parameter",
        "🔧 For slow networks: Increase timeout_seconds",
        "🔧 For batch processing: Use continuation pattern with delays"
    ]
    
    for rec in recommendations:
        print(f"  {rec}")

if __name__ == "__main__":
    print("NiFi MCP Timeout Handling Examples")
    print("=" * 50)
    
    asyncio.run(list_with_timeout_example())
    demonstrate_continuation_pattern()
    show_timeout_benefits()
    
    print("\n" + "=" * 50)
    print("For more information, see the updated tool documentation:")
    print("- list_nifi_objects: Enhanced with timeout_seconds and continuation_token")
    print("- list_nifi_objects_with_streaming: New tool optimized for large hierarchies") 
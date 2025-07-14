import httpx
import os
import sys
from loguru import logger
import uuid
from typing import Any, List, Union # Import Any, List, Union

# This call_tool function is a candidate to be moved here.
# It will require logger and base_url to be passed in or be accessible.
# For now, let's define it to accept them as parameters.

async def call_tool(
    client: httpx.AsyncClient,
    base_url: str,
    tool_name: str,
    arguments: dict,
    headers: dict,
    custom_logger: Any = logger # Changed to Any
) -> Union[dict, List[dict]]:
    """Helper function to make POST requests to the tool endpoint.
    
    Returns:
        The response from the tool. For backward compatibility with existing tests,
        if the response is not already a list, it will be wrapped in a list.
    """
    url = f"{base_url}/tools/{tool_name}"
    payload = {"arguments": arguments}
    custom_logger.debug(f"Calling POST {url} with payload: {payload}")
    try:
        response = await client.post(url, json=payload, headers=headers, timeout=30.0) # Add timeout
        response.raise_for_status() # Raise exception for 4xx/5xx errors
        json_response = response.json()
        custom_logger.debug(f"Received {response.status_code} response: {json_response}")
        
        # For backward compatibility with existing tests that expect list responses,
        # wrap single results in a list if they're not already a list
        if not isinstance(json_response, list):
            custom_logger.debug(f"Wrapping single result in list for backward compatibility: {type(json_response)}")
            return [json_response]
        
        return json_response
    except httpx.HTTPStatusError as e:
        custom_logger.error(f"HTTP error calling tool '{tool_name}': {e.response.status_code} - {e.response.text}")
        raise # Re-raise after logging
    except httpx.RequestError as e:
        custom_logger.error(f"Request error calling tool '{tool_name}': {e}")
        raise # Re-raise after logging
    except Exception as e:
        custom_logger.error(f"Unexpected error calling tool '{tool_name}': {e}", exc_info=True)
        raise # Re-raise after logging

# Placeholder for other utility functions that might be identified during refactoring.
# For example, a function to create a standard test processor,
# or to poll for a component's status.

def get_test_run_id() -> str:
    """Generates a short unique ID for a test run."""
    return str(uuid.uuid4())[:8] 
import asyncio
from typing import List, Dict, Optional, Any, Union, Literal
from datetime import datetime, timedelta

# Import necessary components from parent/utils
from loguru import logger
# Import mcp ONLY
from ..core import mcp
# Removed nifi_api_client import
# Import context variables
from ..request_context import current_nifi_client, current_request_logger # Added

from .utils import (
    tool_phases,
    # ensure_authenticated # Removed
    # No other utils needed for this specific tool
)
from nifi_mcp_server.nifi_client import NiFiClient, NiFiAuthenticationError
from mcp.server.fastmcp.exceptions import ToolError
from config import settings as mcp_settings


# Rate limiting storage for expert help
_expert_help_usage = {}  # {request_id: [timestamp1, timestamp2, ...]}
_EXPERT_HELP_LIMIT = 2  # Max calls per request ID
_EXPERT_HELP_WINDOW_HOURS = 24  # Reset window

def _cleanup_expired_expert_help_usage():
    """Clean up expired rate limit entries."""
    current_time = datetime.now()
    cutoff_time = current_time - timedelta(hours=_EXPERT_HELP_WINDOW_HOURS)
    
    for request_id in list(_expert_help_usage.keys()):
        # Filter out expired timestamps
        _expert_help_usage[request_id] = [
            timestamp for timestamp in _expert_help_usage[request_id]
            if timestamp > cutoff_time
        ]
        # Remove entry if no valid timestamps remain
        if not _expert_help_usage[request_id]:
            del _expert_help_usage[request_id]

def _check_expert_help_rate_limit(request_id: str) -> bool:
    """Check if the request ID has exceeded the expert help rate limit."""
    if not request_id or request_id == "-":
        return True  # Allow if no request ID (shouldn't happen in normal usage)
    
    _cleanup_expired_expert_help_usage()
    
    usage_count = len(_expert_help_usage.get(request_id, []))
    return usage_count < _EXPERT_HELP_LIMIT

def _record_expert_help_usage(request_id: str):
    """Record an expert help usage for the given request ID."""
    if not request_id or request_id == "-":
        return
    
    if request_id not in _expert_help_usage:
        _expert_help_usage[request_id] = []
    
    _expert_help_usage[request_id].append(datetime.now())

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
        processor_name: The display name (e.g., 'GenerateFlowFile'). Case-insensitive.
        bundle_artifact_filter: Optional. Filters by bundle artifact (e.g., 'nifi-standard-nar'). Case-insensitive.

    Returns:
        - If one match: A dictionary with details.
        - If multiple matches: A list of matching dictionaries.
        - If no matches: An empty list.
    """
    # Get client and logger from context
    nifi_client: Optional[NiFiClient] = current_nifi_client.get()
    local_logger = current_request_logger.get() or logger
    if not nifi_client:
        raise ToolError("NiFi client context is not set. This tool requires the X-Nifi-Server-Id header.")
    if not local_logger:
         raise ToolError("Request logger context is not set.")
         
    # Authentication handled by factory
    local_logger = local_logger.bind(processor_name=processor_name, bundle_artifact_filter=bundle_artifact_filter)
    
    local_logger.info(f"Looking up processor type details for name: '{processor_name}'")
    try:
        nifi_req = {"operation": "get_processor_types"}
        local_logger.bind(interface="nifi", direction="request", data=nifi_req).debug("Calling NiFi API")
        all_types = await nifi_client.get_processor_types()
        nifi_resp = {"processor_type_count": len(all_types)}
        local_logger.bind(interface="nifi", direction="response", data=nifi_resp).debug("Received from NiFi API")

        matches = []
        search_name_lower = processor_name.lower()
        filter_artifact_lower = bundle_artifact_filter.lower() if bundle_artifact_filter else None

        for proc_type in all_types:
            # Extract relevant fields, ensuring they are strings and lowercased
            title = proc_type.get("title", "").lower()
            type_str = proc_type.get("type", "").lower()
            description = proc_type.get("description", "").lower()
            tags = [tag.lower() for tag in proc_type.get("tags", [])] # Lowercase all tags

            # Check if the search term is present in any relevant field
            name_match_found = (
                search_name_lower in title or
                search_name_lower in type_str or
                search_name_lower in description or
                any(search_name_lower in tag for tag in tags)
            )

            if name_match_found:
                # Apply bundle artifact filter if specified
                if filter_artifact_lower:
                    bundle = proc_type.get("bundle", {})
                    artifact = bundle.get("artifact", "").lower()
                    if artifact == filter_artifact_lower: # Check lowercase artifact
                        matches.append(_format_processor_type_summary(proc_type))
                else:
                    matches.append(_format_processor_type_summary(proc_type))

        local_logger.info(f"Found {len(matches)} match(es) for '{processor_name}'")
        
        if len(matches) == 1:
            return matches[0]
        else:
            return matches
        
    except (NiFiAuthenticationError, ConnectionError, ToolError) as e:
        local_logger.error(f"API/Tool error looking up processor types: {e}", exc_info=False)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received error from NiFi API")
        raise ToolError(f"Failed to lookup NiFi processor types: {e}")
    except Exception as e:
        local_logger.error(f"Unexpected error looking up processor types: {e}", exc_info=True)
        local_logger.bind(interface="nifi", direction="response", data={"error": str(e)}).debug("Received unexpected error from NiFi API")
        raise ToolError(f"An unexpected error occurred: {e}")


@mcp.tool()
@tool_phases(["Build", "Modify", "Review", "Query"])
async def get_expert_help(question: str) -> str:
    """
    Gets expert help from a configured LLM for complex questions or problems.
    
    Use this when you need additional expertise or a different perspective on a challenging
    NiFi configuration, debugging, or design question. The expert model can provide insights
    without access to the current conversation history.
    
    Rate limited to 2 calls per request session. If the limit is exceeded, explain your
    problem directly to the user instead.

    Args:
        question: The specific question or problem you need expert help with. Be clear and detailed.

    Returns:
        Expert response as plain text, or an error message if the service is unavailable.
    """
    # Get logger from context (no NiFi client needed for this tool)
    local_logger = current_request_logger.get() or logger
    if not local_logger:
        raise ToolError("Request logger context is not set.")
    
    # Get request context for rate limiting
    from config.logging_setup import request_context
    context_data = request_context.get()
    request_id = context_data.get('user_request_id', '-') if context_data else '-'
    
    local_logger = local_logger.bind(question_preview=question[:100] + "..." if len(question) > 100 else question)
    local_logger.info(f"Expert help requested for request_id: {request_id}")
    
    # Check if expert help is available
    if not mcp_settings.is_expert_help_available():
        provider, model = mcp_settings.get_expert_help_config()
        if not provider or not model:
            error_msg = "Expert help is not configured. Please set 'llm.expert_help_model.provider' and 'llm.expert_help_model.model' in config.yaml"
        else:
            error_msg = f"Expert help is configured ({provider}:{model}) but the API key for {provider} is not available."
        local_logger.warning(error_msg)
        return f"Expert help unavailable: {error_msg}"
    
    # Check rate limiting
    if not _check_expert_help_rate_limit(request_id):
        usage_count = len(_expert_help_usage.get(request_id, []))
        error_msg = f"Expert help rate limit exceeded ({usage_count}/{_EXPERT_HELP_LIMIT} calls used). Please explain your current problem or question directly to the user instead of using expert help."
        local_logger.warning(error_msg)
        return error_msg
    
    # Record usage
    _record_expert_help_usage(request_id)
    
    # Validate question
    if not question or not question.strip():
        raise ToolError("Question cannot be empty.")
    
    if len(question) > 2000:  # Reasonable limit
        raise ToolError("Question is too long. Please summarize your question to under 2000 characters.")
    
    try:
        provider, model = mcp_settings.get_expert_help_config()
        local_logger.info(f"Calling expert help: {provider}:{model}")
        
        # Simple message format - just the question as a user message
        messages = [{"role": "user", "content": question.strip()}]
        
        # Call the expert model directly using the appropriate provider
        expert_request_data = {
            "provider": provider,
            "model": model,
            "messages": messages,
            "tools": None
        }
        local_logger.bind(interface="llm", direction="request", data=expert_request_data).debug("Calling Expert LLM")
        
        # Initialize the appropriate client and make the call directly
        if provider.lower() == "perplexity":
            from openai import OpenAI
            import config.settings as config
            
            if not config.PERPLEXITY_API_KEY:
                return "Expert help unavailable: Perplexity API key not configured."
            
            client = OpenAI(
                api_key=config.PERPLEXITY_API_KEY,
                base_url="https://api.perplexity.ai",
                timeout=60.0,
                max_retries=2
            )
            
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.7,
                max_tokens=1000
            )
            
            content = response.choices[0].message.content if response.choices else ""
            
        elif provider.lower() == "openai":
            from openai import OpenAI
            import config.settings as config
            
            if not config.OPENAI_API_KEY:
                return "Expert help unavailable: OpenAI API key not configured."
            
            client = OpenAI(
                api_key=config.OPENAI_API_KEY,
                timeout=60.0,
                max_retries=2
            )
            
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.7,
                max_tokens=1000
            )
            
            content = response.choices[0].message.content if response.choices else ""
            
        elif provider.lower() == "anthropic":
            try:
                import anthropic
                import config.settings as config
                
                if not config.ANTHROPIC_API_KEY:
                    return "Expert help unavailable: Anthropic API key not configured."
                
                client = anthropic.Anthropic(
                    api_key=config.ANTHROPIC_API_KEY,
                    timeout=60.0,
                    max_retries=2
                )
                
                response = client.messages.create(
                    model=model,
                    max_tokens=1000,
                    temperature=0.7,
                    messages=messages
                )
                
                content = response.content[0].text if response.content else ""
                
            except ImportError:
                return "Expert help unavailable: Anthropic library not installed."
            
        elif provider.lower() == "gemini":
            try:
                import google.generativeai as genai
                import config.settings as config
                
                if not config.GOOGLE_API_KEY:
                    return "Expert help unavailable: Google API key not configured."
                
                genai.configure(api_key=config.GOOGLE_API_KEY)
                model_instance = genai.GenerativeModel(model)
                
                # Convert messages to Gemini format
                prompt = messages[0]["content"] if messages else question
                response = model_instance.generate_content(prompt)
                
                content = response.text if response.text else ""
                
            except ImportError:
                return "Expert help unavailable: Google Generative AI library not installed."
            
        else:
            return f"Expert help unavailable: Unsupported provider '{provider}'."
        
        response = content
        
        expert_response_data = {"content_length": len(response) if response else 0}
        local_logger.bind(interface="llm", direction="response", data=expert_response_data).debug("Received from Expert LLM")
        
        if not response:
            return "Expert help received an empty response. Please try rephrasing your question."
        
        local_logger.info(f"Expert help completed successfully for request_id: {request_id}")
        return response
        
    except Exception as e:
        local_logger.error(f"Error getting expert help: {e}", exc_info=True)
        expert_error_data = {"error": str(e)}
        local_logger.bind(interface="llm", direction="response", data=expert_error_data).debug("Received error from Expert LLM")
        return f"Expert help encountered an error: {e}. Please try again or ask your question directly."

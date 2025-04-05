import os
# import logging # Remove standard logging
from loguru import logger # Import Loguru logger
import httpx
from dotenv import load_dotenv
import uuid # Import uuid for client ID generation
from typing import Optional, Dict, Any # Add typing imports

# Load environment variables from .env file
load_dotenv()

# Set up logging - REMOVED standard logging setup
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

class NiFiAuthenticationError(Exception):
    """Raised when there is an error authenticating with NiFi."""
    pass

class NiFiClient:
    """A simple asynchronous client for the NiFi REST API."""

    def __init__(self, base_url=None, username=None, password=None, tls_verify=True):
        self.base_url = base_url or os.getenv("NIFI_API_URL")
        self.username = username or os.getenv("NIFI_USERNAME")
        self.password = password or os.getenv("NIFI_PASSWORD")
        self.tls_verify = tls_verify if os.getenv("NIFI_TLS_VERIFY") is None else os.getenv("NIFI_TLS_VERIFY").lower() == "true"
        self._client = None
        self._token = None
        # Generate a unique client ID for this instance, used for revisions
        self._client_id = str(uuid.uuid4())
        logger.info(f"NiFiClient initialized with client ID: {self._client_id}")

    @property
    def is_authenticated(self) -> bool:
        """Checks if the client currently holds an authentication token."""
        return self._token is not None

    async def _get_client(self):
        """Returns an httpx client instance, configuring auth if token exists."""
        # Always create a new client instance to ensure headers are fresh,
        # especially after authentication. If performance becomes an issue,
        # we could optimize, but this ensures correctness.
        if self._client:
             await self._client.aclose() # Ensure old connection is closed if recreating
             self._client = None

        headers = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
            # NiFi often requires client ID for state changes, let's check if we need it here
            # Might need to parse initial response or call another endpoint if needed.

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            verify=self.tls_verify,
            headers=headers,
            timeout=30.0 # Keep timeout
        )
        return self._client

    async def authenticate(self):
        """Authenticates with NiFi and stores the token."""
        # Use a temporary client for the auth request itself, as it doesn't need the token header
        async with httpx.AsyncClient(base_url=self.base_url, verify=self.tls_verify) as auth_client:
            endpoint = "/access/token"
            try:
                logger.info(f"Authenticating with NiFi at {self.base_url}{endpoint}")
                response = await auth_client.post(
                    endpoint,
                    data={"username": self.username, "password": self.password},
                    headers={"Content-Type": "application/x-www-form-urlencoded"} # Correct header for form data
                )
                response.raise_for_status()
                self._token = response.text # Store the token
                logger.info("Authentication successful.")

                # Force recreation of the main client with the token on next call to _get_client
                if self._client:
                    await self._client.aclose()
                self._client = None

            except httpx.HTTPStatusError as e:
                logger.error(f"Authentication failed: {e.response.status_code} - {e.response.text}")
                raise NiFiAuthenticationError(f"Authentication failed: {e.response.status_code}") from e
            except httpx.RequestError as e:
                logger.error(f"An error occurred during authentication: {e}")
                raise NiFiAuthenticationError(f"An error occurred during authentication: {e}") from e
            except Exception as e:
                logger.error(f"An unexpected error occurred during authentication: {e}", exc_info=True)
                raise NiFiAuthenticationError(f"An unexpected error occurred during authentication: {e}")

    async def close(self):
        """Closes the underlying httpx client."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("NiFi client connection closed.")

    # --- Placeholder for other API methods ---
    async def get_root_process_group_id(self) -> str:
        """Fetches the ID of the root process group."""
        if not self._token:
            raise NiFiAuthenticationError("Client is not authenticated. Call authenticate() first.")

        client = await self._get_client()
        endpoint = "/flow/process-groups/root"
        try:
            logger.info(f"Fetching root process group info from {self.base_url}{endpoint}")
            response = await client.get(endpoint)
            response.raise_for_status()
            data = response.json()
            # According to API docs, the response is ProcessGroupFlowEntity
            # which contains the id directly or within processGroupFlow.id
            root_id = data.get("id")
            if not root_id and "processGroupFlow" in data:
                 root_id = data["processGroupFlow"].get("id") # Check nested structure too

            if not root_id:
                 logger.error(f"Could not find root process group ID in response: {data}")
                 raise ValueError("Root process group ID not found in API response.")

            logger.info(f"Found root process group ID: {root_id}")
            return root_id

        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to get root process group: {e.response.status_code} - {e.response.text}")
            raise ConnectionError(f"Failed to get root process group: {e.response.status_code}") from e
        except (httpx.RequestError, ValueError) as e:
            logger.error(f"Error getting root process group: {e}")
            raise ConnectionError(f"Error getting root process group: {e}") from e
        except Exception as e:
             logger.error(f"An unexpected error occurred getting root process group ID: {e}", exc_info=True)
             raise ConnectionError(f"An unexpected error occurred getting root process group ID: {e}") from e

    async def list_processors(self, process_group_id: str) -> list[dict]:
        """Lists processors within a specified process group."""
        if not self._token:
            raise NiFiAuthenticationError("Client is not authenticated. Call authenticate() first.")

        client = await self._get_client()
        endpoint = f"/process-groups/{process_group_id}/processors"
        try:
            logger.info(f"Fetching processors for group {process_group_id} from {self.base_url}{endpoint}")
            response = await client.get(endpoint)
            response.raise_for_status()
            data = response.json()
            # The response is typically a ProcessorsEntity which has a 'processors' key containing a list
            processors = data.get("processors", [])
            logger.info(f"Found {len(processors)} processors in group {process_group_id}.")
            return processors

        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to list processors for group {process_group_id}: {e.response.status_code} - {e.response.text}")
            raise ConnectionError(f"Failed to list processors: {e.response.status_code}") from e
        except (httpx.RequestError, ValueError) as e:
            logger.error(f"Error listing processors for group {process_group_id}: {e}")
            raise ConnectionError(f"Error listing processors: {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred listing processors: {e}", exc_info=True)
            raise ConnectionError(f"An unexpected error occurred listing processors: {e}") from e

    async def create_processor(
        self,
        process_group_id: str,
        processor_type: str,
        name: str,
        position: Dict[str, float],
        config: Optional[Dict[str, Any]] = None
    ) -> dict:
        """Creates a new processor in the specified process group."""
        if not self._token:
            raise NiFiAuthenticationError("Client is not authenticated. Call authenticate() first.")

        client = await self._get_client()
        endpoint = f"/process-groups/{process_group_id}/processors"

        # Construct the request body (ProcessorEntity)
        request_body = {
            "revision": {
                "clientId": self._client_id,
                "version": 0
            },
            "component": {
                "type": processor_type,
                "name": name,
                "position": position,
            }
        }
        # Add config if provided (simplified - real config might need more structure)
        if config:
            request_body["component"]["config"] = {"properties": config}

        try:
            logger.info(f"Creating processor '{name}' ({processor_type}) in group {process_group_id} at {position}")
            response = await client.post(endpoint, json=request_body)
            response.raise_for_status() # Checks for 4xx/5xx errors
            created_processor_data = response.json()
            logger.info(f"Successfully created processor '{name}' with ID: {created_processor_data.get('id')}")
            return created_processor_data # Return the full response body

        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to create processor '{name}': {e.response.status_code} - {e.response.text}")
            raise ConnectionError(f"Failed to create processor: {e.response.status_code}, {e.response.text}") from e
        except (httpx.RequestError, ValueError) as e:
            logger.error(f"Error creating processor '{name}': {e}")
            raise ConnectionError(f"Error creating processor: {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred creating processor '{name}': {e}", exc_info=True)
            raise ConnectionError(f"An unexpected error occurred creating processor: {e}") from e

    async def create_connection(
        self,
        process_group_id: str,
        source_id: str,
        target_id: str,
        relationships: list[str],
        source_type: str = "PROCESSOR", # Usually PROCESSOR
        target_type: str = "PROCESSOR", # Usually PROCESSOR
        name: Optional[str] = None
    ) -> dict:
        """Creates a connection between two components in a process group."""
        if not self._token:
            raise NiFiAuthenticationError("Client is not authenticated. Call authenticate() first.")

        client = await self._get_client()
        endpoint = f"/process-groups/{process_group_id}/connections"

        # Construct the request body (ConnectionEntity)
        request_body = {
            "revision": {
                "clientId": self._client_id,
                "version": 0
            },
            "component": {
                "name": name or "", # Optional connection name
                "source": {
                    "id": source_id,
                    "groupId": process_group_id,
                    "type": source_type.upper()
                },
                "destination": {
                    "id": target_id,
                    "groupId": process_group_id,
                    "type": target_type.upper()
                },
                "selectedRelationships": relationships
                # Can add other config like flowfileExpiration, backPressureObjectThreshold etc. if needed
            }
        }

        try:
            logger.info(f"Creating connection from {source_id} ({relationships}) to {target_id} in group {process_group_id}")
            response = await client.post(endpoint, json=request_body)
            response.raise_for_status()
            created_connection_data = response.json()
            logger.info(f"Successfully created connection with ID: {created_connection_data.get('id')}")
            return created_connection_data

        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to create connection: {e.response.status_code} - {e.response.text}")
            raise ConnectionError(f"Failed to create connection: {e.response.status_code}, {e.response.text}") from e
        except (httpx.RequestError, ValueError) as e:
            logger.error(f"Error creating connection: {e}")
            raise ConnectionError(f"Error creating connection: {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred creating connection: {e}", exc_info=True)
            raise ConnectionError(f"An unexpected error occurred creating connection: {e}") from e

    async def get_processor_details(self, processor_id: str) -> dict:
        """Fetches the details and configuration of a specific processor."""
        if not self._token:
            raise NiFiAuthenticationError("Client is not authenticated. Call authenticate() first.")

        client = await self._get_client()
        endpoint = f"/processors/{processor_id}"
        try:
            logger.info(f"Fetching details for processor {processor_id} from {self.base_url}{endpoint}")
            response = await client.get(endpoint)
            response.raise_for_status()
            processor_details = response.json()
            logger.info(f"Successfully fetched details for processor {processor_id}")
            return processor_details

        except httpx.HTTPStatusError as e:
            # Handle 404 Not Found specifically
            if e.response.status_code == 404:
                logger.warning(f"Processor with ID {processor_id} not found.")
                raise ValueError(f"Processor with ID {processor_id} not found.") from e # Raise ValueError for not found
            else:
                logger.error(f"Failed to get details for processor {processor_id}: {e.response.status_code} - {e.response.text}")
                raise ConnectionError(f"Failed to get processor details: {e.response.status_code}, {e.response.text}") from e
        except (httpx.RequestError, ValueError) as e:
            logger.error(f"Error getting details for processor {processor_id}: {e}")
            raise ConnectionError(f"Error getting processor details: {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred getting processor details for {processor_id}: {e}", exc_info=True)
            raise ConnectionError(f"An unexpected error occurred getting processor details: {e}") from e

    async def delete_processor(self, processor_id: str, version: int) -> bool:
        """Deletes a processor given its ID and current revision version."""
        if not self._token:
            raise NiFiAuthenticationError("Client is not authenticated. Call authenticate() first.")

        client = await self._get_client()
        # The version must be passed as a query parameter, along with the client ID
        endpoint = f"/processors/{processor_id}?version={version}&clientId={self._client_id}"

        try:
            logger.info(f"Attempting to delete processor {processor_id} (version {version}) using {self.base_url}{endpoint}")
            response = await client.delete(endpoint)
            response.raise_for_status() # Raises HTTPStatusError for 4xx/5xx

            # Check if deletion was successful (usually returns 200 OK with the entity deleted)
            if response.status_code == 200:
                 logger.info(f"Successfully deleted processor {processor_id}.")
                 # We could return the response JSON, but a boolean might suffice
                 return True
            else:
                 # This case might not be reachable if raise_for_status is effective
                 logger.warning(f"Processor deletion for {processor_id} returned status {response.status_code}, but expected 200.")
                 return False

        except httpx.HTTPStatusError as e:
            # Handle specific errors like 404 (Not Found) or 409 (Conflict - likely wrong version)
            if e.response.status_code == 404:
                 logger.warning(f"Processor {processor_id} not found for deletion.")
                 # Consider if this should be True (it's already gone) or False/raise error
                 return False # Treat as failure to delete *now*
            elif e.response.status_code == 409:
                 logger.error(f"Conflict deleting processor {processor_id}. Check revision version ({version}). Response: {e.response.text}")
                 raise ValueError(f"Conflict deleting processor {processor_id}. Ensure correct version ({version}) is used.") from e
            else:
                 logger.error(f"Failed to delete processor {processor_id}: {e.response.status_code} - {e.response.text}")
                 raise ConnectionError(f"Failed to delete processor: {e.response.status_code}, {e.response.text}") from e
        except httpx.RequestError as e:
            logger.error(f"Error deleting processor {processor_id}: {e}")
            raise ConnectionError(f"Error deleting processor: {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred deleting processor {processor_id}: {e}", exc_info=True)
            raise ConnectionError(f"An unexpected error occurred deleting processor: {e}") from e

    async def list_connections(self, process_group_id: str) -> list[dict]:
        """Lists connections within a specified process group."""
        if not self._token:
            raise NiFiAuthenticationError("Client is not authenticated. Call authenticate() first.")

        client = await self._get_client()
        endpoint = f"/process-groups/{process_group_id}/connections"
        try:
            logger.info(f"Fetching connections for group {process_group_id} from {self.base_url}{endpoint}")
            response = await client.get(endpoint)
            response.raise_for_status()
            data = response.json()
            # The response is typically a ConnectionsEntity which has a 'connections' key containing a list
            connections = data.get("connections", [])
            logger.info(f"Found {len(connections)} connections in group {process_group_id}.")
            return connections

        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to list connections for group {process_group_id}: {e.response.status_code} - {e.response.text}")
            raise ConnectionError(f"Failed to list connections: {e.response.status_code}") from e
        except (httpx.RequestError, ValueError) as e:
            logger.error(f"Error listing connections for group {process_group_id}: {e}")
            raise ConnectionError(f"Error listing connections: {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred listing connections: {e}", exc_info=True)
            raise ConnectionError(f"An unexpected error occurred listing connections: {e}") from e

    async def delete_connection(self, connection_id: str, version: int) -> bool:
        """Deletes a connection given its ID and current revision version."""
        if not self._token:
            raise NiFiAuthenticationError("Client is not authenticated. Call authenticate() first.")

        client = await self._get_client()
        # The version must be passed as a query parameter, along with the client ID
        endpoint = f"/connections/{connection_id}?version={version}&clientId={self._client_id}"

        try:
            logger.info(f"Attempting to delete connection {connection_id} (version {version}) using {self.base_url}{endpoint}")
            response = await client.delete(endpoint)
            response.raise_for_status() # Raises HTTPStatusError for 4xx/5xx

            if response.status_code == 200:
                 logger.info(f"Successfully deleted connection {connection_id}.")
                 return True
            else:
                 logger.warning(f"Connection deletion for {connection_id} returned status {response.status_code}, but expected 200.")
                 return False

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                 logger.warning(f"Connection {connection_id} not found for deletion.")
                 return False
            elif e.response.status_code == 409:
                 logger.error(f"Conflict deleting connection {connection_id}. Check revision version ({version}). Response: {e.response.text}")
                 raise ValueError(f"Conflict deleting connection {connection_id}. Ensure correct version ({version}) is used.") from e
            else:
                 logger.error(f"Failed to delete connection {connection_id}: {e.response.status_code} - {e.response.text}")
                 raise ConnectionError(f"Failed to delete connection: {e.response.status_code}, {e.response.text}") from e
        except httpx.RequestError as e:
            logger.error(f"Error deleting connection {connection_id}: {e}")
            raise ConnectionError(f"Error deleting connection: {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred deleting connection {connection_id}: {e}", exc_info=True)
            raise ConnectionError(f"An unexpected error occurred deleting connection: {e}") from e

    async def update_processor_config(
        self,
        processor_id: str,
        config_properties: Dict[str, Any],
        # state: Optional[str] = None # Optional: Allow changing state (RUNNING, STOPPED) too?
    ) -> dict:
        """Updates the configuration properties of a specific processor."""
        if not self._token:
            raise NiFiAuthenticationError("Client is not authenticated. Call authenticate() first.")

        # 1. Get current processor entity to obtain the latest revision
        logger.info(f"Fetching current details for processor {processor_id} before update.")
        try:
            current_entity = await self.get_processor_details(processor_id)
            current_revision = current_entity["revision"]
            current_component = current_entity["component"]
        except (ValueError, ConnectionError) as e:
            logger.error(f"Failed to fetch processor {processor_id} for update: {e}")
            raise # Re-raise the error (could be ValueError for not found, or ConnectionError)

        # 2. Prepare the update payload
        # Start with the essential component details we need to preserve
        # (like id, parentGroupId, position - NiFi API often requires these)
        update_component = {
            "id": current_component["id"],
            "name": current_component.get("name"),
            "position": current_component.get("position"),
            # Add other fields if necessary, but config is the focus
            "config": {
                "properties": config_properties
                # Consider adding scheduling strategy, period, etc. if needed later
            }
        }
        # Add state change if provided (e.g., "RUNNING" or "STOPPED")
        # if state and state.upper() in ["RUNNING", "STOPPED"]:
        #     update_component["state"] = state.upper()

        update_payload = {
            "revision": current_revision, # Use the fetched revision
            "component": update_component
        }

        # 3. Make the PUT request
        client = await self._get_client()
        endpoint = f"/processors/{processor_id}"
        try:
            logger.info(f"Updating configuration for processor {processor_id} (Version: {current_revision.get('version')}). New props: {config_properties}")
            response = await client.put(endpoint, json=update_payload)
            response.raise_for_status()
            updated_entity = response.json()
            logger.info(f"Successfully updated configuration for processor {processor_id}. New revision: {updated_entity.get('revision', {}).get('version')}")
            return updated_entity

        except httpx.HTTPStatusError as e:
            # Handle 409 Conflict (likely stale revision)
            if e.response.status_code == 409:
                logger.error(f"Conflict updating processor {processor_id}. Revision ({current_revision.get('version')}) likely stale. Response: {e.response.text}")
                raise ValueError(f"Conflict updating processor {processor_id}. Revision mismatch.") from e
            else:
                logger.error(f"Failed to update processor {processor_id}: {e.response.status_code} - {e.response.text}")
                raise ConnectionError(f"Failed to update processor: {e.response.status_code}, {e.response.text}") from e
        except (httpx.RequestError, ValueError) as e:
            logger.error(f"Error updating processor {processor_id}: {e}")
            raise ConnectionError(f"Error updating processor: {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred updating processor {processor_id}: {e}", exc_info=True)
            raise ConnectionError(f"An unexpected error occurred updating processor: {e}") from e

    async def update_processor_state(self, processor_id: str, state: str) -> dict:
        """Starts or stops a specific processor."""
        if not self._token:
            raise NiFiAuthenticationError("Client is not authenticated. Call authenticate() first.")

        normalized_state = state.upper()
        if normalized_state not in ["RUNNING", "STOPPED"]:
            raise ValueError("Invalid state specified. Must be 'RUNNING' or 'STOPPED'.")

        # 1. Get current processor entity to obtain the latest revision
        # We need the revision even just to change the state.
        logger.info(f"Fetching current revision for processor {processor_id} before changing state to {normalized_state}.")
        try:
            # Use get_processor_details as it already handles fetching the entity
            current_entity = await self.get_processor_details(processor_id)
            current_revision = current_entity["revision"]
        except (ValueError, ConnectionError) as e:
            logger.error(f"Failed to fetch processor {processor_id} to update state: {e}")
            raise

        # 2. Prepare the update payload for the run-status endpoint
        update_payload = {
            "revision": current_revision,
            "state": normalized_state,
            "disconnectedNodeAcknowledged": False # Usually required, defaults to false
        }

        # 3. Make the PUT request to the run-status endpoint
        client = await self._get_client()
        endpoint = f"/processors/{processor_id}/run-status"
        try:
            logger.info(f"Setting processor {processor_id} state to {normalized_state} (Version: {current_revision.get('version')}).")
            response = await client.put(endpoint, json=update_payload)
            response.raise_for_status()
            updated_entity = response.json() # The response contains the processor entity with updated status
            logger.info(f"Successfully set processor {processor_id} state to {updated_entity.get('component',{}).get('state', 'UNKNOWN')}. New revision: {updated_entity.get('revision', {}).get('version')}")
            return updated_entity

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:
                logger.error(f"Conflict changing state for processor {processor_id}. Revision ({current_revision.get('version')}) likely stale. Response: {e.response.text}")
                raise ValueError(f"Conflict changing processor state for {processor_id}. Revision mismatch.") from e
            else:
                logger.error(f"Failed to change state for processor {processor_id}: {e.response.status_code} - {e.response.text}")
                raise ConnectionError(f"Failed to change processor state: {e.response.status_code}, {e.response.text}") from e
        except (httpx.RequestError, ValueError) as e:
            logger.error(f"Error changing state for processor {processor_id}: {e}")
            raise ConnectionError(f"Error changing processor state: {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred changing state for processor {processor_id}: {e}", exc_info=True)
            raise ConnectionError(f"An unexpected error occurred changing processor state: {e}") from e

    async def get_parameter_context(self, process_group_id: str) -> list:
        """Get parameter contexts assigned to a process group."""
        if not self._token:
            raise NiFiAuthenticationError("Client is not authenticated. Call authenticate() first.")

        client = await self._get_client()
        
        # First, get the process group entity to find the parameter context
        endpoint = f"/process-groups/{process_group_id}"
        try:
            logger.info(f"Fetching process group {process_group_id} to get parameter context")
            response = await client.get(endpoint)
            response.raise_for_status()
            group_data = response.json()
            
            # Check if the process group has a parameter context
            parameter_context_id = group_data.get("component", {}).get("parameterContext", {}).get("id")
            
            if not parameter_context_id:
                logger.info(f"No parameter context found for process group {process_group_id}")
                return []
                
            # Now get the parameter context details
            endpoint = f"/parameter-contexts/{parameter_context_id}"
            response = await client.get(endpoint)
            response.raise_for_status()
            context_data = response.json()
            
            # Extract parameters
            parameters = []
            for param in context_data.get("component", {}).get("parameters", []):
                parameters.append({
                    "name": param.get("parameter", {}).get("name"),
                    "value": param.get("parameter", {}).get("value"),
                    "description": param.get("parameter", {}).get("description"),
                    "sensitive": param.get("parameter", {}).get("sensitive", False)
                })
            
            logger.info(f"Found {len(parameters)} parameters for process group {process_group_id}")
            return parameters
            
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to get parameter context for process group {process_group_id}: {e.response.status_code} - {e.response.text}")
            raise ConnectionError(f"Failed to get parameter context: {e.response.status_code}") from e
        except (httpx.RequestError, ValueError) as e:
            logger.error(f"Error getting parameter context for process group {process_group_id}: {e}")
            raise ConnectionError(f"Error getting parameter context: {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred getting parameter context: {e}", exc_info=True)
            raise ConnectionError(f"An unexpected error occurred getting parameter context: {e}") from e

# Example usage (for testing this module directly)
async def main():
    client = NiFiClient()
    new_processor_details = None
    generate_flowfile_id = None
    log_attribute_id = None

    try:
        logger.info("--- NiFi Client Test Start ---")
        await client.authenticate()
        root_id = await client.get_root_process_group_id()
        logger.info(f"Successfully fetched Root Process Group ID: {root_id}")

        # --- List existing processors and find relevant ones ---
        logger.info(f"Listing existing processors in root group ({root_id})...")
        processors = await client.list_processors(root_id)
        if processors:
            for proc in processors:
                proc_entity = proc
                proc_id = proc_entity.get("id")
                proc_component = proc_entity.get("component", {})
                proc_name = proc_component.get("name", "")
                proc_type = proc_component.get("type", "")
                logger.info(f"  - Found: ID: {proc_id}, Name: {proc_name}, Type: {proc_type}")
                # Store IDs of processors we might want to connect
                if proc_type == "org.apache.nifi.processors.standard.GenerateFlowFile":
                    generate_flowfile_id = proc_id
                    logger.info(f"    (Found GenerateFlowFile ID: {generate_flowfile_id})")
                elif proc_name == "Test LogAttribute (MCP)": # Find the specific one we created
                    log_attribute_id = proc_id
                    logger.info(f"    (Found Test LogAttribute ID: {log_attribute_id})")
        else:
            logger.info(f"No existing processors found in root group ({root_id}).")

        # --- Create processor if it doesn't exist (idempotency-ish) ---
        if not log_attribute_id:
            logger.info("Test LogAttribute processor not found, attempting to create...")
            new_processor_type = "org.apache.nifi.processors.standard.LogAttribute"
            new_processor_name = "Test LogAttribute (MCP)"
            new_processor_position = {"x": 400.0, "y": 0.0}
            try:
                new_processor_details = await client.create_processor(
                    process_group_id=root_id,
                    processor_type=new_processor_type,
                    name=new_processor_name,
                    position=new_processor_position
                )
                log_attribute_id = new_processor_details.get('id')
                logger.info(f"Successfully created processor: {log_attribute_id}")
            except ConnectionError as create_err:
                 logger.error(f"Failed to create placeholder LogAttribute processor: {create_err}")
                 # Decide if we should exit or continue without connection test
                 raise

        # --- Create Connection ---
        if generate_flowfile_id and log_attribute_id:
            logger.info(f"Attempting to connect GenerateFlowFile ({generate_flowfile_id}) to LogAttribute ({log_attribute_id})...")
            try:
                connection_details = await client.create_connection(
                    process_group_id=root_id,
                    source_id=generate_flowfile_id,
                    target_id=log_attribute_id,
                    relationships=["success"] # Connect the 'success' relationship
                )
                logger.info(f"Successfully created connection: {connection_details.get('id')}")
                logger.info(f"Full connection response: {connection_details}")
            except ConnectionError as conn_err:
                logger.error(f"Failed to create connection: {conn_err}")
                # Log error but continue for cleanup
        else:
            logger.warning("Could not find both GenerateFlowFile and LogAttribute processors; skipping connection creation.")

    except NiFiAuthenticationError as e:
        logger.error(f"Authentication Error: {e}")
    except ConnectionError as e:
         logger.error(f"Connection or API error: {e}")
    except Exception as e:
         logger.error(f"An unexpected error occurred in main: {e}", exc_info=True)
    finally:
        await client.close()
        logger.info("--- NiFi Client Test End ---")

if __name__ == "__main__":
    import asyncio
    # Note: Running this directly requires NIFI_API_URL, NIFI_USERNAME, NIFI_PASSWORD
    # to be set in your environment or a .env file.
    # Ensure NIFI_TLS_VERIFY=false is set in .env if using self-signed certs.
    asyncio.run(main())

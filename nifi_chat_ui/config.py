# Placeholder for configuration loading (API keys etc.)
import os
# Remove streamlit import
# import streamlit as st 
from dotenv import load_dotenv # Import dotenv

# Load environment variables from .env file if it exists
load_dotenv()

def load_api_key(key_name: str) -> str | None:
    """Loads an API key from environment variables."""
    # Simplify to only use os.getenv
    return os.getenv(key_name)
    # secret_value = None
    # try:
    #     # Attempt to access the secret. This might fail if secrets.toml doesn't exist.
    #     if key_name in st.secrets:
    #         secret_value = st.secrets[key_name]
    # except FileNotFoundError:
    #     # This is expected if secrets.toml is not used, ignore and proceed.
    #     pass
    # except Exception as e:
    #     # Log other potential unexpected errors during secret access
    #     # Use st.warning or print, depending on where this config runs
    #     print(f"Warning: Error checking Streamlit secrets for {key_name}: {e}")

    # if secret_value:
    #     return secret_value
    # else:
    #     # Fallback to environment variable if not found in secrets (or secrets file missing)
    #     return os.getenv(key_name)

# Load API keys
GOOGLE_API_KEY = load_api_key("GOOGLE_API_KEY")
OPENAI_API_KEY = load_api_key("OPENAI_API_KEY")

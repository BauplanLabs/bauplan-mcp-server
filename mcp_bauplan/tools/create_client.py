import logging
import os

import bauplan
from fastmcp.server.dependencies import get_http_request

logger = logging.getLogger(__name__)


def create_bauplan_client(api_key: str | None = None) -> bauplan.Client:
    """
    Creates and validates a connection Bauplan.
    Retrieves connection parameters from config, establishes a connection.

    Returns:
        Client: A configured Bauplan client instance

    Raises:
        ConnectionError: When connection cannot be established
    """
    try:
        # Establish connection to Bauplan - note that a profile variable
        # will be used if present
        if os.environ.get("BAUPLAN_PROFILE"):
            logger.info("Using Bauplan profile from environment variable")
            client = bauplan.Client(profile=os.environ["BAUPLAN_PROFILE"])
        # if api key is passed, use it
        elif api_key:
            logger.info("Init Bauplan client without profile")
            client = bauplan.Client(api_key=api_key)
        else:
            logger.info("Init Bauplan client without profile or api_key")
            client = bauplan.Client()
        logger.info("Connected to Bauplan")
        return client

    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Failed to connect to Bauplan: {e!s}", exc_info=True)
        raise ConnectionError(f"Unable to connect to Bauplan: {e!s}") from e


def get_bauplan_client() -> bauplan.Client:
    """
    Dependency function for FastMCP Depends().
    Extracts the API key from the HTTP request header (if present) and creates a Bauplan client.
    Falls back to default credentials in stdio transport or when no header is provided.
    """
    api_key = None
    try:
        request = get_http_request()
        raw = request.headers.get("bauplan") or request.headers.get("Bauplan")
        if raw:
            api_key = raw[7:].strip() if raw.lower().startswith("bearer ") else raw
    except Exception:
        pass  # stdio transport — no HTTP request available
    return create_bauplan_client(api_key)

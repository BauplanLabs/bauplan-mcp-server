import logging

import bauplan
from fastmcp.server.dependencies import get_access_token, get_http_request

logger = logging.getLogger(__name__)


def _extract_token(raw: str | None) -> str | None:
    """Normalize bearer-style headers to a plain token string."""
    if not raw:
        return None

    value = raw.strip()
    if not value:
        return None

    if value.lower().startswith("bearer "):
        value = value[7:].strip()

    return value or None


def get_bauplan_client() -> bauplan.Client:
    """
    Dependency function for FastMCP Depends().
    Extracts the API key from the HTTP request header (if present) and creates a Bauplan client.
    Falls back to default credentials in stdio transport or when no header is provided.
    """
    try:
        http_headers = get_http_request().headers
    except Exception:
        # It's not an HTTP request context
        return bauplan.Client()

    # First, check for a Bauplan-specific API key header (e.g., "Bauplan" or "bauplan")
    api_key = _extract_token(http_headers.get("Bauplan"))
    if api_key:
        logger.info("Using Bauplan credentials from HTTP header")
        return bauplan.Client(api_key=api_key)

    # Check for a valid OAuth access token if available
    try:
        access_token = get_access_token()
        if access_token and access_token.token:
            logger.info("Using Bauplan credentials from validated OAuth access token")
            return bauplan.Client(api_key=access_token.token)
    except Exception:
        # No valid access token available, continue to check headers
        pass

    # Finally, check the standard Authorization header for a bearer token
    api_key = _extract_token(http_headers.get("Authorization"))
    if api_key:
        logger.info("Using Bauplan credentials from HTTP header")
        return bauplan.Client(api_key=api_key)

    # No valid credentials found in headers, falling back to default client
    return bauplan.Client()

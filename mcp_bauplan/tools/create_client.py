import bauplan
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def create_bauplan_client(api_key: Optional[str] = None) -> bauplan.Client:
    """
    Creates and validates a connection Bauplan.
    Retrieves connection parameters from config, establishes a connection.

    Returns:
        Client: A configured Bauplan client instance

    Raises:
        ConnectionError: When connection cannot be established
    """
    try:
        # Establish connection to Bauplan
        client = bauplan.Client(api_key=api_key)
        logger.info("Connected to Bauplan")
        return client

    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Failed to connect to Bauplan: {str(e)}", exc_info=True)
        raise ConnectionError(f"Unable to connect to Bauplan: {str(e)}")

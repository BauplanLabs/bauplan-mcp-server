import bauplan
import logging
from typing import Optional, Callable
import os
from functools import wraps

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
        # Establish connection to Bauplan - note that a profile variable
        # will be used if present
        if "BAUPLAN_PROFILE" in os.environ and os.environ["BAUPLAN_PROFILE"]:
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
        logger.error(f"Failed to connect to Bauplan: {str(e)}", exc_info=True)
        raise ConnectionError(f"Unable to connect to Bauplan: {str(e)}")


def with_bauplan_client(func: Callable) -> Callable:
    """
    Decorator that automatically creates and injects a Bauplan client into the decorated function.
    The client is created based on the api_key parameter if provided.

    The decorated function should have 'bauplan_client' as a parameter to receive the client instance.
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Extract api_key from kwargs if present
        api_key = kwargs.get("api_key", None)

        # Create the Bauplan client
        bauplan_client = create_bauplan_client(api_key)

        # Inject the client into kwargs
        kwargs["bauplan_client"] = bauplan_client

        # Remove api_key from kwargs as it's no longer needed
        kwargs.pop("api_key", None)

        # Call the original function
        return await func(*args, **kwargs)

    return wrapper

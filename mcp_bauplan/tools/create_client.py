import bauplan
import logging
from typing import Optional
from functools import wraps

logger = logging.getLogger(__name__)


def create_bauplan_client(api_key: Optional[str] = None):
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
        user = client.info().user
        username = user.username
        full_name = user.full_name

        logger.info(f"Connected to Bauplan.  username={username}, full_name={full_name}")
        return client
        
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Failed to connect to Bauplan: {str(e)}", exc_info=True)
        raise ConnectionError(f"Unable to connect to Bauplan: {str(e)}")


def with_fresh_client(func):
    """
    Decorator that creates a fresh Bauplan client for the wrapped async function.
    
    The decorator:
    1. Intercepts the api_key parameter from the original call
    2. Creates a new Bauplan client instance using the api_key
    3. Calls the wrapped function with bauplan_client instead of api_key
    4. Ensures the client is properly cleaned up after function returns
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Extract and remove api_key from kwargs
        api_key = kwargs.pop('api_key', None)
        
        # Create a fresh client
        client = None
        try:
            client = create_bauplan_client(api_key=api_key)
            
            # Add the client to kwargs
            kwargs['bauplan_client'] = client
            
            # Call the wrapped function with the client (api_key already removed)
            result = await func(*args, **kwargs)
            return result
            
        finally:
            # Clean up the client
            if client is not None:
                try:
                    del client
                except Exception as e:
                    logger.warning(f"Failed to clean up Bauplan client: {e}")
    
    return wrapper

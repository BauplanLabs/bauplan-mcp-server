import bauplan
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Global client instance
_bauplan_client = None

def get_bauplan_client(api_key: Optional[str] = None):
    return create_bauplan_client(api_key=api_key)

def create_bauplan_client(api_key: Optional[str] = None):
    """
    Creates and validates a connection Bauplan.
    Retrieves connection parameters from config, establishes a connection.
    
    Returns:
        Client: A configured Bauplan client instance
        
    Raises:
        ConnectionError: When connection cannot be established
        ConfigurationError: When configuration is invalid
    """
    global _bauplan_client
    global _bauplan_user
    
    try:
        # Establish connection to Bauplan
        client = bauplan.Client(api_key=api_key)
        info = client.info()
        
        # Store in global instance
        _bauplan_client = client
        _bauplan_user = info.user.username

        logger.info(f"Connected to Bauplan.  username={_bauplan_user}")
        return client
        
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Failed to connect to Bauplan: {str(e)}", exc_info=True)
        raise ConnectionError(f"Unable to connect to Bauplan: {str(e)}")

from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import datetime
import re

from .create_client import with_fresh_client

class QueryMetadata(BaseModel):
    row_count: int
    column_names: List[str]
    column_types: List[str]
    query_time: str
    query: str

class QueryOut(BaseModel):
    status: str
    data: List[Dict[str, Any]]
    metadata: Optional[QueryMetadata] = None
    error: Optional[str] = None

def execute_query(
        query: str,
        bauplan_client, 
        ref: Optional[str] = None, 
        namespace: Optional[str] = None
    ) -> QueryOut:

    try:
        # Create a response structure optimized for LLM consumption
        response = {
            "status": "success",
            "data": [],
            "metadata": {},
            "error": None
        }
        
        # Use provided ref/namespace or fall back to config
        query_ref = ref if ref is not None else None #config.branch
        query_namespace = namespace if namespace is not None else "bauplan" #config.namespace
        
        # Execute query and get results as Arrow table
        result = bauplan_client.query(
            query=query,
            ref=query_ref,
            namespace=query_namespace,
        )

        # Convert pyarrow.Table to list of dictionaries with native Python values
        data_rows = [
            dict(zip(result.column_names, [val.as_py() for val in row]))
            for row in zip(*[result[col] for col in result.column_names])
        ]

        # Create metadata
        metadata = QueryMetadata(
            row_count=len(data_rows),
            column_names=result.column_names,
            column_types=[str(field.type) for field in result.schema],
            query_time=datetime.datetime.now().isoformat(),
            query=query
        )
        
        # Return successful response
        return QueryOut(
            status="success",
            data=data_rows,
            metadata=metadata,
            error=None
        )

    except Exception as err:
        raise ToolError(f"Error executing get_table: {err}")


def register_run_query_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="run_query",
        description="Execute a SQL SELECT query on the user's Bauplan data catalog, returning results as a DataFrame using a query, optional ref, and optional namespace."
    )
    @with_fresh_client
    async def run_query(
        query: str,
        bauplan_client,
        ref: Optional[str] = None,
        namespace: Optional[str] = None,
        ctx: Context = None
    ) -> QueryOut:
        """
            Executes a SQL query against the user's Bauplan data lake.
            
            Args:
                query: SQL query to execute

                ref: a reference to a commit that is a state of the user data lake: can be either a hash that starts with "@" and
                has 64 additional characters or a branch name, that is a mnemonic reference to the last commit that follows the "username.name" format.

                namespace: Optional namespace to use.
             
            Returns:
                QueryOut: Response object with query results or error  
        """
        try:

            # Enforce SELECT query for security (prevent other operations)
            # Remove leading/trailing whitespace and normalize to uppercase
            normalized_query = query.strip().upper()
            
            # Remove comments (both -- and /* */ style)
            # Remove single-line comments
            normalized_query = re.sub(r'--.*$', '', normalized_query, flags=re.MULTILINE)
            # Remove multi-line comments
            normalized_query = re.sub(r'/\*.*?\*/', '', normalized_query, flags=re.DOTALL)
            # Remove leading whitespace again after comment removal
            normalized_query = normalized_query.strip()
            
            # Check if it's a SELECT query (also allow WITH for CTEs)
            if not (normalized_query.startswith("SELECT") or normalized_query.startswith("WITH")):
                raise ToolError("Only SELECT queries (including CTEs with WITH) are permitted.")
            
            # Additional security checks for dangerous keywords
            dangerous_keywords = [
                "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", 
                "TRUNCATE", "REPLACE", "MERGE", "CALL", "EXEC", "EXECUTE"
            ]
            
            # Check for dangerous keywords anywhere in the query
            for keyword in dangerous_keywords:
                if keyword in normalized_query:
                    raise ToolError(f"Query contains forbidden keywords: {keyword}") 
           
            result = execute_query(query, bauplan_client, ref, namespace)
            return result
        
        except Exception as err:
            raise ToolError(f"Error executing get_table: {err}")

"""
Execute queries and save results to CSV files.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional
from fastmcp.exceptions import ToolError

from .create_client import with_fresh_client
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)

class QueryToCSVResult(BaseModel):
    path: str
    query: str
    ref: Optional[str]
    namespace: Optional[str]
    success: bool
    message: str

def register_run_query_to_csv_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="run_query_to_csv", 
        description="Execute SQL SELECT queries on a specified table in the user's Bauplan data catalog, saving results to a CSV file, using a query  and table name, returning a file path."
    )
    @with_fresh_client
    async def run_query_to_csv(
        path: str,
        query: str,
        bauplan_client,
        ref: Optional[str] = None,
        namespace: Optional[str] = None,
        client_timeout: int = 120,
        ctx: Context = None
    ) -> QueryToCSVResult:
        """
        Execute SELECT queries and save results directly to CSV file.
        
        Note: CSV format only supports scalar data types (strings, numbers, booleans).
        Queries returning complex types (arrays, lists, nested objects) will fail.
        For complex data, use run_query tool instead or modify SQL to flatten/convert data.
        
        Args:
            path: Output CSV file path where results will be saved.
            query: SQL query to execute (DuckDB SQL syntax).
            ref: Branch/reference to query against (optional).
            namespace: Namespace to use (optional).
            client_timeout: Timeout in seconds (defaults to 120).
            
        Returns:
            QueryToCSVResult: Object indicating success/failure with execution details
        """
        try:
            
            if ctx:
                await ctx.info(f"Executing query to CSV file: {path}")
            
            # Call query_to_csv_file function
            bauplan_client.query_to_csv_file(
                path=path,
                query=query,
                ref=ref,
                namespace=namespace,
                client_timeout=client_timeout
            )
            
            # Log successful execution
            logger.info(f"Successfully executed query and saved results to CSV: {path}")
            
            return QueryToCSVResult(
                path=path,
                query=query,
                ref=ref,
                namespace=namespace,
                success=True,
                message=f"Query executed successfully and results saved to: {path}"
            )
            
        except Exception as e:
            error_msg = str(e)
            # Handle complex data type errors specifically
            if "Unsupported Type" in error_msg and ("list" in error_msg or "array" in error_msg):
                logger.error(f"CSV export failed due to complex data types: {error_msg}")
                raise ToolError(
                    f"Cannot export to CSV: Query contains complex data types (arrays/lists) that are not supported in CSV format. "
                    f"Consider: 1) Using run_query tool instead for complex data, 2) Flattening arrays in SQL with functions like unnest(), "
                    f"3) Converting arrays to strings with array_to_string() or similar functions. Original error: {error_msg}"
                )
            else:
                logger.error(f"Error executing query to CSV {path}: {error_msg}")
                raise ToolError(f"Failed to execute query to CSV {path}: {error_msg}")
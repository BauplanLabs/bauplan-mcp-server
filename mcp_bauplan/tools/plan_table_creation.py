"""
Create a table import plan from an S3 location.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional
from fastmcp.exceptions import ToolError

from .create_client import with_fresh_client
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)

class TablePlanCreated(BaseModel):
    job_id: str
    table_name: str
    search_uri: str
    success: bool
    message: str
    namespace: Optional[str]
    branch: Optional[str]

def register_plan_table_creation_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="plan_table_creation", 
        description="Generate a YAML schema plan for importing a table from an S3 URI in the user's Bauplan data catalog returning a job ID for tracking)."
    )
    @with_fresh_client
    async def plan_table_creation(
        table: str,
        search_uri: str,
        bauplan_client,
        namespace: Optional[str] = None,
        branch: Optional[str] = None,
        partitioned_by: Optional[str] = None,
        replace: Optional[bool] = None,
        ctx: Context = None
    ) -> TablePlanCreated:
        """
        Create a table import plan from an S3 location.
        
        This operation will attempt to create a table based of schemas of N parquet files found by a given search uri.
        A YAML file containing the schema and plan is returned and if there are no conflicts, it is automatically applied.
        
        Args:
            table: Name of the table to plan creation for.
            search_uri: S3 URI to search for parquet files.
            namespace: Optional namespace (defaults to "bauplan").
            branch: Optional branch name.
            partitioned_by: Optional partitioning column.
            replace: Optional flag to replace existing table.
            
        Returns:
            TablePlanCreated: Object indicating success/failure with job tracking details
        """
        try:
            
            if ctx:
                await ctx.info(f"Creating table plan for '{table}' from search URI '{search_uri}'")
            
            # Call plan_table_creation function
            result = bauplan_client.plan_table_creation(
                table=table,
                search_uri=search_uri,
                namespace=namespace,
                branch=branch,
                partitioned_by=partitioned_by,
                replace=replace
            )
            
            # Extract job_id from TableCreatePlanState object
            job_id = result.job_id
            
            # Log successful plan creation with job_id
            logger.info(f"Successfully created table plan for '{table}' with job_id: {job_id}")
            
            return TablePlanCreated(
                job_id=job_id,
                table_name=table,
                search_uri=search_uri,
                success=True,
                message=f"Table plan created successfully for '{table}' with job_id: {job_id}",
                namespace=namespace,
                branch=branch
            )
            
        except Exception as e:
            logger.error(f"Error creating table plan for {table}: {str(e)}")
            raise ToolError(f"Failed to create table plan for {table}: {str(e)}")
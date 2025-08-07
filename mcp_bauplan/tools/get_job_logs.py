"""
Get job logs by job ID prefix.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import List, Optional
from fastmcp.exceptions import ToolError

from .create_client import get_bauplan_client
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)

class JobLogInfo(BaseModel):
    message: str
    stream: str

class JobLogsList(BaseModel):
    logs: List[JobLogInfo]
    total_count: int

def register_get_job_logs_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_job_logs", 
        description="Retrieve a list of job logs using a job ID prefix, where JobLog is a model representing a job's log message."    
    )
    async def get_job_logs(
        job_id_prefix: str,
        api_key: Optional[str] = None,
        ctx: Context = None
    ) -> JobLogsList:
        """
        Get job logs by job ID prefix.
        
        Args:
            job_id_prefix: The job ID prefix to search for logs.
            
        Returns:
            JobLogsList: Object containing list of job logs with their messages and streams
        """
        try:
            bauplan_client = get_bauplan_client(api_key)
            
            if ctx:
                await ctx.info(f"Getting job logs for job ID prefix: {job_id_prefix}")
            
            # Call get_job_logs function
            logs_result = bauplan_client.get_job_logs(job_id_prefix=job_id_prefix)
            
            # Convert JobLog objects to JobLogInfo BaseModel instances
            job_log_info_list = []
            for log in logs_result:
                job_log_info = JobLogInfo(
                    message=log.message,
                    stream=str(log.stream)
                )
                job_log_info_list.append(job_log_info)
            
            # Log successful retrieval
            logger.info(f"Successfully retrieved {len(job_log_info_list)} job logs for prefix: {job_id_prefix}")
            
            return JobLogsList(
                logs=job_log_info_list,
                total_count=len(job_log_info_list)
            )
            
        except Exception as e:
            # Handle job-related errors more gracefully
            error_msg = str(e)
            logger.error(f"Error getting job logs for prefix {job_id_prefix}: {error_msg}")
            raise ToolError(f"Failed to get job logs for prefix {job_id_prefix}: {error_msg}")
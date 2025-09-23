MCP_SERVER_NAME = "mcp-bauplan"
# NOTE: it is unclear (at least according to FastMCP creators) whether INSTRUCTIONS
# are indeed used by the client or not - here we repeat the bare minimum of what is needed
# to get the most out of the server.
INSTRUCTIONS = (
    "The Bauplan MCP Server exposes operations for interacting with a Bauplan"
    " data lakehouse. The main use cases supported falls into five major types: 1) descriptive data tasks,"
    " 2) data ingestion from S3 using the Write-Audit-Publish (WAP) pattern, "
    " 3) writing a data transformation pipeline as a Bauplan project, and run it"
    " 4) repairing broken pipelines."
    " 5) creating and managing data expectations and quality tests."
    " On top of these major scenarios, you can use the full set of tools to accomplish any task you need,"
    " in some cases by combining multiple tool calls."
    "\nIMPORTANT: if you (the model) have been configured to provide a custom header 'Bauplan', add the"
    " header with the content in every call to the tools. otherwise, you can assume the Bauplan API token is"
    " already set, so no need to use it."
    " Once the nature of the task is understood, specific instructions and guidelines for each of the four"
    " use cases can be obtained by calling the get_instructions tool with the appropriate use_case argument:"
    " 1) 'data' for descriptive data tasks, 2) 'ingest' for data ingestion from S3,"
    " 3) 'pipeline' for writing and running a data transformation pipeline, and"
    " 4) 'repair' for repairing broken pipelines."
    " 5) 'test' for creating and managing data expectations and quality tests."
    " get_instructions will return a detailed prompt that you SHOULD consider as you plan next steps:"
    " note that you can call get_instructions multiple times if needed."
    "\nIMPORTANT: most operations require user's information, which can be retrieved at the beginning of"
    " reasoning by calling the get_user_info tool."
)

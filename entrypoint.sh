#!/bin/sh

# Launch the MCP server with OpenTelemetry auto-instrumentation.
if [ "${MCP_TRANSPORT:-streamable-http}" = "stdio" ]; then
  exec opentelemetry-instrument \
    python main.py \
    --transport stdio
fi

exec opentelemetry-instrument \
  gunicorn "mcp_bauplan.app:create_http_app()" \
  --workers "${MCP_WORKERS:-4}" \
  --worker-class uvicorn.workers.UvicornWorker \
  --keep-alive "${MCP_KEEP_ALIVE:-65}" \
  --bind "0.0.0.0:${PORT:-8000}" \
  --access-logfile "-" \
  --error-logfile "-"

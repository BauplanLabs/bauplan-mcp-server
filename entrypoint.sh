#!/bin/sh

# Launch the MCP server with OpenTelemetry auto-instrumentation.
# When OTEL env vars (OTEL_EXPORTER_OTLP_ENDPOINT, etc.) are not set,
# opentelemetry-instrument is a no-op — the server behaves identically.
exec opentelemetry-instrument \
  python main.py \
  --transport ${MCP_TRANSPORT:-streamable-http} \
  --host 0.0.0.0 \
  --port ${PORT:-8000}

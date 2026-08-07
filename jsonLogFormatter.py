"""
Structured JSON log formatter for stdout logging on Kubernetes.

Rather than shipping logs to New Relic directly via an HTTP handler, the
script logs plain structured JSON lines to stdout. On Kubernetes this is the
standard pattern: the container runtime captures stdout/stderr, and a
cluster-level log forwarder (e.g. New Relic's Kubernetes integration /
Fluent Bit, or whatever DaemonSet your cluster runs) tails and ships it --
no outbound network calls or credentials needed inside the script itself.

Usage (see main.py):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
"""
import json
import logging
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Formats each LogRecord as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)

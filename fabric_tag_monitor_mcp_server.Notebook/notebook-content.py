# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

"""Standalone MCP server exposing a health-check tool for the Fabric tag
monitoring system. Run this as its own process (not inside a Fabric
notebook) - an MCP client (Claude Desktop, Claude Code, etc.) connects to
it and can call get_tag_monitor_health() as a tool.

Requires: pip install mcp deltalake pyarrow

Auth note: reading the notifications table over abfss:// still needs a
OneLake storage token, the same way the notebooks get one - this example
uses a service-principal client-credentials flow via environment
variables, since there's no notebookutils available outside Fabric. Set
these before running:
  FABRIC_TENANT_ID, FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET, FABRIC_TABLE_PATH
"""
import os
import requests

from mcp.server.mcpserver import MCPServer
from health_check_logic import get_tag_monitor_health

mcp = MCPServer("fabric-tag-monitor-health")

ONELAKE_STORAGE_SCOPE = "https://storage.azure.com/.default"


def _get_storage_token() -> str:
    tenant_id = os.environ["FABRIC_TENANT_ID"]
    client_id = os.environ["FABRIC_CLIENT_ID"]
    client_secret = os.environ["FABRIC_CLIENT_SECRET"]
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    resp = requests.post(token_url, data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": ONELAKE_STORAGE_SCOPE,
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


@mcp.tool()
def get_tag_monitor_health_check() -> dict:
    '''Returns the current health/status of the Fabric tag-governance
    monitor: how many workspaces are tracked, how many are currently
    non-compliant, and how many have an alert pending/failed to send.'''
    table_path = os.environ["FABRIC_TABLE_PATH"]
    storage_options = {"bearer_token": _get_storage_token(), "use_fabric_endpoint": "true"}
    return get_tag_monitor_health(table_path, storage_options)


if __name__ == "__main__":
    mcp.run()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

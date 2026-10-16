# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

"""Core health-check logic - reads the SAME tag_notifications Delta table
both fabric_tag_monitor_python.ipynb and fabric_send_alerts.ipynb already
read/write, using the exact same deltalake/pyarrow approach. This is the
part an MCP server would wrap - kept separate and independently testable
from the MCP protocol plumbing itself."""
from datetime import datetime, timezone
from typing import Any, Dict


def get_tag_monitor_health(table_path: str, storage_options=None) -> Dict[str, Any]:
    '''Summarizes the current state of the tag-monitoring system by
    reading the shared notifications table directly - no separate
    "health" table or heartbeat mechanism needed, since this table's
    contents ARE the system's current state.'''
    from deltalake import DeltaTable

    try:
        dt = DeltaTable(table_path, storage_options=storage_options)
    except Exception as e:
        return {
            "status": "error",
            "error": f"Could not open notifications table at {table_path}: {e}",
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    rows = dt.to_pyarrow_table().to_pylist()

    total_tracked = len(rows)
    non_compliant = [r for r in rows if (r.get("violation_count") or 0) > 0]
    compliant = [r for r in rows if (r.get("violation_count") or 0) == 0]
    unsent_alerts = [r for r in non_compliant if not r.get("notification_sent")]
    sent_alerts = [r for r in non_compliant if r.get("notification_sent")]

    return {
        "status": "ok",
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "table_path": table_path,
        "total_workspaces_tracked": total_tracked,
        "currently_non_compliant": len(non_compliant),
        "currently_compliant": len(compliant),
        "alerts_sent_and_active": len(sent_alerts),
        "alerts_pending_or_failed": len(unsent_alerts),
        "pending_workspace_ids": [r.get("workspace_id") for r in unsent_alerts],
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

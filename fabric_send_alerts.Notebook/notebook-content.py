# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "28db7020-c7df-4c7f-91a0-bad2f9997486",
# META       "default_lakehouse_name": "monitoring_lakehouse",
# META       "default_lakehouse_workspace_id": "f3c50251-d451-442f-8631-b12b6ad76de2",
# META       "known_lakehouses": [
# META         {
# META           "id": "28db7020-c7df-4c7f-91a0-bad2f9997486"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Send Alerts (Fabric-native, standalone)
# 
# Second stage of the split pipeline:
# 1. `fabric_tag_monitor.ipynb` scans and writes ONE CONSOLIDATED row per
#    non-compliant workspace to the findings Delta table (append-only -
#    `write_findings()` already groups all of a workspace's violations,
#    workspace-level and item-level, into one row's `violations_json`).
# 2. **This notebook** reads those already-consolidated rows (keeping only
#    the most recent one per workspace, in case multiple tag-monitor runs
#    accumulated rows before this notebook processed them), applies the
#    resend-throttle (don't re-alert on an unchanged violation set within
#    `resend_reminder_hours`), formats the alert body as a **table** listing
#    every non-compliant workspace/item and its missing tags, writes the
#    result to the notifications Delta table, and calls `sendAlerts()` for
#    whichever workspaces should actually get notified.
# 
# `sendAlerts()` is a **template** - fill in actual delivery (email via
# Graph/SMTP, Teams, a ticketing system, invoking another notebook, etc.).


# PARAMETERS CELL ********************

# --- Parameters cell -----------------------------------------------------
CONFIG_RELATIVE_PATH = "config/config.json"
LAKEHOUSE_MOUNT = "/lakehouse/default/Files"
ALERT_TYPE = "tag_noncompliance"
PIPELINE_RUN_SUBJECT = ""
ERROR_MSG = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%pip install requests deltalake pyarrow --quiet


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Imports & base logger setup ---------------------------------------------
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import pyarrow as pa

try:
    import notebookutils
except ImportError:
    notebookutils = None
    print("WARNING: notebookutils not available - not running inside Fabric?")

logger = logging.getLogger("send_alerts")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.propagate = False
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                                         datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(_console)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%run fabric_shared_lib

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Table-formatted alert body -----------------------------------------------
# No more throttle-signature helper here - the send/skip decision is now
# purely (compliant, notification_sent), no signature comparison needed.

def build_consolidated_table_body(workspace_name: str, violations: List[Dict[str, Any]]) -> str:
    '''Formats every non-compliant workspace/item as rows of a simple
    aligned plain-text table (Resource | Type | Missing Tags) - readable
    in any delivery channel (plain-text email, Teams, logs) without
    assuming HTML rendering support.'''
    try:
        # headers = ["Resource", "Type", "Missing Tags"]
        rows = []
        for v in violations:
            if v["resource_type"] == "workspace":
                resource_name = f"{workspace_name} (workspace)"
                resource_kind = "workspace"
            else:
                resource_name = v.get("resource_name") or v.get("resource_id", "")
                resource_kind = v.get("resource_kind", "")
            rows.append([resource_name, resource_kind, ", ".join(v.get("missing_tags", []))])

        col_widths = [len(h) for h in headers]
        for row in rows:
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(cell)))

        def format_row(cells):
            return " | ".join(str(c).ljust(col_widths[i]) for i, c in enumerate(cells))

        separator = "-+-".join("-" * w for w in col_widths)
        lines = [f"Non-compliant resources in workspace '{workspace_name}':", ""]
        lines.append(format_row(headers))
        lines.append(separator)
        for row in rows:
            lines.append(format_row(row))
        return "\n".join(lines)
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise        


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- sendAlerts() TEMPLATE - fill in actual delivery here --------------------

def sendAlerts(workspace_id: str, workspace_name: str,
               subject: str, body: str, violations: List[Dict[str, Any]]) -> bool:
    '''TEMPLATE - implement actual alert delivery here (email via Graph/SMTP,
    Teams, a ticketing system, invoking another notebook via
    notebookutils.notebook.run(), etc.). `body` is already formatted as a
    table (see build_consolidated_table_body). Return True if delivery
    succeeded, False otherwise - the caller uses this to set
    notification_sent and decide whether the resend-throttle should retry
    on a later run.'''
    # TODO: implement delivery.
    logger.info("[sendAlerts TEMPLATE] Would send alert for workspace '%s' (%s): %s\n%s",
                workspace_name, workspace_id, subject, body)
    return True


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def sendEmail(workspace_id: str, workspace_name: str,
               subject: str, body: str, violations: List[Dict[str, Any]]) -> bool:
    '''TEMPLATE - implement actual alert delivery here (email via Graph/SMTP,
    Teams, a ticketing system, invoking another notebook via
    notebookutils.notebook.run(), etc.). `body` is already formatted as a
    table (see build_consolidated_table_body). Return True if delivery
    succeeded, False otherwise - the caller uses this to set
    notification_sent and decide whether the resend-throttle should retry
    on a later run.'''
    # TODO: implement delivery.
    logger.info("pipeline success / failure")
    return True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Main run ----------------------------------------------------------------
# Reads the ONE shared table (written by fabric_tag_monitor_python.ipynb) and
# determines each record's CURRENT compliance directly from violation_count
# (0 = compliant) - there's no separate findings table/row-presence signal
# anymore, since records are never deleted.
#
# State machine (per record):
#   violation_count == 0 + compliant=True   -> already up to date, skip
#   violation_count == 0 + compliant=False  -> SILENT update to compliant=True;
#                                               notification_sent UNCHANGED; record kept
#   violation_count > 0  + compliant=True   -> REGRESSION: reset notification_sent=False
#                                               in the SAME existing record, then fall
#                                               through to the send-check below
#   violation_count > 0  + notification_sent=True  -> stay silent, no matter what changed
#   violation_count > 0  + notification_sent=False -> send via sendAlerts(), then set
#                                               notification_sent=True (or leave False
#                                               to retry next run if it failed)

def run():
    try:
        if ALERT_TYPE == "tag_noncompliance":
            notifications = load_notifications(cfg["alerting"]["notifications_delta_table_path"])

            now_iso = datetime.now(timezone.utc).isoformat()
            summary = {"alerts_sent": 0, "already_notified_skipped": 0,
                    "marked_compliant_silently": 0, "regressed_and_realerted": 0}

            for key, rec in notifications.items():
                currently_compliant = (rec.get("violation_count") or 0) == 0

                if currently_compliant:
                    if rec.get("compliant") is True:
                        continue  # already marked compliant, nothing changed
                    # Just became compliant - SILENT update. notification_sent UNCHANGED.
                    rec["compliant"] = True
                    notifications[key] = rec
                    summary["marked_compliant_silently"] += 1
                    continue

                # --- Currently NON-COMPLIANT ---
                workspace_id = rec.get("workspace_id")
                workspace_name = rec.get("workspace_name")
                violations = rec.get("violations", [])

                # REGRESSION: was compliant, now isn't - reset notification_sent so
                # this SAME pass falls through to the send-alert branch below.
                if rec.get("compliant") is True:
                    rec["notification_sent"] = False
                    summary["regressed_and_realerted"] += 1

                rec["compliant"] = False

                if rec.get("notification_sent"):
                    # Already notified for this non-compliant state - stay silent.
                    notifications[key] = rec
                    summary["already_notified_skipped"] += 1
                    continue

                # notification_sent is False AND compliant is False -> send.
                subject = cfg["alerting"]["subject_template"].format(
                    item_name=f"{len(violations)} resource(s)", workspace_name=workspace_name)
                body = build_consolidated_table_body(workspace_name, violations)
                sent = sendAlerts(workspace_id, workspace_name, subject, body, violations)

                rec["notification_sent"] = sent
                # first_alert_utc is set ONCE and never changes after that.
                rec["first_alert_utc"] = rec.get("first_alert_utc") or now_iso
                if sent:
                    # latest_alert_utc updates every time a new alert actually sends
                    # (including on regression -> re-alert, which is a NEW send).
                    rec["latest_alert_utc"] = now_iso
                    summary["alerts_sent"] += 1

                notifications[key] = rec

            save_notifications(cfg["alerting"]["notifications_delta_table_path"], notifications)
            logger.info("=== Send alerts complete: %s ===", summary)
            return summary

        elif ALERT_TYPE == "execution_success":
            print("success")
        elif ALERT_TYPE == "execution_failure":
            print("failed")
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise RuntimeError(
            f"Processing failed with error: {str(e)}"
        ) from e


run_summary = run()
run_summary


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Exit value for pipeline orchestration ------------------------------------
import json as _json
if notebookutils is not None:
    notebookutils.notebook.exit(_json.dumps(run_summary))
else:
    print(_json.dumps(run_summary, indent=2))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

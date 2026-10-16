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

# # Fabric Workspace(s) / Item(s) Tag Monitor
# 
# Runs as a Python notebook: scans every
# workspace in the tenant (full scan each run)
# via the Admin Scanner APIs, checks workspace- and item-level tags. Sends
# one consolidated notification per non-compliant workspace using SNS basis APPID and tracks notification history.
# 
# Gets executed from a Fabric pipeline. Stores notification history in a Delta table in the lakehouse. The config file lives under the Lakehouse's `Files` area.

# PARAMETERS CELL ********************

# --- Parameters cell ---------------------------------------------------------

# Path to config.json, relative to the attached Lakehouse's Files area.
CONFIG_RELATIVE_PATH = "config/config.json"

# Lakehouse mount point for config file
LAKEHOUSE_MOUNT = "/lakehouse/default/Files"

# Parameter to store environment from pipeline
ENV = "ws"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Python notebooks don't yet support persistent custom environments, so the external dependencies are installed inline each session.
#
# requests: Fabric Admin API calls .
#
# deltalake (delta-rs): reads/writes the notifications Delta table under Tables/ without needing a Spark session - supported by python using delta-rs
#
# pyarrow: listed explicitly rather than assumed as a transitive dependency of deltalake.
# Few deltalake versions (e.g. 1.6.x, which use their own arro3-core array layer) do not pull pyarrow in automatically, and this code uses pyarrow.Table directly.
%pip install requests deltalake pyarrow --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# All Library Imports
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import requests
from deltalake import write_deltalake, DeltaTable
import pyarrow as pa
import re,sys
import base64
import hashlib
import importlib
import os
import subprocess
import sys
import zipfile

try:
    import notebookutils  # provided automatically inside Fabric notebooks
except ImportError:
    notebookutils = None
    print("WARNING: notebookutils not available - not running inside Fabric? "
          "Key Vault secret retrieval (auth.method='key_vault') will not work.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Base logger setup

logger = logging.getLogger("fabric_monitor")
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

# subprocess.run([sys.executable, "-m", "pip", "install", "--disable-pip-version-check", "abfss://aw_tag_man_ws@onelake.dfs.fabric.microsoft.com/monitoring_lakehouse.Lakehouse/Files/config/fabric_sample_lib-1.0.0-py3-none-any.whl"], capture_output=False, text=True)
path = "/lakehouse/default/Files/config/fabric_sample_lib-1.0.0-py3-none-any.whl"
sha256=None
pip_args=()
if path.startswith(("http://", "https://")):
    target = f"{path}#sha256={sha256}" if sha256 and "#" not in path else path
else:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Wheel not found: {path}")
    if sha256 and hashlib.sha256(open(path, "rb").read()).hexdigest() != sha256.lower():
        raise RuntimeError(f"sha256 mismatch for {path} - refusing to install")
    target = path
r = subprocess.run([sys.executable, "-m", "pip", "install", "--disable-pip-version-check", *pip_args, target],
                    capture_output=True, text=True)
print(r)
if r.returncode != 0:
    raise RuntimeError(f"pip install failed for {path}:\n{r.stdout[-1500:]}\n{r.stderr[-1500:]}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Prefix based Tag compliance check, separate for workspace and item, based on prefixes in config properties for each.

def extract_item_tags(resource: Dict[str, Any], tag_id_to_name: Optional[Dict[str, str]] = None) -> List[str]:
    try:
        tag_id_to_name = tag_id_to_name or {}
        raw_tags = resource.get("tags") or []
        normalized: List[str] = []
        unresolved_ids: List[str] = []

        for t in raw_tags:
            if isinstance(t, str):
                if t in tag_id_to_name:
                    normalized.append(tag_id_to_name[t])
                elif tag_id_to_name:
                    unresolved_ids.append(t)
                else:
                    normalized.append(t)
                continue

            if not isinstance(t, dict):
                continue
            name = t.get("displayName") or t.get("name")
            if name:
                normalized.append(name)
                continue
            tag_id = t.get("id") or t.get("tagId")
            if tag_id:
                resolved = tag_id_to_name.get(tag_id)
                if resolved:
                    normalized.append(resolved)
                else:
                    unresolved_ids.append(tag_id)

        if unresolved_ids:
            logger.warning("Could not resolve %d tag id(s) to a display name for resource '%s': %s",
                        len(unresolved_ids), resource.get("name", resource.get("id")), unresolved_ids)
        return normalized
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise


def _prefix_config_key(resource_type: str) -> str:
    return "req_tag_prefix_workspace" if resource_type == "workspace" else "req_tag_prefix_items"


def is_compliant(resource: Dict[str, Any], tags_cfg: Dict[str, Any], resource_type: str,
                  tag_id_to_name: Optional[Dict[str, str]] = None) -> Tuple[bool, List[str]]:
    required_prefixes = tags_cfg.get(_prefix_config_key(resource_type), [])
    case_insensitive = tags_cfg.get("case_insensitive", True)
    present = extract_item_tags(resource, tag_id_to_name)

    try:
        if not required_prefixes:
            compliant = len(present) > 0
            return compliant, ([] if compliant else ["<any tag>"])

        if case_insensitive:
            present_lower = [p.lower() for p in present]
            missing = [prefix for prefix in required_prefixes
                    if not any(tag.startswith(prefix.lower()) for tag in present_lower)]
        else:
            missing = [prefix for prefix in required_prefixes
                    if not any(tag.startswith(prefix) for tag in present)]

        return (len(missing) == 0), missing
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Function to include only workspaces which are not listed in "scan.excluded_workspaces" config property AND
# whose name ends with the current ENV parameter's value (TST/STG/PROD)
def filter_excluded_nonenv_relevant_workspaces(workspaces: List[Dict[str, Any]], scan_cfg: Dict[str, Any],
                                                env: str) -> List[Dict[str, Any]]:    
    excluded = set(scan_cfg.get("excluded_workspaces", []))
    
    try:
        env_suffix = (env or "").strip()

        if not env_suffix:
            logger.warning("ENV parameter is empty - skipping environment-suffix filtering; "
                        "only scan.excluded_workspaces exclusions will apply this run.")

        kept = []
        for ws in workspaces:
            name = ws.get("name") or ""
            ws_id = ws.get("id")

            print("workspace name",name)

            if ws_id in excluded or name in excluded:
                logger.info("Skipping excluded workspace '%s' (%s)", name, ws_id)
                continue
            kept.append(ws)

        logger.info("Workspace filter: %d workspace(s) -> %d after exclusions and ENV='%s' suffix filter",
                    len(workspaces), len(kept), env_suffix or "<none>")
        return kept
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise        

# Finds the value portion of a workspace tag matching a given prefix.
# Handles all Scanner API tag shapes (id-string, id-dict, name-dict); "" if absent.
def get_workspace_tag_value(workspace: Dict[str, Any], tag_prefix: str,
                             tag_id_to_name: Optional[Dict[str, str]] = None) -> str:
    '''Finds the first workspace tag starting with tag_prefix (e.g. "env:")
    and returns everything after the prefix (e.g. "prod" from "env:prod").
    Returns "" if the workspace has no tag matching that prefix, or if
    tag_prefix itself is empty/falsy (treated as "extraction disabled").
 
    Handles the same tag shapes the Scanner API can return: plain id
    strings (resolved via tag_id_to_name), {"tagId"/"id": ...} dicts
    (also resolved), or {"displayName"/"name": ...} dicts (already a
    real name, no resolution needed).'''
    try:
        if not tag_prefix:
            return ""
        tag_id_to_name = tag_id_to_name or {}
    
        for tag in workspace.get("tags", []) or []:
            name = None
            if isinstance(tag, str):
                name = tag_id_to_name.get(tag, tag)
            elif isinstance(tag, dict):
                name = tag.get("displayName") or tag.get("name")
                if not name:
                    tag_id = tag.get("tagId") or tag.get("id")
                    if tag_id:
                        name = tag_id_to_name.get(tag_id, tag_id)
    
            if name and name.lower().startswith(tag_prefix.lower()):
                return name[len(tag_prefix):]
    
        return ""
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# workspace_notification_key, ONELAKE_STORAGE_SCOPE, _storage_options,
# NOTIFICATION_SCHEMA, load_notifications, save_notifications now come from
# the shared library notebook - see the %run cell right after the Auth
# cell above. Only this notebook's OWN logic (violation detection/upsert,
# owner-email resolution, item-type filtering) remains defined here.

# Builds a stable fingerprint string from a list of violations.
# Stored for audit/reference - no longer used to gate alert decisions
# (that logic now lives entirely in fabric_send_alerts.ipynb).
def _violation_signature(violations: List[Dict[str, Any]]) -> str:
    try:
        parts = sorted(
            f"{v['resource_type']}:{v['resource_id']}:{'|'.join(sorted(v['missing_tags']))}"
            for v in violations
        )
        return ";".join(parts)
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise    

# Pure in-memory update - no I/O. Refreshes ONLY the violation-related
# fields (workspace_name, violation_count, violation_signature,
# violations_json, tag_value) for this workspace's row. Explicitly does
# NOT touch compliant/notification_sent/first_alert_utc/latest_alert_utc -
# those belong to fabric_send_alerts.ipynb, which reads violation_count to
# decide compliance and manages those fields itself. A brand-new record
# (never seen before) gets those fields left as None, which
# fabric_send_alerts.ipynb's logic already treats correctly as "not yet
# notified, not compliant". Records are NEVER deleted here, even when a
# workspace becomes fully compliant (violation_count=0) - the row stays,
# only its violation fields (now including tag_value) are refreshed.
def upsert_violation_data(records: Dict[str, Any], workspace: Dict[str, Any],
                           violations: List[Dict[str, Any]], tag_prefix: str,
                           tag_id_to_name: Optional[Dict[str, str]] = None) -> None:
    try:
        ws_id = workspace.get("id")
        key = workspace_notification_key(ws_id)
    
        if key not in records and not violations:
            # Never tracked before, and still fully compliant - nothing to record.
            return
    
        existing = records.get(key, {})
        existing["workspace_id"] = ws_id
        existing["workspace_name"] = workspace.get("name")
        existing["violation_count"] = len(violations)
        existing["violation_signature"] = _violation_signature(violations)
        existing["violations"] = violations
        existing["appid_value"] = get_workspace_tag_value(workspace, tag_prefix, tag_id_to_name)
        records[key] = existing
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise    


# Extracts an email/UPN from a Fabric API "createdBy"-style field.
# Handles both dict and plain-string shapes; returns None if neither.
def _creator_from_dict(creator: Any) -> Optional[str]:
    try:
        if isinstance(creator, dict):
            return creator.get("userPrincipalName") or creator.get("emailAddress")
        if isinstance(creator, str) and "@" in creator:
            return creator
        return None
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise    

# Finds the email of the workspace's Admin-role user, if any.
# Used as a fallback owner when a resource has no direct creator.
def _workspace_admin_email(workspace: Dict[str, Any]) -> Optional[str]:
    try:
        for user in workspace.get("users", []) or []:
            role = (user.get("groupUserAccessRight") or user.get("role") or "").lower()
            if role == "admin":
                email = user.get("emailAddress") or user.get("userPrincipalName")
                if email:
                    return email
        return None
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# Resolves the best-known owner email for a workspace or item.
# Prefers the resource's own creator, falls back to the workspace admin.
def resolve_owner_email(resource: Dict[str, Any], resource_type: str) -> Optional[str]:
    try:
        creator = resource.get("createdBy") or resource.get("configuredBy")
        owner = _creator_from_dict(creator) if creator is not None else None
        if owner:
            return owner
        if resource_type == "workspace":
            return _workspace_admin_email(resource)
        return None
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# Checks whether an item's type is allowed to trigger a notification.
# Empty notify_item_types config means every type is allowed.
def should_notify_item_type(cfg: Dict[str, Any], item_type: str) -> bool:
    try:
        notify_types = cfg["tags"].get("notify_item_types") or []
        if not notify_types:
            return True
        return (item_type or "").lower() in {t.lower() for t in notify_types}
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Main run --------------------------------------------------------------
# No state/last-scan tracking - every run evaluates every workspace in the
# tenant (see get_modified_workspaces(client, "")), not just ones
# changed since a previous run.
#
# notifications is loaded ONCE before the scan loop and saved ONCE after
# it - not per workspace - to avoid a full table read/write cycle per
# workspace at scale.

def run():
    try:
        # print(test1234)
        run_start = datetime.now(timezone.utc)
        logger.info("=== Fabric monitor run starting at %s ===",run_start)
        items = []
        appid_prefix = cfg["tags"].get("appid_prefix", "")
        all_workspaces = get_modified_workspaces(cfg, "")
        candidates = filter_excluded_nonenv_relevant_workspaces(all_workspaces, cfg["scan"], ENV)

        print("candidates", candidates)
        summary = {
            "workspaces_evaluated": 0,
            "items_evaluated": 0,
            "missing_tags": 0,
            "notified": 0,
        }

        notifications = load_notifications(cfg["alerting"]["notifications_delta_table_path"])

        if not candidates:
            logger.info("No non-excluded workspaces to scan.")
            save_notifications(cfg["alerting"]["notifications_delta_table_path"], notifications)
            logger.info("=== Fabric monitor run complete (nothing to scan) ===")
            return summary

        workspace_ids = [w["id"] for w in candidates]
        scan_result = scan_workspaces(cfg, workspace_ids)

        tag_id_to_name = list_tags(cfg)

        for workspace in scan_result.get("workspaces", []):
            summary["workspaces_evaluated"] += 1
            violations = []

            # --- workspace-level tag check -----------------------------------------
            ws_compliant, ws_missing = is_compliant(workspace, cfg["tags"], "workspace", tag_id_to_name)
            if not ws_compliant:
                summary["missing_tags"] += 1
                logger.warning("Workspace '%s' (%s) missing tags: %s",
                            workspace.get("name"), workspace.get("id"), ws_missing)
                violations.append({
                    "resource_type": "workspace",
                    "resource_id": workspace.get("id"),
                    "resource_name": workspace.get("name"),
                    "resource_kind": "workspace",
                    "missing_tags": ws_missing,
                    "owner_email": resolve_owner_email(workspace, "workspace"),
                })

            # --- item-level tag check ------------------------------------------------

            item_type_list = cfg["scan"].get("items_to_scan", [])

            for item_type in item_type_list:
                items += workspace.get(item_type, [])

            for item in items:
                summary["items_evaluated"] += 1
                compliant, missing = is_compliant(item, cfg["tags"], "item", tag_id_to_name)
                if compliant:
                    continue

                summary["missing_tags"] += 1
                item_type = item.get("type", "")
                if not should_notify_item_type(cfg, item_type):
                    logger.info("Item '%s' (%s) missing tags %s but type '%s' is excluded via "
                                "tags.notify_item_types - logged, not included in notification",
                                item.get("name"), item.get("id"), missing, item_type)
                    continue

                logger.warning("Item '%s' (%s) in workspace '%s' missing tags: %s",
                            item.get("name"), item.get("id"), workspace.get("name"), missing)
                violations.append({
                    "resource_type": "item",
                    "resource_id": item.get("id"),
                    "resource_name": item.get("name"),
                    "resource_kind": item_type or "item",
                    "missing_tags": missing,
                    "owner_email": resolve_owner_email(item, "item"),
                })

            summary["notified"] += len(violations)
            upsert_violation_data(notifications, workspace, violations, appid_prefix, tag_id_to_name)

        logger.info("Scan evaluated %d workspace(s) and %d item(s); %d missing required tags",
                    summary["workspaces_evaluated"], summary["items_evaluated"], summary["missing_tags"])

        save_notifications(cfg["alerting"]["notifications_delta_table_path"], notifications)

        elapsed = (datetime.now(timezone.utc) - run_start).total_seconds()
        logger.info("=== Fabric monitor run complete (%.1fs) ===", elapsed)
        # return_status = True;
        return_status = {
                            "success": True,
                            "status": "Succeeded",
                            "message": "Processing completed successfully",
                        }
        print(json.dumps(return_status))
    except Exception as e:
        return_status = {
                    "success": False,
                    "status": "Failed",
                    "message": "Processing has Failed with error"+str(e), 
                }
        print(json.dumps(return_status))
        # raise
        raise RuntimeError(
            f"Processing failed with error: {str(e)}"
        ) from e
    finally:
        _file_handler.flush()
        time.sleep(2)

run_summary = run()
run_summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Exit value for pipeline orchestration (tend to change) ------------------------------------

# notebookutils.notebook.exit(json.dumps(run_summary))
# notebookutils.notebook.exit(run_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

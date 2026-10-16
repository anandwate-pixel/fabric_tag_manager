# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Tag Library Audit (Fabric-native build)
# 
# Runs **entirely inside Microsoft Fabric** as a Python notebook: reads the
# tenant's tag-library **SharePoint List** (documenting each governed tag and
# its intended usage), finds every tag whose usage-description column is
# empty, and sends **ONE consolidated alert** listing all of them.
# 
# This is a separate, standalone concern from `fabric_tag_monitor.ipynb`
# (the tag-*compliance* monitor that scans Fabric workspaces/items) - this
# notebook audits the tag *library documentation* instead, reading from
# SharePoint rather than the Fabric Admin API. It shares the same
# `config.json` (reusing the `auth` service principal and the
# `alerting.alert_notebook_name` delivery notebook), but runs on its own
# schedule via its own pipeline.
# 
# **Alert delivery is delegated**, same pattern as the compliance monitor:
# this notebook does not send email itself - it invokes the SAME
# alert-delivery notebook (`alerting.alert_notebook_name`) via
# `notebookutils.notebook.run()`, passing recipient/subject/body. One
# delivery notebook serves both monitors.
# 
# **Auth**: needs a **Microsoft Graph** token (not the Fabric Admin API
# scope this monitor doesn't call) - the same service principal, via Key
# Vault, as the compliance monitor. The app registration needs
# **Sites.Read.All** (or **Sites.Selected**, scoped to just this one site)
# as a Microsoft Graph APPLICATION permission, admin-consented.
# 
# **ASSUMPTION**: the tag library is a genuine SharePoint **List** (a
# structured object with defined columns), not an ad-hoc HTML table
# embedded in a wiki/modern page's content - Lists expose reliable
# per-column values via Microsoft Graph, which is what this depends on. If
# your tag library is a table on a regular page instead, `get_tag_library_items()`
# below is the one function that would need replacing.


# PARAMETERS CELL ********************

# --- Parameters cell -----------------------------------------------------
CONFIG_RELATIVE_PATH = "config/config.json"
LAKEHOUSE_MOUNT = "/lakehouse/default/Files"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This notebook only ever calls Microsoft Graph, not Delta Lake - no
# deltalake/pyarrow needed here, just requests.
%pip install requests --quiet


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Imports & base logger setup ---------------------------------------------
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

try:
    import notebookutils  # provided automatically inside Fabric notebooks
except ImportError:
    notebookutils = None
    print("WARNING: notebookutils not available - not running inside Fabric? "
          "Key Vault secret retrieval (auth.method='key_vault') will not work.")

logger = logging.getLogger("tag_library_audit")
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
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Config loading --------------------------------------------------------
# Reuses the SAME config.json as fabric_tag_monitor.ipynb, but only
# validates the sections THIS notebook actually touches (auth, tag_library,
# alerting.alert_notebook_name, logging) - the compliance-monitor-only
# sections (scan, tags, deletion, notifications_delta_table_path) are
# allowed to be absent if you ever split this into its own config file.

class ConfigError(Exception):
    pass

REQUIRED_TOP_LEVEL_KEYS = ["auth", "tag_library", "alerting", "logging"]


def _resolve_path(raw_value: str, base_dir: str, label: str) -> Path:
    expanded = os.path.expandvars(os.path.expanduser(raw_value))
    p = Path(expanded)
    if not p.is_absolute():
        p = Path(base_dir) / p
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise ConfigError(f"Could not create directory for {label} at '{p.parent}': {e}") from e
    return p


def load_config(config_path: str, base_dir: str) -> Dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise ConfigError(
            f"Config file not found: '{config_path}'. Upload config.json to "
            f"'{CONFIG_RELATIVE_PATH}' under the attached Lakehouse's Files area."
        )
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        raise ConfigError(f"Config file '{path}' is not valid JSON: {e}") from e

    missing = [k for k in REQUIRED_TOP_LEVEL_KEYS if k not in cfg]
    if missing:
        raise ConfigError(f"Config is missing required sections: {missing}")
    if "alert_notebook_name" not in cfg["alerting"]:
        raise ConfigError("config.alerting.alert_notebook_name is required (the alert-delivery notebook to invoke).")

    cfg["logging"]["log_file"] = str(_resolve_path(cfg["logging"]["log_file"], base_dir, "logging.log_file"))
    return cfg


config_full_path = str(Path(LAKEHOUSE_MOUNT) / CONFIG_RELATIVE_PATH)
cfg = load_config(config_full_path, LAKEHOUSE_MOUNT)

_file_handler = logging.FileHandler(cfg["logging"]["log_file"])
_file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                                              datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(_file_handler)
logger.setLevel(getattr(logging, str(cfg["logging"].get("level", "INFO")).upper(), logging.INFO))

logger.info("=== Tag library audit starting (in-Fabric notebook) ===")
logger.info("Config file: %s", config_full_path)
logger.info("Resolved log file: %s", cfg["logging"]["log_file"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Auth ------------------------------------------------------------------
# Same service principal / Key Vault pattern as fabric_tag_monitor.ipynb,
# but requests a MICROSOFT GRAPH token, not the Fabric Admin API scope -
# this notebook never calls the Fabric Scanner APIs.
#
# NOTE ON STYLE: a service principal is just three strings, so it's kept as
# a plain dict ({"tenant_id", "client_id", "client_secret"}) rather than a
# class/dataclass - nothing here needs bundled behavior, just data passed
# explicitly between functions.

class AuthError(Exception):
    pass


def _sp_from_key_vault(auth_cfg: Dict[str, Any]) -> Dict[str, str]:
    if notebookutils is None:
        raise AuthError("notebookutils is unavailable; auth.method='key_vault' only works inside a Fabric notebook.")
    kv_url = auth_cfg["key_vault_url"]
    names = auth_cfg["key_vault_secret_names"]

    tenant_id = auth_cfg.get("tenant_id") or notebookutils.credentials.getSecret(kv_url, names["tenant_id"])
    client_id = auth_cfg.get("client_id") or notebookutils.credentials.getSecret(kv_url, names["client_id"])
    client_secret = notebookutils.credentials.getSecret(kv_url, names["client_secret"])
    return {"tenant_id": tenant_id, "client_id": client_id, "client_secret": client_secret}


def _sp_from_config(auth_cfg: Dict[str, Any]) -> Dict[str, str]:
    return {
        "tenant_id": auth_cfg.get("tenant_id", ""),
        "client_id": auth_cfg.get("client_id", ""),
        "client_secret": auth_cfg.get("client_secret", ""),
    }


def get_service_principal(cfg: Dict[str, Any]) -> Dict[str, str]:
    auth_cfg = cfg["auth"]
    method = auth_cfg.get("method", "key_vault")

    if method == "key_vault":
        sp = _sp_from_key_vault(auth_cfg)
    elif method == "config":
        sp = _sp_from_config(auth_cfg)
    else:
        raise AuthError(f"Unsupported auth.method '{method}' in this Fabric build. Use 'key_vault' or 'config'.")

    if not (sp["tenant_id"] and sp["client_id"] and sp["client_secret"]):
        raise AuthError(f"Service principal credentials incomplete for auth.method='{method}'.")

    logger.info("Resolved service principal via auth.method='%s' (client_id=%s...)", method, sp["client_id"][:8])
    return sp


def get_access_token(sp: Dict[str, str], scope: str,
                      authority_base: str = "https://login.microsoftonline.com") -> str:
    token_url = f"{authority_base}/{sp['tenant_id']}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": sp["client_id"],
        "client_secret": sp["client_secret"],
        "scope": scope,
    }
    resp = requests.post(token_url, data=data, timeout=30)
    if resp.status_code != 200:
        logger.error("Token acquisition failed for scope '%s': %s %s", scope, resp.status_code, resp.text)
        raise AuthError(f"Failed to acquire token for scope '{scope}': {resp.status_code} {resp.text}")
    token = resp.json().get("access_token")
    if not token:
        raise AuthError(f"Token response for scope '{scope}' did not contain an access_token")
    return token


sp = get_service_principal(cfg)
graph_token = get_access_token(sp, "https://graph.microsoft.com/.default")
logger.info("Acquired Microsoft Graph access token")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Tag library audit logic -----------------------------------------------
# Reads the SharePoint List (Microsoft Graph), finds tags with a missing
# usage description, and delegates the alert to the SAME alert-delivery
# notebook the compliance monitor uses (alerting.alert_notebook_name) -
# not Graph sendMail/SMTP directly, keeping alert delivery in one place.

GRAPH_BASE = "https://graph.microsoft.com/v1.0"


class TagLibraryAuditError(Exception):
    pass


def get_tag_library_items(cfg: Dict[str, Any], graph_token: str) -> List[Dict[str, Any]]:
    '''Fetches every item in the tag-library SharePoint List, paginating
    via @odata.nextLink. Returns each item's "fields" dict (the actual
    column values), not the raw Graph list-item wrapper.'''
    lib_cfg = cfg["tag_library"]
    site_id = lib_cfg["site_id"]
    list_id = lib_cfg["list_id_or_name"]

    url = f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items"
    params = {"expand": "fields"}
    headers = {"Authorization": f"Bearer {graph_token}"}

    items: List[Dict[str, Any]] = []
    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            logger.error("Failed to read tag library list (%s): %s", resp.status_code, resp.text)
            raise TagLibraryAuditError(
                f"Could not read SharePoint list '{list_id}' on site '{site_id}': "
                f"{resp.status_code} {resp.text}"
            )
        data = resp.json()
        for raw_item in data.get("value", []):
            fields = raw_item.get("fields", {})
            if fields:
                items.append(fields)

        url = data.get("@odata.nextLink")
        params = None  # nextLink already includes query params

    logger.info("Read %d tag library item(s) from SharePoint list '%s'", len(items), list_id)
    return items


def find_tags_missing_description(
    items: List[Dict[str, Any]], tag_name_column: str, usage_description_column: str
) -> List[Dict[str, str]]:
    '''Pure function: returns [{"tag_name": ..., "row_index": ...}, ...]
    for every item whose usage-description column is missing, empty, or
    whitespace-only.'''
    missing = []
    for i, item in enumerate(items):
        tag_name = (item.get(tag_name_column) or "").strip()
        usage_description = (item.get(usage_description_column) or "").strip()
        if not usage_description:
            missing.append({"tag_name": tag_name or f"<unnamed row {i}>", "row_index": str(i)})
    return missing


def build_alert_body(missing_tags: List[Dict[str, str]], list_display_name: str) -> str:
    lines = [f"  - {t['tag_name']}" for t in missing_tags]
    return (
        f"{len(missing_tags)} tag(s) in the '{list_display_name}' tag library are missing a "
        f"usage description:\n\n" + "\n".join(lines) +
        "\n\nPlease add a usage description for each of these tags so consumers know when to "
        "apply them."
    )


def send_tag_library_alert(cfg: Dict[str, Any], missing_tags: List[Dict[str, str]]) -> bool:
    '''Delegates to the SAME alert-delivery notebook the compliance monitor
    uses. Reuses that notebook's parameter contract (recipient_email/
    subject/body/workspace_id/workspace_name/violation_count/violations_json) -
    workspace_id is left blank and workspace_name carries the tag library's
    display name instead, since there's no Fabric workspace involved here.'''
    if not missing_tags:
        logger.info("Every tag in the library has a usage description - nothing to alert on.")
        return True

    notify_cfg = cfg["tag_library"]["notify"]
    if not notify_cfg.get("enabled", True):
        logger.info("tag_library.notify.enabled is false; found %d tag(s) missing a description "
                    "but not sending an alert.", len(missing_tags))
        return False

    recipients = notify_cfg.get("recipient_emails", [])
    if not recipients:
        logger.warning("tag_library.notify.recipient_emails is empty; nothing to send to")
        return False

    if notebookutils is None:
        raise AuthError("notebookutils is unavailable; invoking the alert-delivery notebook requires a Fabric notebook.")

    list_display_name = cfg["tag_library"].get("list_id_or_name", "Tag Library")
    subject = notify_cfg.get("subject_template", "[Tag Governance] {count} tag(s) missing a usage description") \
        .format(count=len(missing_tags))
    body = build_alert_body(missing_tags, list_display_name)

    alerting_cfg = cfg["alerting"]
    notebook_name = alerting_cfg["alert_notebook_name"]
    timeout_seconds = alerting_cfg.get("alert_notebook_timeout_seconds", 90)

    params = {
        "recipient_email": ", ".join(recipients),
        "subject": subject,
        "body": body,
        "workspace_id": "",
        "workspace_name": list_display_name,
        "violation_count": len(missing_tags),
        "violations_json": json.dumps(missing_tags),
    }

    try:
        result = notebookutils.notebook.run(notebook_name, timeout_seconds, params)
        logger.info("Invoked alert notebook '%s' for tag library audit; result: %s", notebook_name, result)
        return True
    except Exception as e:
        logger.error("Failed to invoke alert notebook '%s' for tag library audit: %s", notebook_name, e)
        return False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Main run ----------------------------------------------------------------

def run():
    items = get_tag_library_items(cfg, graph_token)
    missing_tags = find_tags_missing_description(
        items, cfg["tag_library"]["tag_name_column"], cfg["tag_library"]["usage_description_column"]
    )

    if missing_tags:
        logger.warning("%d tag(s) missing a usage description: %s",
                       len(missing_tags), [t["tag_name"] for t in missing_tags])
    else:
        logger.info("Every tag in the library has a usage description.")

    alert_sent = send_tag_library_alert(cfg, missing_tags)

    summary = {
        "tags_checked": len(items),
        "tags_missing_description": len(missing_tags),
        "missing_tag_names": [t["tag_name"] for t in missing_tags],
        "alert_sent": alert_sent,
    }
    logger.info("=== Tag library audit complete: %s ===", summary)
    return summary


run_summary = run()
run_summary


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
# META   "language_group": "synapse_pyspark"
# META }

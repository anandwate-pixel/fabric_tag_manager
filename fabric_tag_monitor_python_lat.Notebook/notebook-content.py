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

# Loading configurations by resolving paths against the Lakehouse Files mount

REQUIRED_TOP_LEVEL_KEYS = ["auth", "api", "scan", "tags", "alerting", "deletion", "logging"]

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

    notif_path = cfg["alerting"]["notifications_delta_table_path"]
    if not notif_path.startswith("abfss://"):
        raise ConfigError(
            f"alerting.notifications_delta_table_path must be an abfss:// OneLake URI, "
            f"got: '{notif_path}'"
        )
    cfg["alerting"]["notifications_delta_table_path"] = notif_path

    cfg["logging"]["log_file"] = str(_resolve_path(cfg["logging"]["log_file"], base_dir, "logging.log_file"))
    return cfg


config_full_path = str(Path(LAKEHOUSE_MOUNT) / CONFIG_RELATIVE_PATH)
cfg = load_config(config_full_path, LAKEHOUSE_MOUNT)

# Now that we know the log path, attach a file handler too.
_file_handler = logging.FileHandler(cfg["logging"]["log_file"])
_file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                                              datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(_file_handler)
# if not cfg["logging"].get("console", True):
#     logger.removeHandler(_console)
logger.setLevel(getattr(logging, str(cfg["logging"].get("level", "INFO")).upper(), logging.INFO))

logger.info("=== Fabric monitor run starting (in-Fabric notebook) ===")
logger.info("Config file: %s", config_full_path)
logger.info("Resolved notifications Delta table path: %s", cfg["alerting"]["notifications_delta_table_path"])
logger.info("Resolved log file: %s", cfg["logging"]["log_file"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Custom exception type for all auth/token-acquisition failures.
# Raised throughout this module instead of letting raw errors propagate.
class AuthError(Exception):
    pass


# Resolves service principal credentials, preferring plain config values.
# Falls back to Key Vault secrets for any of the three not set directly in config.
def _sp_from_key_vault(auth_cfg: Dict[str, Any]) -> Dict[str, str]:
    if notebookutils is None:
        raise AuthError("notebookutils is unavailable; auth.method='key_vault' only works inside a Fabric notebook.")
    kv_url = auth_cfg["key_vault_url"]
    names = auth_cfg["key_vault_secret_names"]

    tenant_id = auth_cfg.get("tenant_id") or notebookutils.credentials.getSecret(kv_url, names["tenant_id"])
    client_id = auth_cfg.get("client_id") or notebookutils.credentials.getSecret(kv_url, names["client_id"])
    client_secret = auth_cfg.get("client_secret") or notebookutils.credentials.getSecret(kv_url, names["client_secret"])
    return {"tenant_id": tenant_id, "client_id": client_id, "client_secret": client_secret}


# Validates auth.method and dispatches to the Key Vault credential resolver.
# Raises if the method is unsupported or any resolved credential is empty.
def get_service_principal(cfg: Dict[str, Any]) -> Dict[str, str]:
    auth_cfg = cfg["auth"]
    method = auth_cfg.get("method", "key_vault")

    if method != "key_vault":
        raise AuthError(f"Unsupported auth.method '{method}' in this Fabric build. Only 'key_vault' is supported.")

    sp = _sp_from_key_vault(auth_cfg)

    if not (sp["tenant_id"] and sp["client_id"] and sp["client_secret"]):
        raise AuthError("Service principal credentials incomplete - check the Key Vault secrets named in "
                        "auth.key_vault_secret_names actually contain values.")

    logger.info("Resolved service principal via Key Vault (client_id=%s...)", sp["client_id"][:8])
    return sp


# Exchanges service principal credentials for an AAD token via client-credentials flow.
# Raises AuthError on a non-200 response or a missing access_token in the reply.
def get_access_token(sp: Dict[str, str], scope: str,
                      authority_base: str) -> str:
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
fabric_scope = cfg["auth"].get("scope", "https://api.fabric.microsoft.com/.default")
auth_base_url = cfg["auth"].get("authority_base", "https://login.microsoftonline.com")
fabric_token = get_access_token(sp, fabric_scope, auth_base_url)
logger.info("Acquired Fabric API access token")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Custom exception type for all Fabric Admin API request failures.
# Raised by _request() whenever a call returns a 4xx/5xx status.
class FabricApiError(Exception):
    pass


# Builds the plain-dict client state: base URLs, timeout, scan config, session.
# Attaches the bearer token once so every subsequent call reuses the same session.
def build_fabric_client(cfg: Dict[str, Any], access_token: str) -> Dict[str, Any]:
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    })
    return {
        "base_url": cfg["api"]["base_url"].rstrip("/"),
        "admin_base_url": cfg["adminApi"]["base_url"].rstrip("/"),
        "timeout": cfg["api"].get("request_timeout_seconds", 60),
        "scan_cfg": cfg["scan"],
        "session": session,
    }


# Low-level HTTP call shared by every Fabric API function below.
# Routes DELETEs and /admin/tags to the admin base URL, raises FabricApiError on 4xx/5xx.
def _request(client: Dict[str, Any], method: str, path: str, **kwargs) -> requests.Response:
    if method == "DELETE" or path == "/admin/tags":
        url = f"{client['admin_base_url']}{path}"
    else:
        url = f"{client['base_url']}{path}"
    resp = client["session"].request(method, url, timeout=client["timeout"], **kwargs)

    if resp.status_code >= 400:
        logger.error("Fabric API error %s %s -> %s: %s", method, url, resp.status_code, resp.text)
        raise FabricApiError(f"{method} {url} failed: {resp.status_code} {resp.text}")
    return resp


# Lists workspaces changed since a given timestamp (or all, if None).
# Respects scan.include_personal_workspaces when building the request params.
def get_modified_workspaces(client: Dict[str, Any], modified_since_iso: Optional[str]) -> List[Dict[str, Any]]:
    scan_cfg = client["scan_cfg"]
    params = {
        "excludePersonalWorkspaces": str(
            not scan_cfg.get("include_personal_workspaces", False)
        ).lower()
    }
    if modified_since_iso:
        params["modifiedSince"] = modified_since_iso

    resp = _request(client, "GET", "/admin/workspaces/modified", params=params)
    workspaces = resp.json()
    logger.info("Fabric reports %d modified workspace(s) since %s",
                len(workspaces), modified_since_iso or "<beginning>")
    return workspaces


# Triggers a new Admin Scanner run over a batch of workspace ids.
# Returns the scan id used to poll status and later fetch results.
def start_scan(client: Dict[str, Any], workspace_ids: List[str]) -> str:
    opts = client["scan_cfg"].get("scan_options", {})
    params = {k: str(v).lower() for k, v in opts.items()}
    body = {"workspaces": workspace_ids}
    resp = _request(client, "POST", "/admin/workspaces/getInfo", params=params, json=body)
    scan_id = resp.json().get("id")
    if not scan_id:
        raise FabricApiError("Scan trigger response missing scan id")
    logger.info("Started scan %s for %d workspace(s)", scan_id, len(workspace_ids))
    return scan_id


# Polls a scan's status until it succeeds, fails, or times out.
# Raises FabricApiError on failure or on exceeding poll_timeout_seconds.
def wait_for_scan(client: Dict[str, Any], scan_id: str) -> None:
    scan_cfg = client["scan_cfg"]
    poll_interval = scan_cfg.get("poll_interval_seconds", 5)
    timeout = scan_cfg.get("poll_timeout_seconds", 300)
    waited = 0
    while waited <= timeout:
        resp = _request(client, "GET", f"/admin/workspaces/scanStatus/{scan_id}")
        status = resp.json().get("status")
        logger.debug("Scan %s status=%s (waited=%ss)", scan_id, status, waited)
        if status == "Succeeded":
            return
        if status == "Failed":
            raise FabricApiError(f"Scan {scan_id} failed")
        time.sleep(poll_interval)
        waited += poll_interval
    raise FabricApiError(f"Scan {scan_id} timed out after {timeout}s")


# Fetches the full scan result payload for a completed scan id.
# Called only after wait_for_scan() confirms the scan succeeded.
def get_scan_result(client: Dict[str, Any], scan_id: str) -> Dict[str, Any]:
    resp = _request(client, "GET", f"/admin/workspaces/scanResult/{scan_id}")
    return resp.json()


# Runs the full scan cycle (start, wait, fetch) across all workspace ids.
# Batches requests per scan.batch_size and merges results into one list.
def scan_workspaces(client: Dict[str, Any], workspace_ids: List[str]) -> Dict[str, Any]:
    batch_size = client["scan_cfg"].get("batch_size", 100)
    merged_workspaces: List[Dict[str, Any]] = []
    for i in range(0, len(workspace_ids), batch_size):
        batch = workspace_ids[i:i + batch_size]
        scan_id = start_scan(client, batch)
        wait_for_scan(client, scan_id)
        result = get_scan_result(client, scan_id)
        merged_workspaces.extend(result.get("workspaces", []))
    return {"workspaces": merged_workspaces}

# Builds a tag-id -> display-name map from GET /admin/tags, paginated.
# Lets tag_checker match prefixes against real names instead of raw GUIDs.
def list_tags(client: Dict[str, Any]) -> Dict[str, str]:
    tag_map: Dict[str, str] = {}
    continuation_token = None
    while True:
        params = {"continuationToken": continuation_token} if continuation_token else {}
        try:
            resp = _request(client, "GET", "/admin/tags", params=params)
        except FabricApiError as e:
            logger.warning("Could not list tenant tags (GET /admin/tags failed): %s. "
                           "Tag-id resolution will be skipped.", e)
            return tag_map
        data = resp.json()
        for t in (data.get("value") or data.get("tags") or []):
            tag_id = t.get("id")
            name = t.get("displayName") or t.get("name")
            if tag_id and name:
                tag_map[tag_id] = name
        continuation_token = data.get("continuationToken")
        if not continuation_token:
            break
    logger.info("Loaded %d tag definition(s) from the tenant for id->name resolution", len(tag_map))
    return tag_map


client = build_fabric_client(cfg, fabric_token)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Prefix based Tag compliance check, separate for workspace and item, based on prefixes in config properties for each.

def extract_item_tags(resource: Dict[str, Any], tag_id_to_name: Optional[Dict[str, str]] = None) -> List[str]:
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


def _prefix_config_key(resource_type: str) -> str:
    return "req_tag_prefix_workspace" if resource_type == "workspace" else "req_tag_prefix_items"


def is_compliant(resource: Dict[str, Any], tags_cfg: Dict[str, Any], resource_type: str,
                  tag_id_to_name: Optional[Dict[str, str]] = None) -> Tuple[bool, List[str]]:
    required_prefixes = tags_cfg.get(_prefix_config_key(resource_type), [])
    case_insensitive = tags_cfg.get("case_insensitive", True)
    present = extract_item_tags(resource, tag_id_to_name)

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Function to include only workspaces which are not listed in "scan.excluded_workspaces" config property AND
# whose name ends with the current ENV parameter's value (TST/STG/PROD)
def filter_excluded_workspaces(workspaces: List[Dict[str, Any]], scan_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:    
    excluded = set(scan_cfg.get("excluded_workspaces", []))

    kept = []
    for ws in workspaces:
        name = ws.get("name")
        ws_id = ws.get("id")
        if ws_id in excluded or name in excluded:
            logger.info("Skipping excluded workspace '%s' (%s)", name, ws_id)
            continue
        kept.append(ws)

    logger.info("Workspace filter: %d workspace(s) -> %d after exclusions",
                len(workspaces), len(kept))
    return kept

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Builds the notification_records dict key for a given workspace.
# Format: "workspace_consolidated:{workspace_id}".
def workspace_notification_key(workspace_id: str) -> str:
    return f"workspace_consolidated:{workspace_id}"
ONELAKE_STORAGE_SCOPE = "https://storage.azure.com/.default"

# Returns OneLake auth headers for Delta reads/writes via abfss://.
# Tries the native Fabric storage token first, falls back to the SP flow.
def _storage_options(table_path: str) -> Dict[str, str]:
    if notebookutils is not None:
        try:
            token = notebookutils.credentials.getToken("storage")
            logger.info("Acquired OneLake storage token via notebookutils.credentials.getToken('storage')")
            return {"bearer_token": token, "use_fabric_endpoint": "true"}
        except Exception as e:
            logger.warning("notebookutils.credentials.getToken('storage') failed (%s); falling back to "
                           "the service principal client-credentials flow for the storage token.", e)

    token = get_access_token(sp, ONELAKE_STORAGE_SCOPE)
    logger.info("Acquired OneLake storage token via service principal client-credentials flow (fallback)")
    return {"bearer_token": token, "use_fabric_endpoint": "true"}


# SINGLE shared table - this notebook and fabric_send_alerts.ipynb both
# read/write it. Each notebook owns a different set of columns:
#   - THIS notebook (tag monitor) owns: workspace_name, violation_count,
#     violation_signature, violations_json - refreshed every run based on
#     current scan results.
#   - fabric_send_alerts.ipynb owns: compliant, notification_sent,
#     first_alert_utc, latest_alert_utc - this notebook NEVER sets these,
#     only preserves whatever value already exists on a row so it never
#     clobbers send-alert's state when refreshing violation data.
NOTIFICATION_SCHEMA = pa.schema([
    ("key", pa.string()),
    ("workspace_id", pa.string()),
    ("workspace_name", pa.string()),
    ("compliant", pa.bool_()),
    ("notification_sent", pa.bool_()),
    ("violation_count", pa.int64()),
    ("violation_signature", pa.string()),
    ("violations_json", pa.string()),
    ("first_alert_utc", pa.string()),
    ("last_alert_utc", pa.string()),
])
_SCHEMA_FIELD_NAMES = [f.name for f in NOTIFICATION_SCHEMA]


# Reads the whole shared table into a dict keyed by "key" column value.
# Called ONCE per run, before the scan loop - not per workspace.
def load_notifications(table_path: str) -> Dict[str, Any]:
    try:
        dt = DeltaTable(table_path, storage_options=_storage_options(table_path))
    except Exception as e:
        logger.info("No existing notifications Delta table at %s - this is EXPECTED on the "
                    "first run and a new table will be created on save(). "
                    "(technical detail, safe to ignore: %s)",
                    table_path, e)
        return {}

    try:
        rows = dt.to_pyarrow_table().to_pylist()
    except Exception as e:
        logger.warning("Could not read notifications Delta table at %s, starting fresh: %s", table_path, e)
        return {}

    records = {}
    for row in rows:
        key = row.pop("key", None)
        if not key:
            continue
        row["violations"] = json.loads(row.pop("violations_json", "") or "[]")
        records[key] = row
    logger.info("Loaded %d notification record(s) from Delta table %s", len(records), table_path)
    return records


# Writes the full in-memory records dict back to the shared table in ONE
# write - called ONCE after the scan loop, not per workspace. Conforms
# every row to NOTIFICATION_SCHEMA, so compliant/notification_sent/
# alert-date fields (which this notebook never sets) come through as
# whatever value already existed on that record - never overwritten with
# a default.
def save_notifications(table_path: str, records: Dict[str, Any]) -> None:
    if not records:
        return

    rows = []
    for key, rec in records.items():
        row = dict(rec)
        row["key"] = key
        row["violations_json"] = json.dumps(row.pop("violations", []))
        rows.append(row)

    conformed_rows = [{field: row.get(field) for field in _SCHEMA_FIELD_NAMES} for row in rows]
    table = pa.Table.from_pylist(conformed_rows, schema=NOTIFICATION_SCHEMA)

    write_deltalake(table_path, table, mode="overwrite", storage_options=_storage_options(table_path))
    logger.debug("Saved %d notification record(s) to Delta table %s", len(rows), table_path)


# Builds a stable fingerprint string from a list of violations.
# Stored for audit/reference - no longer used to gate alert decisions
# (that logic now lives entirely in fabric_send_alerts.ipynb).
def _violation_signature(violations: List[Dict[str, Any]]) -> str:
    parts = sorted(
        f"{v['resource_type']}:{v['resource_id']}:{'|'.join(sorted(v['missing_tags']))}"
        for v in violations
    )
    return ";".join(parts)


# Pure in-memory update - no I/O. Refreshes ONLY the violation-related
# fields (workspace_name, violation_count, violation_signature,
# violations_json) for this workspace's row. Explicitly does NOT touch
# compliant/notification_sent/first_alert_utc/latest_alert_utc - those
# belong to fabric_send_alerts.ipynb, which reads violation_count to
# decide compliance and manages those fields itself. A brand-new record
# (never seen before) gets those fields left as None, which
# fabric_send_alerts.ipynb's logic already treats correctly as "not yet
# notified, not compliant". Records are NEVER deleted here, even when a
# workspace becomes fully compliant (violation_count=0) - the row stays,
# only its violation fields are refreshed to reflect zero violations.
def upsert_violation_data(records: Dict[str, Any], workspace: Dict[str, Any],
                           violations: List[Dict[str, Any]]) -> None:
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
    records[key] = existing


# Extracts an email/UPN from a Fabric API "createdBy"-style field.
# Handles both dict and plain-string shapes; returns None if neither.
def _creator_from_dict(creator: Any) -> Optional[str]:
    if isinstance(creator, dict):
        return creator.get("userPrincipalName") or creator.get("emailAddress")
    if isinstance(creator, str) and "@" in creator:
        return creator
    return None


# Finds the email of the workspace's Admin-role user, if any.
# Used as a fallback owner when a resource has no direct creator.
def _workspace_admin_email(workspace: Dict[str, Any]) -> Optional[str]:
    for user in workspace.get("users", []) or []:
        role = (user.get("groupUserAccessRight") or user.get("role") or "").lower()
        if role == "admin":
            email = user.get("emailAddress") or user.get("userPrincipalName")
            if email:
                return email
    return None


# Resolves the best-known owner email for a workspace or item.
# Prefers the resource's own creator, falls back to the workspace admin.
def resolve_owner_email(resource: Dict[str, Any], resource_type: str) -> Optional[str]:
    creator = resource.get("createdBy") or resource.get("configuredBy")
    owner = _creator_from_dict(creator) if creator is not None else None
    if owner:
        return owner
    if resource_type == "workspace":
        return _workspace_admin_email(resource)
    return None


# Checks whether an item's type is allowed to trigger a notification.
# Empty notify_item_types config means every type is allowed.
def should_notify_item_type(cfg: Dict[str, Any], item_type: str) -> bool:
    notify_types = cfg["tags"].get("notify_item_types") or []
    if not notify_types:
        return True
    return (item_type or "").lower() in {t.lower() for t in notify_types}


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
    run_start = datetime.now(timezone.utc)

    items = []
    all_workspaces = get_modified_workspaces(client, "")
    candidates = filter_excluded_workspaces(all_workspaces, cfg["scan"])

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
    scan_result = scan_workspaces(client, workspace_ids)

    tag_id_to_name = list_tags(client)

    for workspace in scan_result.get("workspaces", []):
        if workspace.get("name").strip().endswith(ENV):
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
            upsert_violation_data(notifications, workspace, violations)

    logger.info("Scan evaluated %d workspace(s) and %d item(s); %d missing required tags",
                summary["workspaces_evaluated"], summary["items_evaluated"], summary["missing_tags"])

    save_notifications(cfg["alerting"]["notifications_delta_table_path"], notifications)

    elapsed = (datetime.now(timezone.utc) - run_start).total_seconds()
    logger.info("=== Fabric monitor run complete (%.1fs) ===", elapsed)
    return summary


run_summary = run()
run_summary


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Exit value for pipeline orchestration (tend to change) ------------------------------------

if notebookutils is not None:
    notebookutils.notebook.exit(json.dumps(run_summary))
else:
    print(json.dumps(run_summary, indent=2))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

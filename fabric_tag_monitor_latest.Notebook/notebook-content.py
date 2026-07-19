# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d0abd680-6d9d-4c9a-b7db-a1fbaa98e516",
# META       "default_lakehouse_name": "lakehouse_1",
# META       "default_lakehouse_workspace_id": "8c6d0f6a-bae8-463a-9943-fe17dca9be81",
# META       "known_lakehouses": [
# META         {
# META           "id": "d0abd680-6d9d-4c9a-b7db-a1fbaa98e516"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# # Fabric Tag Monitor

# PARAMETERS CELL ********************

CONFIG_RELATIVE_PATH = "config/config.json"
LAKEHOUSE_MOUNT = "/lakehouse/default/Files"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%pip install requests --quiet
pip install azure-eventhub azure-identity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import ClientSecretCredential

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Imports & base logger setup ---------------------------------------------
import base64
import hashlib
import hmac
import json
import logging
import os
import smtplib
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    import notebookutils
except ImportError:
    notebookutils = None
    print("WARNING: notebookutils not available")

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

# --- Config loading (paths resolve against the Lakehouse Files mount) --------

class ConfigError(Exception):
    pass

REQUIRED_TOP_LEVEL_KEYS = ["auth", "api", "scan", "state", "tags", "alerting", "deletion", "logging"]


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

    cfg["state"]["state_file"] = str(_resolve_path(cfg["state"]["state_file"], base_dir, "state.state_file"))
    cfg["alerting"]["notification_log_file"] = str(
        _resolve_path(cfg["alerting"]["notification_log_file"], base_dir, "alerting.notification_log_file")
    )
    cfg["logging"]["log_file"] = str(_resolve_path(cfg["logging"]["log_file"], base_dir, "logging.log_file"))
    return cfg


config_full_path = str(Path(LAKEHOUSE_MOUNT) / CONFIG_RELATIVE_PATH)
cfg = load_config(config_full_path, LAKEHOUSE_MOUNT)

# Now that we know the log path, attach a file handler too.
_file_handler = logging.FileHandler(cfg["logging"]["log_file"])
_file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                                              datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(_file_handler)
logger.setLevel(getattr(logging, str(cfg["logging"].get("level", "INFO")).upper(), logging.INFO))

logger.info("=== Fabric monitor run starting (in-Fabric notebook) ===")
logger.info("Config file: %s", config_full_path)
logger.info("Resolved state file: %s", cfg["state"]["state_file"])
logger.info("Resolved notification log: %s", cfg["alerting"]["notification_log_file"])
logger.info("Resolved log file: %s", cfg["logging"]["log_file"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

@dataclass
class ServicePrincipal:
    tenant_id: str
    client_id: str
    client_secret: str


class AuthError(Exception):
    pass


def _sp_from_key_vault(auth_cfg: Dict[str, Any]) -> ServicePrincipal:
    if notebookutils is None:
        raise AuthError("notebookutils is unavailable; auth.method='key_vault' only works inside a Fabric notebook.")
    kv_url = auth_cfg["key_vault_url"]
    names = auth_cfg["key_vault_secret_names"]

    tenant_id = auth_cfg.get("tenant_id") or notebookutils.credentials.getSecret(kv_url, names["tenant_id"])
    client_id = auth_cfg.get("client_id") or notebookutils.credentials.getSecret(kv_url, names["client_id"])
    client_secret = notebookutils.credentials.getSecret(kv_url, names["client_secret"])
    return ServicePrincipal(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)


def _sp_from_config(auth_cfg: Dict[str, Any]) -> ServicePrincipal:
    return ServicePrincipal(
        tenant_id=auth_cfg.get("tenant_id", ""),
        client_id=auth_cfg.get("client_id", ""),
        client_secret=auth_cfg.get("client_secret", ""),
    )


def get_service_principal(cfg: Dict[str, Any]) -> ServicePrincipal:
    auth_cfg = cfg["auth"]
    method = auth_cfg.get("method", "key_vault")

    if method == "key_vault":
        sp = _sp_from_key_vault(auth_cfg)
    elif method == "config":
        sp = _sp_from_config(auth_cfg)
    else:
        raise AuthError(f"Unsupported auth.method '{method}' in this Fabric build. Use 'key_vault' or 'config'.")

    if not (sp.tenant_id and sp.client_id and sp.client_secret):
        raise AuthError(f"Service principal credentials incomplete for auth.method='{method}'.")

    logger.info("Resolved service principal via auth.method='%s' (client_id=%s...)", method, sp.client_id[:8])
    return sp


def get_access_token(sp: ServicePrincipal, scope: str,
                      authority_base: str = "https://login.microsoftonline.com") -> str:
    token_url = f"{authority_base}/{sp.tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": sp.client_id,
        "client_secret": sp.client_secret,
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
fabric_token = get_access_token(sp, fabric_scope)
logger.info("Acquired Fabric API access token")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Fabric Admin Scanner API client --------------------------------------

class FabricApiError(Exception):
    pass


class FabricScannerClient:
    def __init__(self, cfg: Dict[str, Any], access_token: str):
        self.base_url = cfg["api"]["base_url"].rstrip("/")
        self.timeout = cfg["api"].get("request_timeout_seconds", 60)
        self.scan_cfg = cfg["scan"]
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        })

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{path}"
        resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
        if resp.status_code >= 400:
            logger.error("Fabric API error %s %s -> %s: %s", method, url, resp.status_code, resp.text)
            raise FabricApiError(f"{method} {url} failed: {resp.status_code} {resp.text}")
        return resp

    def get_modified_workspaces(self, modified_since_iso: Optional[str]) -> List[Dict[str, Any]]:
        params = {"excludePersonalWorkspaces": str(not self.scan_cfg.get("include_personal_workspaces", False)).lower()}
        if modified_since_iso:
            params["modifiedSince"] = modified_since_iso
        resp = self._request("GET", "/admin/workspaces/modified", params=params)
        workspaces = resp.json()
        logger.info("Fabric reports %d modified workspace(s) since %s",
                    len(workspaces), modified_since_iso or "<beginning>")
        return workspaces

    def start_scan(self, workspace_ids: List[str]) -> str:
        opts = self.scan_cfg.get("scan_options", {})
        params = {k: str(v).lower() for k, v in opts.items()}
        body = {"workspaces": workspace_ids}
        resp = self._request("POST", "/admin/workspaces/getInfo", params=params, json=body)
        scan_id = resp.json().get("id")
        if not scan_id:
            raise FabricApiError("Scan trigger response missing scan id")
        logger.info("Started scan %s for %d workspace(s)", scan_id, len(workspace_ids))
        return scan_id

    def wait_for_scan(self, scan_id: str) -> None:
        poll_interval = self.scan_cfg.get("poll_interval_seconds", 5)
        timeout = self.scan_cfg.get("poll_timeout_seconds", 300)
        waited = 0
        while waited <= timeout:
            resp = self._request("GET", f"/admin/workspaces/scanStatus/{scan_id}")
            status = resp.json().get("status")
            logger.debug("Scan %s status=%s (waited=%ss)", scan_id, status, waited)
            if status == "Succeeded":
                return
            if status == "Failed":
                raise FabricApiError(f"Scan {scan_id} failed")
            time.sleep(poll_interval)
            waited += poll_interval
        raise FabricApiError(f"Scan {scan_id} timed out after {timeout}s")

    def get_scan_result(self, scan_id: str) -> Dict[str, Any]:
        resp = self._request("GET", f"/admin/workspaces/scanResult/{scan_id}")
        return resp.json()

    def scan_workspaces(self, workspace_ids: List[str]) -> Dict[str, Any]:
        batch_size = self.scan_cfg.get("batch_size", 100)
        merged_workspaces: List[Dict[str, Any]] = []
        for i in range(0, len(workspace_ids), batch_size):
            batch = workspace_ids[i:i + batch_size]
            scan_id = self.start_scan(batch)
            self.wait_for_scan(scan_id)
            result = self.get_scan_result(scan_id)
            merged_workspaces.extend(result.get("workspaces", []))
        return {"workspaces": merged_workspaces}

    def delete_item(self, workspace_id: str, item_id: str) -> None:
        self._request("DELETE", f"/workspaces/{workspace_id}/items/{item_id}")
        logger.info("Deleted item %s in workspace %s", item_id, workspace_id)

    def delete_workspace(self, workspace_id: str) -> None:
        self._request("DELETE", f"/workspaces/{workspace_id}")
        logger.info("Deleted workspace %s", workspace_id)


client = FabricScannerClient(cfg, fabric_token)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- State manager (persisted as JSON on the Lakehouse) -----------------------

DEFAULT_STATE = {"last_scan_timestamp_utc": None, "last_run_completed_utc": None, "run_count": 0}


class StateManager:
    def __init__(self, state_file: str):
        self.state_file = state_file
        self.state = self._load()

    def _load(self) -> Dict[str, Any]:
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.info("Loaded state from %s (last_scan_timestamp_utc=%s)",
                            self.state_file, data.get("last_scan_timestamp_utc"))
                return {**DEFAULT_STATE, **data}
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Could not read state file (%s), starting fresh: %s", self.state_file, e)
        else:
            logger.info("No prior state file found at %s; this is the first run", self.state_file)
        return dict(DEFAULT_STATE)

    def get_last_scan_timestamp(self) -> Optional[str]:
        return self.state.get("last_scan_timestamp_utc")

    def save(self, new_scan_timestamp: datetime) -> None:
        self.state["last_scan_timestamp_utc"] = new_scan_timestamp.astimezone(timezone.utc).isoformat()
        self.state["last_run_completed_utc"] = datetime.now(timezone.utc).isoformat()
        self.state["run_count"] = int(self.state.get("run_count", 0)) + 1
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2)
        logger.info("Saved state to %s (last_scan_timestamp_utc=%s)",
                    self.state_file, self.state["last_scan_timestamp_utc"])


state = StateManager(cfg["state"]["state_file"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Tag compliance check (workspace-level AND item-level) --------------------

def extract_item_tags(resource: Dict[str, Any]) -> List[str]:
    raw_tags = resource.get("tags") or []
    normalized = []
    for t in raw_tags:
        if isinstance(t, dict):
            name = t.get("name") or t.get("displayName")
            if name:
                normalized.append(name)
        elif isinstance(t, str):
            normalized.append(t)
    return normalized


def is_compliant(resource: Dict[str, Any], tags_cfg: Dict[str, Any]) -> Tuple[bool, List[str]]:
    required = tags_cfg.get("required_tags", [])
    case_insensitive = tags_cfg.get("case_insensitive", True)
    present = extract_item_tags(resource)

    if not required:
        compliant = len(present) > 0
        return compliant, ([] if compliant else ["<any tag>"])

    if case_insensitive:
        present_norm = {p.lower() for p in present}
        missing = [r for r in required if r.lower() not in present_norm]
    else:
        present_set = set(present)
        missing = [r for r in required if r not in present_set]

    return (len(missing) == 0), missing


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Workspace exclusion filter -------------------------------------------

def filter_excluded_workspaces(workspaces: List[Dict[str, Any]], scan_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    excluded = set(scan_cfg.get("excluded_workspaces", []))
    if not excluded:
        return workspaces
    kept = []
    for ws in workspaces:
        if ws.get("id") in excluded or ws.get("name") in excluded:
            logger.info("Skipping excluded workspace '%s' (%s)", ws.get("name"), ws.get("id"))
            continue
        kept.append(ws)
    logger.info("Workspace filter: %d modified -> %d after exclusions", len(workspaces), len(kept))
    return kept


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Notifications: store + email (Microsoft Graph, SMTP, or Fabric Activator) --

def notification_key(resource_type: str, resource_id: str) -> str:
    return f"{resource_type}:{resource_id}"


class NotificationStore:
    def __init__(self, notification_log_file: str):
        self.path = notification_log_file
        self.records: Dict[str, Dict[str, Any]] = self._load()

    def _load(self) -> Dict[str, Any]:
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Could not read notification log (%s): %s", self.path, e)
        return {}

    def save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.records, f, indent=2)

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        return self.records.get(key)

    def upsert_alert(self, key: str, record: Dict[str, Any]) -> None:
        existing = self.records.get(key, {})
        existing.update(record)
        existing.setdefault("history", [])
        existing["history"].append({"event": "alert_sent", "at_utc": record.get("last_alert_utc")})
        self.records[key] = existing

    def mark_deleted(self, key: str) -> None:
        if key in self.records:
            self.records[key]["status"] = "deleted"
            self.records[key]["deleted_at_utc"] = datetime.now(timezone.utc).isoformat()
            self.records[key].setdefault("history", []).append(
                {"event": "resource_deleted", "at_utc": self.records[key]["deleted_at_utc"]})

    def mark_resolved(self, key: str) -> None:
        if key in self.records:
            self.records[key]["status"] = "resolved"
            self.records[key]["resolved_at_utc"] = datetime.now(timezone.utc).isoformat()
            self.records[key].setdefault("history", []).append(
                {"event": "tags_added", "at_utc": self.records[key]["resolved_at_utc"]})


def _creator_from_dict(creator: Any) -> Optional[str]:
    if isinstance(creator, dict):
        return creator.get("userPrincipalName") or creator.get("emailAddress")
    if isinstance(creator, str) and "@" in creator:
        return creator
    return None


def _workspace_admin_email(workspace: Dict[str, Any]) -> Optional[str]:
    for user in workspace.get("users", []) or []:
        role = (user.get("groupUserAccessRight") or user.get("role") or "").lower()
        if role == "admin":
            email = user.get("emailAddress") or user.get("userPrincipalName")
            if email:
                return email
    return None


def resolve_owner_email(resource: Dict[str, Any], resource_type: str) -> Optional[str]:
    creator = resource.get("createdBy") or resource.get("configuredBy")
    owner = _creator_from_dict(creator) if creator is not None else None
    if owner:
        return owner
    if resource_type == "workspace":
        return _workspace_admin_email(resource)
    return None


def _generate_sas_token(resource_uri: str, key_name: str, key: str, ttl_seconds: int = 3600) -> str:
    import urllib.parse as _urlparse
    encoded_uri = _urlparse.quote_plus(resource_uri)
    expiry = str(int(time.time()) + ttl_seconds)
    string_to_sign = f"{encoded_uri}\n{expiry}".encode("utf-8")
    signature = base64.b64encode(hmac.new(key.encode("utf-8"), string_to_sign, hashlib.sha256).digest())
    signature_encoded = _urlparse.quote(signature)
    return f"SharedAccessSignature sr={encoded_uri}&sig={signature_encoded}&se={expiry}&skn={key_name}"


def _get_activator_sas_key(cfg: Dict[str, Any]) -> str:
    key =""
    if notebookutils is None:
        raise AuthError("notebookutils is unavailable; Activator SAS key retrieval requires a Fabric notebook.")
    activator_cfg = cfg["alerting"].get("activator", {})
    kv_url = cfg["auth"].get("key_vault_url") or activator_cfg.get("key_vault_url")
    secret_name = activator_cfg["sas_key_key_vault_secret_name"]

    if activator_cfg["key_store"] == "config":
        key = secret_name
    else:  
        key = notebookutils.credentials.getSecret(kv_url, secret_name)
    return key


def build_activator_event(cfg, resource, resource_type, missing_tags, owner_email, workspace=None, subject="", body=""):
    ws = resource if resource_type == "workspace" else (workspace or {})
    deletion_cfg = cfg["deletion"]
    return {
        "event_type": "tag_violation",
        "resource_type": resource_type,
        "resource_id": resource.get("id"),
        "resource_name": resource.get("name"),
        "resource_kind": "workspace" if resource_type == "workspace" else resource.get("type", "item"),
        "workspace_id": ws.get("id"),
        "workspace_name": ws.get("name"),
        "owner_email": owner_email or "",
        "missing_tags_csv": ", ".join(missing_tags),
        "missing_tags_count": len(missing_tags),
        "subject": subject,
        "body": body,
        "deletion_enabled": bool(deletion_cfg.get("enabled")),
        "hours_to_delete_post_alert": deletion_cfg.get("hours_to_delete_post_alert"),
        "alert_timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }


def send_event_activator(cfg: Dict[str, Any], event: Dict[str, Any]) -> bool:
    activator_cfg = cfg["alerting"]["activator"]
    try:
        sas_key = _get_activator_sas_key(cfg)
    except AuthError as e:
        logger.error("Could not retrieve Activator SAS key: %s", e)
        return False

    namespace = activator_cfg["namespace"]
    event_hub_name = activator_cfg["event_hub_name"]
    key_name = activator_cfg["sas_key_name"]
    envelope_format = activator_cfg.get("envelope_format", False)

    resource_uri = f"https://{namespace}/{event_hub_name}"
    send_url = f"{resource_uri}/messages"
    token = _generate_sas_token(resource_uri, key_name, sas_key)
    payload = json.dumps({"body": json.dumps(event)}) if envelope_format else json.dumps(event)

    resp = requests.post(send_url, data=payload, headers={"Authorization": token, "Content-Type": "application/json"},
                          timeout=30)
    if not (200 <= resp.status_code < 300):
        logger.error("Activator Eventstream endpoint rejected event (%s): %s", resp.status_code, resp.text)
        return False
    logger.info("Published tag-violation event to Activator Eventstream for resource '%s' (%s)",
                event.get("resource_name"), event.get("resource_id"))
    return True

def alert_if_missing_tags(cfg, sp, resource, resource_type, missing_tags, store, workspace=None):
    resource_id = resource.get("id")
    key = notification_key(resource_type, resource_id)
    owner = resolve_owner_email(resource, resource_type)
    now_iso = datetime.now(timezone.utc).isoformat()
    ws = resource if resource_type == "workspace" else (workspace or {})
    resource_label = "workspace" if resource_type == "workspace" else f"{resource.get('type', 'item')}"

    if not owner:
        logger.warning("No owner/admin email found for %s '%s' (%s); skipping email, logging notification only",
                       resource_type, resource.get("name"), resource_id)

    subject = cfg["alerting"]["subject_template"].format(
        item_name=resource.get("name", resource_id), workspace_name=ws.get("name", ws.get("id", "")))

    if resource_type == "workspace":
        body = (f"Workspace '{resource.get('name')}' itself is missing required tag(s): "
                f"{', '.join(missing_tags)}.\n\nPlease add the missing tag(s) to the workspace. ")
    else:
        body = (f"Resource '{resource.get('name')}' (type: {resource.get('type')}) in workspace "
                f"'{ws.get('name')}' is missing required tag(s): {', '.join(missing_tags)}.\n\n"
                f"Please add the missing tag(s). ")

    deletion_cfg = cfg["deletion"]
    if deletion_cfg.get("enabled"):
        target = "workspace" if resource_type == "workspace" else "resource"
        body += (f"If tags are not added within {deletion_cfg['hours_to_delete_post_alert']} "
                 f"hour(s) of this alert, the {target} will be automatically deleted.")

    sent = False
    method = cfg["alerting"].get("method", "graph")
    if cfg["alerting"].get("enabled", True) and owner:
        if method == "activator":
            event = build_activator_event(cfg, resource, resource_type, missing_tags, owner,
                                           workspace=ws, subject=subject, body=body)
            sent = send_event_activator(cfg, event)

    existing = store.get(key) or {}
    store.upsert_alert(key, {
        "resource_type": resource_type, "resource_id": resource_id, "resource_name": resource.get("name"),
        "resource_kind": resource_label, "workspace_id": ws.get("id"), "workspace_name": ws.get("name"),
        "owner_email": owner, "missing_tags": missing_tags, "status": "alerted",
        "alerting_method": method, "email_sent": sent,
        "first_alert_utc": existing.get("first_alert_utc") or now_iso, "last_alert_utc": now_iso,
    })
    store.save()


def should_delete(cfg: Dict[str, Any], record: Dict[str, Any]) -> bool:
    deletion_cfg = cfg["deletion"]
    if not deletion_cfg.get("enabled"):
        return False
    if record.get("status") not in ("alerted",):
        return False
    first_alert = record.get("first_alert_utc")
    if not first_alert:
        return False
    first_alert_dt = datetime.fromisoformat(first_alert)
    deadline = first_alert_dt + timedelta(hours=deletion_cfg.get("hours_to_delete_post_alert", 48))
    return datetime.now(timezone.utc) >= deadline


store = NotificationStore(cfg["alerting"]["notification_log_file"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Main run --------------------------------------------------------------

def process_pending_deletions(cfg, client, store):
    deletion_cfg = cfg["deletion"]
    if not deletion_cfg.get("enabled"):
        return
    for key, record in list(store.records.items()):
        if not should_delete(cfg, record):
            continue
        resource_type = record.get("resource_type", "item")
        resource_id = record.get("resource_id")
        resource_name = record.get("resource_name")
        workspace_id = record.get("workspace_id")

        if deletion_cfg.get("dry_run", True):
            logger.info("[DRY RUN] Would delete %s '%s' (%s)%s (tags still missing %s+ hours after alert)",
                        resource_type, resource_name, resource_id,
                        f" in workspace {workspace_id}" if resource_type == "item" else "",
                        deletion_cfg.get("hours_to_delete_post_alert"))
            continue
        try:
            if resource_type == "workspace":
                client.delete_workspace(resource_id)
            else:
                client.delete_item(workspace_id, resource_id)
            store.mark_deleted(key)
            store.save()
            logger.info("Deleted non-compliant %s '%s' (%s)", resource_type, resource_name, resource_id)
        except FabricApiError as e:
            logger.error("Failed to delete %s '%s' (%s): %s", resource_type, resource_name, resource_id, e)


def run():
    run_start = datetime.now(timezone.utc)

    modified = client.get_modified_workspaces("")
    candidates = filter_excluded_workspaces(modified, cfg["scan"])

    summary = {"workspaces_evaluated": 0, "items_evaluated": 0, "missing_tags": 0}

    if not candidates:
        logger.info("No changed, non-excluded workspaces to scan this run.")
        state.save(run_start)
        process_pending_deletions(cfg, client, store)
        logger.info("=== Fabric monitor run complete (nothing to scan) ===")
        return summary

    workspace_ids = [w["id"] for w in candidates]
    scan_result = client.scan_workspaces(workspace_ids)

    for workspace in scan_result.get("workspaces", []):
        summary["workspaces_evaluated"] += 1
        ws_key = notification_key("workspace", workspace.get("id"))
        ws_compliant, ws_missing = is_compliant(workspace, cfg["tags"])

        if ws_compliant:
            existing = store.get(ws_key)
            if existing and existing.get("status") == "alerted":
                store.mark_resolved(ws_key)
                store.save()
                logger.info("Workspace '%s' now compliant; marked resolved", workspace.get("name"))
        else:
            summary["missing_tags"] += 1
            logger.warning("Workspace '%s' (%s) missing tags: %s", workspace.get("name"), workspace.get("id"), ws_missing)
            alert_if_missing_tags(cfg, sp, workspace, "workspace", ws_missing, store)

        items = workspace.get("items") or workspace.get("datasets", []) + \
            workspace.get("reports", []) + workspace.get("dataflows", [])
        for item in items:
            summary["items_evaluated"] += 1
            item_key = notification_key("item", item.get("id"))
            compliant, missing = is_compliant(item, cfg["tags"])
            if compliant:
                existing = store.get(item_key)
                if existing and existing.get("status") == "alerted":
                    store.mark_resolved(item_key)
                    store.save()
                    logger.info("Item '%s' now compliant; marked resolved", item.get("name"))
                continue
            summary["missing_tags"] += 1
            logger.warning("Item '%s' (%s) in workspace '%s' missing tags: %s",
                           item.get("name"), item.get("id"), workspace.get("name"), missing)
            alert_if_missing_tags(cfg, sp, item, "item", missing, store, workspace=workspace)

    logger.info("Scan evaluated %d workspace(s) and %d item(s); %d missing required tags",
                summary["workspaces_evaluated"], summary["items_evaluated"], summary["missing_tags"])

    process_pending_deletions(cfg, client, store)
    state.save(run_start)
    logger.info("=== Fabric monitor run complete ===")
    return summary


run_summary = run()
run_summary


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Exit value for pipeline orchestration ------------------------------------
# If this notebook is invoked from a Data Factory pipeline's Notebook activity,
# calling notebookutils.notebook.exit() surfaces this JSON as the activity's
# output "exitValue", so downstream pipeline activities can branch on it
# (e.g. only proceed / notify on non-zero missing_tags).
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

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Unsent Notifications Report (Fabric-native, standalone)
# 
# Runs **entirely inside Microsoft Fabric** as a Python notebook: reads the
# shared notifications Delta table (the same one `fabric_tag_monitor.ipynb`
# writes to) and reports every record where `notification_sent` is `False` -
# i.e. a workspace was tracked as needing an alert, but that alert was never
# actually confirmed delivered (the resend throttle suppressed it, or the
# alert-delivery notebook invocation failed).
# 
# This is a **separate, read-only** notebook from the compliance monitor -
# it never scans Fabric, never writes to the notifications table, and only
# needs a small subset of `config.json` (`auth.*` for the storage-token
# fallback, `alerting.notifications_delta_table_path`, `logging.*`). It
# shares that same `config.json` rather than needing its own.
# 
# **Auth**: reads the notifications table via the same Key Vault-backed
# service principal as the compliance monitor, requesting a OneLake
# storage-scoped token - tries `notebookutils.credentials.getToken("storage")`
# first (Fabric's own mechanism), falling back to the service principal's
# client-credentials flow if that fails.


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

# requests: AAD token calls (Key Vault SP fallback for the storage token).
# deltalake/pyarrow: reads the notifications Delta table - no Spark session needed.
%pip install requests deltalake pyarrow --quiet


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
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests 

try:
    import notebookutils  # provided automatically inside Fabric notebooks
except ImportError:
    notebookutils = None
    print("WARNING: notebookutils not available - not running inside Fabric? "
          "Key Vault secret retrieval (auth.method='key_vault') will not work.")

logger = logging.getLogger("unsent_notifications")
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
# validates the sections THIS notebook actually touches (auth, alerting,
# logging) - the compliance-monitor-only sections (scan, tags, deletion,
# adminApi) are allowed to be absent if you ever split this into its own
# config file.

class ConfigError(Exception):
    pass

REQUIRED_TOP_LEVEL_KEYS = ["auth", "alerting", "logging"]


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

    # notifications_delta_table_path is always a remote OneLake abfss:// URI
    # (enforced by fabric_tag_monitor.ipynb's own load_config()) - this
    # read-only notebook just trusts that invariant and fails clearly if
    # it's ever violated, rather than silently trying to resolve a local path.
    notif_path = cfg["alerting"]["notifications_delta_table_path"]
    if not notif_path.startswith("abfss://"):
        raise ConfigError(
            f"alerting.notifications_delta_table_path must be an abfss:// OneLake URI, "
            f"got: '{notif_path}'"
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

logger.info("=== Unsent notifications report starting ===")
logger.info("Config file: %s", config_full_path)
logger.info("Notifications Delta table path: %s", cfg["alerting"]["notifications_delta_table_path"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Auth (Key Vault only) --------------------------------------------------
# Only used as a FALLBACK for the OneLake storage token, if
# notebookutils.credentials.getToken("storage") fails - see
# get_unsent_notifications() below.

class AuthError(Exception):
    pass


def _sp_from_key_vault(auth_cfg: Dict[str, Any]) -> Dict[str, str]:
    if notebookutils is None:
        raise AuthError("notebookutils is unavailable; auth.method='key_vault' only works inside a Fabric notebook.")
    kv_url = auth_cfg["key_vault_url"]
    names = auth_cfg["key_vault_secret_names"]

    tenant_id = notebookutils.credentials.getSecret(kv_url, names["tenant_id"])
    client_id = notebookutils.credentials.getSecret(kv_url, names["client_id"])
    client_secret = notebookutils.credentials.getSecret(kv_url, names["client_secret"])
    return {"tenant_id": tenant_id, "client_id": client_id, "client_secret": client_secret}


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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Unsent notifications retrieval ------------------------------------------

ONELAKE_STORAGE_SCOPE = "https://storage.azure.com/.default"


def _storage_options(table_path: str) -> Dict[str, str]:
    '''Always returns a bearer-token dict - notifications_delta_table_path
    is guaranteed to be an abfss:// OneLake URI (validated in load_config()
    above). Tries notebookutils.credentials.getToken("storage") first
    (Fabric's own OneLake token mechanism), falling back to the service
    principal client-credentials flow if that fails - same dual-mechanism
    pattern as fabric_tag_monitor.ipynb.'''
    if notebookutils is not None:
        try:
            token = notebookutils.credentials.getToken("storage")
            logger.info("Acquired OneLake storage token via notebookutils.credentials.getToken('storage')")
            return {"bearer_token": token, "use_fabric_endpoint": "true"}
        except Exception as e:
            logger.warning("notebookutils.credentials.getToken('storage') failed (%s); falling back to "
                           "the service principal client-credentials flow for the storage token.", e)

    sp = get_service_principal(cfg)
    token = get_access_token(sp, ONELAKE_STORAGE_SCOPE)
    logger.info("Acquired OneLake storage token via service principal client-credentials flow (fallback)")
    return {"bearer_token": token, "use_fabric_endpoint": "true"}


def get_unsent_notifications(table_path: str) -> Dict[str, Any]:
    '''Reads the notifications Delta table and filters to rows where
    notification_sent is False. Useful for spotting alerts that were
    tracked in the table but never actually delivered - e.g. to retry
    them, or audit how often sends are failing/being suppressed. Returns
    the same record shape load_notifications() uses in the compliance
    monitor (violations/history JSON fields decoded back into lists).'''
    try:
        from deltalake import DeltaTable
    except ImportError as e:
        raise ImportError("The 'deltalake' package is required - see the %pip install cell above.") from e

    try:
        dt = DeltaTable(table_path, storage_options=_storage_options(table_path))
    except Exception as e:
        logger.info("No existing notifications Delta table at %s: %s", table_path, e)
        return {}

    import pyarrow.compute as pc
    table = dt.to_pyarrow_table()
    filtered = table.filter(pc.field("notification_sent") == False)
    rows = filtered.to_pylist()

    records = {}
    for row in rows:
        key = row.pop("key", None)
        if not key:
            continue
        row["violations"] = json.loads(row.pop("violations_json", "") or "[]")
        row["history"] = json.loads(row.pop("history_json", "") or "[]")
        records[key] = row
    logger.info("Found %d notification record(s) with notification_sent=False in %s", len(records), table_path)
    return records


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Main run ----------------------------------------------------------------

def run():
    unsent = get_unsent_notifications(cfg["alerting"]["notifications_delta_table_path"])

    for key, rec in unsent.items():
        logger.warning("UNSENT: %s (%s) - %s violation(s), status=%s, first_alert_utc=%s",
                       rec.get("workspace_name"), rec.get("workspace_id"),
                       rec.get("violation_count"), rec.get("status"), rec.get("first_alert_utc"))

    summary = {
        "unsent_count": len(unsent),
        "unsent_workspace_ids": [rec.get("workspace_id") for rec in unsent.values()],
    }
    logger.info("=== Unsent notifications report complete: %s ===", summary)
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

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Create Notifications Table (Fabric-native, standalone, run once)
# 
# **Run this ONCE, before the first run of `fabric_tag_monitor_python.ipynb`
# or `fabric_send_alerts.ipynb`** - a setup step, not a recurring pipeline
# stage. It pre-creates the shared `tag_notifications` Delta table via
# `delta-rs` (this same Python approach both other notebooks already use),
# **not** Spark SQL DDL.
# 
# **Why not just use a Spark `CREATE TABLE` DDL?** Fabric's Spark runtime
# can auto-enable Delta's "Column Mapping" table feature on tables it
# creates - directly, or implicitly via OneLake's Iceberg-format table
# compatibility. `delta-rs` (the engine behind the `deltalake` Python
# package both other notebooks use) has known, currently-open gaps
# supporting column-mapped tables, producing exactly this error the moment
# either notebook tries to read or write the table:
# ```
# CommitFailedError: Unsupported reader features required: [ColumnMapping]
# ```
# Creating the table via `delta-rs` directly instead sidesteps this
# entirely - verified below to produce a table at the lowest Delta
# protocol version, with no optional features enabled at all.
# 
# **Is this notebook strictly required?** No - `fabric_tag_monitor_python.ipynb`
# already auto-creates this table correctly on its own first write, via the
# exact same `write_deltalake()` mechanism used here. This notebook exists
# for the case where you want the table to visibly exist (e.g., in
# Lakehouse Explorer) *before* either main notebook has ever run - or to
# recreate it cleanly if it was previously created via Spark DDL and is now
# hitting the column-mapping error above.


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
# deltalake/pyarrow: creates the table - no Spark session needed.
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
import pyarrow as pa

try:
    import notebookutils  # provided automatically inside Fabric notebooks
except ImportError:
    notebookutils = None
    print("WARNING: notebookutils not available - not running inside Fabric? "
          "Key Vault secret retrieval (auth.method='key_vault') will not work.")

logger = logging.getLogger("create_notifications_table")
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
# Shares the SAME config.json as the other notebooks, validating only the
# sections this one needs: auth, alerting.notifications_delta_table_path, logging.

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


def _require_abfss(value: str, label: str) -> str:
    if not value.startswith("abfss://"):
        raise ConfigError(f"{label} must be an abfss:// OneLake URI, got: '{value}'")
    return value


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

    _require_abfss(cfg["alerting"]["notifications_delta_table_path"], "alerting.notifications_delta_table_path")

    cfg["logging"]["log_file"] = str(_resolve_path(cfg["logging"]["log_file"], base_dir, "logging.log_file"))
    return cfg


config_full_path = str(Path(LAKEHOUSE_MOUNT) / CONFIG_RELATIVE_PATH)
cfg = load_config(config_full_path, LAKEHOUSE_MOUNT)

_file_handler = logging.FileHandler(cfg["logging"]["log_file"])
_file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                                              datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(_file_handler)
logger.setLevel(getattr(logging, str(cfg["logging"].get("level", "INFO")).upper(), logging.INFO))

logger.info("=== Create notifications table starting ===")
logger.info("Target table: %s", cfg["alerting"]["notifications_delta_table_path"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Auth (Key Vault only, fallback for the storage token) -------------------

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
        raise AuthError(f"Unsupported auth.method '{method}'. Only 'key_vault' is supported.")
    sp = _sp_from_key_vault(auth_cfg)
    if not (sp["tenant_id"] and sp["client_id"] and sp["client_secret"]):
        raise AuthError("Service principal credentials incomplete - check auth.key_vault_secret_names.")
    logger.info("Resolved service principal via Key Vault (client_id=%s...)", sp["client_id"][:8])
    return sp


def get_access_token(sp: Dict[str, str], scope: str,
                      authority_base: str = "https://login.microsoftonline.com") -> str:
    token_url = f"{authority_base}/{sp['tenant_id']}/oauth2/v2.0/token"
    data = {"grant_type": "client_credentials", "client_id": sp["client_id"],
            "client_secret": sp["client_secret"], "scope": scope}
    resp = requests.post(token_url, data=data, timeout=30)
    if resp.status_code != 200:
        raise AuthError(f"Failed to acquire token for scope '{scope}': {resp.status_code} {resp.text}")
    token = resp.json().get("access_token")
    if not token:
        raise AuthError(f"Token response for scope '{scope}' did not contain an access_token")
    return token


ONELAKE_STORAGE_SCOPE = "https://storage.azure.com/.default"


def _storage_options(table_path: str) -> Dict[str, str]:
    if notebookutils is not None:
        try:
            token = notebookutils.credentials.getToken("storage")
            logger.info("Acquired OneLake storage token via notebookutils.credentials.getToken('storage')")
            return {"bearer_token": token, "use_fabric_endpoint": "true"}
        except Exception as e:
            logger.warning("notebookutils.credentials.getToken('storage') failed (%s); falling back.", e)
    sp = get_service_principal(cfg)
    token = get_access_token(sp, ONELAKE_STORAGE_SCOPE)
    logger.info("Acquired OneLake storage token via service principal client-credentials flow (fallback)")
    return {"bearer_token": token, "use_fabric_endpoint": "true"}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Table creation (pure delta-rs, no Spark) ---------------------------------
# Must exactly match NOTIFICATION_SCHEMA in both fabric_tag_monitor_python.ipynb
# and fabric_send_alerts.ipynb - keep all three in sync by hand if this
# schema ever changes.

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
    ("latest_alert_utc", pa.string()),
])


def create_tag_notifications_table(table_path: str) -> str:
    '''Creates the shared notifications table via delta-rs directly - NOT
    Spark DDL - avoiding the column-mapping auto-enablement that can come
    from Fabric's Spark table-creation path. Safe to re-run: if the table
    already exists, does nothing rather than risk overwriting real data.
    Returns "created" or "already_existed".'''
    try:
        from deltalake import DeltaTable
        DeltaTable(table_path, storage_options=_storage_options(table_path))
        logger.info("Table already exists at %s - doing nothing (safe no-op).", table_path)
        return "already_existed"
    except Exception as e:
        logger.info("Table does not exist yet at %s (%s) - creating it.", table_path, e)

    from deltalake import write_deltalake
    empty_table = pa.Table.from_pylist([], schema=NOTIFICATION_SCHEMA)
    write_deltalake(table_path, empty_table, mode="error", storage_options=_storage_options(table_path))
    logger.info("Created empty tag_notifications table at %s via delta-rs (no Spark, no column mapping).",
                table_path)
    return "created"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Main run ----------------------------------------------------------------

def run():
    result = create_tag_notifications_table(cfg["alerting"]["notifications_delta_table_path"])
    summary = {"table_path": cfg["alerting"]["notifications_delta_table_path"], "result": result}
    logger.info("=== Create notifications table complete: %s ===", summary)
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

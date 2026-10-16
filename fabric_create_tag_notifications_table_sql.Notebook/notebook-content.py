# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Create Notifications Table from SQL (Fabric Python notebook, run once)
# 
# Creates the shared `tag_notifications` Delta table from a **SQL `CREATE TABLE`** definition, on a plain
# **Python** cluster - no Spark. Run it once before the first run of `fabric_tag_monitor_python.ipynb`
# (optional: that notebook also creates the table itself on its first write).
# 
# **Why the SQL isn't just executed as SQL.** Spark SQL needs a Spark cluster, and Fabric's Spark runtime can
# enable Delta's *Column Mapping* feature on tables it creates, which `delta-rs` (used by both pipeline
# notebooks) can't read (`Unsupported reader features required: [ColumnMapping]`). `%%tsql` in a Python
# notebook can run full DDL against a warehouse or SQL database, but only read-only queries against a
# Lakehouse SQL analytics endpoint, so it can't create this table either. So this notebook parses the SQL
# (`sqlglot`) and creates the table with `delta-rs`, which yields the baseline Delta protocol with no optional features.
# 
# **What the SQL may contain** (anything else raises an error instead of being guessed at or silently dropped):
# one `CREATE TABLE [IF NOT EXISTS] ... USING DELTA`; column types `STRING`, `BOOLEAN`, `TINYINT`, `SMALLINT`,
# `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DATE`, `TIMESTAMP`; and `NOT NULL`. The table name must equal the last
# segment of `alerting.notifications_delta_table_path`.
# 
# **It never alters an existing table.** With `IF NOT EXISTS` an existing table is left as it is, and any
# difference between it and the SQL is reported as *schema drift* (logged as a warning and returned in the exit
# value). Notebooks writing a column list that differs from the table fail with `SchemaMismatchError`, so keep the
# SQL, `NOTIFICATION_SCHEMA` in `fabric_shared_lib`, and `fabric_send_alerts` identical.


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

# sqlglot parses the SQL; pinned to the version this was tested with (its parse tree can change between major versions).
%pip install requests deltalake pyarrow sqlglot==30.21.0 --quiet


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
import sqlglot
from sqlglot import exp
from deltalake import DeltaTable

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

# --- Table definition (SQL) ---------------------------------------------------
# Edit the table here. Keep it identical to NOTIFICATION_SCHEMA in the other notebooks.
TAG_NOTIFICATIONS_DDL = """
-- Shared notifications table used by fabric_tag_monitor_python and fabric_send_alerts.
-- The column list must match NOTIFICATION_SCHEMA in fabric_shared_lib / fabric_send_alerts exactly.
CREATE TABLE IF NOT EXISTS tag_notifications (
    key                   STRING NOT NULL,   -- "workspace_consolidated:{workspace_id}"
    workspace_id          STRING,
    workspace_name        STRING,
    compliant             BOOLEAN,           -- set by fabric_send_alerts only
    notification_sent     BOOLEAN,           -- set by fabric_send_alerts only
    violation_count       BIGINT,
    violation_signature   STRING,
    violations_json       STRING,
    tag_value             STRING,
    first_alert_utc       STRING,            -- set by fabric_send_alerts only
    latest_alert_utc      STRING             -- set by fabric_send_alerts only
)
USING DELTA
"""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- SQL runner (parses the CREATE TABLE and creates the table with delta-rs) ---------
_SQL_TO_ARROW = {
    exp.DataType.Type.TEXT: pa.string(),
    exp.DataType.Type.BOOLEAN: pa.bool_(),
    exp.DataType.Type.TINYINT: pa.int8(),
    exp.DataType.Type.SMALLINT: pa.int16(),
    exp.DataType.Type.INT: pa.int32(),
    exp.DataType.Type.BIGINT: pa.int64(),
    exp.DataType.Type.FLOAT: pa.float32(),
    exp.DataType.Type.DOUBLE: pa.float64(),
    exp.DataType.Type.DATE: pa.date32(),
    exp.DataType.Type.TIMESTAMPTZ: pa.timestamp("us", tz="UTC"),   # Spark/Delta TIMESTAMP
    exp.DataType.Type.TIMESTAMP: pa.timestamp("us"),               # TIMESTAMP_NTZ
}


def parse_create_table(ddl):
    """Parses ONE Spark-style CREATE TABLE statement into
    (table_name, if_not_exists, pyarrow_schema). Strict by design: any type or
    clause this runner does not support raises ValueError instead of being
    guessed at or silently dropped."""
    statements = [s for s in sqlglot.parse(ddl, read="spark") if s is not None]
    if len(statements) != 1 or not isinstance(statements[0], exp.Create):
        raise ValueError("Expected exactly one CREATE TABLE statement.")
    create = statements[0]
    if (create.args.get("kind") or "").upper() != "TABLE" or not isinstance(create.this, exp.Schema):
        raise ValueError("Expected 'CREATE TABLE <name> (<column definitions>)'.")

    for prop in (create.args.get("properties").expressions if create.args.get("properties") else []):
        is_delta = isinstance(prop, exp.FileFormatProperty) and str(prop.this).upper() == "DELTA"
        if not is_delta:
            raise ValueError(f"Unsupported table clause for this runner: {prop.sql(dialect='spark')} "
                             "(only 'USING DELTA' is allowed; TBLPROPERTIES / PARTITIONED BY are not applied).")

    fields = []
    for col in create.this.expressions:
        if not isinstance(col, exp.ColumnDef):
            raise ValueError(f"Unsupported element in column list: {col.sql(dialect='spark')}")
        dtype = col.args["kind"]
        if dtype.this not in _SQL_TO_ARROW or dtype.expressions:
            raise ValueError(f"Column '{col.name}': unsupported type {dtype.sql(dialect='spark')}. "
                             f"Supported: STRING, BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DATE, TIMESTAMP.")
        for constraint in col.args.get("constraints", []):
            if not isinstance(constraint.kind, exp.NotNullColumnConstraint):
                raise ValueError(f"Column '{col.name}': unsupported constraint {constraint.sql(dialect='spark')} "
                                 "(only NOT NULL is applied).")
        not_null = bool(col.args.get("constraints"))
        fields.append(pa.field(col.name, _SQL_TO_ARROW[dtype.this], nullable=not not_null))

    if not fields:
        raise ValueError("The CREATE TABLE has no columns.")
    return create.this.this.name, bool(create.args.get("exists")), pa.schema(fields)


def describe_schema_drift(existing, expected):
    """Differences between an existing table's schema and the DDL's schema, as a
    list of readable strings ([] = identical). Column order is ignored."""
    ex = {f.name: f for f in existing}
    want = {f.name: f for f in expected}
    drift = []
    for name in want:
        if name not in ex:
            drift.append(f"column '{name}' is in the SQL but missing from the table")
    for name in ex:
        if name not in want:
            drift.append(f"column '{name}' is in the table but not in the SQL")
    for name in want:
        if name in ex:
            if ex[name].type != want[name].type:
                drift.append(f"column '{name}' type differs: table has {ex[name].type}, SQL has {want[name].type}")
            if ex[name].nullable != want[name].nullable:
                drift.append(f"column '{name}' nullability differs: table nullable={ex[name].nullable}, "
                             f"SQL nullable={want[name].nullable}")
    return drift


def run_create_table(ddl, table_path, storage_options=None):
    """Runs the DDL on a Python cluster by creating the Delta table with delta-rs
    (no Spark session). Mirrors SQL semantics: with IF NOT EXISTS an existing
    table is left untouched; without it, an existing table is an error.
    Returns (result, drift): result is 'created' or 'already_existed'; drift lists
    how an EXISTING table differs from the SQL (an untouched table can silently
    disagree with the SQL, so it is reported, never altered).
    mode='error' on create is the safety net: it can never overwrite a table."""
    name, if_not_exists, schema = parse_create_table(ddl)
    path_name = table_path.rstrip("/").rsplit("/", 1)[-1]
    if path_name != name:
        raise ValueError(f"DDL creates table '{name}' but the target path ends in '{path_name}': {table_path}")

    if DeltaTable.is_deltatable(table_path, storage_options=storage_options):
        if not if_not_exists:
            raise ValueError(f"Table already exists at {table_path} and the DDL has no IF NOT EXISTS.")
        existing = pa.schema(DeltaTable(table_path, storage_options=storage_options).schema().to_arrow())
        return "already_existed", describe_schema_drift(existing, schema)

    DeltaTable.create(table_path, schema=schema, mode="error", storage_options=storage_options)
    return "created", []


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Main run ----------------------------------------------------------------

def run():
    table_path = cfg["alerting"]["notifications_delta_table_path"]
    result, drift = run_create_table(TAG_NOTIFICATIONS_DDL, table_path,
                                     storage_options=_storage_options(table_path))
    if result == "created":
        logger.info("Created table at %s from the SQL definition.", table_path)
    else:
        logger.info("Table already exists at %s - left untouched.", table_path)
    for d in drift:
        logger.warning("Schema drift: %s", d)
    if drift:
        logger.warning("The existing table does NOT match the SQL. This notebook never alters an existing table. "
                       "Notebooks writing a different column list will fail with SchemaMismatchError - align the "
                       "SQL and NOTIFICATION_SCHEMA with the table, or migrate the table.")
    summary = {"table_path": table_path, "result": result, "schema_drift": drift}
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

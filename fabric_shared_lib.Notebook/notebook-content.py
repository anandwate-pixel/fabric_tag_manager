# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   }
# META }

# MARKDOWN ********************

# # Shared Functions Library
# 
# Common functions used by BOTH `fabric_tag_monitor_python.ipynb` and
# `fabric_send_alerts.ipynb`, pulled in via `%run fabric_shared_lib` rather
# than duplicated in each notebook.
# 
# **Depends on globals already existing in the calling notebook** when
# these functions are actually *called* (not when this notebook is `%run`,
# which just defines them): `logger`, `notebookutils`, `sp`,
# `get_access_token`. Run `%run fabric_shared_lib` **after** your Auth cell
# in each calling notebook, not before.
# 
# **Deliberately NOT included**: `ConfigError`/`AuthError`/`get_service_principal`/
# `get_access_token`/`_sp_from_key_vault` - Auth currently has genuinely
# different behavior between the two calling notebooks (tag monitor allows
# a plain-config fallback for `tenant_id`/`client_id`/`client_secret`;
# send_alerts is Key-Vault-only). Unifying these needs a decision about
# which behavior should win across both notebooks, not a silent merge -
# ask for this separately once that's decided.


# CELL ********************

# Logging handler that buffers records and appends them in batches to one Lakehouse log file
# via notebookutils.fs.append, instead of holding an open file handle on the mounted path.
class LakehouseAppendHandler(logging.Handler):

    # Stores the target file path, the batch size (capacity) and the level that forces an
    # immediate write (flush_level), and starts with an empty in-memory buffer.
    def __init__(self, path, capacity=100, flush_level=logging.ERROR):
        super().__init__()
        self.path = path
        self.capacity = capacity
        self.flush_level = flush_level
        self._buf = []
        self._threshold = capacity

    # Formats the record and buffers it, writing the batch once it reaches capacity or the record
    # is at flush_level or above. Errors inside go to handleError, never up into the calling code.
    def emit(self, record):
        try:
            self._buf.append(self.format(record))
            if len(self._buf) >= self._threshold or record.levelno >= self.flush_level:
                self.flush()
        except Exception:
            self.handleError(record)

    # Appends all buffered records to the log file in one fs.append call, then clears the buffer.
    # On failure it keeps the records for a later retry and prints a warning to stderr.
    def flush(self):
        self.acquire()
        try:
            if not self._buf:
                return
            try:
                notebookutils.fs.append(self.path, "\n".join(self._buf) + "\n", True)
                self._buf.clear()
                self._threshold = self.capacity
            except Exception as e:
                # Don't hammer a failing store: wait for another `capacity`
                # records before the automatic retry (explicit flush() always tries).
                self._threshold = len(self._buf) + self.capacity
                sys.stderr.write(
                    f"LakehouseAppendHandler: append to {self.path} failed "
                    f"({type(e).__name__}: {e}); {len(self._buf)} record(s) kept for retry\n"
                )
        finally:
            self.release()

    # Makes a final write attempt at shutdown and prints any still-unwritten records to stderr
    # so they are not lost silently, then runs the base class cleanup.
    def close(self):
        self.flush()
        if self._buf:
            sys.stderr.write("Unwritten log records:\n" + "\n".join(self._buf) + "\n")
        super().close()

# Builds a month-stamped log path, e.g. fabric_monitor_202610.log.
# The month is fixed at call time, so a run crossing midnight stays in the month it started in.
def monthly_log_path(base_path, now=None):
    try:
        now = now or datetime.now(timezone.utc)
        stem, ext = os.path.splitext(base_path)
        return f"{stem}_{now:%Y%m}{ext}"
    except Exception as e:
        raise

# Deletes <name>_YYYYMM<ext> logs outside the last keep_months calendar months (current month counts),
# judged by file-name month. Logs listing/delete failures instead of raising.
def purge_old_monthly_logs(base_path, keep_months=6, now=None):
    now = now or datetime.now(timezone.utc)
    keep_months = max(int(keep_months), 1)                 # always keep at least the current month
    current = now.year * 12 + (now.month - 1)
 
    log_dir = os.path.dirname(base_path)
    base_name, ext = os.path.splitext(os.path.basename(base_path))
    pattern = re.compile(rf"^{re.escape(base_name)}_(\d{{4}})(\d{{2}}){re.escape(ext)}$")
 
    purged = []
    try:
        entries = notebookutils.fs.ls(log_dir)
    except Exception as e:
        logger.warning("Log purge skipped: could not list %s (%s: %s)", log_dir, type(e).__name__, e)
        raise
 
    for entry in entries:
        m = pattern.match(entry.name)
        if not m:
            continue
        year, month = int(m.group(1)), int(m.group(2))
        if not 1 <= month <= 12:
            continue
        if year * 12 + (month - 1) > current - keep_months:    # inside the retention window
            continue
        target = f"{log_dir}/{entry.name}"                     # same path form used for appends

        try:
            notebookutils.fs.rm(target)
            logger.info("Purged old log %s", target)
            purged.append(entry.name)
        except Exception as e:
            logger.warning("Could not purge %s (%s: %s)", target, type(e).__name__, e)
            raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Loading configurations by resolving paths against the Lakehouse Files mount

class ConfigError(Exception):
    pass

REQUIRED_TOP_LEVEL_KEYS = ["auth", "api", "scan", "tags", "alerting", "deletion", "logging"]

def _resolve_path(raw_value: str, base_dir: str, label: str) -> Path:
    try:
        expanded = os.path.expandvars(os.path.expanduser(raw_value))
        p = Path(expanded)
        if not p.is_absolute():
            p = Path(base_dir) / p
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise ConfigError(f"Could not create directory for {label} at '{p.parent}': {e}") from e
        return p
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise        


def load_config(config_path: str, base_dir: str) -> Dict[str, Any]:
    try:
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
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

try:
    config_full_path = str(Path(LAKEHOUSE_MOUNT) / CONFIG_RELATIVE_PATH)
    cfg = load_config(config_full_path, LAKEHOUSE_MOUNT)

    # Now that we know the log path, attach a file handler too.
    _file_handler = LakehouseAppendHandler(monthly_log_path(cfg["logging"]["log_file"]))
    
    _file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                                                datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(_file_handler)
    # if not cfg["logging"].get("console", True):
    #     logger.removeHandler(_console)
    logger.setLevel(getattr(logging, str(cfg["logging"].get("level", "INFO")).upper(), logging.INFO))

    purge_old_monthly_logs(cfg["logging"]["log_file"],
                        keep_months=cfg["logging"].get("retention_months", 6))
except Exception as e:
    logging.error(f"Error Occurred: {str(e)}")
    raise

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
    try:
        if notebookutils is None:
            raise AuthError("notebookutils is unavailable; auth.method='key_vault' only works inside a Fabric notebook.")
        kv_url = auth_cfg["key_vault_url"]
        names = auth_cfg["key_vault_secret_names"]

        tenant_id = auth_cfg.get("tenant_id") or notebookutils.credentials.getSecret(kv_url, names["tenant_id"])
        client_id = auth_cfg.get("client_id") or notebookutils.credentials.getSecret(kv_url, names["client_id"])
        client_secret = auth_cfg.get("client_secret") or notebookutils.credentials.getSecret(kv_url, names["client_secret"])
        return {"tenant_id": tenant_id, "client_id": client_id, "client_secret": client_secret}
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# Validates auth.method and dispatches to the Key Vault credential resolver.
# Raises if the method is unsupported or any resolved credential is empty.
def get_service_principal(cfg: Dict[str, Any]) -> Dict[str, str]:
    try:
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
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# Exchanges service principal credentials for an AAD token via client-credentials flow.
# Raises AuthError on a non-200 response or a missing access_token in the reply.
def get_access_token(sp: Dict[str, str], scope: str,
                      authority_base: str) -> str:
    try:
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
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

try:
    sp = get_service_principal(cfg)
    fabric_scope = cfg["auth"].get("scope", "https://api.fabric.microsoft.com/.default")
    auth_base_url = cfg["auth"].get("authority_base", "https://login.microsoftonline.com")
    fabric_token = get_access_token(sp, fabric_scope, auth_base_url)
    logger.info("Acquired Fabric API access token")
except Exception as e:
    logging.error(f"Error Occurred: {str(e)}")
    raise

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
    try:
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
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# Low-level HTTP call shared by every Fabric API function below.
# Routes DELETEs and /admin/tags to the admin base URL, raises FabricApiError on 4xx/5xx.
def _request(cfg: Dict[str, Any], method: str, path: str, **kwargs) -> requests.Response:
    try:
        if method == "DELETE" or path == "/admin/tags":
            url = f"{fabric_client['admin_base_url']}{path}"
        else:
            url = f"{fabric_client['base_url']}{path}"
        resp = fabric_client["session"].request(method, url, timeout=fabric_client["timeout"], **kwargs)

        if resp.status_code >= 400:
            logger.error("Fabric API error %s %s -> %s: %s", method, url, resp.status_code, resp.text)
            raise FabricApiError(f"{method} {url} failed: {resp.status_code} {resp.text}")
        return resp
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# Lists workspaces changed since a given timestamp (or all, if None).
# Respects scan.include_personal_workspaces when building the request params.
def get_modified_workspaces(cfg: Dict[str, Any], modified_since_iso: Optional[str]) -> List[Dict[str, Any]]:
    try:
        scan_cfg = cfg["scan"]
        params = {
            "excludePersonalWorkspaces": str(
                not scan_cfg.get("include_personal_workspaces", False)
            ).lower()
        }
        if modified_since_iso:
            params["modifiedSince"] = modified_since_iso

        resp = _request(cfg, "GET", "/admin/workspaces/modified", params=params)
        workspaces = resp.json()
        logger.info("Fabric reports %d modified workspace(s) since %s",
                    len(workspaces), modified_since_iso or "<beginning>")
        return workspaces
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# Triggers a new Admin Scanner run over a batch of workspace ids.
# Returns the scan id used to poll status and later fetch results.
def start_scan(cfg: Dict[str, Any], workspace_ids: List[str]) -> str:
    try:
        opts = cfg["scan"].get("scan_options", {})
        params = {k: str(v).lower() for k, v in opts.items()}
        body = {"workspaces": workspace_ids}
        resp = _request(cfg, "POST", "/admin/workspaces/getInfo", params=params, json=body)
        scan_id = resp.json().get("id")
        if not scan_id:
            raise FabricApiError("Scan trigger response missing scan id")
        logger.info("Started scan %s for %d workspace(s)", scan_id, len(workspace_ids))
        return scan_id
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# Polls a scan's status until it succeeds, fails, or times out.
# Raises FabricApiError on failure or on exceeding poll_timeout_seconds.
def wait_for_scan(cfg: Dict[str, Any], scan_id: str) -> None:
    try:
        scan_cfg = cfg["scan"]
        poll_interval = scan_cfg.get("poll_interval_seconds", 5)
        timeout = scan_cfg.get("poll_timeout_seconds", 300)
        waited = 0
        while waited <= timeout:
            resp = _request(cfg, "GET", f"/admin/workspaces/scanStatus/{scan_id}")
            status = resp.json().get("status")
            logger.debug("Scan %s status=%s (waited=%ss)", scan_id, status, waited)
            if status == "Succeeded":
                return
            if status == "Failed":
                raise FabricApiError(f"Scan {scan_id} failed")
            time.sleep(poll_interval)
            waited += poll_interval
        raise FabricApiError(f"Scan {scan_id} timed out after {timeout}s")
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# Fetches the full scan result payload for a completed scan id.
# Called only after wait_for_scan() confirms the scan succeeded.
def get_scan_result(cfg: Dict[str, Any], scan_id: str) -> Dict[str, Any]:
    try:
        resp = _request(cfg, "GET", f"/admin/workspaces/scanResult/{scan_id}")
        return resp.json()
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# Runs the full scan cycle (start, wait, fetch) across all workspace ids.
# Batches requests per scan.batch_size and merges results into one list.
def scan_workspaces(cfg: Dict[str, Any], workspace_ids: List[str]) -> Dict[str, Any]:
    try:
        batch_size = cfg["scan"].get("batch_size", 100)
        merged_workspaces: List[Dict[str, Any]] = []
        for i in range(0, len(workspace_ids), batch_size):
            batch = workspace_ids[i:i + batch_size]
            scan_id = start_scan(cfg, batch)
            wait_for_scan(cfg, scan_id)
            result = get_scan_result(cfg, scan_id)
            merged_workspaces.extend(result.get("workspaces", []))
        return {"workspaces": merged_workspaces}
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise
try:        
    fabric_client = build_fabric_client(cfg, fabric_token)
except Exception as e:
    logging.error(f"Error Occurred: {str(e)}")
    raise    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- OneLake storage auth -----------------------------------------------------
# Returns OneLake auth headers for Delta reads/writes via abfss://.
# Tries the native Fabric storage token first, falls back to the SP flow.

ONELAKE_STORAGE_SCOPE = "https://storage.azure.com/.default"


def _storage_options(table_path: str) -> Dict[str, str]:
    try:
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
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Shared notifications table: schema + read/write --------------------------
# Matches the DDL exactly: key, workspace_id, workspace_name, compliant,
# notification_sent, violation_count, violation_signature, violations_json,
# first_alert_utc, latest_alert_utc.
#
# fabric_tag_monitor_python.ipynb owns and refreshes: workspace_name,
# violation_count, violation_signature, violations_json.
# fabric_send_alerts.ipynb owns: compliant, notification_sent,
# first_alert_utc, latest_alert_utc.
# Each notebook only ever touches its own columns - see each notebook's
# own upsert logic for how that separation is enforced.

NOTIFICATION_SCHEMA = pa.schema([
    ("key", pa.string()),
    ("workspace_id", pa.string()),
    ("workspace_name", pa.string()),
    ("compliant", pa.bool_()),
    ("notification_sent", pa.bool_()),
    ("violation_count", pa.int64()),
    ("violation_signature", pa.string()),
    ("violations_json", pa.string()),
    ("appid_value", pa.string()),
    ("first_alert_utc", pa.string()),
    ("last_alert_utc", pa.string()),
])
_NOTIFICATION_FIELD_NAMES = [f.name for f in NOTIFICATION_SCHEMA]


# Builds the notification_records dict key for a given workspace.
# Format: "workspace_consolidated:{workspace_id}".
def workspace_notification_key(workspace_id: str) -> str:
    return f"workspace_consolidated:{workspace_id}"


# Reads the whole shared table into a dict keyed by "key" column value.
# Called ONCE per run, before the scan/processing loop - not per workspace.
def load_notifications(table_path: str) -> Dict[str, Any]:
    try:
        records = dict()
        from deltalake import DeltaTable
        dt = DeltaTable(table_path, storage_options=_storage_options(table_path))
        rows = dt.to_pyarrow_table().to_pylist()

        for row in rows:
            key = row.pop("key", None)
            if not key:
                continue
            row["violations"] = json.loads(row.pop("violations_json", "") or "[]")
            records[key] = row
        logger.info("Loaded %d notification record(s) from Delta table %s", len(records), table_path)
        return records
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise        


# Writes the full in-memory records dict back to the shared table in ONE
# write - called ONCE after the scan/processing loop, not per workspace.
# Conforms every row to NOTIFICATION_SCHEMA, so any field a given caller
# doesn't set on a particular record comes through as whatever value
# already existed there - never silently defaulted or dropped.
def save_notifications(table_path: str, records: Dict[str, Any]) -> None:
    try:
        from deltalake import write_deltalake

        if not records:
            return

        rows = []
        for key, rec in records.items():
            row = dict(rec)
            row["key"] = key
            row["violations_json"] = json.dumps(row.pop("violations", []))
            rows.append(row)

        conformed_rows = [{field: row.get(field) for field in _NOTIFICATION_FIELD_NAMES} for row in rows]
        table = pa.Table.from_pylist(conformed_rows, schema=NOTIFICATION_SCHEMA)

        write_deltalake(table_path, table, mode="overwrite", storage_options=_storage_options(table_path))
        logger.debug("Saved %d notification record(s) to Delta table %s", len(rows), table_path)
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Builds a tag-id -> display-name map from GET /admin/tags, paginated.
# Lets tag_checker match prefixes against real names instead of raw GUIDs.
def list_tags(cfg: Dict[str, Any]) -> Dict[str, str]:
    try:
        tag_map: Dict[str, str] = {}
        continuation_token = None
        while True:
            params = {"continuationToken": continuation_token} if continuation_token else {}
            resp = _request(cfg, "GET", "/admin/tags", params=params)
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
    real name, no resolution needed).

    If a workspace's name matches patterns for MORE than one environment,
    that's not this function's concern - it just returns the FIRST
    matching tag's value, in whatever order workspace["tags"] lists them.'''
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
    except Exception as e:
        logging.error(f"Error Occurred: {str(e)}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

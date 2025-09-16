import os
import time
import json
import logging
from datetime import datetime, timezone, timedelta

import azure.functions as func
import requests
from requests.exceptions import RequestException
from dateutil import parser as dateparser

from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

# ---- Konstanty / výchozí hodnoty ----
BINANCE_LIMIT = 1000
SLEEP_SEC = 0.2
DEFAULT_GLOBAL_TIME_BUDGET_SEC = 500   # ~8m20s
DEFAULT_SYMBOL_TIME_BUDGET_SEC = 120   # 2 min na symbol

# Hlavičky
BASE_HEADER = "openTime,open,high,low,close,volume,closeTime,quoteVolume,numTrades,takerBuyBase,takerBuyQuote,ignore"
NEW_HEADER = BASE_HEADER + ",closeTimeISO"

logger = logging.getLogger("fetch_klines")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ---------- Pomocné časové ----------
def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)

def iso_utc(ms: int) -> str:
    return from_ms(ms).strftime("%Y-%m-%dT%H:%M:%SZ")

def floor_to_interval(dt: datetime, minutes: int) -> datetime:
    m = (dt.minute // minutes) * minutes
    return dt.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=m)

def interval_to_minutes(interval_str: str) -> int:
    """Podporuje 'Xm','Xh','Xd','Xw','XM' (M ~30d pro zarovnání)."""
    if not interval_str or len(interval_str) < 2:
        raise RuntimeError(f"Invalid BINANCE_INTERVAL: {interval_str!r}")
    unit = interval_str[-1]
    val = int(interval_str[:-1])
    if unit == "m":
        return val
    if unit == "h":
        return val * 60
    if unit == "d":
        return val * 60 * 24
    if unit == "w":
        return val * 60 * 24 * 7
    if unit == "M":
        return val * 60 * 24 * 30
    raise RuntimeError(f"Unsupported interval unit in BINANCE_INTERVAL: {interval_str!r}")

# ---------- Env ----------
def get_env(name: str, default: str = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def get_env_int(name: str, default_int: int) -> int:
    raw = os.getenv(name, "")
    if not raw:
        return default_int
    try:
        return int(raw)
    except ValueError:
        return default_int

def parse_symbols(env_val: str) -> list[str]:
    if not env_val:
        return []
    raw = env_val.replace(";", ",").split(",")
    out = []
    for piece in raw:
        for token in piece.strip().split():
            if token:
                out.append(token.upper())
    seen = set(); res = []
    for s in out:
        if s not in seen:
            seen.add(s); res.append(s)
    return res

# ---------- Binance HTTP s retry ----------
def binance_get(url: str, params: dict, max_attempts: int = 6):
    backoff = 0.5
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (418, 429) or 500 <= r.status_code < 600:
                logger.warning(f"Binance {r.status_code} attempt {attempt}: {r.text[:200]}")
                last_err = RuntimeError(f"Binance API error {r.status_code}: {r.text}")
            else:
                raise RuntimeError(f"Binance API error {r.status_code}: {r.text}")
        except RequestException as e:
            logger.warning(f"Binance RequestException attempt {attempt}: {e}")
            last_err = e
        if attempt < max_attempts:
            time.sleep(backoff); backoff *= 2
    raise (last_err or RuntimeError("Unknown Binance error"))

def fetch_klines(base_url: str, symbol: str, interval_str: str, start_ms: int, end_ms: int, limit: int = BINANCE_LIMIT):
    url = f"{base_url}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval_str, "limit": limit, "startTime": start_ms, "endTime": end_ms}
    return binance_get(url, params)

# ---------- Block Blob append (jen stage+commit, nikdy upload_blob) ----------
def _ensure_container(container: ContainerClient):
    try:
        container.create_container()
    except ResourceExistsError:
        pass

def _blob_exists(blob: BlobClient) -> bool:
    try:
        blob.get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False

def _put_full_body_as_single_block(blob: BlobClient, data_bytes: bytes):
    from azure.storage.blob import BlobBlock
    import base64, secrets
    block_id = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    blob.stage_block(block_id=block_id, data=data_bytes)
    blob.commit_block_list([BlobBlock(block_id)])

def _create_block_blob_with_header(blob: BlobClient, header_line: str):
    line = header_line if header_line.endswith("\n") else header_line + "\n"
    _put_full_body_as_single_block(blob, line.encode("utf-8"))

def _ensure_block_blob_type(blob: BlobClient):
    try:
        props = blob.get_blob_properties()
        blob_type = getattr(props, "blob_type", None)
        if str(blob_type).lower() != "blockblob":
            data = blob.download_blob().readall()
            _put_full_body_as_single_block(blob, data)
            logger.info(f"{blob.blob_name}: Converted to BlockBlob via block-list (size={len(data)}).")
    except ResourceNotFoundError:
        pass

def _ensure_header_present_and_migrate(blob: BlobClient):
    try:
        props = blob.get_blob_properties()
        size = props.size or 0
        if size == 0:
            _create_block_blob_with_header(blob, NEW_HEADER)
            logger.info(f"{blob.blob_name}: empty -> wrote NEW_HEADER (block-list).")
            return

        head_bytes = blob.download_blob(offset=0, length=8192).readall()
        head = head_bytes.decode("utf-8", errors="ignore")
        head_stripped = head.lstrip("\ufeff").lstrip()
        first_line = head_stripped.splitlines()[0] if head_stripped else ""

        if first_line.replace("\r", "") == NEW_HEADER:
            return

        if first_line.replace("\r", "") == BASE_HEADER:
            full = blob.download_blob().readall().decode("utf-8", errors="ignore")
            lines = full.splitlines()
            if not lines:
                _create_block_blob_with_header(blob, NEW_HEADER)
                logger.info(f"{blob.blob_name}: old header but no data -> wrote NEW_HEADER.")
                return

            out_lines = [NEW_HEADER]
            for ln in lines[1:]:
                if not ln.strip():
                    continue
                parts = ln.split(",")
                if len(parts) >= 12 and parts[6].isdigit():
                    ct_ms = int(parts[6]); parts.append(iso_utc(ct_ms))
                    out_lines.append(",".join(parts))
                else:
                    out_lines.append(ln + ",")
            new_body = ("\n".join(out_lines) + "\n").encode("utf-8")
            _put_full_body_as_single_block(blob, new_body)
            logger.info(f"{blob.blob_name}: Migrated BASE_HEADER -> NEW_HEADER (rows={len(out_lines)-1}).")
            return

        full = blob.download_blob().readall()
        prefix = (NEW_HEADER + "\n").encode("utf-8") if not NEW_HEADER.endswith("\n") else NEW_HEADER.encode("utf-8")
        _put_full_body_as_single_block(blob, prefix + full)
        logger.info(f"{blob.blob_name}: Header missing/unknown -> prepended NEW_HEADER (block-list).")
    except ResourceNotFoundError:
        pass

def _extract_committed_ids(block_list_obj):
    committed = []
    if hasattr(block_list_obj, "committed_blocks"):
        committed = block_list_obj.committed_blocks or []
    elif isinstance(block_list_obj, (list, tuple)):
        if isinstance(block_list_obj, tuple) and len(block_list_obj) > 0:
            committed = block_list_obj[0] or []
        else:
            committed = block_list_obj
    else:
        committed = []
    ids = []
    for b in committed:
        bid = getattr(b, "id", None) or getattr(b, "name", None)
        if bid:
            ids.append(bid)
    return ids

def _append_block_blob(blob: BlobClient, payload_bytes: bytes):
    from azure.storage.blob import BlobBlock
    import base64, secrets
    bl = blob.get_block_list(block_list_type="committed")
    committed_ids = _extract_committed_ids(bl)
    new_id = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    logger.info(f"{blob.blob_name}: staging block id={new_id} size={len(payload_bytes)}")
    blob.stage_block(block_id=new_id, data=payload_bytes)
    new_list = [BlobBlock(i) for i in committed_ids] + [BlobBlock(new_id)]
    logger.info(f"{blob.blob_name}: committing block list: prev={len(committed_ids)} +1")
    blob.commit_block_list(new_list)

def kline_to_csv_line(k):
    return ",".join([str(k[i]) for i in range(12)] + [iso_utc(int(k[6]))]) + "\n"

# ---------- Stavový blob (rotace start indexu) ----------
def _get_state_blob(container: ContainerClient, state_blob_name: str) -> BlobClient:
    return container.get_blob_client(state_blob_name)

def _read_state(state_blob: BlobClient) -> dict:
    try:
        data = state_blob.download_blob().readall()
        return json.loads(data.decode("utf-8"))
    except ResourceNotFoundError:
        return {}
    except Exception as e:
        logger.warning(f"State read failed, starting fresh: {e}")
        return {}

def _write_state(state_blob: BlobClient, state: dict):
    data = json.dumps(state, separators=(",", ":")).encode("utf-8")
    _put_full_body_as_single_block(state_blob, data)

# ---------- Pomocné: výpočet názvu blobu pro symbol ----------
def _blob_path_for(symbol: str, interval_str: str, blob_dir: str, blob_name_tmpl: str) -> str:
    blob_file = blob_name_tmpl.replace("{SYMBOL}", symbol).replace("{INTERVAL}", interval_str)
    return f"{blob_dir.strip('/ ')}/{blob_file}" if blob_dir.strip() else blob_file

# ---------- Pre-init: vytvoř CSV s hlavičkou pro VŠECHNY symboly ----------
def _pre_init_all_symbols(symbols, interval_str, container: ContainerClient, blob_dir, blob_name_tmpl):
    created = 0
    for s in symbols:
        path = _blob_path_for(s, interval_str, blob_dir, blob_name_tmpl)
        blob = container.get_blob_client(path)
        if not _blob_exists(blob):
            logger.info(f"Pre-init: creating CSV for {s} at '{path}'")
            _create_block_blob_with_header(blob, NEW_HEADER)
        else:
            logger.info(f"Pre-init: CSV already exists for {s} at '{path}'")
        _ensure_block_blob_type(blob)
        _ensure_header_present_and_migrate(blob)
        created += 1
    logger.info(f"Pre-init done for {created} symbols.")

# ---------- Zpracování JEDNOHO symbolu s časovým budgetem ----------
def process_symbol(symbol: str,
                   interval_str: str,
                   container: ContainerClient,
                   blob_dir: str,
                   blob_name_tmpl: str,
                   start_date_str: str,
                   base_url: str,
                   INTERVAL_MIN: int,
                   target_end_ms: int,
                   time_budget_sec: int) -> int:
    start_exec = time.time()
    interval_ms = INTERVAL_MIN * 60 * 1000

    blob_path = _blob_path_for(symbol, interval_str, blob_dir, blob_name_tmpl)
    blob: BlobClient = container.get_blob_client(blob_path)

    # (pre-init už to udělal) – pro jistotu necháme idempotentní kontroly
    if not _blob_exists(blob):
        _create_block_blob_with_header(blob, NEW_HEADER)
    _ensure_block_blob_type(blob)
    _ensure_header_present_and_migrate(blob)

    # Najdi start_ms
    start_ms = None
    try:
        props = blob.get_blob_properties()
        size = props.size or 0
        if size > 0:
            chunk = blob.download_blob(offset=max(0, size - 65536), length=65536)\
                        .readall().decode("utf-8", errors="ignore")
            lines = [ln for ln in chunk.splitlines() if ln.strip()]
            if lines:
                parts = lines[-1].split(",")
                if len(parts) >= 7 and parts[6].isdigit():
                    last_close_ms = int(parts[6])
                    start_ms = last_close_ms + 1
    except Exception as e:
        logger.warning(f"{blob.blob_name}: tail parse failed: {e}")

    if start_ms is None:
        start_dt = dateparser.isoparse(start_date_str).astimezone(timezone.utc)
        start_ms = to_ms(start_dt)

    if start_ms > target_end_ms:
        logger.info(f"{symbol}: Up to date – nothing to fetch. (blob={blob.blob_name})")
        return 0

    logger.info(f"{symbol}: start={from_ms(start_ms)} end={from_ms(target_end_ms)} interval={interval_str} budget={time_budget_sec}s -> blob={blob.blob_name}")
    try:
        ping = requests.get(f"{base_url}/api/v3/ping", timeout=10)
        if ping.status_code != 200:
            raise RuntimeError(f"Ping failed {ping.status_code}: {ping.text}")
    except RequestException as e:
        raise RuntimeError(f"{symbol}: Binance ping failed: {e}") from e

    fetched_total = 0
    while start_ms <= target_end_ms:
        if time.time() - start_exec > time_budget_sec:
            logger.info(f"{symbol}: time budget reached, stopping symbol loop.")
            break

        batch_window_ms = BINANCE_LIMIT * interval_ms
        end_ms = min(start_ms + batch_window_ms - 1, target_end_ms)

        klines = fetch_klines(base_url, symbol, interval_str, start_ms, end_ms, BINANCE_LIMIT)
        if not klines:
            start_ms += interval_ms
            time.sleep(SLEEP_SEC)
            continue

        rows = []
        for k in klines:
            if k[6] <= target_end_ms:
                rows.append(kline_to_csv_line(k))

        if rows:
            payload = "".join(rows).encode("utf-8")
            _append_block_blob(blob, payload)
            added = len(rows)
            fetched_total += added
            start_ms = klines[-1][6] + 1
            logger.info(f"{symbol}: appended {added} rows; next={from_ms(start_ms)}")
        else:
            start_ms += interval_ms

        time.sleep(SLEEP_SEC)

    logger.info(f"{symbol}: TOTAL appended {fetched_total} rows -> {blob.blob_name}")
    return fetched_total

# ---------- Hlavní ----------
def main(mytimer: func.TimerRequest) -> None:
    run_start = time.time()

    symbols_env = get_env("BINANCE_SYMBOLS", "")
    symbols = parse_symbols(symbols_env)
    if not symbols:
        single = get_env("BINANCE_SYMBOL", "")
        if single:
            symbols = [single.upper()]
        else:
            raise RuntimeError("Set BINANCE_SYMBOLS (e.g. 'BTCUSDT, ETHUSDT') or BINANCE_SYMBOL.")
    n_symbols = len(symbols)

    interval_str = get_env("BINANCE_INTERVAL", "15m")
    INTERVAL_MIN = interval_to_minutes(interval_str)

    global_budget = get_env_int("GLOBAL_TIME_BUDGET_SEC", DEFAULT_GLOBAL_TIME_BUDGET_SEC)
    symbol_budget = get_env_int("SYMBOL_TIME_BUDGET_SEC", DEFAULT_SYMBOL_TIME_BUDGET_SEC)

    container_name = get_env("BLOB_CONTAINER", required=True)
    blob_name_tmpl = get_env("BLOB_NAME_TEMPLATE", "{SYMBOL}_{INTERVAL}.csv")
    blob_dir = get_env("BLOB_DIR", "")
    start_date_str = get_env("START_DATE_UTC", "2020-01-01T00:00:00Z")
    base_url = get_env("BINANCE_BASE_URL", "https://api.binance.com")
    conn_str = get_env("AzureWebJobsStorage", required=True)

    state_blob_name = get_env("STATE_BLOB_NAME", f"_state/scheduler_{interval_str}.json")

    now_utc = datetime.now(timezone.utc)
    last_closed = floor_to_interval(now_utc, INTERVAL_MIN) - timedelta(minutes=INTERVAL_MIN)
    target_end_ms = to_ms(last_closed) + (INTERVAL_MIN * 60 * 1000) - 1

    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container = blob_service.get_container_client(container_name)
    _ensure_container(container)

    logger.info(f"Parsed symbols: {symbols}")
    logger.info(f"Blob dir='{blob_dir}', name template='{blob_name_tmpl}', container='{container_name}'")

    # 1) PRE-INIT: vždy založ CSV pro všechny symboly (aby vznikly soubory hned)
    _pre_init_all_symbols(symbols, interval_str, container, blob_dir, blob_name_tmpl)

    # 2) Načti stav rotace a spusť symboly v rotovaném pořadí s time budgety
    state_blob = _get_state_blob(container, state_blob_name)
    state = _read_state(state_blob)
    start_idx = int(state.get("next_start_index", 0)) % n_symbols

    logger.info(f"Rotation start_index={start_idx} | global_budget={global_budget}s | per_symbol_budget={symbol_budget}s")

    total_rows = 0
    processed = 0
    i = start_idx
    while processed < n_symbols:
        elapsed_global = time.time() - run_start
        remaining_global = max(0, global_budget - elapsed_global)
        if remaining_global < 5:
            logger.info("Global time budget nearly exhausted, stopping run.")
            break

        symbol = symbols[i]
        per_symbol_budget = int(min(symbol_budget, max(5, remaining_global - 5)))

        try:
            added = process_symbol(
                symbol=symbol,
                interval_str=interval_str,
                container=container,
                blob_dir=blob_dir,
                blob_name_tmpl=blob_name_tmpl,
                start_date_str=start_date_str,
                base_url=base_url,
                INTERVAL_MIN=INTERVAL_MIN,
                target_end_ms=target_end_ms,
                time_budget_sec=per_symbol_budget,
            )
            total_rows += added
        except Exception as e:
            logger.error(f"{symbol}: FAILED -> {e}")

        processed += 1
        i = (i + 1) % n_symbols
        time.sleep(0.2)

    # Ulož next_start_index pro příští běh
    new_start_idx = i % n_symbols
    state_out = {
        "next_start_index": new_start_idx,
        "last_run_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "symbols": symbols,
        "interval": interval_str,
    }
    _write_state(state_blob, state_out)
    logger.info(f"RUN DONE: processed={processed}/{n_symbols}, total_rows_appended={total_rows}, next_start_index={new_start_idx}")

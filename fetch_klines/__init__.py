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

SENSITIVE_KEYS = {"AzureWebJobsStorage", "WEBSITE_CONTENTAZUREFILECONNECTIONSTRING",
                  "WEBSITE_RUN_FROM_PACKAGE", "APPINSIGHTS_INSTRUMENTATIONKEY"}

def safe_env():
    out = {}
    for k, v in os.environ.items():
        if k in SENSITIVE_KEYS:
            out[k] = "<redacted>"
        elif "KEY" in k.upper() or "SECRET" in k.upper() or "PASSWORD" in k.upper():
            out[k] = "<redacted>"
        else:
            out[k] = v
    return out

def preview_blob_paths(symbols, interval_str, blob_dir, blob_name_tmpl):
    paths = []
    for s in symbols:
        fname = blob_name_tmpl.replace("{SYMBOL}", s).replace("{INTERVAL}", interval_str)
        p = f"{blob_dir.strip('/ ')}/{fname}" if blob_dir.strip() else fname
        paths.append((s, p))
    return paths

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

# ---------- Ověření existence symbolu ----------
def symbol_exists(base_url: str, symbol: str) -> bool:
    """
    True, pokud symbol existuje na Binance Spot a je v statusu TRADING.
    Pokud Binance vrátí 400 (neznámý symbol) nebo jiný stav, vrací False.
    """
    url = f"{base_url}/api/v3/exchangeInfo"
    try:
        r = requests.get(url, params={"symbol": symbol}, timeout=15)
        if r.status_code == 400:
            return False
        r.raise_for_status()
        data = r.json()
        syms = data.get("symbols", [])
        if not syms:
            return False
        s0 = syms[0]
        return (s0.get("symbol") == symbol) and (s0.get("status") == "TRADING")
    except RequestException as e:
        logger.warning(f"symbol_exists: RequestException for {symbol}: {e}")
        return False
    except Exception as e:
        logger.warning(f"symbol_exists: Unexpected error for {symbol}: {e}")
        return False

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
    import base64, secrets
    block_id = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    blob.stage_block(block_id=block_id, data=data_bytes)
    # Pozn.: commit pouze s list[str] (base64 IDs)
    blob.commit_block_list([block_id])

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

def _append_block_blob(blob: BlobClient, payload_bytes: bytes):
    import base64, secrets

    # 1) načti committed block IDs jako čisté stringy
    bl = blob.get_block_list(block_list_type="committed")
    committed_ids: list[str] = []
    if bl:
        blocks = getattr(bl, "committed_blocks", None) or []
        for b in blocks:
            bid = getattr(b, "id", None) or getattr(b, "name", None)
            if isinstance(bid, bytes):
                bid = bid.decode("ascii")
            if isinstance(bid, str) and bid.strip():
                committed_ids.append(bid)

    # 2) stage nového bloku
    new_id = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    blob.stage_block(block_id=new_id, data=payload_bytes)

    # 3) commit – pošli list[str] včetně všech předchozích + nový
    new_list_ids = committed_ids + [new_id]
    blob.commit_block_list(new_list_ids)

def kline_to_csv_line(k):
    return ",".join([str(k[i]) for i in range(12)] + [iso_utc(int(k[6]))]) + "\n"

# ---------- Pomocné: výpočet názvu blobu pro symbol ----------
def _blob_path_for(symbol: str, interval_str: str, blob_dir: str, blob_name_tmpl: str) -> str:
    blob_file = blob_name_tmpl.replace("{SYMBOL}", symbol).replace("{INTERVAL}", interval_str)
    return f"{blob_dir.strip('/ ')}/{blob_file}" if blob_dir.strip() else blob_file

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
    # Bezpečnostní brzda – kdyby se někde volalo mimo hlavní filtr
    if not symbol_exists(base_url, symbol):
        logger.info(f"{symbol}: not found or not trading on Binance – skipping (no CSV).")
        return 0

    start_exec = time.time()
    interval_ms = INTERVAL_MIN * 60 * 1000

    blob_path = _blob_path_for(symbol, interval_str, blob_dir, blob_name_tmpl)
    blob: BlobClient = container.get_blob_client(blob_path)

    # CSV vytvoříme jen pro platný symbol:
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
    try:
        # ---- načti symboly ----
        symbols_env = get_env("BINANCE_SYMBOLS", "")
        symbols = parse_symbols(symbols_env)
        if not symbols:
            single = get_env("BINANCE_SYMBOL", "")
            if single:
                symbols = [single.upper()]
            else:
                raise RuntimeError("Set BINANCE_SYMBOLS (např. 'BTCUSDT, ETHUSDT') nebo BINANCE_SYMBOL.")

        interval_str = get_env("BINANCE_INTERVAL", "15m")
        INTERVAL_MIN = interval_to_minutes(interval_str)

        container_name = get_env("BLOB_CONTAINER", required=True)
        blob_name_tmpl = get_env("BLOB_NAME_TEMPLATE", "{SYMBOL}_{INTERVAL}.csv")
        blob_dir = get_env("BLOB_DIR", "")
        # normalizace BLOB_DIR
        if not blob_dir or str(blob_dir).strip().lower() in ("", "none", "/"):
            blob_dir = ""

        start_date_str = get_env("START_DATE_UTC", "2020-01-01T00:00:00Z")
        base_url = get_env("BINANCE_BASE_URL", "https://api.binance.com")
        conn_str = get_env("AzureWebJobsStorage", required=True)

        # --- DEBUG LOGS ---
        logger.info(f"Symbols parsed (input): {symbols}")
        logger.info(f"Interval: {interval_str} ({INTERVAL_MIN} min)")
        logger.info(f"Blob container: {container_name}")
        logger.info(f"Blob dir: '{blob_dir}', name template: '{blob_name_tmpl}'")
        for sym, path in preview_blob_paths(symbols, interval_str, blob_dir, blob_name_tmpl):
            logger.info(f"RESOLVED PATH -> {sym}: '{path}'")

        # ---- filtr na existující páry (TRADING) ----
        valid_symbols = []
        skipped_symbols = []
        for s in symbols:
            if symbol_exists(base_url, s):
                valid_symbols.append(s)
            else:
                skipped_symbols.append(s)

        if skipped_symbols:
            logger.warning(f"Skipping non-existing/non-trading symbols (no CSV will be created): {skipped_symbols}")
        if not valid_symbols:
            logger.error("No valid Binance symbols to process after verification. Exiting.")
            return

        # ---- časový cíl ----
        now_utc = datetime.now(timezone.utc)
        last_closed = floor_to_interval(now_utc, INTERVAL_MIN) - timedelta(minutes=INTERVAL_MIN)
        target_end_ms = to_ms(last_closed) + (INTERVAL_MIN * 60 * 1000) - 1

        # ---- Blob klient ----
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container = blob_service.get_container_client(container_name)
        _ensure_container(container)

        # Pre-init: CSV vytvoříme **pouze** pro ověřené symboly
        for s in valid_symbols:
            path = _blob_path_for(s, interval_str, blob_dir, blob_name_tmpl)
            blob = container.get_blob_client(path)
            if not _blob_exists(blob):
                logger.info(f"Init file for {s}: '{path}'")
                _create_block_blob_with_header(blob, NEW_HEADER)
            _ensure_block_blob_type(blob)
            _ensure_header_present_and_migrate(blob)

        total_rows = 0
        for s in valid_symbols:
            added = process_symbol(
                symbol=s,
                interval_str=interval_str,
                container=container,
                blob_dir=blob_dir,
                blob_name_tmpl=blob_name_tmpl,
                start_date_str=start_date_str,
                base_url=base_url,
                INTERVAL_MIN=INTERVAL_MIN,
                target_end_ms=target_end_ms,
                time_budget_sec=10**9,
            )
            total_rows += added
            time.sleep(0.2)
        logger.info(f"ALL DONE: {len(valid_symbols)} symbols processed, total rows appended: {total_rows}")

    except Exception as e:
        # Kompletní traceback do logu
        logger.error("UNHANDLED EXCEPTION in main()")
        logger.error("Environment snapshot (safe): %s", {k: safe_env().get(k) for k in [
            "BINANCE_SYMBOLS","BINANCE_SYMBOL","BINANCE_INTERVAL",
            "BLOB_CONTAINER","BLOB_NAME_TEMPLATE","BLOB_DIR","START_DATE_UTC"
        ]})
        logger.exception(e)
        raise

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

# ------------------- Konstanty -------------------
BINANCE_LIMIT = 1000
SLEEP_SEC = 0.2
BASE_HEADER = "openTime,open,high,low,close,volume,closeTime,quoteVolume,numTrades,takerBuyBase,takerBuyQuote,ignore"
NEW_HEADER = BASE_HEADER + ",closeTimeISO"

logger = logging.getLogger("fetch_klines")
logging.basicConfig(level=logging.INFO)

# ------------------- Časové pomocné -------------------
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
    unit = interval_str[-1]
    val = int(interval_str[:-1])
    if unit == "m": return val
    if unit == "h": return val * 60
    if unit == "d": return val * 60 * 24
    if unit == "w": return val * 60 * 24 * 7
    if unit == "M": return val * 60 * 24 * 30
    raise RuntimeError(f"Unsupported interval: {interval_str!r}")

# ------------------- ENV -------------------
def get_env(name: str, default: str = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return val

def parse_symbols(env_val: str) -> list[str]:
    if not env_val: return []
    raw = env_val.replace(";", ",").split(",")
    out = []
    for piece in raw:
        for token in piece.strip().split():
            if token: out.append(token.upper())
    seen = set(); res = []
    for s in out:
        if s not in seen:
            seen.add(s); res.append(s)
    return res

# ------------------- Binance -------------------
def binance_get(url: str, params: dict):
    r = requests.get(url, params=params, timeout=30)
    if r.status_code == 200:
        return r.json()
    if r.status_code == 400:
        return None
    r.raise_for_status()
    return r.json()

def fetch_klines(base_url: str, symbol: str, interval_str: str, start_ms: int, end_ms: int):
    url = f"{base_url}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval_str,
              "limit": BINANCE_LIMIT, "startTime": start_ms, "endTime": end_ms}
    return binance_get(url, params)

def symbol_exists(base_url: str, symbol: str) -> bool:
    url = f"{base_url}/api/v3/exchangeInfo"
    try:
        r = requests.get(url, params={"symbol": symbol}, timeout=15)
        if r.status_code == 400:
            return False
        r.raise_for_status()
        data = r.json()
        syms = data.get("symbols", [])
        return bool(syms and syms[0].get("status") == "TRADING")
    except Exception:
        return False

# ------------------- Blob helpers -------------------
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
    blob.commit_block_list([block_id])

def _create_block_blob_with_header(blob: BlobClient):
    _put_full_body_as_single_block(blob, (NEW_HEADER + "\n").encode("utf-8"))

def _append_block_blob(blob: BlobClient, payload_bytes: bytes):
    import base64, secrets
    try:
        bl = blob.get_block_list(block_list_type="committed")
        committed_ids = []
        if bl:
            blocks = getattr(bl, "committed_blocks", None) or []
            for b in blocks:
                bid = getattr(b, "id", None) or getattr(b, "name", None)
                if isinstance(bid, bytes):
                    bid = bid.decode("ascii")
                if isinstance(bid, str) and bid.strip():
                    committed_ids.append(bid)
        if committed_ids:
            new_id = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
            blob.stage_block(block_id=new_id, data=payload_bytes)
            blob.commit_block_list(committed_ids + [new_id])
            return
        existing = b""
        try:
            props = blob.get_blob_properties()
            if (props.size or 0) > 0:
                existing = blob.download_blob().readall()
        except ResourceNotFoundError:
            existing = b""
        combined = existing + payload_bytes
        _put_full_body_as_single_block(blob, combined)
    except Exception:
        raise

def kline_to_csv_line(k):
    return ",".join([str(k[i]) for i in range(12)] + [iso_utc(int(k[6]))]) + "\n"

def _blob_path_for(symbol: str, interval_str: str, blob_dir: str, blob_name_tmpl: str) -> str:
    blob_file = blob_name_tmpl.replace("{SYMBOL}", symbol).replace("{INTERVAL}", interval_str)
    return f"{blob_dir.strip('/ ')}/{blob_file}" if blob_dir.strip() else blob_file

# ------------------- Zpracování symbolu -------------------
def process_symbol(symbol: str,
                   interval_str: str,
                   container: ContainerClient,
                   blob_dir: str,
                   blob_name_tmpl: str,
                   start_date_str: str,
                   base_url: str,
                   INTERVAL_MIN: int,
                   target_end_ms: int) -> int:
    if not symbol_exists(base_url, symbol):
        return 0

    blob_path = _blob_path_for(symbol, interval_str, blob_dir, blob_name_tmpl)
    blob: BlobClient = container.get_blob_client(blob_path)
    if not _blob_exists(blob):
        _create_block_blob_with_header(blob)

    # najdi start_ms
    start_ms = None
    try:
        props = blob.get_blob_properties()
        if (props.size or 0) > 0:
            chunk = blob.download_blob(offset=max(0, props.size - 65536), length=65536)\
                        .readall().decode("utf-8", errors="ignore")
            lines = [ln for ln in chunk.splitlines() if ln.strip()]
            if lines:
                parts = lines[-1].split(",")
                if len(parts) >= 7 and parts[6].isdigit():
                    start_ms = int(parts[6]) + 1
    except Exception:
        pass
    if start_ms is None:
        start_dt = dateparser.isoparse(start_date_str).astimezone(timezone.utc)
        start_ms = to_ms(start_dt)

    if start_ms > target_end_ms:
        return 0

    fetched_total = 0
    while start_ms <= target_end_ms:
        batch_window_ms = BINANCE_LIMIT * (INTERVAL_MIN * 60 * 1000)
        end_ms = min(start_ms + batch_window_ms - 1, target_end_ms)
        klines = fetch_klines(base_url, symbol, interval_str, start_ms, end_ms)
        if not klines:
            start_ms += INTERVAL_MIN * 60 * 1000
            continue
        rows = [kline_to_csv_line(k) for k in klines if k[6] <= target_end_ms]
        if rows:
            payload = "".join(rows).encode("utf-8")
            _append_block_blob(blob, payload)
            fetched_total += len(rows)
            start_ms = klines[-1][6] + 1
        else:
            start_ms += INTERVAL_MIN * 60 * 1000
    return fetched_total

# ------------------- Main -------------------
def main(mytimer: func.TimerRequest) -> None:
    try:
        symbols_env = get_env("BINANCE_SYMBOLS", "")
        symbols = parse_symbols(symbols_env)
        if not symbols:
            single = get_env("BINANCE_SYMBOL", "")
            if single: symbols = [single.upper()]
            else: raise RuntimeError("No symbols provided.")

        interval_str = get_env("BINANCE_INTERVAL", "15m")
        INTERVAL_MIN = interval_to_minutes(interval_str)

        container_name = get_env("BLOB_CONTAINER", required=True)
        blob_name_tmpl = get_env("BLOB_NAME_TEMPLATE", "{SYMBOL}_{INTERVAL}.csv")
        blob_dir = get_env("BLOB_DIR", "")
        if not blob_dir or blob_dir.strip().lower() in ("", "none", "/"): blob_dir = ""

        start_date_str = get_env("START_DATE_UTC", "2020-01-01T00:00:00Z")
        base_url = get_env("BINANCE_BASE_URL", "https://api.binance.com")
        conn_str = get_env("AzureWebJobsStorage", required=True)

        now_utc = datetime.now(timezone.utc)
        last_closed = floor_to_interval(now_utc, INTERVAL_MIN) - timedelta(minutes=INTERVAL_MIN)
        target_end_ms = to_ms(last_closed) + (INTERVAL_MIN * 60 * 1000) - 1

        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container = blob_service.get_container_client(container_name)
        _ensure_container(container)

        total_rows = 0
        for s in symbols:
            total_rows += process_symbol(
                symbol=s,
                interval_str=interval_str,
                container=container,
                blob_dir=blob_dir,
                blob_name_tmpl=blob_name_tmpl,
                start_date_str=start_date_str,
                base_url=base_url,
                INTERVAL_MIN=INTERVAL_MIN,
                target_end_ms=target_end_ms,
            )
            time.sleep(0.1)
        logger.info(f"Uploaded {total_rows} rows total for {len(symbols)} symbols")
    except Exception as e:
        logger.error(f"UNHANDLED EXCEPTION: {e}", exc_info=True)
        raise

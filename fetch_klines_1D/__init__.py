import os
import time
import json
import logging
from datetime import datetime, timezone, timedelta
import csv
import io

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

# VŽDY POUŽÍT 1D
FIXED_INTERVAL_STR = "1d"
FIXED_INTERVAL_MIN = 60 * 24  # 1440

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

# ------------------- ENV -------------------
def get_env(name: str, default: str = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return val

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

# ------------------- Načtení aktivních tokenů z CSV -------------------
def _truthy(val) -> bool:
    if val is None:
        return False
    s = str(val).strip().lower()
    return s in ("1", "true", "yes", "y")

def load_active_base_tokens(blob_service: BlobServiceClient, container_name: str, csv_path: str) -> list[str]:
    """
    Čte models-recalc/CoinDeskModels.csv a vrací seznam unikátních základních tickerů (např. 'BTC'),
    pro které je is_active == true. (Dedup zachovává pořadí výskytu.)
    """
    blob = blob_service.get_blob_client(container=container_name, blob=csv_path)
    try:
        raw = blob.download_blob().readall()
    except ResourceNotFoundError:
        raise RuntimeError(f"CSV s modely nenalezen: {csv_path}")

    text = raw.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    fieldnames = [f.lower() for f in (reader.fieldnames or [])]
    token_cols = ["token", "symbol", "ticker", "asset", "coin"]
    token_col = next((c for c in token_cols if c in fieldnames), None)
    if not token_col:
        raise RuntimeError(
            f"V CSV chybí sloupec s názvem tokenu (zkus 'token'/'symbol'/'ticker'). Nalezené sloupce: {reader.fieldnames}"
        )

    active_col_candidates = ["is_active", "active", "enabled"]
    active_col = next((c for c in active_col_candidates if c in fieldnames), None)
    if not active_col:
        raise RuntimeError(
            f"V CSV chybí sloupec 'is_active' (nebo 'active'/'enabled'). Nalezené sloupce: {reader.fieldnames}"
        )

    seen = set()
    res = []
    for row in reader:
        token = (row.get(token_col) or "").strip().upper()
        is_active_val = row.get(active_col)
        if token and _truthy(is_active_val) and token not in seen:
            seen.add(token)
            res.append(token)
    return res

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
        # Povinné ENV
        container_name = get_env("BLOB_CONTAINER", required=True)
        start_date_str = get_env("START_DATE_UTC", "2020-01-01T00:00:00Z")
        base_url = get_env("BINANCE_BASE_URL", "https://api.binance.com")
        conn_str = get_env("AzureWebJobsStorage", required=True)

        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container = blob_service.get_container_client(container_name)
        _ensure_container(container)

        # Načti aktivní tokeny (unikátně, deduplikace uvnitř)
        models_csv_path = "models-recalc/CoinDeskModels.csv"
        base_tokens = load_active_base_tokens(blob_service, container_name, models_csv_path)
        if not base_tokens:
            logger.info("V CSV nebyly nalezeny žádné aktivní tokeny.")
            return

        # Cíl pro poslední uzavřenou denní svíčku
        now_utc = datetime.now(timezone.utc)
        last_closed = floor_to_interval(now_utc, FIXED_INTERVAL_MIN) - timedelta(minutes=FIXED_INTERVAL_MIN)
        target_end_ms = to_ms(last_closed) + (FIXED_INTERVAL_MIN * 60 * 1000) - 1

        # Fixní výstup: do složky "1D" a soubor "{SYMBOL}.csv"
        blob_dir = "1D"
        blob_name_tmpl = "{SYMBOL}.csv"

        total_rows = 0
        for base_token in base_tokens:
            symbol = f"{base_token}USDC"  # doplň na USDC
            total_rows += process_symbol(
                symbol=symbol,
                interval_str=FIXED_INTERVAL_STR,
                container=container,
                blob_dir=blob_dir,
                blob_name_tmpl=blob_name_tmpl,
                start_date_str=start_date_str,
                base_url=base_url,
                INTERVAL_MIN=FIXED_INTERVAL_MIN,
                target_end_ms=target_end_ms,
            )
            time.sleep(0.1)

        logger.info(f"Uploaded {total_rows} rows total for {len(base_tokens)} symbols → folder '{blob_dir}' (interval: {FIXED_INTERVAL_STR})")
    except Exception as e:
        logger.error(f"UNHANDLED EXCEPTION: {e}", exc_info=True)
        raise

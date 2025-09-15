import os
import time
import logging
from datetime import datetime, timezone, timedelta

import azure.functions as func
import requests
from requests.exceptions import RequestException
from dateutil import parser as dateparser

from azure.storage.blob import (
    BlobServiceClient,
    ContainerClient,
    BlobClient,
    BlobBlock
)
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

# ---- Konfigurace ----
BINANCE_LIMIT = 1000
INTERVAL_MIN = 15
SLEEP_SEC = 0.2
MAX_RUNTIME_SEC = 8 * 60  # bezpečný limit pro Consumption (10 min hard)

logger = logging.getLogger("fetch_klines")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

def get_env(name: str, default: str = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)

def floor_to_interval(dt: datetime, minutes: int) -> datetime:
    m = (dt.minute // minutes) * minutes
    return dt.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=m)

# ---- Robustní HTTP volání na Binance ----
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
                # jiné HTTP chyby – bez retry
                raise RuntimeError(f"Binance API error {r.status_code}: {r.text}")
        except RequestException as e:
            # zachytí DNS/SSL/timeout/connection reset atd.
            logger.warning(f"Binance RequestException on attempt {attempt}: {e}")
            last_err = e
        # backoff, pokud budou další pokusy
        if attempt < max_attempts:
            time.sleep(backoff)
            backoff *= 2
    # po vyčerpání pokusů
    raise (last_err or RuntimeError("Unknown Binance error"))

def fetch_klines(base_url: str, symbol: str, start_ms: int, end_ms: int, limit: int = BINANCE_LIMIT):
    url = f"{base_url}/api/v3/klines"
    params = {
        "symbol": symbol, "interval": "15m", "limit": limit,
        "startTime": start_ms, "endTime": end_ms
    }
    return binance_get(url, params)

# ---- Block Blob "append" přes stage/commit ----
import base64, secrets

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

def _create_block_blob_with_header(blob: BlobClient, header_line: str):
    blob.upload_blob(header_line.encode("utf-8"), overwrite=True)

def _append_block_blob(blob: BlobClient, payload_bytes: bytes):
    bl = blob.get_block_list(block_list_type="committed")
    committed_ids = [b.id for b in (bl.committed_blocks or [])]
    new_id = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    blob.stage_block(block_id=new_id, data=payload_bytes)
    new_list = [BlobBlock(block_id=i) for i in committed_ids] + [BlobBlock(block_id=new_id)]
    blob.commit_block_list(new_list)

def kline_to_csv_line(k):
    return ",".join([str(k[i]) for i in range(12)]) + "\n"

def main(mytimer: func.TimerRequest) -> None:
    start_exec = time.time()

    # Env
    symbol = get_env("BINANCE_SYMBOL", required=True)
    interval = get_env("BINANCE_INTERVAL", "15m")
    if interval != "15m":
        raise RuntimeError(f"BINANCE_INTERVAL must be '15m', got '{interval}'")
    container_name = get_env("BLOB_CONTAINER", required=True)
    blob_name = get_env("BLOB_NAME", f"{symbol}_15m.csv")
    start_date_str = get_env("START_DATE_UTC", "2020-01-01T00:00:00Z")
    base_url = get_env("BINANCE_BASE_URL", "https://api.binance.com")
    conn_str = get_env("AzureWebJobsStorage", required=True)

    # Časové okno
    now_utc = datetime.now(timezone.utc)
    last_closed = floor_to_interval(now_utc, INTERVAL_MIN) - timedelta(minutes=INTERVAL_MIN)
    target_end_ms = to_ms(last_closed) + (INTERVAL_MIN * 60 * 1000) - 1

    # Blob klienti
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container = blob_service.get_container_client(container_name)
    _ensure_container(container)
    blob = container.get_blob_client(blob_name)

    # Zajisti existenci blobu s hlavičkou
    header = "openTime,open,high,low,close,volume,closeTime,quoteVolume,numTrades,takerBuyBase,takerBuyQuote,ignore\n"
    if not _blob_exists(blob):
        _create_block_blob_with_header(blob, header)

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
                if len(parts) >= 7 and parts[0].isdigit() and parts[6].isdigit():
                    last_close_ms = int(parts[6])
                    start_ms = last_close_ms + 1
    except Exception as e:
        logger.warning(f"Tail parse failed: {e}")

    if start_ms is None:
        start_dt = dateparser.isoparse(start_date_str).astimezone(timezone.utc)
        start_ms = to_ms(start_dt)

    if start_ms > target_end_ms:
        logger.info("Up to date – nothing to fetch.")
        return

    logger.info(f"Downloading {symbol} 15m klíny from {from_ms(start_ms)} to {from_ms(target_end_ms)}")

    # --- PRE-FLIGHT PING NA BINANCE (jasná diagnostika sítě/SSL) ---
    try:
        ping = requests.get(f"{base_url}/api/v3/ping", timeout=15)
        logger.info(f"Binance ping status={ping.status_code}, body={ping.text[:120]}")
        if ping.status_code != 200:
            raise RuntimeError(f"Ping failed {ping.status_code}: {ping.text}")
    except RequestException as e:
        logger.error(f"Binance ping RequestException: {e}")
        raise

    # Stahování s hlídáním runtime
    fetched_total = 0
    while start_ms <= target_end_ms:
        if time.time() - start_exec > MAX_RUNTIME_SEC:
            logger.info("Reached safe runtime limit, will continue next run.")
            break

        batch_window_ms = BINANCE_LIMIT * INTERVAL_MIN * 60 * 1000
        end_ms = min(start_ms + batch_window_ms - 1, target_end_ms)

        try:
            klines = fetch_klines(base_url, symbol, start_ms, end_ms, BINANCE_LIMIT)
        except Exception as e:
            logger.error(f"Binance fetch error: {e}")
            raise

        if not klines:
            start_ms += INTERVAL_MIN * 60 * 1000
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
            logger.info(f"Appended {added} rows; next start={from_ms(start_ms)}")
        else:
            start_ms += INTERVAL_MIN * 60 * 1000

        time.sleep(SLEEP_SEC)

    logger.info(f"TOTAL appended: {fetched_total} rows to {container_name}/{blob_name}")

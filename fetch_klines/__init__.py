import os
import time
import json
import traceback
from datetime import datetime, timezone, timedelta
import logging

import azure.functions as func  # ✅ správný import

import requests
from azure.storage.blob import BlobServiceClient, ContainerClient, AppendBlobClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from dateutil import parser as dateparser

BINANCE_LIMIT = 1000  # max svíček na request

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

def fetch_klines(base_url: str, symbol: str, interval: str, start_ms: int, end_ms: int, limit: int = BINANCE_LIMIT):
    url = f"{base_url}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit, "startTime": start_ms, "endTime": end_ms}
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Binance API error {r.status_code}: {r.text}")
    return r.json()

def ensure_append_blob(client: AppendBlobClient, include_header: bool) -> None:
    try:
        client.get_blob_properties()
        return
    except ResourceNotFoundError:
        pass
    client.create()
    if include_header:
        header = "openTime,open,high,low,close,volume,closeTime,quoteVolume,numTrades,takerBuyBase,takerBuyQuote,ignore\n"
        client.append_block(header.encode("utf-8"))

def dump_env_for_debug():
    keys = ["FUNCTIONS_WORKER_RUNTIME","BINANCE_SYMBOL","BINANCE_INTERVAL","BLOB_CONTAINER","BLOB_NAME","START_DATE_UTC","BINANCE_BASE_URL"]
    logger.info(f"[ENV] {json.dumps({k: os.getenv(k) for k in keys})}")

def main(mytimer: func.TimerRequest) -> None:
    start_exec = time.time()
    try:
        logger.info("=== fetch_klines start ===")
        dump_env_for_debug()

        symbol = get_env("BINANCE_SYMBOL", required=True)
        interval = get_env("BINANCE_INTERVAL", "15m")
        if interval != "15m":
            raise RuntimeError(f"BINANCE_INTERVAL must be '15m', got '{interval}'")
        container_name = get_env("BLOB_CONTAINER", required=True)
        blob_name = get_env("BLOB_NAME", f"{symbol}_15m.csv")
        start_date_str = get_env("START_DATE_UTC", "2020-01-01T00:00:00Z")
        base_url = get_env("BINANCE_BASE_URL", "https://api.binance.com")

        now_utc = datetime.now(timezone.utc)
        last_closed = floor_to_interval(now_utc, 15) - timedelta(minutes=15)
        target_end_ms = to_ms(last_closed) + (15 * 60 * 1000) - 1

        conn_str = get_env("AzureWebJobsStorage", required=True)
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container: ContainerClient = blob_service.get_container_client(container_name)
        try:
            container.create_container()
            logger.info(f"Created container '{container_name}'.")
        except ResourceExistsError:
            logger.info(f"Container '{container_name}' already exists.")
        append_blob: AppendBlobClient = container.get_append_blob_client(blob_name)
        ensure_append_blob(append_blob, include_header=True)

        start_ms = None
        try:
            props = append_blob.get_blob_properties()
            size = props.size or 0
            logger.info(f"Blob '{blob_name}' size={size} B")
            if size > 0:
                chunk = append_blob.download_blob(offset=max(0, size - 65536), length=65536)\
                                   .readall().decode("utf-8", errors="ignore")
                lines = [ln for ln in chunk.splitlines() if ln.strip()]
                if lines:
                    parts = lines[-1].split(",")
                    if len(parts) >= 7 and parts[0].isdigit() and parts[6].isdigit():
                        last_close_ms = int(parts[6])
                        start_ms = last_close_ms + 1
                        logger.info(f"Inferred start_ms from blob: {from_ms(start_ms)} ({start_ms})")
        except Exception as e:
            logger.warning(f"Could not infer last closeTime from blob tail: {e}")

        if start_ms is None:
            start_dt = dateparser.isoparse(start_date_str).astimezone(timezone.utc)
            start_ms = to_ms(start_dt)
            logger.info(f"Using START_DATE_UTC as start: {start_dt.isoformat()} ({start_ms})")

        if start_ms > target_end_ms:
            logger.info("Data jsou aktuální – není co stahovat.")
            return

        logger.info(f"Downloading {symbol} 15m klíny from {from_ms(start_ms)} to {from_ms(target_end_ms)}")

        fetched_total = 0
        while start_ms <= target_end_ms:
            batch_window_ms = BINANCE_LIMIT * 15 * 60 * 1000
            end_ms = min(start_ms + batch_window_ms - 1, target_end_ms)

            klines = fetch_klines(base_url, symbol, "15m", start_ms, end_ms, BINANCE_LIMIT)
            if not klines:
                start_ms += 15 * 60 * 1000
                continue

            rows = []
            for k in klines:
                if k[6] <= target_end_ms:
                    rows.append(",".join([str(k[i]) for i in range(12)]) + "\n")

            if rows:
                append_blob.append_block("".join(rows).encode("utf-8"))
                added = len(rows)
                fetched_total += added
                start_ms = klines[-1][6] + 1
                logger.info(f"Appended {added} rows, next start_ms={start_ms}")
            else:
                start_ms += 15 * 60 * 1000

            time.sleep(0.2)

        logger.info(f"Appended TOTAL {fetched_total} klínů do {container_name}/{blob_name}")

    except Exception as e:
        logger.error("UNHANDLED ERROR: %s\n%s", e, traceback.format_exc())
        raise
    finally:
        logger.info("=== fetch_klines end (duration %d ms) ===", int((time.time() - start_exec) * 1000))

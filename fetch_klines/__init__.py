import os
import time
from datetime import datetime, timezone, timedelta
import logging
import requests

import azure.functions as func
from dateutil import parser as dateparser
from azure.storage.blob import BlobServiceClient, ContainerClient, AppendBlobClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

# --- Konstanta Binance ---
BINANCE_LIMIT = 1000  # max 1000 svíček na dotaz
INTERVAL_MINUTES = 15
APPEND_SLEEP_SEC = 0.2  # šetrnost k limitům
MAX_RUNTIME_SEC = 8 * 60  # bezpečná mez pro Consumption (10 min hard)

logger = logging.getLogger("fetch_klines")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


# -------------------- Pomocné funkce --------------------
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

def binance_get(url: str, params: dict, max_attempts: int = 6):
    """
    GET s exponenciálním retry pro Binance 429/418/5xx.
    """
    backoff = 0.5
    for attempt in range(1, max_attempts + 1):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        # Retry na transient stavy
        if r.status_code in (418, 429) or 500 <= r.status_code < 600:
            logger.warning(f"Binance {r.status_code} on attempt {attempt}: {r.text[:200]}")
            if attempt == max_attempts:
                raise RuntimeError(f"Binance API error {r.status_code}: {r.text}")
            time.sleep(backoff)
            backoff *= 2
            continue
        # jiné chyby – neretryujeme
        raise RuntimeError(f"Binance API error {r.status_code}: {r.text}")

def fetch_klines(base_url: str, symbol: str, interval: str, start_ms: int, end_ms: int, limit: int = BINANCE_LIMIT):
    url = f"{base_url}/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
        "startTime": start_ms,
        "endTime": end_ms
    }
    return binance_get(url, params)

def ensure_container(container: ContainerClient):
    try:
        container.create_container()
        logger.info(f"Created container '{container.container_name}'.")
    except ResourceExistsError:
        # OK
        pass

def ensure_append_blob(client: AppendBlobClient, include_header: bool = True) -> None:
    """
    Vytvoří append blob, pokud neexistuje. Pokud existuje, nic nedělá.
    """
    try:
        client.get_blob_properties()
        return
    except ResourceNotFoundError:
        pass
    client.create()
    if include_header:
        header = "openTime,open,high,low,close,volume,closeTime,quoteVolume,numTrades,takerBuyBase,takerBuyQuote,ignore\n"
        client.append_block(header.encode("utf-8"))

def kline_to_csv_line(k):
    # Binance pořadí:
    # [0] openTime, [1] open, [2] high, [3] low, [4] close, [5] volume,
    # [6] closeTime, [7] quoteVolume, [8] numTrades, [9] takerBuyBase,
    # [10] takerBuyQuote, [11] ignore
    return ",".join([str(k[i]) for i in range(12)]) + "\n"


# -------------------- Hlavní funkce --------------------
def main(mytimer: func.TimerRequest) -> None:
    start_exec = time.time()

    # ---- Env vars ----
    symbol = get_env("BINANCE_SYMBOL", required=True)
    interval = get_env("BINANCE_INTERVAL", "15m")
    if interval != "15m":
        raise RuntimeError(f"BINANCE_INTERVAL must be '15m', got '{interval}'")
    container_name = get_env("BLOB_CONTAINER", required=True)
    blob_name = get_env("BLOB_NAME", f"{symbol}_15m.csv")
    start_date_str = get_env("START_DATE_UTC", "2020-01-01T00:00:00Z")
    base_url = get_env("BINANCE_BASE_URL", "https://api.binance.com")
    conn_str = get_env("AzureWebJobsStorage", required=True)

    # ---- Časové okno: poslední kompletní 15m svíčka ----
    now_utc = datetime.now(timezone.utc)
    last_closed = floor_to_interval(now_utc, INTERVAL_MINUTES) - timedelta(minutes=INTERVAL_MINUTES)
    target_end_ms = to_ms(last_closed) + (INTERVAL_MINUTES * 60 * 1000) - 1

    # ---- Blob klienti ----
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container: ContainerClient = blob_service.get_container_client(container_name)
    ensure_container(container)
    append_blob: AppendBlobClient = container.get_append_blob_client(blob_name)
    ensure_append_blob(append_blob, include_header=True)

    # ---- Zjisti start_ms: z tailu CSV, jinak ze START_DATE_UTC ----
    start_ms = None
    try:
        props = append_blob.get_blob_properties()
        size = props.size or 0
        if size > 0:
            chunk = append_blob.download_blob(
                offset=max(0, size - 65536),
                length=65536
            ).readall().decode("utf-8", errors="ignore")
            lines = [ln for ln in chunk.splitlines() if ln.strip()]
            if lines:
                parts = lines[-1].split(",")
                if len(parts) >= 7 and parts[0].isdigit() and parts[6].isdigit():
                    last_close_ms = int(parts[6])  # closeTime
                    start_ms = last_close_ms + 1
    except Exception as e:
        logger.warning(f"Could not infer last closeTime from blob tail: {e}")

    if start_ms is None:
        start_dt = dateparser.isoparse(start_date_str).astimezone(timezone.utc)
        start_ms = to_ms(start_dt)

    if start_ms > target_end_ms:
        logger.info("Data jsou aktuální – není co stahovat.")
        return

    logger.info(f"Downloading {symbol} 15m klíny from {from_ms(start_ms)} to {from_ms(target_end_ms)}")

    # ---- Stahování po dávkách s hlídáním času běhu ----
    fetched_total = 0
    while start_ms <= target_end_ms:
        # hlídej maximální běh
        if time.time() - start_exec > MAX_RUNTIME_SEC:
            logger.info("Reached safe runtime limit, finishing; navážeme v dalším běhu.")
            break

        # velikost jedné várky (max 1000 svíček)
        batch_window_ms = BINANCE_LIMIT * INTERVAL_MINUTES * 60 * 1000
        end_ms = min(start_ms + batch_window_ms - 1, target_end_ms)

        klines = fetch_klines(base_url, symbol, interval, start_ms, end_ms, BINANCE_LIMIT)
        if not klines:
            # žádná data – posuň se o jednu svíčku, ať nezacyklíme
            start_ms += INTERVAL_MINUTES * 60 * 1000
            time.sleep(APPEND_SLEEP_SEC)
            continue

        rows = []
        for k in klines:
            if k[6] <= target_end_ms:  # closeTime
                rows.append(kline_to_csv_line(k))

        if rows:
            append_blob.append_block("".join(rows).encode("utf-8"))
            added = len(rows)
            fetched_total += added
            start_ms = klines[-1][6] + 1
            logger.info(f"Appended {added} rows; next start = {from_ms(start_ms)}")
        else:
            start_ms += INTERVAL_MINUTES * 60 * 1000

        time.sleep(APPEND_SLEEP_SEC)  # šetrně k API

    logger.info(f"Appended TOTAL {fetched_total} klínů do {container_name}/{blob_name}")

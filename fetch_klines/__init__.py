# Plně funkční verze BEZ importů třetích stran na úrovni modulu.
# Všechny knihovny importujeme uvnitř main(), abychom zachytili a zalogovali chyby.
import os
import time
import logging
from datetime import datetime, timezone, timedelta

# základní logger
logger = logging.getLogger("fetch_klines")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# === Pomocné čistě standardní funkce ===
def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def _from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)

def _floor_to_interval(dt: datetime, minutes: int) -> datetime:
    m = (dt.minute // minutes) * minutes
    return dt.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=m)

def _get_env(name: str, default: str = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def main(mytimer):
    start_exec = time.time()
    BINANCE_LIMIT = 1000
    INTERVAL_MIN = 15
    MAX_RUNTIME_SEC = 8 * 60
    SLEEP_SEC = 0.2

    try:
        logger.info("=== fetch_klines: start ===")

        # --------- Importy třetích stran uvnitř main() ---------
        try:
            import requests
            from dateutil import parser as dateparser
            from azure.storage.blob import BlobServiceClient, ContainerClient, AppendBlobClient
            from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
            logger.info("Imports OK (requests, dateutil, azure.storage.blob, azure.core)")
        except Exception as e:
            logger.error(f"Import error: {e}")
            raise

        # --------- Env vars ---------
        symbol = _get_env("BINANCE_SYMBOL", required=True)
        interval = _get_env("BINANCE_INTERVAL", "15m")
        if interval != "15m":
            raise RuntimeError(f"BINANCE_INTERVAL must be '15m', got '{interval}'")
        container_name = _get_env("BLOB_CONTAINER", required=True)
        blob_name = _get_env("BLOB_NAME", f"{symbol}_15m.csv")
        start_date_str = _get_env("START_DATE_UTC", "2020-01-01T00:00:00Z")
        base_url = _get_env("BINANCE_BASE_URL", "https://api.binance.com")
        conn_str = _get_env("AzureWebJobsStorage", required=True)

        # --------- Časové okno: poslední uzavřená 15m svíčka ---------
        now_utc = datetime.now(timezone.utc)
        last_closed = _floor_to_interval(now_utc, INTERVAL_MIN) - timedelta(minutes=INTERVAL_MIN)
        target_end_ms = _to_ms(last_closed) + (INTERVAL_MIN * 60 * 1000) - 1

        # --------- Blob klienti ---------
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container: ContainerClient = blob_service.get_container_client(container_name)
        try:
            container.create_container()
            logger.info(f"Container '{container_name}' created.")
        except ResourceExistsError:
            pass

        append_blob: AppendBlobClient = container.get_append_blob_client(blob_name)

        # Vytvoř append blob pokud neexistuje a napiš hlavičku
        try:
            append_blob.get_blob_properties()
        except ResourceNotFoundError:
            append_blob.create()
            header = "openTime,open,high,low,close,volume,closeTime,quoteVolume,numTrades,takerBuyBase,takerBuyQuote,ignore\n"
            append_blob.append_block(header.encode("utf-8"))

        # --------- Zjisti start_ms z tailu CSV, jinak ze START_DATE_UTC ---------
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
            logger.warning(f"Tail parse failed: {e}")

        if start_ms is None:
            start_dt = dateparser.isoparse(start_date_str).astimezone(timezone.utc)
            start_ms = _to_ms(start_dt)

        if start_ms > target_end_ms:
            logger.info("Up to date – nothing to fetch.")
            return

        logger.info(f"Downloading {symbol} 15m klíny from {_from_ms(start_ms)} to {_from_ms(target_end_ms)}")

        # --------- Helper: Binance GET s retry ---------
        def binance_get(path: str, params: dict, max_attempts: int = 6):
            url = f"{base_url}{path}"
            backoff = 0.5
            for attempt in range(1, max_attempts + 1):
                r = requests.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (418, 429) or 500 <= r.status_code < 600:
                    logger.warning(f"Binance {r.status_code} attempt {attempt}: {r.text[:200]}")
                    if attempt == max_attempts:
                        raise RuntimeError(f"Binance API error {r.status_code}: {r.text}")
                    time.sleep(backoff); backoff *= 2
                    continue
                raise RuntimeError(f"Binance API error {r.status_code}: {r.text}")

        # --------- Stahování po dávkách s hlídáním doby běhu ---------
        fetched_total = 0
        while start_ms <= target_end_ms:
            if time.time() - start_exec > MAX_RUNTIME_SEC:
                logger.info("Reached safe runtime limit, will continue next run.")
                break

            batch_window_ms = BINANCE_LIMIT * INTERVAL_MIN * 60 * 1000
            end_ms = min(start_ms + batch_window_ms - 1, target_end_ms)

            params = {
                "symbol": symbol,
                "interval": "15m",
                "limit": BINANCE_LIMIT,
                "startTime": start_ms,
                "endTime": end_ms
            }
            klines = binance_get("/api/v3/klines", params)

            if not klines:
                start_ms += INTERVAL_MIN * 60 * 1000
                time.sleep(SLEEP_SEC)
                continue

            # Zápis do append blobu
            rows = []
            for k in klines:
                if k[6] <= target_end_ms:
                    rows.append(",".join([str(k[i]) for i in range(12)]) + "\n")

            if rows:
                append_blob.append_block("".join(rows).encode("utf-8"))
                added = len(rows)
                fetched_total += added
                start_ms = klines[-1][6] + 1
                logger.info(f"Appended {added} rows; next start={_from_ms(start_ms)}")
            else:
                start_ms += INTERVAL_MIN * 60 * 1000

            time.sleep(SLEEP_SEC)

        logger.info(f"TOTAL appended: {fetched_total} rows to {container_name}/{blob_name}")

    except Exception as e:
        # zaloguj detail výjimky
        import traceback
        logger.error("UNHANDLED ERROR: %s\n%s", e, traceback.format_exc())
        raise
    finally:
        dur_ms = int((time.time() - start_exec) * 1000)
        logger.info(f"=== fetch_klines: end ({dur_ms} ms) ===")

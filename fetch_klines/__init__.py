import os
import time
import logging
from datetime import datetime, timezone, timedelta

import azure.functions as func
import requests
from requests.exceptions import RequestException
from dateutil import parser as dateparser

from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

# ---- Konfigurace ----
BINANCE_LIMIT = 1000
INTERVAL_MIN = 15
SLEEP_SEC = 0.2
MAX_RUNTIME_SEC = 8 * 60  # bezpečná mez (10 min je hard limit na Consumption)

# Hlavičky (stará vs. nová s datetime)
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

# ---------- Env ----------
def get_env(name: str, default: str = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

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

def fetch_klines(base_url: str, symbol: str, start_ms: int, end_ms: int, limit: int = BINANCE_LIMIT):
    url = f"{base_url}/api/v3/klines"
    params = {"symbol": symbol, "interval": "15m", "limit": limit, "startTime": start_ms, "endTime": end_ms}
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
    """Nastaví obsah blobu přes block-list (1 blok). Bezpečné pro další append."""
    from azure.storage.blob import BlobBlock
    import base64, secrets
    block_id = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    blob.stage_block(block_id=block_id, data=data_bytes)
    blob.commit_block_list([BlobBlock(block_id)])

def _create_block_blob_with_header(blob: BlobClient, header_line: str):
    # založ blob jako block-list (hlavička je 1. blok)
    line = header_line if header_line.endswith("\n") else header_line + "\n"
    _put_full_body_as_single_block(blob, line.encode("utf-8"))

def _ensure_block_blob_type(blob: BlobClient):
    """Pokud blob není BlockBlob (Append/Page), přenahrát ho jako BlockBlob (stage+commit)."""
    try:
        props = blob.get_blob_properties()
        blob_type = getattr(props, "blob_type", None)
        if str(blob_type).lower() != "blockblob":
            data = blob.download_blob().readall()
            _put_full_body_as_single_block(blob, data)
            logger.info(f"Blob type converted to BlockBlob via block-list (size={len(data)}).")
    except ResourceNotFoundError:
        pass

def _ensure_header_present_and_migrate(blob: BlobClient):
    """
    Zajistí, že první řádek je NEW_HEADER.
    - prázdný → NEW_HEADER (stage+commit)
    - BASE_HEADER → MIGRACE: dopočítá closeTimeISO, upload přes block-list
    - chybí hlavička → prepended NEW_HEADER, upload přes block-list
    """
    try:
        props = blob.get_blob_properties()
        size = props.size or 0
        if size == 0:
            _create_block_blob_with_header(blob, NEW_HEADER)
            logger.info("Blob empty -> wrote NEW_HEADER (block-list).")
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
                logger.info("Blob had old header but no data -> wrote NEW_HEADER (block-list).")
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
            logger.info(f"Migrated CSV BASE_HEADER -> NEW_HEADER (rows={len(out_lines)-1}).")
            return

        # Neznámý první řádek → vlož NEW_HEADER na začátek
        full = blob.download_blob().readall()
        prefix = (NEW_HEADER + "\n").encode("utf-8") if not NEW_HEADER.endswith("\n") else NEW_HEADER.encode("utf-8")
        _put_full_body_as_single_block(blob, prefix + full)
        logger.info(f"Header missing/unknown -> prepended NEW_HEADER (block-list).")
    except ResourceNotFoundError:
        pass

def _extract_committed_ids(block_list_obj):
    """Vrátí list ID (base64) committed bloků z různých tvarů návratu get_block_list."""
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
    """Append přes stage/commit – zachová stávající block-list."""
    from azure.storage.blob import BlobBlock
    import base64, secrets

    bl = blob.get_block_list(block_list_type="committed")
    committed_ids = _extract_committed_ids(bl)

    new_id = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    logger.info(f"Staging new block id={new_id} size={len(payload_bytes)}")
    blob.stage_block(block_id=new_id, data=payload_bytes)

    new_list = [BlobBlock(i) for i in committed_ids] + [BlobBlock(new_id)]
    logger.info(f"Committing block list: prev={len(committed_ids)} +1")
    blob.commit_block_list(new_list)

def kline_to_csv_line(k):
    # [openTime, open, high, low, close, volume, closeTime, quoteVolume, numTrades, takerBuyBase, takerBuyQuote, ignore] + closeTimeISO
    return ",".join([str(k[i]) for i in range(12)] + [iso_utc(int(k[6]))]) + "\n"

# ---------- Hlavní ----------
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

    # Časové okno: poslední kompletně uzavřená 15m svíčka
    now_utc = datetime.now(timezone.utc)
    last_closed = floor_to_interval(now_utc, INTERVAL_MIN) - timedelta(minutes=INTERVAL_MIN)
    target_end_ms = to_ms(last_closed) + (INTERVAL_MIN * 60 * 1000) - 1

    # Blob klienti
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container = blob_service.get_container_client(container_name)
    _ensure_container(container)
    blob = container.get_blob_client(blob_name)

    # Zajisti existenci CSV s NOVOU hlavičkou (přes block-list)
    if not _blob_exists(blob):
        _create_block_blob_with_header(blob, NEW_HEADER)

    # Ujisti se, že typ je BlockBlob a že první obsah je přes block-list + migruj hlavičku
    _ensure_block_blob_type(blob)
    _ensure_header_present_and_migrate(blob)

    # Najdi start_ms z tailu CSV, jinak START_DATE_UTC
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
                # closeTime je pořád index 6
                if len(parts) >= 7 and parts[6].isdigit():
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

    # Pre-flight ping (diagnostika sítě/SSL)
    try:
        ping = requests.get(f"{base_url}/api/v3/ping", timeout=15)
        logger.info(f"Binance ping status={ping.status_code}, body={ping.text[:120]}")
        if ping.status_code != 200:
            raise RuntimeError(f"Ping failed {ping.status_code}: {ping.text}")
    except RequestException as e:
        logger.error(f"Binance ping RequestException: {e}")
        raise

    # Stahování a append s hlídáním délky běhu
    fetched_total = 0
    while start_ms <= target_end_ms:
        if time.time() - start_exec > MAX_RUNTIME_SEC:
            logger.info("Reached safe runtime limit, will continue next run.")
            break

        batch_window_ms = BINANCE_LIMIT * INTERVAL_MIN * 60 * 1000
        end_ms = min(start_ms + batch_window_ms - 1, target_end_ms)

        klines = fetch_klines(base_url, symbol, start_ms, end_ms, BINANCE_LIMIT)
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

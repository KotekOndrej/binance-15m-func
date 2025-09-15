import logging
import azure.functions as func

log = logging.getLogger("fetch_klines")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

def _try_import(name, importer):
    try:
        mod = importer()
        log.info(f"DIAG2: import OK -> {name}")
        return mod
    except Exception as e:
        log.error(f"DIAG2: import FAIL -> {name}: {e}")
        raise

# --- postupně otestujeme problematické balíky ---
requests = _try_import("requests", lambda: __import__("requests"))
dateutil_parser = _try_import("dateutil.parser", lambda: __import__("dateutil.parser", fromlist=["parser"]))
azure_storage = _try_import("azure.storage.blob", lambda: __import__("azure.storage.blob", fromlist=["BlobServiceClient"]))
azure_core = _try_import("azure.core.exceptions", lambda: __import__("azure.core.exceptions", fromlist=["ResourceExistsError"]))

def main(mytimer: func.TimerRequest) -> None:
    log.info("DIAG2: main() entered OK")

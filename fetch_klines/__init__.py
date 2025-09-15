def main(mytimer):
    # základní logging (funguje i bez konfigurace hostu)
    import logging, traceback, os, json
    logger = logging.getLogger("fetch_klines_diag3")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

    logger.info("DIAG3: main() vstup OK")

    def try_import(name, call):
        try:
            mod = call()
            logger.info(f"DIAG3: import OK -> {name}")
            return mod
        except Exception as e:
            logger.error(f"DIAG3: import FAIL -> {name}: {e}\n{traceback.format_exc()}")
            raise

    # krok 1: postupné importy
    # azure.functions necháme úplně na konci, ať případná chyba v něm neblokuje předchozí logy
    requests = try_import("requests", lambda: __import__("requests"))
    dateutil_parser = try_import("dateutil.parser", lambda: __import__("dateutil.parser", fromlist=["parser"]))
    azure_core_exc = try_import("azure.core.exceptions",
                                lambda: __import__("azure.core.exceptions", fromlist=["ResourceExistsError", "ResourceNotFoundError"]))
    azure_storage_blob = try_import("azure.storage.blob",
                                    lambda: __import__("azure.storage.blob", fromlist=["BlobServiceClient", "ContainerClient", "AppendBlobClient"]))
    # Nakonec zkusíme azure.functions (host ji obvykle poskytuje, ale kdyby náhodou…)
    azure_functions = try_import("azure.functions", lambda: __import__("azure.functions"))

    # krok 2: vypiš klíčové env (bez tajemství)
    keys = ["FUNCTIONS_WORKER_RUNTIME","BINANCE_SYMBOL","BINANCE_INTERVAL","BLOB_CONTAINER","BLOB_NAME","START_DATE_UTC","BINANCE_BASE_URL"]
    safe_env = {k: os.getenv(k) for k in keys}
    logger.info(f"DIAG3: ENV = {json.dumps(safe_env)}")

    # krok 3: zkusme úplný minimum „Hello world“ interakce se Storage, ať víme, že je přístup
    try:
        from azure.storage.blob import BlobServiceClient
        conn = os.getenv("AzureWebJobsStorage")
        assert conn, "AzureWebJobsStorage není v env!"
        svc = BlobServiceClient.from_connection_string(conn)
        logger.info("DIAG3: BlobServiceClient OK")
    except Exception as e:
        logger.error(f"DIAG3: BlobServiceClient FAIL: {e}\n{traceback.format_exc()}")
        raise

    # pokud jsme došli sem, imports + storage klient jsou OK
    logger.info("DIAG3: imports i připojení ke Storage vypadají OK – můžeme přejít zpět na produkční kód.")

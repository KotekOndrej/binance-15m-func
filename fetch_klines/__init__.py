import logging
import azure.functions as func

def main(mytimer: func.TimerRequest) -> None:
    logging.getLogger("fetch_klines").info("DIAG1: main() entered OK")

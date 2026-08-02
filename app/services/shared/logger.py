import logging
from pythonjsonlogger import json as jsonlogger
import os

logger = logging.getLogger(os.getenv("SERVICE_NAME"))
logger.setLevel(logging.INFO)

log_handler = logging.StreamHandler()

formatter = jsonlogger.JsonFormatter(
    '%(levelname)s %(name)s %(message)s', json_ensure_ascii=False
)

log_handler.setFormatter(formatter)
logger.addHandler(log_handler)

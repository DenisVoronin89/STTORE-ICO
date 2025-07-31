import logging
import os
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

load_dotenv()

DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() == "true"

LOG_FILE_NAME = "site_logs.log"  # как ты просишь
LOG_LEVEL = logging.DEBUG if DEBUG_MODE else logging.INFO

LOG_FORMAT = (
    "[%(asctime)s] [%(levelname)s] "
    "[%(filename)s:%(lineno)d] - %(message)s"
)
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Создаем корневой логгер
logger = logging.getLogger("site_logger")
logger.setLevel(LOG_LEVEL)
logger.propagate = False  # чтобы не дублировались логи

# Очистим обработчики, если уже были добавлены
if logger.hasHandlers():
    logger.handlers.clear()

# Обработчик в файл с ротацией
file_handler = RotatingFileHandler(
    LOG_FILE_NAME, maxBytes=1_000_000, backupCount=5, encoding='utf-8'
)
file_handler.setLevel(LOG_LEVEL)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
logger.addHandler(file_handler)

# Обработчик в консоль (только если DEBUG_MODE=True)
if DEBUG_MODE:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVEL)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
    logger.addHandler(console_handler)


def get_logger() -> logging.Logger:
    """
    Возвращает настроенный логгер
    """
    return logger

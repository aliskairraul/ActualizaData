import logging
from pathlib import Path
from datetime import datetime


def get_logger(name: str) -> logging.Logger:
    # Crear carpeta de logs si no existe
    log_folder = Path("logs")
    log_folder.mkdir(exist_ok=True)

    # Nombre del archivo de log con fecha
    log_file = log_folder / f"log_{name}_{datetime.now().strftime('%Y-%m-%d')}.log"

    # Crear logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Evitar duplicados si ya tiene handlers
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - [%(name)s] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Handler para archivo
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Handler para consola
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

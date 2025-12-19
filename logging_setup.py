import logging
import sys
from typing import Optional

def configure_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    handlers = []

    stream_handler = logging.StreamHandler(sys.stdout)
    handlers.append(stream_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        handlers.append(file_handler)

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )

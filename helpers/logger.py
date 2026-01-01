"""Trading logger utility."""
import logging
import os
from datetime import datetime


class TradingLogger:
    """Simple trading logger."""

    def __init__(self, exchange: str, ticker: str, log_to_console: bool = True):
        """Initialize logger."""
        self.exchange = exchange
        self.ticker = ticker
        self.log_to_console = log_to_console

        # Create logger
        self.logger = logging.getLogger(f"{exchange}_{ticker}")
        self.logger.setLevel(logging.INFO)

        # Remove existing handlers
        self.logger.handlers.clear()

        # Create formatters
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Add console handler if requested
        if log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

        # Add file handler
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = f"{log_dir}/{exchange}_{ticker}_{datetime.now().strftime('%Y%m%d')}.log"

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        self.logger.propagate = False

    def log(self, message: str, level: str = "INFO"):
        """Log a message."""
        if level == "INFO":
            self.logger.info(message)
        elif level == "WARNING":
            self.logger.warning(message)
        elif level == "ERROR":
            self.logger.error(message)
        elif level == "DEBUG":
            self.logger.debug(message)

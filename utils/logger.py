"""
Utility functions for logging.
"""

import logging
import sys
from pathlib import Path

# Global set to track loggers we've already configured
_configured_loggers = set()


def setup_logger(name, level=logging.INFO):
    """
    Set up a logger with console handler (singleton pattern).
    
    Args:
        name: Logger name
        level: Logging level
    
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    
    # If already configured, just return it
    if name in _configured_loggers:
        return logger
    
    # Mark as configured
    _configured_loggers.add(name)
    
    # Set level
    logger.setLevel(level)
    
    # Prevent propagation to root logger (prevents duplicates)
    logger.propagate = False
    
    # Remove any existing handlers (just in case)
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Add single console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

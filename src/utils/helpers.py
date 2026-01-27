"""
CUB Data Pipeline - Utility Helpers

This module contains shared utility functions for the pipeline:
- Logging configuration
- Date/month calculations
- File path helpers
- PDF parsing utilities (to be implemented)
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name, typically __name__ of the calling module
    
    Returns:
        Configured Logger instance
    """
    return logging.getLogger(name)


def get_reference_month(today: datetime | None = None) -> Tuple[int, int]:
    """
    Calculate the expected reference month for CUB data.
    
    CUB data is typically published in the first week of each month
    for the PREVIOUS month. For example:
    - If today is Feb 5th 2026, we expect January 2026 data
    - If today is Jan 10th 2026, we expect December 2025 data
    
    Args:
        today: Optional datetime for testing, defaults to now
    
    Returns:
        Tuple of (month, year) for the expected reference period
    """
    if today is None:
        today = datetime.now()
    
    # Previous month calculation
    if today.month == 1:
        return (12, today.year - 1)
    else:
        return (today.month - 1, today.year)


def get_project_root() -> Path:
    """
    Get the project root directory path.
    
    Returns:
        Path object pointing to the project root
    """
    return Path(__file__).parent.parent.parent


def get_data_path(subdir: str = "output") -> Path:
    """
    Get path to a data subdirectory.
    
    Args:
        subdir: Subdirectory name ("raw" or "output")
    
    Returns:
        Path object to the data subdirectory
    """
    path = get_project_root() / "data" / subdir
    path.mkdir(parents=True, exist_ok=True)
    return path


def month_name_pt(month: int) -> str:
    """
    Get Portuguese month name for display/matching.
    
    Args:
        month: Month number (1-12)
    
    Returns:
        Portuguese month name (e.g., "Janeiro", "Fevereiro")
    """
    months = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março",
        4: "Abril", 5: "Maio", 6: "Junho",
        7: "Julho", 8: "Agosto", 9: "Setembro",
        10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    return months.get(month, "")

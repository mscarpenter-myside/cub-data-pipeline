"""
CUB Data Pipeline - Base Scraper Interface

This module defines the Abstract Base Class (BaseScraper) that serves as the
contract for all state-specific scraper implementations.

Design Pattern: Strategy Pattern
"""

import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Tuple

from src.core.models import CUBData
from src.utils.helpers import get_logger, month_name_pt

logger = get_logger(__name__)

# Mapeamento de nomes de meses em português para números (1-12)
MONTHS_PT = {
    'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4,
    'maio': 5, 'junho': 6, 'julho': 7, 'agosto': 8,
    'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12
}


class BaseScraper(ABC):
    """
    Abstract Base Class defining the scraper contract.
    
    All state-specific scrapers MUST inherit from this class and implement
    both abstract methods. This ensures a uniform interface for the orchestrator.
    
    New methods:
    - get_latest_available(): Discovers the most recent available data
    - extract_latest(): Convenience method to discover and extract in one call
    """
    
    def __init__(self, estado: str, base_url: str):
        """Initialize the base scraper."""
        self.estado = estado.upper()
        self.base_url = base_url
        self.headless = True  # Default, can be overridden by subclasses
    
    @abstractmethod
    def check_availability(self, month: int, year: int) -> bool:
        """
        Check if CUB data is available for the specified reference period.
        
        Args:
            month: Reference month (1-12)
            year: Reference year (e.g., 2026)
        
        Returns:
            bool: True if data is available, False if not yet published
        """
        pass
    
    @abstractmethod
    def extract(self, month: int, year: int) -> CUBData:
        """
        Extract CUB data for the specified reference period.
        
        Args:
            month: Reference month (1-12)
            year: Reference year (e.g., 2026)
        
        Returns:
            CUBData: Validated Pydantic model with extracted data
        """
        pass
    
    def get_latest_available(self) -> Optional[Tuple[int, int]]:
        """
        Discover the most recent CUB data available.
        
        Default implementation: Try current month backwards until data is found.
        Subclasses can override this with page-specific logic for better efficiency.
        
        Returns:
            Tuple (month, year) of the most recent data available, or None if 
            no valid data was found after checking several months.
        
        Example:
            >>> scraper = ScraperSC()
            >>> latest = scraper.get_latest_available()
            >>> print(latest)  # (12, 2025) for December 2025
        """
        logger.info(f"🔍 [{self.estado}] Discovering latest available CUB data...")
        
        # Start from current month and go backwards
        now = datetime.now()
        year = now.year
        month = now.month
        
        # Try up to 6 months back
        for _ in range(6):
            month_name = month_name_pt(month)
            logger.debug(f"   Checking {month_name}/{year}...")
            
            if self.check_availability(month, year):
                logger.info(f"✅ [{self.estado}] Most recent data: {month_name}/{year}")
                return (month, year)
            
            # Go back one month
            month -= 1
            if month < 1:
                month = 12
                year -= 1
        
        logger.warning(f"❌ [{self.estado}] No data found in the last 6 months")
        return None
    
    def extract_latest(self) -> Optional[CUBData]:
        """
        Convenience method: Discover the latest available date and extract its data.
        
        This combines get_latest_available() + extract() in a single call.
        Extracts CUB SEM DESONERAÇÃO (typically R8-N or equivalent).
        
        Returns:
            CUBData object with the most recent data, or None if extraction fails.
        
        Example:
            >>> scraper = ScraperSC()
            >>> data = scraper.extract_latest()
            >>> print(data.mes_referencia, data.ano_referencia, data.valores[0].valor)
        """
        latest = self.get_latest_available()
        
        if not latest:
            logger.error(f"❌ [{self.estado}] Could not determine the latest available data")
            return None
        
        month, year = latest
        month_name = month_name_pt(month)
        
        logger.info(f"📊 [{self.estado}] Extracting CUB data for {month_name}/{year}...")
        
        try:
            return self.extract(month, year)
        except Exception as e:
            logger.error(f"💥 [{self.estado}] Extraction failed: {e}")
            return None
    
    @staticmethod
    def _month_name_to_number(month_name: str) -> Optional[int]:
        """
        Convert Portuguese month name to number (1-12).
        
        Args:
            month_name: Month name in Portuguese (e.g., "Dezembro", "Janeiro")
        
        Returns:
            Month number (1-12) or None if not recognized.
        """
        normalized = month_name.lower().strip()
        return MONTHS_PT.get(normalized)
    
    @staticmethod
    def _parse_brl_currency(value_str: str) -> float:
        """
        Parse Brazilian currency string to float.
        
        Examples:
            "R$ 3.012,64" -> 3012.64
            "3.012,64" -> 3012.64
            "1.234.567,89" -> 1234567.89
        
        Args:
            value_str: Currency string in BRL format
        
        Returns:
            Float value
        """
        if not value_str:
            raise ValueError("Empty currency string")
        
        # Remove "R$" and whitespace
        cleaned = value_str.replace("R$", "").strip()
        
        # Remove thousand separators (dots) and replace decimal comma with dot
        cleaned = cleaned.replace(".", "").replace(",", ".")
        
        try:
            return float(cleaned)
        except ValueError:
            raise ValueError(f"Could not parse currency: '{value_str}'")
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(estado='{self.estado}')"

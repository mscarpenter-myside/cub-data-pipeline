"""
CUB Data Pipeline - Base Scraper Interface

This module defines the Abstract Base Class (BaseScraper) that serves as the
contract for all state-specific scraper implementations.

Design Pattern: Strategy Pattern
"""

from abc import ABC, abstractmethod

from src.core.models import CUBData


class BaseScraper(ABC):
    """
    Abstract Base Class defining the scraper contract.
    
    All state-specific scrapers MUST inherit from this class and implement
    both abstract methods. This ensures a uniform interface for the orchestrator.
    """
    
    def __init__(self, estado: str, base_url: str):
        """Initialize the base scraper."""
        self.estado = estado.upper()
        self.base_url = base_url
    
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
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(estado=\'{self.estado}\')"

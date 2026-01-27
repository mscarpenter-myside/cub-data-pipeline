"""
CUB Data Pipeline - Santa Catarina (SC) Scraper

Concrete implementation of BaseScraper for extracting CUB data
from Sinduscon-SC website.

Target: https://sinduscon-fpolis.org.br/servico/cub-mensal/
Method: Direct HTML Card scraping (no PDF download required)

The website displays CUB values on HTML cards that we scrape directly.
"""

import re
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from src.scrapers.base import BaseScraper
from src.core.models import CUBData, CUBValor
from src.utils.helpers import get_logger, get_data_path, month_name_pt

logger = get_logger(__name__)


class ScraperSC(BaseScraper):
    """
    Scraper for Santa Catarina (SC) CUB data.
    
    Extracts CUB values directly from HTML cards on the page.
    """
    
    def __init__(self, headless: bool = True):
        """Initialize the SC scraper with parent attributes."""
        # Initialize the Parent Class (Critical)
        super().__init__(
            estado="SC",
            base_url="https://sinduscon-fpolis.org.br/servico/cub-mensal/"
        )
        self.headless = headless
    
    def check_availability(self, month: int, year: int) -> bool:
        """
        Check if CUB data is available for the specified month.
        
        Looks for the reference month text on the page cards.
        
        Args:
            month: Reference month (1-12)
            year: Reference year
        
        Returns:
            True if data is available, False otherwise
        """
        month_name = month_name_pt(month)
        logger.info(f"Checking availability for {month_name}/{year}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("networkidle")
                
                # Dismiss cookie banner if present
                self._dismiss_cookies(page)
                
                # Look for the reference month pattern
                # Pattern: "Mês de Referência: Dezembro/2025" (with flexible whitespace)
                ref_pattern = re.compile(
                    rf"Mês\s+de\s+Referência[:\s]*{month_name}/{year}",
                    re.IGNORECASE
                )
                
                ref_element = page.get_by_text(ref_pattern).first
                available = ref_element.count() > 0 and ref_element.is_visible()
                
                if available:
                    logger.info(f"Data available for {month_name}/{year}")
                else:
                    logger.warning(f"Data NOT available for {month_name}/{year}")
                
                return available
                
            except PlaywrightTimeout:
                logger.error("Timeout while checking availability")
                return False
            except Exception as e:
                logger.error(f"Error checking availability: {e}")
                return False
            finally:
                browser.close()
    
    def extract(self, month: int, year: int) -> CUBData:
        """
        Extract CUB data from HTML cards on the page.
        
        Locates the "Residencial Médio" card and extracts the value.
        
        Args:
            month: Reference month (1-12)
            year: Reference year
        
        Returns:
            CUBData object with extracted value
        """
        month_name = month_name_pt(month)
        logger.info(f"Extracting CUB data for {month_name}/{year}")
        
        error_screenshot_path = get_data_path("raw").parent / "error_screenshot.png"
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("networkidle")
                
                # Dismiss cookie banner
                self._dismiss_cookies(page)
                
                # Wait for content to load
                page.wait_for_timeout(2000)
                
                # Strategy: Find the card with "Residencial Médio" and the correct reference month
                logger.info("Locating Residencial Médio card...")
                
                # Find "Residencial Médio" text
                residencial_header = page.get_by_text("Residencial Médio", exact=False).first
                
                if not residencial_header.is_visible():
                    page.screenshot(path=str(error_screenshot_path))
                    raise ValueError("Could not find 'Residencial Médio' card")
                
                # Navigate to the card container (parent elements)
                # Cards usually have a structure: Card > Header > Value
                card = residencial_header.locator("xpath=ancestor::*[contains(@class,'card') or contains(@class,'box') or contains(@class,'item')][1]")
                
                if card.count() == 0:
                    # Fallback: just use parent traversal
                    card = residencial_header.locator("xpath=../..")
                
                # Look for reference month to verify we have the right data
                ref_pattern = re.compile(
                    rf"Mês\s+de\s+Referência[:\s]*{month_name}/{year}",
                    re.IGNORECASE
                )
                
                # Check if reference month is on the page
                ref_check = page.get_by_text(ref_pattern).first
                if not ref_check.is_visible():
                    logger.warning(f"Reference month {month_name}/{year} not found, data may be outdated")
                
                # Find the currency value (R$ X.XXX,XX pattern)
                logger.info("Extracting value...")
                
                # Try multiple strategies to find the value
                value_text = None
                
                # Strategy 1: Look for R$ pattern in the card
                price_elements = card.locator("text=/R\\$\\s*[\\d\\.]+,[\\d]+/").all()
                if price_elements:
                    value_text = price_elements[0].text_content()
                
                # Strategy 2: Look globally for R$ near "Residencial"
                if not value_text:
                    all_prices = page.locator("text=/R\\$\\s*[\\d\\.]+,[\\d]+/").all()
                    for price in all_prices:
                        try:
                            value_text = price.text_content()
                            if value_text and "R$" in value_text:
                                break
                        except:
                            continue
                
                # Strategy 3: Get text content and parse
                if not value_text:
                    card_text = card.inner_text()
                    match = re.search(r'R\$\s*([\d\.]+,\d{2})', card_text)
                    if match:
                        value_text = f"R$ {match.group(1)}"
                
                if not value_text:
                    # Debug: dump what we found
                    logger.error("Could not find value. Card content:")
                    try:
                        print(card.inner_text()[:500])
                    except:
                        pass
                    page.screenshot(path=str(error_screenshot_path))
                    raise ValueError("Could not extract currency value from card")
                
                logger.info(f"Found value: {value_text}")
                
                # Parse the value
                valor = self._parse_brl_currency(value_text)
                logger.info(f"Parsed value: {valor}")
                
                # Create CUBValor object
                # R8-N is the standard code for "Residencial Médio" (8 pavimentos)
                valor_obj = CUBValor(
                    projeto="R8-N",
                    valor=valor,
                    unidade="R$/m²"
                )
                
                return CUBData(
                    estado="SC",
                    mes_referencia=month,
                    ano_referencia=year,
                    data_extracao=datetime.now(),
                    valores=[valor_obj]
                )
                
            except Exception as e:
                logger.error(f"Extraction error: {e}")
                try:
                    page.screenshot(path=str(error_screenshot_path))
                except:
                    pass
                raise
            finally:
                browser.close()
    
    def _dismiss_cookies(self, page) -> None:
        """Dismiss cookie consent banner if present."""
        try:
            cookie_btn = page.get_by_text("Ok", exact=True)
            if cookie_btn.count() > 0:
                cookie_btn.click(timeout=2000)
                logger.debug("Cookie banner dismissed")
        except:
            pass
        
        try:
            cookie_btn = page.get_by_text("Aceitar", exact=False)
            if cookie_btn.count() > 0:
                cookie_btn.first.click(timeout=2000)
        except:
            pass
    
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


# Test block
if __name__ == "__main__":
    scraper = ScraperSC(headless=False)
    
    # Test availability
    available = scraper.check_availability(12, 2025)
    print(f"Available: {available}")
    
    if available:
        # Test extraction
        data = scraper.extract(12, 2025)
        print(f"Extracted: {data}")

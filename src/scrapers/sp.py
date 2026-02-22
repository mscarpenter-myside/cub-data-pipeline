"""
CUB Data Pipeline - São Paulo (SP) Scraper

Concrete implementation of BaseScraper for extracting CUB data
from Sinduscon-SP website.

Target: https://sindusconsp.com.br/servicos/cub/
Method: Direct HTML Card scraping (no PDF download required)

The website displays CUB values on HTML cards that we scrape directly.
"""

import re
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from src.scrapers.base import BaseScraper
from src.core.models import CUBData, CUBValor
from src.utils.helpers import get_logger, month_name_pt

logger = get_logger(__name__)


class ScraperSP(BaseScraper):
    """
    Scraper for São Paulo (SP) CUB data.
    
    Extracts CUB values directly from HTML cards on the page.
    Target card: "Sem desoneração . R8-N"
    """
    
    def __init__(self, headless: bool = True):
        """Initialize the SP scraper with parent attributes."""
        super().__init__(
            estado="SP",
            base_url="https://sindusconsp.com.br/servicos/cub/"
        )
        self.headless = headless
    
    def check_availability(self, month: int, year: int) -> bool:
        """
        Check if CUB data is available for the specified month.
        
        Looks for the reference month text in the page header.
        Pattern: "Dezembro 2025 - R$/m²" or similar.
        
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
                # Pattern: "Dezembro 2025" (flexible whitespace)
                ref_pattern = re.compile(
                    rf"{month_name}\s+{year}",
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
        
        Locates the "Sem desoneração . R8-N" card and extracts the value.
        
        Args:
            month: Reference month (1-12)
            year: Reference year
        
        Returns:
            CUBData object with extracted value
        """
        month_name = month_name_pt(month)
        logger.info(f"Extracting CUB data for {month_name}/{year}")
        
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
                
                # Verify reference month is present
                ref_pattern = re.compile(rf"{month_name}\s+{year}", re.IGNORECASE)
                ref_check = page.get_by_text(ref_pattern).first
                if not ref_check.is_visible():
                    logger.warning(f"Reference month {month_name}/{year} not found, data may be outdated")
                
                # Strategy: Find the card with "Sem desoneração . R8-N"
                logger.info("Locating 'Sem desoneração . R8-N' card...")
                
                # Find the R8-N label (various possible formats)
                r8n_patterns = [
                    "Sem desoneração . R8-N",
                    "Sem desoneração R8-N",
                    "R8-N",
                ]
                
                card_label = None
                for pattern in r8n_patterns:
                    label = page.get_by_text(pattern, exact=False).first
                    if label.count() > 0 and label.is_visible():
                        card_label = label
                        logger.info(f"Found card with pattern: '{pattern}'")
                        break
                
                if not card_label:
                    page.screenshot(path="data/error_screenshot.png")
                    raise ValueError("Could not find 'R8-N' card")
                
                # Navigate to the card container
                # Cards usually have structure: Card > Label + Value
                card = card_label.locator("xpath=ancestor::*[contains(@class,'card') or contains(@class,'box') or contains(@class,'item') or contains(@class,'cub')][1]")
                
                if card.count() == 0:
                    # Fallback: use parent traversal
                    card = card_label.locator("xpath=../..")
                
                # Find the currency value
                logger.info("Extracting value...")
                value_text = None
                
                # Strategy 1: Look for currency pattern in the card
                # Format: "2.123,87" or "R$ 2.123,87"
                price_pattern = re.compile(r'R?\$?\s*([\d\.]+,\d{2})')
                
                card_text = card.inner_text()
                match = price_pattern.search(card_text)
                if match:
                    value_text = match.group(0)
                
                # Strategy 2: Look for sibling or child with number
                if not value_text:
                    # Find elements that look like prices near the label
                    price_elements = card.locator("text=/[\\d\\.]+,\\d{2}/").all()
                    for elem in price_elements:
                        try:
                            text = elem.text_content()
                            if text and re.search(r'[\d\.]+,\d{2}', text):
                                value_text = text.strip()
                                break
                        except:
                            continue
                
                # Strategy 3: Look globally near R8-N text
                if not value_text:
                    all_prices = page.locator("text=/[\\d\\.]+,\\d{2}/").all()
                    for price in all_prices:
                        try:
                            text = price.text_content()
                            if text and re.search(r'[\d\.]+,\d{2}', text):
                                value_text = text.strip()
                                break
                        except:
                            continue
                
                if not value_text:
                    logger.error("Could not find value. Card content:")
                    try:
                        print(card_text[:500])
                    except:
                        pass
                    page.screenshot(path="data/error_screenshot.png")
                    raise ValueError("Could not extract currency value from card")
                
                logger.info(f"Found value: {value_text}")
                
                # Parse the value
                valor = self._parse_brl_currency(value_text)
                logger.info(f"Parsed value: {valor}")
                
                # Create CUBValor object
                valor_obj = CUBValor(
                    projeto="R8-N",
                    valor=valor,
                    unidade="R$/m²"
                )
                
                return CUBData(
                    estado="SP",
                    mes_referencia=month,
                    ano_referencia=year,
                    data_extracao=datetime.now(),
                    valores=[valor_obj]
                )
                
            except Exception as e:
                logger.error(f"Extraction error: {e}")
                try:
                    page.screenshot(path="data/error_screenshot.png")
                except:
                    pass
                raise
            finally:
                browser.close()
    
    def _dismiss_cookies(self, page) -> None:
        """Dismiss cookie consent banner if present."""
        try:
            cookie_btn = page.get_by_text("Aceitar", exact=False)
            if cookie_btn.count() > 0:
                cookie_btn.first.click(timeout=2000)
                logger.debug("Cookie banner dismissed")
        except:
            pass
        
        try:
            cookie_btn = page.get_by_text("Ok", exact=True)
            if cookie_btn.count() > 0:
                cookie_btn.click(timeout=2000)
        except:
            pass
        
        try:
            cookie_btn = page.get_by_text("Entendi", exact=False)
            if cookie_btn.count() > 0:
                cookie_btn.first.click(timeout=2000)
        except:
            pass
    
    @staticmethod
    def _parse_brl_currency(value_str: str) -> float:
        """
        Parse Brazilian currency string to float.
        
        Examples:
            "R$ 2.123,87" -> 2123.87
            "2.123,87" -> 2123.87
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
    scraper = ScraperSP(headless=False)
    
    print("=" * 60)
    print("🔍 Testando extração automática do CUB mais recente (SP)...")
    print("=" * 60)
    
    # Use extract_latest() - discovers and extracts automatically
    data = scraper.extract_latest()
    
    if data:
        print(f"\n✅ SUCESSO!")
        print(f"   Estado: {data.estado}")
        print(f"   Mês/Ano: {data.mes_referencia}/{data.ano_referencia}")
        print(f"   Projeto: {data.valores[0].projeto}")
        print(f"   Valor: R$ {data.valores[0].valor:,.2f}")
        print(f"   Extraído em: {data.data_extracao}")
    else:
        print("\n❌ Falha na extração")


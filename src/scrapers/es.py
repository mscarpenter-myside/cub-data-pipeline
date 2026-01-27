"""
CUB Data Pipeline - Espírito Santo (ES) Scraper

Concrete implementation of BaseScraper for extracting CUB data
from Sinduscon-ES website.

Target: http://www.sinduscon-es.com.br/v2/cgi-bin/cub_valor.asp?menu2=25
Method: Dynamic Web Scraping (form interaction, no PDF)
Target Value: "Valor" column which maps to R8-N
"""

import re
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from src.scrapers.base import BaseScraper
from src.core.models import CUBData, CUBValor
from src.utils.helpers import get_logger, month_name_pt

logger = get_logger(__name__)


class ScraperES(BaseScraper):
    """
    Scraper for Espírito Santo (ES) CUB data.
    
    Target: R8-N value extracted from "Valor" column in dynamic table.
    
    Navigation:
    - Page has dropdowns for Month and Year
    - Click "buscar" to search
    - Extract value from the result table row matching the requested period
    """
    
    def __init__(self, headless: bool = True):
        """Initialize the ES scraper with parent attributes."""
        super().__init__(
            estado="ES",
            base_url="http://www.sinduscon-es.com.br/v2/cgi-bin/cub_valor.asp?menu2=25"
        )
        self.headless = headless
    
    def check_availability(self, month: int, year: int) -> bool:
        """
        Check if CUB data is available for the specified month/year.
        
        Interacts with the form to select the specific month/year,
        then checks if the result row appears.
        
        Args:
            month: Reference month (1-12)
            year: Reference year
        
        Returns:
            True if data for the requested period is found
        """
        month_name = month_name_pt(month).lower()
        logger.info(f"Checking availability for ES - {month_name}/{year}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                
                # Wait for form to be ready
                page.wait_for_timeout(1000)
                
                # Try to interact with the form
                if not self._search_period(page, month_name, year):
                    return False
                
                # Check if the target row exists using strict table matching
                target_text = f"{month_name}/{year}"
                
                # Use strict table row locator
                row = page.locator("tr").filter(has_text=target_text).last
                
                if row.count() > 0:
                    logger.info(f"Data available for {month_name}/{year}")
                    return True
                
                # Fallback: check for cell with target text
                cell = page.locator("td", has_text=target_text).first
                if cell.count() > 0:
                    logger.info(f"Data available for {month_name}/{year}")
                    return True
                
                logger.warning(f"Data NOT available for {month_name}/{year}")
                return False
                
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
        Extract CUB data for the specified month/year.
        
        Uses strict Table Row Strategy:
        1. Find the specific <tr> containing the target date
        2. Get the 2nd <td> (index 1) which contains the Value
        
        Args:
            month: Reference month (1-12)
            year: Reference year
        
        Returns:
            CUBData object with extracted R8-N value
        """
        month_name = month_name_pt(month).lower()
        logger.info(f"Extracting CUB data for ES - {month_name}/{year}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                
                # Wait for form to be ready
                page.wait_for_timeout(1000)
                
                # Interact with form
                if not self._search_period(page, month_name, year):
                    raise ValueError(f"Could not search for period {month_name}/{year}")
                
                # 1. Define target string
                target_text = f"{month_name}/{year}"
                logger.info(f"Looking for table row with text: {target_text}")
                
                # 2. Strict Table Row Locator
                # Find a row 'tr' that has a cell containing the target date
                # Layout: <tr> <td>Date</td> <td>Value</td> ... </tr>
                row = page.locator("tr").filter(has_text=target_text).last
                # Using .last because tables may have hidden duplicates or nested structures
                
                if not row.count():
                    # Fallback: Try strict cell matching and navigate to parent row
                    cell = page.locator("td", has_text=target_text).first
                    if cell.count() > 0:
                        row = cell.locator("xpath=..")
                    else:
                        raise ValueError(f"Could not find table row for {target_text}")
                
                if not row.count():
                    raise ValueError(f"Could not find table row for {target_text}")
                
                logger.info("Found target row in table")
                
                # 3. Get the Value Cell (2nd Column -> Index 1)
                cells = row.locator("td")
                cell_count = cells.count()
                logger.debug(f"Row has {cell_count} cells")
                
                if cell_count < 2:
                    raise ValueError(f"Table row has insufficient cells ({cell_count})")
                
                value_cell = cells.nth(1)
                raw_text = value_cell.inner_text()
                logger.info(f"Raw value text from cell: '{raw_text}'")
                
                # 4. Clean and Parse
                # Remove R$, spaces, convert 2.791,83 -> 2791.83
                clean_text = raw_text.replace("R$", "").replace(".", "").replace(",", ".").strip()
                
                # Safety check: ensure it looks like a number
                if not re.match(r"^\d+(\.\d+)?$", clean_text):
                    # Try regex search if there is garbage text
                    match = re.search(r"(\d+(?:\.\d+)?)", clean_text)
                    if match:
                        clean_text = match.group(1)
                    else:
                        raise ValueError(f"Could not extract numeric value from: '{raw_text}'")
                
                valor = float(clean_text)
                logger.info(f"Extracted value: {valor}")
                
            except Exception as e:
                logger.error(f"Extraction error: {e}")
                raise
            finally:
                browser.close()
        
        # Create CUBValor object (mapped to R8-N per user confirmation)
        valor_obj = CUBValor(
            projeto="R8-N",
            valor=valor,
            unidade="R$/m²"
        )
        
        return CUBData(
            estado="ES",
            mes_referencia=month,
            ano_referencia=year,
            data_extracao=datetime.now(),
            valores=[valor_obj]
        )
    
    def _search_period(self, page, month_name: str, year: int) -> bool:
        """
        Interact with the form to search for a specific period.
        
        Args:
            page: Playwright page object
            month_name: Month name in Portuguese (lowercase)
            year: Year as integer
        
        Returns:
            True if search was successful (form submitted)
        """
        try:
            # Try different selector strategies for month dropdown
            month_selectors = [
                "select[name='mes']",
                "select[name='Mes']",
                "select[name='MES']",
                "#mes",
            ]
            
            month_selected = False
            for selector in month_selectors:
                try:
                    dropdown = page.locator(selector)
                    if dropdown.count() > 0:
                        page.select_option(selector, label=month_name)
                        month_selected = True
                        logger.debug(f"Selected month '{month_name}' using selector: {selector}")
                        break
                except:
                    continue
            
            if not month_selected:
                # Fallback: Try first combobox
                logger.warning("Could not select month via dropdown, trying combobox...")
                try:
                    month_dropdown = page.get_by_role("combobox").first
                    if month_dropdown.count() > 0:
                        month_dropdown.select_option(label=month_name)
                        month_selected = True
                except:
                    pass
            
            if not month_selected:
                logger.error("Failed to select month in form")
                return False
            
            # Try different selector strategies for year dropdown
            year_selectors = [
                "select[name='ano']",
                "select[name='Ano']",
                "select[name='ANO']",
                "#ano",
            ]
            
            year_selected = False
            for selector in year_selectors:
                try:
                    dropdown = page.locator(selector)
                    if dropdown.count() > 0:
                        page.select_option(selector, label=str(year))
                        year_selected = True
                        logger.debug(f"Selected year '{year}' using selector: {selector}")
                        break
                except:
                    continue
            
            if not year_selected:
                # Fallback: Try second combobox
                logger.warning("Could not select year via dropdown, trying combobox...")
                try:
                    year_dropdown = page.get_by_role("combobox").nth(1)
                    if year_dropdown.count() > 0:
                        year_dropdown.select_option(label=str(year))
                        year_selected = True
                except:
                    pass
            
            if not year_selected:
                logger.error("Failed to select year in form")
                return False
            
            # Click the search button
            search_selectors = [
                "input[type='submit']",
                "button[type='submit']",
                "input[value*='buscar' i]",
                "input[value*='Buscar']",
                "input[value*='pesquisar' i]",
            ]
            
            for selector in search_selectors:
                try:
                    btn = page.locator(selector)
                    if btn.count() > 0:
                        logger.info("Clicking search button...")
                        btn.first.click()
                        
                        # Wait for results to load
                        page.wait_for_load_state("networkidle", timeout=10000)
                        page.wait_for_timeout(1000)
                        
                        return True
                except:
                    continue
            
            logger.error("Could not find search button")
            return False
                
        except Exception as e:
            logger.error(f"Error during form interaction: {e}")
            return False
    
    @staticmethod
    def _parse_brl_currency(value_str: str) -> float:
        """
        Parse Brazilian currency string to float.
        
        Examples:
            "R$ 2.791,83" -> 2791.83
            "2.791,83" -> 2791.83
        
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
        
        # Remove any remaining non-numeric characters except dot
        cleaned = re.sub(r'[^\d.]', '', cleaned)
        
        try:
            return float(cleaned)
        except ValueError:
            raise ValueError(f"Could not parse currency: '{value_str}'")


# Test block for direct execution
if __name__ == "__main__":
    scraper = ScraperES(headless=False)
    
    # Test with December 2025
    month, year = 12, 2025
    
    print(f"\n{'='*50}")
    print(f"Testing ES Scraper for {month}/{year}")
    print(f"{'='*50}\n")
    
    available = scraper.check_availability(month, year)
    print(f"Available: {available}")
    
    if available:
        try:
            data = scraper.extract(month, year)
            print(f"\n{'='*50}")
            print(f"SUCCESS! Extracted Data:")
            print(f"  State: {data.estado}")
            print(f"  Period: {data.mes_referencia}/{data.ano_referencia}")
            print(f"  Project: {data.valores[0].projeto}")
            print(f"  Value: {data.valores[0].valor}")
            print(f"{'='*50}\n")
        except Exception as e:
            print(f"EXTRACTION FAILED: {e}")
    else:
        print(f"Data not available for {month}/{year}")

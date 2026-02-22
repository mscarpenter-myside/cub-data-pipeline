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
from typing import Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from src.scrapers.base import BaseScraper, MONTHS_PT
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
        
        Implements a two-phase approach:
        1. Happy Path: Exact regex match for the reference month pattern
        2. Diagnostic Fallback: If exact match fails, captures page content
           and logs all dates found to help distinguish between "data not 
           published yet" vs "layout change broke the scraper"
        
        Args:
            month: Reference month (1-12)
            year: Reference year
        
        Returns:
            True if exact match found, False otherwise (with diagnostic logs)
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
                
                # ═══════════════════════════════════════════════════════════════
                # PHASE 1: Happy Path - Exact Regex Match
                # ═══════════════════════════════════════════════════════════════
                # Pattern: "Mês de Referência: Dezembro/2025" (with flexible whitespace)
                ref_pattern = re.compile(
                    rf"Mês\s+de\s+Referência[:\s]*{month_name}/{year}",
                    re.IGNORECASE
                )
                
                ref_element = page.get_by_text(ref_pattern).first
                available = ref_element.count() > 0 and ref_element.is_visible()
                
                if available:
                    logger.info(f"✅ Data available for {month_name}/{year} (exact match found)")
                    return True
                
                # ═══════════════════════════════════════════════════════════════
                # PHASE 2: Diagnostic Fallback - Analyze page when exact match fails
                # ═══════════════════════════════════════════════════════════════
                logger.warning(f"❌ Exact match failed for '{month_name}/{year}'. Entering diagnostic mode...")
                
                # Attempt to capture text from visual containers in order of specificity
                container_selectors = [
                    ".card",
                    ".box",
                    ".elementor-widget-container",
                    ".elementor-element",
                    "[class*='cub']",
                    "[class*='referencia']",
                    "main",
                    "article",
                    "body"
                ]
                
                captured_text = ""
                container_used = None
                
                for selector in container_selectors:
                    try:
                        container = page.locator(selector).first
                        if container.count() > 0:
                            captured_text = container.inner_text(timeout=3000)
                            if captured_text and len(captured_text.strip()) > 50:
                                container_used = selector
                                break
                    except Exception:
                        continue
                
                # Apply generic date regex to find any dates on the page
                # Pattern matches: "Dezembro/2025", "Jan/2026", "Janeiro/2025", etc.
                date_pattern = re.compile(r'\b([A-Za-zÀ-ú]+)\s*/\s*(\d{4})\b')
                dates_found = date_pattern.findall(captured_text)
                
                # Remove duplicates while preserving order
                unique_dates = []
                seen = set()
                for date_tuple in dates_found:
                    date_str = f"{date_tuple[0]}/{date_tuple[1]}"
                    if date_str.lower() not in seen:
                        seen.add(date_str.lower())
                        unique_dates.append(date_str)
                
                # Generate diagnostic log
                logger.info("=" * 70)
                logger.info("📊 DIAGNOSTIC REPORT - check_availability()")
                logger.info("=" * 70)
                logger.info(f"   Target: {month_name}/{year}")
                logger.info(f"   Exact pattern: Mês de Referência: {month_name}/{year}")
                logger.info(f"   Container analyzed: {container_used or 'none found'}")
                
                if unique_dates:
                    logger.info(f"   📅 Dates found on page: {unique_dates}")
                    
                    # Check if target date exists in any format
                    target_found_in_alt_format = any(
                        month_name.lower() in d.lower() and str(year) in d
                        for d in unique_dates
                    )
                    if target_found_in_alt_format:
                        logger.warning(
                            f"   ⚠️  Target month '{month_name}/{year}' found but NOT in expected format! "
                            "Possible layout change detected."
                        )
                    else:
                        logger.info(
                            f"   ℹ️  Target month '{month_name}/{year}' NOT found among detected dates. "
                            "Data likely not published yet."
                        )
                else:
                    logger.warning("   ⚠️  No dates found on page! Possible major layout change or page load issue.")
                
                # Log sample text (truncated for readability)
                sample_text = captured_text[:500].replace('\n', ' ').strip() if captured_text else "(empty)"
                logger.info(f"   📝 Sample text: '{sample_text}...'")
                logger.info("=" * 70)
                
                return False
                
            except PlaywrightTimeout:
                logger.error("⏱️ Timeout while checking availability")
                return False
            except Exception as e:
                logger.error(f"💥 Error checking availability: {e}")
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
    
    def get_latest_available(self) -> Optional[Tuple[int, int]]:
        """
        Discover the most recent CUB data available on the page.
        
        Scans the page for date patterns and returns the most recent one found.
        This is useful when you don't know which month is currently published.
        
        Returns:
            Tuple (month, year) of the most recent data available, or None if 
            no valid dates were found.
        
        Example:
            >>> scraper = ScraperSC()
            >>> latest = scraper.get_latest_available()
            >>> print(latest)  # (12, 2025) for December 2025
        """
        logger.info("🔍 Discovering latest available CUB data...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("networkidle")
                
                # Dismiss cookie banner
                self._dismiss_cookies(page)
                
                # Wait for content to load
                page.wait_for_timeout(1500)
                
                # Capture text from main content area
                container_selectors = [
                    "main",
                    ".elementor-widget-container",
                    "article",
                    "body"
                ]
                
                captured_text = ""
                for selector in container_selectors:
                    try:
                        container = page.locator(selector).first
                        if container.count() > 0:
                            captured_text = container.inner_text(timeout=3000)
                            if captured_text and len(captured_text.strip()) > 100:
                                break
                    except Exception:
                        continue
                
                if not captured_text:
                    logger.error("❌ Could not capture page text")
                    return None
                
                # Find all date patterns: "Dezembro/2025", "Janeiro/2026", etc.
                date_pattern = re.compile(r'\b([A-Za-zÀ-ú]+)\s*/\s*(\d{4})\b')
                dates_found = date_pattern.findall(captured_text)
                
                if not dates_found:
                    logger.error("❌ No date patterns found on page")
                    return None
                
                # Parse and sort dates to find the most recent
                valid_dates = []
                for month_name, year_str in dates_found:
                    month_num = self._month_name_to_number(month_name)
                    if month_num:
                        year = int(year_str)
                        valid_dates.append((month_num, year, month_name))
                
                if not valid_dates:
                    logger.error("❌ Could not parse any valid dates")
                    return None
                
                # Sort by year DESC, then month DESC to get most recent first
                valid_dates.sort(key=lambda x: (x[1], x[0]), reverse=True)
                
                latest_month, latest_year, latest_name = valid_dates[0]
                
                logger.info(f"📅 Dates found on page: {[(d[2], d[1]) for d in valid_dates[:5]]}")
                logger.info(f"✅ Most recent data: {latest_name}/{latest_year}")
                
                return (latest_month, latest_year)
                
            except PlaywrightTimeout:
                logger.error("⏱️ Timeout while discovering latest data")
                return None
            except Exception as e:
                logger.error(f"💥 Error discovering latest data: {e}")
                return None
            finally:
                browser.close()
    
    def extract_latest(self) -> Optional[CUBData]:
        """
        Convenience method: Discover the latest available date and extract its data.
        
        This combines get_latest_available() + extract() in a single call.
        Extracts CUB SEM DESONERAÇÃO (Residencial Médio - R8-N).
        
        Returns:
            CUBData object with the most recent data, or None if extraction fails.
        
        Example:
            >>> scraper = ScraperSC()
            >>> data = scraper.extract_latest()
            >>> print(data.mes_referencia, data.ano_referencia, data.valores[0].valor)
        """
        latest = self.get_latest_available()
        
        if not latest:
            logger.error("❌ Could not determine the latest available data")
            return None
        
        month, year = latest
        month_name = month_name_pt(month)
        
        logger.info(f"📊 Extracting CUB data for {month_name}/{year} (CUB sem desoneração)...")
        
        try:
            return self.extract(month, year)
        except Exception as e:
            logger.error(f"💥 Extraction failed: {e}")
            return None
    
    # _month_name_to_number and _parse_brl_currency are inherited from BaseScraper


# Test block
if __name__ == "__main__":
    scraper = ScraperSC(headless=False)
    
    print("=" * 60)
    print("🔍 Testando extração automática do CUB mais recente...")
    print("=" * 60)
    
    # Use the new extract_latest() method
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


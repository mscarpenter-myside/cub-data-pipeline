"""
CUB Data Pipeline - INCC Scraper

Concrete implementation of BaseScraper for extracting INCC-M data
from FGV IBRE website.

Target: https://portalibre.fgv.br/press-releases
Method: 
1. Strict Filter Synchronization (Wait for DOM rebuilds)
2. Regex Match for Title
3. PDF Download & Parsing
"""

import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import pdfplumber
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from src.scrapers.base import BaseScraper
from src.core.models import CUBData, CUBValor
from src.utils.helpers import get_logger, get_data_path, month_name_pt

logger = get_logger(__name__)


class ScraperINCC(BaseScraper):
    def __init__(self, headless: bool = True):
        super().__init__(
            estado="BR", 
            base_url="https://portalibre.fgv.br/press-releases"
        )
        self.headless = headless

    def _unhide_selects(self, page):
        """Helper to force-show hidden Drupal select elements."""
        page.evaluate("""
            () => {
                document.querySelectorAll('select').forEach(el => {
                    el.style.display = 'block';
                    el.style.visibility = 'visible';
                    el.style.opacity = '1';
                    el.classList.remove('hidden', 'shs-hidden', 'visually-hidden');
                });
            }
        """)

    def _apply_filters(self, page):
        """
        Apply filters with strict waiting for AJAX rebuilds.
        """
        logger.info("Applying filters for INCC-M...")
        
        try:
            # 1. Select Category
            self._unhide_selects(page)
            logger.info("Selecting Category: Índice Geral de Preço")
            sel_cat = page.locator("select").first
            sel_cat.select_option(label="Índice Geral de Preço", force=True)
            sel_cat.dispatch_event("change")
            
            # CRITICAL: Wait for the site to process the category change.
            # Drupal usually replaces the second dropdown DOM element entirely.
            logger.info("Waiting for Index dropdown to rebuild...")
            page.wait_for_timeout(3000) 
            
            # 2. Select Index (INCC-M)
            # We must unhide again because the new dropdown was created hidden
            self._unhide_selects(page)
            
            # Find the select that specifically contains "INCC-M"
            target_select = page.locator("select:has(option:text-is('INCC-M'))").first
            
            # Retry loop for the second dropdown appearance
            for i in range(3):
                if target_select.count() > 0:
                    break
                logger.warning("INCC-M dropdown not found yet, waiting...")
                page.wait_for_timeout(1000)
                self._unhide_selects(page)
            
            logger.info("Selecting Index: INCC-M")
            target_select.select_option(label="INCC-M", force=True)
            target_select.dispatch_event("change")
            
            # 3. Validation: Verify selection sticked before filtering
            # We check if the value of the select is truthy
            is_selected = page.evaluate("el => el.selectedIndex !== -1", target_select.element_handle())
            if not is_selected:
                logger.warning("Selection might have failed, forcing index...")
                # Fallback: manually select the option via JS matches text
                page.evaluate("""
                    (text) => {
                        const selects = document.querySelectorAll('select');
                        selects.forEach(s => {
                            for (let i = 0; i < s.options.length; i++) {
                                if (s.options[i].text === text) {
                                    s.selectedIndex = i;
                                    s.dispatchEvent(new Event('change'));
                                }
                            }
                        })
                    }
                """, "INCC-M")

            page.wait_for_timeout(1000)
            
            # 4. Click Filter
            logger.info("Clicking Filter button...")
            btn_filter = page.locator("input[value='Filtrar'], button:has-text('Filtrar')").first
            
            # Tracking request to ensure new search happens
            with page.expect_response(lambda response: response.status == 200, timeout=10000) as response_info:
                btn_filter.click(force=True)
            
            # Wait for results reload
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(2000)
            
        except Exception as e:
            logger.error(f"Filter process warning: {e}")
            # Continue anyway, extraction logic handles finding the link

    def _find_release_link(self, page, month: int, year: int):
        """
        Finds the specific release link in the results.
        """
        month_ext = month_name_pt(month).lower()
        # Pattern: "INCC-M de dezembro de 2025"
        regex = re.compile(rf"INCC-M\s+de\s+{month_ext}\s+de\s+{year}", re.IGNORECASE)
        
        logger.info(f"Scanning page 1 for release: '{month_ext}/{year}'")
        
        # Debug: Print top results to confirm filter worked
        titles = page.locator(".views-row .field-content a").all_inner_texts()
        if titles:
            logger.info(f"Top visible results: {titles[:3]}")

        link = page.get_by_text(regex).first
        if link.count() > 0 and link.is_visible():
            logger.info(f"🎯 Found target release: {link.inner_text()}")
            return link
            
        return None

    def check_availability(self, month: int, year: int) -> bool:
        logger.info(f"Checking availability for INCC - {month}/{year}")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            try:
                page.goto(self.base_url, timeout=60000)
                self._apply_filters(page)
                link = self._find_release_link(page, month, year)
                return link is not None
            except Exception as e:
                logger.error(f"Check failed: {e}")
                return False
            finally:
                browser.close()

    def extract(self, month: int, year: int) -> CUBData:
        logger.info(f"Extracting INCC data for {month}/{year}")
        raw_path = get_data_path("raw")
        pdf_path = raw_path / f"incc_{year}_{month:02d}.pdf"
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            try:
                page.goto(self.base_url, timeout=60000)
                self._apply_filters(page)
                
                # Find Link
                link = self._find_release_link(page, month, year)
                if not link:
                    raise ValueError(f"Release not found for {month}/{year}. Filters may have failed.")
                
                # Navigate
                logger.info(f"Navigating to release page...")
                link.click(force=True)
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)
                
                # Find PDF
                logger.info("Searching for PDF download link...")
                # Strategies: .pdf href, 'Download' text, 'Íntegra' text
                pdf_link = page.locator("a[href$='.pdf']").first
                if pdf_link.count() == 0:
                    pdf_link = page.locator("a:has-text('Download')").first
                if pdf_link.count() == 0:
                    pdf_link = page.locator("a:has-text('Íntegra')").first
                
                if pdf_link.count() == 0:
                    raise ValueError("No PDF link found on release page")
                
                href = pdf_link.get_attribute("href")
                if not href.startswith("http"):
                    href = urljoin("https://portalibre.fgv.br", href)
                
                # Download
                logger.info(f"Downloading PDF: {href}")
                response = page.request.get(href)
                if response.status != 200:
                    raise ValueError(f"Download error: {response.status}")
                
                with open(pdf_path, "wb") as f:
                    f.write(response.body())
                logger.info(f"Saved: {pdf_path}")
                
            except Exception as e:
                logger.error(f"Extraction error: {e}")
                raise
            finally:
                browser.close()
        
        # Parse PDF
        valor = self._parse_pdf(pdf_path)
        logger.info(f"Final Value: {valor}")
        
        return CUBData(
            estado="BR",
            mes_referencia=month,
            ano_referencia=year,
            data_extracao=datetime.now(),
            valores=[CUBValor(projeto="INCC-M", valor=valor, unidade="Pontos")]
        )

    def _parse_pdf(self, pdf_path: Path) -> float:
        logger.info(f"Parsing: {pdf_path}")
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                # Look for Tabela 2
                if "Tabela 2" in text or "TABELA 2" in text:
                    logger.info(f"Found Table 2 on page {page.page_number}")
                    for line in text.split('\n'):
                        if line.strip().startswith("INCC-M"):
                            # Regex to grab the Index Number (usually > 500)
                            # Matches: 1.234,56
                            matches = re.findall(r'(\d{1,4}(?:[.,]\d{3})*[.,]\d+)', line)
                            for m in matches:
                                val_clean = m.replace(".", "").replace(",", ".")
                                try:
                                    val = float(val_clean)
                                    # Heuristic check: INCC index is a large number (1000+)
                                    if val > 500: return val
                                except: continue
                                
        raise ValueError("INCC Index not found in PDF")

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
    
    scraper = ScraperINCC(headless=False)
    month, year = 12, 2025
    
    print(f"\n--- Testing INCC Scraper for {month}/{year} ---")
    if scraper.check_availability(month, year):
        try:
            data = scraper.extract(month, year)
            print(f"SUCCESS: {data.valores[0].valor}")
        except Exception as e:
            print(f"FAIL: {e}")
    else:
        print("Unavailable")
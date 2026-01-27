"""
CUB Data Pipeline - Pará (PA) Scraper

Concrete implementation of BaseScraper for extracting CUB data
from Sinduscon-PA website.

Target: https://www.sindusconpa.org.br/cub
Method: Cookie Bypass + Smart Parent Scroll -> DOM Pattern Search -> PDF Download
Target Value: R-8 from PADRÃO NORMAL (middle column)
"""

import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import pdfplumber
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from src.scrapers.base import BaseScraper
from src.core.models import CUBData, CUBValor
from src.utils.helpers import get_logger, get_data_path, month_name_pt

logger = get_logger(__name__)


class ScraperPA(BaseScraper):
    """
    Scraper for Pará (PA) CUB data.
    
    Target: R-8 (Residencial 8 pavimentos) from the PADRÃO NORMAL column.
    
    Strategy:
    1. Select Year (soft text match loop)
    2. Dismiss Cookie Banner
    3. Ensure Visibility via Smart Parent Scroll
    4. Find Link:
       A. Exact file pattern matching "CUB_YYYYMM.pdf" in href
       B. DOM structure matching (.box-boletim with month name)
    """
    
    def __init__(self, headless: bool = True):
        """Initialize the PA scraper with parent attributes."""
        super().__init__(
            estado="PA",
            base_url="https://www.sindusconpa.org.br/cub"
        )
        self.headless = headless
    
    def _handle_cookie_banner(self, page):
        """Dismiss cookie banner if present."""
        try:
            selectors = [
                "button:has-text('Concordar')",
                "button:has-text('Aceitar')",
                "a:has-text('Concordar')",
                "div[class*='cookie'] button",
                "#cookie-consent button"
            ]
            
            for selector in selectors:
                btn = page.locator(selector).first
                if btn.count() > 0 and btn.is_visible():
                    logger.info(f"Dismissing cookie banner via {selector}...")
                    btn.click()
                    page.wait_for_timeout(500)
                    return
        except Exception as e:
            logger.debug(f"Cookie banner handling ignored: {e}")

    def _select_year_soft(self, page, year: int) -> bool:
        """Soft year selection - non-blocking."""
        try:
            logger.info(f"Attempting soft year selection: {year}")
            try:
                page.wait_for_selector("select", state="attached", timeout=3000)
            except PlaywrightTimeout:
                return False
            
            select = page.locator("select").first
            if select.count() == 0: return False
            
            try:
                select.select_option(label=str(year))
                select.dispatch_event("change")
            except:
                try:
                    select.select_option(value=str(year))
                    select.dispatch_event("change")
                except:
                    return False
            
            page.wait_for_timeout(1500)
            return True
        except:
            return False

    def _ensure_month_visible(self, page, month_name: str):
        """
        Ensure the target month card is visible by finding and scrolling 
        the specific DOM container that holds the cards.
        """
        logger.info(f"Ensuring Month '{month_name}' is visible (Smart Parent Scroll)...")
        
        # 1. Try finding the target immediately
        if page.locator(".box-boletim").filter(has_text=re.compile(rf"\b{month_name}\b", re.IGNORECASE)).count() > 0:
            return

        # 2. Execute JS to find the scrollable container of the cards and scroll it
        logger.info("Executing JS to find and scroll the inner container...")
        scrolled = page.evaluate("""
            () => {
                // Find a reference card (e.g., the first one)
                const card = document.querySelector('.box-boletim');
                if (!card) return false;

                // Traverse parents to find the scroller
                let parent = card.parentElement;
                while (parent && parent !== document.body) {
                    const style = window.getComputedStyle(parent);
                    // Check if element is scrollable (content > height AND overflow is set)
                    const isScrollable = (parent.scrollHeight > parent.clientHeight) && 
                                         (style.overflowY === 'auto' || style.overflowY === 'scroll');
                    
                    if (isScrollable) {
                        // Found the scroll container! Scroll to bottom.
                        parent.scrollTop = parent.scrollHeight;
                        return true; // Signal success
                    }
                    parent = parent.parentElement;
                }
                return false; // No nested scroller found
            }
        """)

        if scrolled:
            logger.info("Inner container scrolled via JS. Waiting for lazy load...")
            page.wait_for_timeout(2000) # Give time for December to render
        else:
            logger.warning("No inner scroll container found. Trying fallback body scroll.")
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(1000)

        # 3. Debug: List what we can see now
        try:
            visible_months = page.locator(".box-boletim h2").all_inner_texts()
            logger.info(f"Visible months in DOM: {visible_months}")
        except:
            pass

    def _find_cub_link_for_month(self, page, month: int, year: int) -> str | None:
        """
        Find CUB link using Pattern Matching or DOM Structure.
        
        Args:
            page: Playwright page object
            month: Month integer (1-12)
            year: Year integer
        """
        month_name = month_name_pt(month).upper()
        logger.info(f"Finding CUB link for {month_name}/{year}")

        # 1. Clear obstructions & Ensure DOM is loaded
        self._handle_cookie_banner(page)
        self._ensure_month_visible(page, month_name)
        
        # 2. STRATEGY A: File Pattern Match (Most Robust)
        # Expected pattern based on DOM inspection: "CUB_202512"
        file_pattern = f"CUB_{year}{month:02d}"
        logger.info(f"Searching for link matching pattern: {file_pattern}")
        
        try:
            # Locate anchor with href containing the pattern
            # We iterate manually to check for .pdf extension safety
            links = page.locator(f"a[href*='{file_pattern}']").all()
            for link in links:
                href = link.get_attribute("href")
                if href and ".pdf" in href.lower():
                    logger.info(f"🎯 Exact file match found: {href}")
                    return href
        except Exception as e:
            logger.debug(f"Pattern search error: {e}")

        # 3. STRATEGY B: DOM Structure (.box-boletim)
        logger.info("Falling back to .box-boletim structure search...")
        try:
            # Find the box containing the month name (case insensitive)
            # We filter .box-boletim elements that have the month name
            target_box = page.locator(".box-boletim").filter(has_text=re.compile(rf"\b{month_name}\b", re.IGNORECASE)).last
            
            if target_box.count() > 0:
                target_box.scroll_into_view_if_needed()
                # Get the link inside (usually class='btn', sometimes just the 'a')
                link = target_box.locator("a").first
                href = link.get_attribute("href")
                
                if href and ".pdf" in href.lower():
                    logger.info(f"🎯 Structure match found: {href}")
                    return href
                
                # Try specific button class if generic 'a' fails
                btn_link = target_box.locator("a.btn").first
                href = btn_link.get_attribute("href")
                if href and ".pdf" in href.lower():
                    logger.info(f"🎯 Structure match found (btn class): {href}")
                    return href
                    
        except Exception as e:
            logger.debug(f"Structure search error: {e}")

        logger.warning(f"No PDF link found for {month_name}/{year}")
        return None

    def check_availability(self, month: int, year: int) -> bool:
        """Check if CUB data is available for the specified month/year."""
        month_name = month_name_pt(month).upper()
        logger.info(f"Checking availability for PA - {month_name}/{year}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)
                
                self._select_year_soft(page, year)
                
                href = self._find_cub_link_for_month(page, month, year)
                
                if href:
                    logger.info(f"Data available for {month_name}/{year}: {href}")
                    return True
                
                logger.warning(f"Data NOT available for {month_name}/{year}")
                return False
                
            except Exception as e:
                logger.error(f"Error checking availability: {e}")
                return False
            finally:
                browser.close()
    
    def extract(self, month: int, year: int) -> CUBData:
        """Extract CUB data (R-8 Normal) for the specified month/year."""
        month_name = month_name_pt(month).upper()
        logger.info(f"Extracting CUB data for PA - {month_name}/{year}")
        
        raw_path = get_data_path("raw")
        pdf_filename = f"cub_pa_{year}_{month:02d}.pdf"
        pdf_path = raw_path / pdf_filename
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)
                
                self._select_year_soft(page, year)
                
                href = self._find_cub_link_for_month(page, month, year)
                
                if not href:
                    raise ValueError(f"Could not find CUB PDF for {month_name}/{year}")
                
                if not href.startswith("http"):
                    href = urljoin(self.base_url, href)
                
                logger.info(f"Downloading PDF from: {href}")
                
                response = page.request.get(href)
                if response.status != 200:
                    raise ValueError(f"Download failed: {response.status}")
                    
                with open(pdf_path, "wb") as f:
                    f.write(response.body())
                
                logger.info(f"PDF saved to: {pdf_path}")
                
            except Exception as e:
                logger.error(f"Extraction error: {e}")
                raise
            finally:
                browser.close()
        
        # Parse PDF
        valor = self._parse_pdf(pdf_path)
        logger.info(f"Extracted R-8 Normal value: {valor}")
        
        valores = [CUBValor(projeto="R-8", valor=valor, unidade="R$/m²")]
        return CUBData(
            estado="PA",
            mes_referencia=month,
            ano_referencia=year,
            data_extracao=datetime.now(),
            valores=valores
        )
    
    def _parse_pdf(self, pdf_path: Path) -> float:
        """Parse PDF using Column Isolation Strategy for R-8 Normal."""
        logger.info(f"Parsing PDF: {pdf_path}")
        
        with pdfplumber.open(str(pdf_path)) as pdf:
            page = pdf.pages[0]
            words = page.extract_words(keep_blank_chars=True)
            
            header_normal = None
            header_baixo = None
            header_alto = None
            
            for word in words:
                text = word['text'].upper()
                if word['top'] > page.height / 2: continue
                
                if "NORMAL" in text and not header_normal:
                    header_normal = word
                elif "BAIXO" in text and not header_baixo:
                    header_baixo = word
                elif "ALTO" in text and not header_alto:
                    header_alto = word
            
            if not header_normal:
                return self._find_r8_fallback(page)
            
            norm_x0, norm_x1 = float(header_normal['x0']), float(header_normal['x1'])
            y0, y1 = float(header_normal['top']), float(header_normal['top']) + 500
            
            x0 = (float(header_baixo['x1']) + norm_x0) / 2 if (header_baixo and float(header_baixo['x1']) < norm_x0) else norm_x0 - 80
            x1 = (norm_x1 + float(header_alto['x0'])) / 2 if (header_alto and float(header_alto['x0']) > norm_x1) else norm_x1 + 80
            
            x0, x1 = max(0.0, x0), min(float(page.width), x1)
            y0, y1 = max(0.0, y0), min(float(page.height), y1)
            
            if x0 >= x1:
                x0, x1 = max(0.0, norm_x0 - 100), min(float(page.width), norm_x1 + 100)
            
            crop = page.crop((x0, y0, x1, y1))
            text = crop.extract_text(layout=True)
            
            if not text: return self._find_r8_fallback(page)
            
            for line in text.split('\n'):
                if re.search(r'\bR[-\s]?8\b', line, re.IGNORECASE):
                    match = re.search(r'R-?8.*?(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                    if match: return self._parse_brl_currency(match.group(1))
                    
                    match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                    if match: return self._parse_brl_currency(match.group(1))
            
            return self._find_r8_fallback(page)
    
    def _find_r8_fallback(self, page) -> float:
        """Fallback extraction strategy."""
        text = page.extract_text(layout=True)
        if not text: raise ValueError("Could not extract text from PDF")
        
        for line in text.split('\n'):
            if re.search(r'\bR[-\s]?8\b', line, re.IGNORECASE):
                match = re.search(r'R-?8.*?(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                if match: return self._parse_brl_currency(match.group(1))
                
                matches = re.findall(r'(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                if len(matches) >= 2: return self._parse_brl_currency(matches[1])
                elif matches: return self._parse_brl_currency(matches[0])
        
        raise ValueError("Could not extract R-8 value from PDF")
    
    @staticmethod
    def _parse_brl_currency(value_str: str) -> float:
        """Parse Brazilian currency string to float."""
        cleaned = value_str.replace("R$", "").strip().replace(".", "").replace(",", ".")
        return float(cleaned)


if __name__ == "__main__":
    scraper = ScraperPA(headless=False)
    month, year = 12, 2025
    
    print(f"\n{'='*50}")
    print(f"Testing PA Scraper for {month}/{year}")
    print(f"{'='*50}\n")
    
    if scraper.check_availability(month, year):
        try:
            data = scraper.extract(month, year)
            print(f"SUCCESS! Value: {data.valores[0].valor}")
        except Exception as e:
            print(f"FAILED: {e}")
    else:
        print(f"Data not available")

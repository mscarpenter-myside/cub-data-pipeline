"""
CUB Data Pipeline - Minas Gerais (MG) Scraper

Concrete implementation of BaseScraper for extracting CUB data
from Sinduscon-MG website.

Target: https://sinduscon-mg.org.br/cub/tabela-do-cub/
Method: Direct PDF Download (Bypassing Viewer) + PDFPlumber extraction

The website has an accordion with Year buttons that reveal a "SELECIONAR" button.
Clicking "SELECIONAR" opens the month list.
Each Month button/link points to a PDF file.
"""

import re
from datetime import datetime
from pathlib import Path
import time

import pdfplumber
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from src.scrapers.base import BaseScraper
from src.core.models import CUBData, CUBValor
from src.utils.helpers import get_logger, get_data_path, month_name_pt

logger = get_logger(__name__)


class ScraperMG(BaseScraper):
    """
    Scraper for Minas Gerais (MG) CUB data.
    
    Downloads PDF files from the Sinduscon-MG website and parses
    the table to extract CUB values using column isolation.
    Target: R8-N (Residencial Multifamiliar - Padrão Normal)
    """
    
    def __init__(self, headless: bool = True):
        """Initialize the MG scraper with parent attributes."""
        # Note: headless=False might be useful for debugging, but direct download usually works in headless
        super().__init__(
            estado="MG",
            base_url="https://sinduscon-mg.org.br/cub/tabela-do-cub/"
        )
        self.headless = headless
    
    def check_availability(self, month: int, year: int) -> bool:
        """
        Check if CUB data is available for the specified month.
        
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
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)
                self._dismiss_cookies(page)
                
                # 1. Check if Month is already visible (Default state for current year)
                month_pattern = re.compile(month_name, re.IGNORECASE)
                month_btn = page.get_by_text(month_pattern).first
                
                if month_btn.is_visible():
                    logger.info(f"Month {month_name} found immediately.")
                    return True
                    
                # 2. Expand Year
                logger.info(f"Month not visible. Expanding Year {year}...")
                
                # Find year text
                year_text = page.get_by_text(str(year), exact=True).first
                if year_text.count() == 0:
                     logger.warning(f"Year text '{year}' not found on page.")
                     return False
                
                # Parent Traversal Strategy to find SELECIONAR
                # Try 1 level up
                select_btn = year_text.locator("xpath=..").locator("text=SELECIONAR")
                if select_btn.count() == 0:
                    # Try 2 levels up
                    select_btn = year_text.locator("xpath=../..").locator("text=SELECIONAR")
                
                # Fallback: First visible SELECIONAR
                if select_btn.count() == 0 or not select_btn.is_visible():
                    select_btn = page.locator("text=SELECIONAR").first
                
                if select_btn.count() > 0 and select_btn.is_visible():
                    select_btn.click()
                    page.wait_for_timeout(2000) # Wait for animation
                    
                    # Check again
                    if month_btn.is_visible():
                        logger.info(f"Month {month_name} found after expansion.")
                        return True
                else:
                    logger.warning("Could not interact with any 'SELECIONAR' button.")
                
                return False

            except Exception as e:
                logger.error(f"Error checking availability: {e}")
                return False
            finally:
                browser.close()
    
    def extract(self, month: int, year: int) -> CUBData:
        """
        Extract CUB data by directly downloading the PDF from the button URL.
        
        Args:
            month: Reference month (1-12)
            year: Reference year
        
        Returns:
            CUBData object with extracted value
        """
        month_name = month_name_pt(month)
        logger.info(f"Extracting CUB data for {month_name}/{year}")
        
        # Setup paths
        raw_path = get_data_path("raw")
        pdf_filename = f"cub_mg_{year}_{month:02d}.pdf"
        pdf_path = raw_path / pdf_filename
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)
                self._dismiss_cookies(page)
                
                # 1. Expand Year Logic
                month_pattern = re.compile(month_name, re.IGNORECASE)
                month_btn = page.get_by_text(month_pattern).first
                
                if not month_btn.is_visible():
                    logger.info(f"Month hidden, clicking 'SELECIONAR' for {year}...")
                    
                    year_text = page.get_by_text(str(year), exact=True).first
                    if year_text.count() == 0:
                        raise ValueError(f"Year text {year} not found")

                    select_btn = year_text.locator("xpath=..").locator("text=SELECIONAR")
                    if select_btn.count() == 0:
                        select_btn = year_text.locator("xpath=../..").locator("text=SELECIONAR")
                    
                    if select_btn.count() == 0 or not select_btn.is_visible():
                        logger.warning("Specific Select button not found. Using fallback...")
                        select_btn = page.locator("text=SELECIONAR").first
                    
                    if select_btn.count() == 0:
                        raise ValueError("No SELECIONAR button found")
                        
                    select_btn.click()
                    page.wait_for_timeout(2000)
                
                if not month_btn.is_visible():
                    raise ValueError(f"Month {month_name} not found or not visible after expansion")
                
                # 2. Extract PDF URL
                # Case 1: The element itself is an anchor with href
                pdf_url = month_btn.get_attribute("href")
                
                if not pdf_url:
                    # Case 2: The element is inside an anchor (e.g. span inside a)
                    pdf_url = month_btn.locator("xpath=..").get_attribute("href")
                
                if not pdf_url:
                    # Backup: Dump inner HTML to log
                    logger.warning("No 'href' found on month button or parent.")
                    logger.warning(month_btn.evaluate("el => el.outerHTML"))
                    raise ValueError("Could not extract PDF URL from month button")
                
                logger.info(f"Found PDF URL: {pdf_url}")
                
                # 3. Direct Download
                logger.info("Downloading PDF directly via API...")
                response = page.request.get(pdf_url)
                
                if response.status != 200:
                    raise ValueError(f"Download failed with status code {response.status}")
                
                # Save body to file
                with open(pdf_path, "wb") as f:
                    f.write(response.body())
                    
                logger.info(f"PDF saved successfully to: {pdf_path}")
                
            finally:
                browser.close()
        
        # Step C: Parse the PDF (Offline)
        valor = self._parse_pdf(pdf_path)
        logger.info(f"Extracted R8-N value: {valor}")
        
        # Create CUBValor object
        valor_obj = CUBValor(
            projeto="R8-N",
            valor=valor,
            unidade="R$/m²"
        )
        
        return CUBData(
            estado="MG",
            mes_referencia=month,
            ano_referencia=year,
            data_extracao=datetime.now(),
            valores=[valor_obj]
        )
    
    def _parse_pdf(self, pdf_path: Path) -> float:
        """
        Parse the PDF to extract R8-N CUB value using 2D column cropping.
        Identical robust logic to PR scraper.
        """
        logger.info(f"Parsing PDF with Geometrically Validated Crop: {pdf_path}")
        
        with pdfplumber.open(str(pdf_path)) as pdf:
            page = pdf.pages[0]
            words = page.extract_words(keep_blank_chars=True)
            
            # 1. Locate Header Geometry (BAIXO, NORMAL, ALTO)
            header_normal = None
            header_baixo = None
            header_alto = None
            
            for word in words:
                text = word['text'].upper()
                if word['top'] > page.height / 2: continue
                
                if "NORMAL" in text and not header_normal:
                    header_normal = word
                elif ("BAIXO" in text or "BAΙΧΟ" in text) and not header_baixo:
                    header_baixo = word
                elif "ALTO" in text and not header_alto:
                    header_alto = word
            
            if not header_normal:
                return self._find_r8n_fallback(page)
            
            # 2. Calculate Crop Box
            norm_x0 = float(header_normal['x0'])
            norm_x1 = float(header_normal['x1'])
            y0 = float(header_normal['top'])
            y1 = y0 + 400
            
            # X Bounds: Isolate NORMAL
            if header_baixo and float(header_baixo['x1']) < norm_x0:
                x0 = (float(header_baixo['x1']) + norm_x0) / 2
            else:
                x0 = norm_x0 - 80
            
            if header_alto and float(header_alto['x0']) > norm_x1:
                x1 = (norm_x1 + float(header_alto['x0'])) / 2
            else:
                x1 = norm_x1 + 80
            
            # Safety Clamps
            x0 = max(0.0, x0)
            x1 = min(float(page.width), x1)
            
            # Sanity Check
            if x0 >= x1:
                x0 = max(0.0, norm_x0 - 100)
                x1 = min(float(page.width), norm_x1 + 100)
            
            logger.info(f"Crop Box: x={x0:.1f}-{x1:.1f}, y={y0:.1f}-{y1:.1f}")
            
            # 3. Crop and Extract
            crop = page.crop((x0, y0, x1, y1))
            text = crop.extract_text(layout=True)
            
            if not text:
                return self._find_r8n_fallback(page)
            
            # 4. Regex Lookahead
            for line in text.split('\n'):
                if re.search(r'\bR[-\s]?8\b', line, re.IGNORECASE):
                    # Value AFTER R-8
                    match = re.search(r'R-?8.*?(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                    if match:
                        val_str = match.group(1)
                        return self._parse_brl_currency(val_str)
            
            return self._find_r8n_fallback(page)
    
    def _find_r8n_fallback(self, page) -> float:
        """Fallback position-based extraction."""
        logger.info("Using fallback extraction strategy...")
        text = page.extract_text(layout=True)
        if not text:
            raise ValueError("Could not extract text from PDF")
            
        for line in text.split('\n'):
            if re.search(r'\bR[-\s]?8\b', line, re.IGNORECASE):
                match = re.search(r'R-?8.*?(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                if match:
                    return self._parse_brl_currency(match.group(1))
                
                matches = re.findall(r'(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                if len(matches) >= 2:
                    return self._parse_brl_currency(matches[1]) # Normal
                elif len(matches) == 1:
                    return self._parse_brl_currency(matches[0])
                    
        raise ValueError("Could not extract R8-N value from PDF")
    
    def _dismiss_cookies(self, page) -> None:
        """Dismiss cookie consent banner."""
        try:
            for txt in ["Aceitar", "Ok", "Entendi"]:
                btn = page.get_by_text(txt, exact=False)
                if btn.count() > 0:
                    btn.first.click(timeout=1000)
                    break
        except:
            pass
    
    @staticmethod
    def _parse_brl_currency(value_str: str) -> float:
        if not value_str: raise ValueError("Empty currency")
        cleaned = value_str.replace("R$", "").strip().replace(".", "").replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            raise ValueError(f"Could not parse currency: '{value_str}'")


if __name__ == "__main__":
    scraper = ScraperMG(headless=False)
    available = scraper.check_availability(12, 2025)
    print(f"Available: {available}")
    if available:
        data = scraper.extract(12, 2025)
        print(f"Extracted: {data}")

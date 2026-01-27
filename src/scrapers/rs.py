"""
CUB Data Pipeline - Rio Grande do Sul (RS) Scraper

Concrete implementation of BaseScraper for extracting CUB data
from Sinduscon-RS website.

Target: https://sinduscon-rs.com.br/cub-rs/
Method: PDF Download + PDFPlumber text scanning

The page lists reports by Month/Year header.
Example Header: "CUB/m²/RS – Dezembro 2025 – Divulgado em 2.01.2026"
Target Link: "Preço e Custos da Construção – Composição" (or similar variations)
"""

import re
from datetime import datetime
from pathlib import Path

import pdfplumber
from playwright.sync_api import sync_playwright

from src.scrapers.base import BaseScraper
from src.core.models import CUBData, CUBValor
from src.utils.helpers import get_logger, get_data_path, month_name_pt

logger = get_logger(__name__)


class ScraperRS(BaseScraper):
    """
    Scraper for Rio Grande do Sul (RS) CUB data.
    
    Downloads PDF reports and extracts the "R-8-N" value by scanning text lines
    for that specific project code.
    Target: R-8-N (Residencial Multifamiliar - Padrão Normal)
    """
    
    def __init__(self, headless: bool = True):
        super().__init__(
            estado="RS",
            base_url="https://sinduscon-rs.com.br/cub-rs/"
        )
        self.headless = headless
    
    def check_availability(self, month: int, year: int) -> bool:
        """
        Check if CUB data is available for the specified month.
        """
        month_name = month_name_pt(month)
        logger.info(f"Checking availability for {month_name}/{year} (RS)")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            try:
                page.goto(self.base_url, timeout=60000, wait_until="domcontentloaded")
                
                # Regex matches "CUB...Dezembro...2025"
                # Matches: "CUB/m²/RS – Dezembro 2025"
                header_pattern = re.compile(rf"CUB.*{month_name}.*{year}", re.IGNORECASE)
                
                header = page.get_by_text(header_pattern).first
                if header.is_visible():
                    logger.info(f"Header for {month_name}/{year} found.")
                    return True
                
                return False
            except Exception as e:
                logger.error(f"Error checking availability: {e}")
                return False
            finally:
                browser.close()

    def extract(self, month: int, year: int) -> CUBData:
        """
        Extract CUB data by downloading and parsing the PDF.
        """
        month_name = month_name_pt(month)
        logger.info(f"Extracting CUB data for {month_name}/{year}")
        
        pdf_filename = f"cub_rs_{year}_{month:02d}.pdf"
        pdf_path = get_data_path("raw") / pdf_filename
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=60000, wait_until="domcontentloaded")
                
                # 1. Locate Header
                header_pattern = re.compile(rf"CUB.*{month_name}.*{year}", re.IGNORECASE)
                header = page.get_by_text(header_pattern).first
                
                if not header.is_visible():
                    raise ValueError(f"Header for {month_name}/{year} not found")
                
                logger.info(f"Found header: '{header.text_content().strip()}'")

                # 2. Locate Link Relative to Header
                logger.info("Locating target link relative to header...")
                
                # Primary target
                target_link = header.locator("xpath=following::a[contains(text(), 'Preço e Custos da Construção')][1]")
                
                if target_link.count() == 0:
                    target_link = header.locator("xpath=following::a[contains(text(), 'Composição')][1]")
                
                if target_link.count() == 0 or not target_link.is_visible():
                    # Last resort fallback: Look for "Preço e Custos"
                    target_link = header.locator("xpath=following::a[contains(text(), 'Preço') and contains(text(), 'Custos')][1]")

                if target_link.count() == 0 or not target_link.is_visible():
                    raise ValueError("Target download link not found relative to header")
                
                link_text = target_link.text_content()
                href = target_link.get_attribute("href")
                
                logger.info(f"Found match: '{link_text}' -> {href}")
                
                if not href:
                    raise ValueError("Link found but href attribute is empty")
                
                # 3. Download
                logger.info("Downloading PDF via API...")
                response = page.request.get(href)
                if response.status != 200:
                    raise ValueError(f"Download failed with status {response.status}")
                
                with open(pdf_path, "wb") as f:
                    f.write(response.body())
                logger.info(f"PDF saved to {pdf_path}")
                
            finally:
                browser.close()

        # 4. Parse PDF
        valores = self._parse_pdf(pdf_path)
        logger.info(f"Extracted R8-N value: {valores[0].valor if valores else 'None'}")
        
        if not valores:
             raise ValueError("Failed to extract any values from PDF")
             
        # Return CUBData object (using the first extracted value)
        return CUBData(
            estado="RS",
            mes_referencia=month,
            ano_referencia=year,
            data_extracao=datetime.now(),
            valores=valores
        )

    def _parse_pdf(self, pdf_path: Path) -> list[CUBValor]:
        """
        Parse PDF and extract R-8-N value using flexible regex.
        """
        valores = []
        logger.info(f"Parsing PDF: {pdf_path}")
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                # Iterate pages just in case, usually page 1
                for page in pdf.pages:
                    text = page.extract_text()
                    if not text:
                        continue

                    # Split simple newlines
                    lines = text.split('\n')
                    for line in lines:
                        # Clean line for checking
                        line_clean = line.strip()
                        
                        # Debug finding potential lines
                        if "R" in line_clean and "8" in line_clean and "N" in line_clean:
                            logger.debug(f"Scanning potential line: {line_clean}")
                            
                        # Flexible Regex: Allow spaces or dashes between R-8-N
                        # Matches: "R-8-N", "R 8-N", "R 8 - N", "R-8 N"
                        # Pattern: R (space/dash/optional) 8 (space/dash/optional) N
                        if re.search(r"R\s*[- ]?\s*8\s*[- ]?\s*N", line_clean, re.IGNORECASE):
                            logger.info(f"Found R-8-N line match: {line_clean}")
                            
                            # Extract currency value (1.234,56)
                            # Look for the last currency-like pattern in the line usually
                            # But specifically searching for the pattern to be safe
                            
                            match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2})', line_clean)
                            if match:
                                val_str = match.group(1)
                                valor = self._parse_brl_currency(val_str)
                                valores.append(CUBValor(projeto="R8-N", valor=valor, unidade="R$/m²"))
                                return valores

        except Exception as e:
            logger.error(f"Error parsing PDF: {e}")
            return []
            
        logger.warning("R-8-N value not found in PDF")
        return []

    def _dismiss_cookies(self, page) -> None:
        """Dismiss cookie consent."""
        try:
            btn = page.get_by_text("Aceitar", exact=False).first
            if btn.is_visible(): btn.click()
        except: pass
    
    @staticmethod
    def _parse_brl_currency(value_str: str) -> float:
        if not value_str: raise ValueError("Empty currency")
        cleaned = value_str.replace("R$", "").strip().replace(".", "").replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            raise ValueError(f"Could not parse currency: '{value_str}'")


if __name__ == "__main__":
    scraper = ScraperRS(headless=False)
    # Test
    if scraper.check_availability(12, 2025):
        try:
            data = scraper.extract(12, 2025)
            print(f"Extracted: {data}")
        except Exception as e:
            print(f"Extraction failed: {e}")

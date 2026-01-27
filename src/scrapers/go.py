
import re
import os
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import pdfplumber
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from src.scrapers.base import BaseScraper
from src.core.models import CUBData, CUBValor
from src.utils.helpers import get_logger, get_data_path, month_name_pt

logger = get_logger(__name__)

class ScraperGO(BaseScraper):
    """
    Scraper for Goiás (GO) CUB data.
    
    Target: R-16 (Residencial 16 pavimentos) from the **HIGH STANDARD (PADRÃO ALTO)** column.
    Note: Unlike other states, GO reports R-16 under the Alto column.
    """
    
    def __init__(self, headless: bool = True):
        super().__init__(
            estado="GO",
            base_url="https://www.sinduscongoias.com.br/index.php/cub-custo-unitario-basico"
        )
        self.headless = headless

    def check_availability(self, month: int, year: int) -> bool:
        """
        Check if CUB data is available for the specified month.
        Pattern: "Tabela - CUB de {Month_Name}/{Year}"
        """
        month_name = month_name_pt(month)
        logger.info(f"Checking availability for {month_name}/{year}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                
                # Pattern: "Tabela - CUB de Dezembro/2025"
                pattern_str = rf"Tabela\s*-\s*CUB\s*de\s*{month_name}/{year}"
                
                # Look for link/button with this text
                ref_element = page.get_by_text(re.compile(pattern_str, re.IGNORECASE)).first
                
                available = ref_element.count() > 0 and ref_element.is_visible()
                
                if available:
                    logger.info(f"Data available for {month_name}/{year}")
                else:
                    logger.warning(f"Data NOT available for {month_name}/{year}")
                
                return available
                
            except Exception as e:
                logger.error(f"Error checking availability: {e}")
                return False
            finally:
                browser.close()

    def extract(self, month: int, year: int) -> CUBData:
        """
        Extract CUB data (R-16 Normal) for the specified month/year.
        """
        month_name = month_name_pt(month)
        logger.info(f"Extracting CUB data for {month_name}/{year}")
        
        raw_path = get_data_path("raw")
        pdf_filename = f"cub_go_{year}_{month:02d}.pdf"
        pdf_path = raw_path / pdf_filename
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                
                # Step A: Locate Link
                pattern_str = rf"Tabela\s*-\s*CUB\s*de\s*{month_name}/{year}"
                link_locator = page.get_by_text(re.compile(pattern_str, re.IGNORECASE)).first
                
                if not link_locator.count():
                    raise ValueError(f"Link not found for {month_name}/{year}")
                
                # Extract HREF
                # First check if the located element is an <a> tag
                href = None
                tag_name = link_locator.evaluate("el => el.tagName")
                
                if tag_name == "A":
                    href = link_locator.get_attribute("href")
                else:
                    # Check ancestor
                    parent_a = link_locator.locator("xpath=ancestor::a[1]")
                    if parent_a.count() > 0:
                        href = parent_a.first.get_attribute("href")
                
                if not href:
                    raise ValueError(f"Could not extract href from element text: '{pattern_str}'")
                
                # Resolve relative URLs
                if not href.startswith("http"):
                    href = urljoin(self.base_url, href)
                
                # Step B: Direct Download
                logger.info(f"Downloading from: {href}")
                response = page.request.get(href)
                
                if response.status != 200:
                    raise ValueError(f"Download failed with status {response.status}")
                
                with open(pdf_path, "wb") as f:
                    f.write(response.body())
                
                logger.info(f"PDF saved to: {pdf_path}")
                
            except Exception as e:
                logger.error(f"Download error: {e}")
                raise
            finally:
                browser.close()
        
        # Step C: Parse PDF
        valor = self._parse_pdf(pdf_path)
        logger.info(f"Extracted R-16 ALTO value: {valor}")
        
        valor_obj = CUBValor(
            projeto="R-16",
            valor=valor,
            unidade="R$/m²"
        )
        
        return CUBData(
            estado="GO",
            mes_referencia=month,
            ano_referencia=year,
            data_extracao=datetime.now(),
            valores=[valor_obj]
        )

    def _parse_pdf(self, pdf_path: Path) -> float:
        """
        Parse the PDF using Column Isolation Strategy (Crop).
        Target: PADRÃO ALTO (High Standard) -> R-16.
        
        Note: In GO, R-16 is reported under the Alto column, not Normal.
        """
        logger.info(f"Parsing PDF for R-16 HIGH STANDARD (ALTO): {pdf_path}")
        
        with pdfplumber.open(str(pdf_path)) as pdf:
            page = pdf.pages[0]
            words = page.extract_words(keep_blank_chars=True)
            
            # 1. Locate Headers
            header_normal = None
            header_alto = None
            
            for word in words:
                text = word['text'].upper()
                if word['top'] > page.height / 2:
                    continue
                
                if "NORMAL" in text and not header_normal:
                    header_normal = word
                    logger.debug(f"Found NORMAL header at x={word['x0']:.1f}-{word['x1']:.1f}")
                elif "ALTO" in text and not header_alto:
                    header_alto = word
                    logger.info(f"Found ALTO header at x={word['x0']:.1f}-{word['x1']:.1f}")
            
            if not header_alto:
                # Fallback: If 'ALTO' header not found, guess position based on page width
                # Usually High column is the last 1/3 of the page
                logger.warning("'ALTO' header not explicitly found. Using fallback geometry.")
                x0 = page.width * 0.65
                x1 = page.width
                y0 = page.height * 0.15
            else:
                # 2. Calculate Crop Box for ALTO Column
                # x0 should be between Normal and Alto to exclude Normal values
                if header_normal:
                    x0 = (float(header_normal['x1']) + float(header_alto['x0'])) / 2
                else:
                    x0 = float(header_alto['x0']) - 50
                
                x1 = float(page.width)
                y0 = float(header_alto['top'])
            
            y1 = y0 + 600  # Sufficient height
            
            # Safety checks
            x0 = max(0.0, x0)
            x1 = min(float(page.width), x1)
            y0 = max(0.0, y0)
            y1 = min(float(page.height), y1)
            
            logger.info(f"Cropping High Column: x={x0:.1f}-{x1:.1f}, y={y0:.1f}-{y1:.1f}")
            
            # 3. Crop & Extract
            crop = page.crop((x0, y0, x1, y1))
            text = crop.extract_text(layout=True)
            
            if not text:
                raise ValueError("No text in High column crop")
            
            # 4. Find R-16 Value
            # Regex to find R-16 followed by value
            for line in text.split('\n'):
                # Match R-16 (flexible)
                if re.search(r'\bR[-\s]?16\b', line, re.IGNORECASE):
                    logger.info(f"Found R-16 line: {line.strip()}")
                    match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                    if match:
                        val_str = match.group(1)
                        logger.info(f"Extracted R-16 value: {val_str}")
                        return self._parse_brl_currency(val_str)
            
            # Fallback scan if line-by-line misses (sometimes regex helps on full text)
            match = re.search(r'R-?16.*?(\d{1,3}(?:\.\d{3})*,\d{2})', text, re.DOTALL)
            if match:
                val_str = match.group(1)
                logger.info(f"Fallback extracted R-16 value: {val_str}")
                return self._parse_brl_currency(val_str)
            
            logger.error("R-16 value not found in Alto column.")
            logger.debug(f"Crop Text Content:\n{text}")
            raise ValueError("R-16 High Standard value missing")

    @staticmethod
    def _parse_brl_currency(value_str: str) -> float:
        """Parse '1.234,56' to 1234.56"""
        clean = value_str.replace("R$", "").strip()
        clean = clean.replace(".", "").replace(",", ".")
        return float(clean)

if __name__ == "__main__":
    # Test block for direct execution
    scraper = ScraperGO(headless=False) # Set headless=False to see the browser
    month = 12
    year = 2025
    
    if scraper.check_availability(month, year):
        try:
            data = scraper.extract(month, year)
            print("\n" + "="*50)
            print(f"SUCCESS! Extracted Data for GO {month}/{year}:")
            print(f"Value: {data.valores[0].valor}")
            print("="*50 + "\n")
        except Exception as e:
            print(f"EXTRACTION FAILED: {e}")
    else:
        print(f"Data not available for {month}/{year}")
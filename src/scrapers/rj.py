"""
CUB Data Pipeline - Rio de Janeiro (RJ) Scraper

Concrete implementation of BaseScraper for extracting CUB data
from Sinduscon-Rio website.

Target: https://www.sinduscon-rio.com.br/wp/servicos/custo-unitario-basico/
Method: Click accordion "Custo Unitário Básico (CUB)" -> PDF Download + PDFPlumber extraction
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


class ScraperRJ(BaseScraper):
    """
    Scraper for Rio de Janeiro (RJ) CUB data.
    
    Target: R-8 (Residencial 8 pavimentos) from the PADRÃO NORMAL column.
    
    Navigation:
    - The page has an accordion/button labeled "Custo Unitário Básico (CUB)"
    - Clicking it reveals (or is) the download link for the latest CUB PDF
    """
    
    def __init__(self, headless: bool = True):
        """Initialize the RJ scraper with parent attributes."""
        super().__init__(
            estado="RJ",
            base_url="https://www.sinduscon-rio.com.br/wp/servicos/custo-unitario-basico/"
        )
        self.headless = headless
    
    def check_availability(self, month: int, year: int) -> bool:
        """
        Check if CUB data is available.
        
        Since the RJ website shows the latest report via a button/accordion,
        we check if the button "Custo Unitário Básico (CUB)" is visible.
        
        Args:
            month: Reference month (1-12)
            year: Reference year
        
        Returns:
            True if the CUB button is visible, False otherwise
        """
        month_name = month_name_pt(month)
        logger.info(f"Checking availability for RJ - {month_name}/{year}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                
                # Look for the accordion/button with CUB text
                cub_button = page.get_by_text(
                    re.compile(r"Custo\s+Unitário\s+Básico\s*\(CUB\)", re.IGNORECASE)
                ).first
                
                available = cub_button.count() > 0 and cub_button.is_visible()
                
                if available:
                    logger.info(f"CUB button found - data likely available for {month_name}/{year}")
                else:
                    logger.warning(f"CUB button NOT found on page")
                
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
        Extract CUB data (R-8 Normal) for the specified month/year.
        
        Steps:
        1. Find the "Custo Unitário Básico (CUB)" element
        2. Extract the PDF download link (href)
        3. Download the PDF
        4. Parse using Column Isolation strategy
        
        Args:
            month: Reference month (1-12)
            year: Reference year
        
        Returns:
            CUBData object with extracted R-8 value
        """
        month_name = month_name_pt(month)
        logger.info(f"Extracting CUB data for RJ - {month_name}/{year}")
        
        # Setup paths
        raw_path = get_data_path("raw")
        pdf_filename = f"cub_rj_{year}_{month:02d}.pdf"
        pdf_path = raw_path / pdf_filename
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                
                # Wait a bit for dynamic content
                page.wait_for_timeout(2000)
                
                # Step A: Locate the CUB element
                logger.info("Locating 'Custo Unitário Básico (CUB)' element...")
                
                cub_element = page.get_by_text(
                    re.compile(r"Custo\s+Unitário\s+Básico\s*\(CUB\)", re.IGNORECASE)
                ).first
                
                if not cub_element.count():
                    raise ValueError("Could not find 'Custo Unitário Básico (CUB)' element")
                
                # Try to extract href - check if element is <a> or find nearby link
                href = None
                
                # Check if the element itself is a link
                tag_name = cub_element.evaluate("el => el.tagName")
                
                if tag_name == "A":
                    href = cub_element.get_attribute("href")
                    logger.info(f"Element is <a> tag, href: {href}")
                else:
                    # Try to find link in parent/ancestor
                    parent_a = cub_element.locator("xpath=ancestor::a[1]")
                    if parent_a.count() > 0:
                        href = parent_a.first.get_attribute("href")
                        logger.info(f"Found ancestor <a> tag, href: {href}")
                    else:
                        # Try child link
                        child_a = cub_element.locator("a").first
                        if child_a.count() > 0:
                            href = child_a.get_attribute("href")
                            logger.info(f"Found child <a> tag, href: {href}")
                        else:
                            # Try clicking to expand accordion and find link
                            logger.info("No direct link found, trying to click accordion...")
                            cub_element.click()
                            page.wait_for_timeout(1000)
                            
                            # Look for PDF links that appeared
                            pdf_links = page.locator("a[href*='.pdf']")
                            if pdf_links.count() > 0:
                                href = pdf_links.first.get_attribute("href")
                                logger.info(f"Found PDF link after accordion click: {href}")
                            else:
                                # Try to find any download link nearby
                                download_link = page.locator("a[href*='download'], a[href*='cub'], a[href*='CUB']").first
                                if download_link.count() > 0:
                                    href = download_link.get_attribute("href")
                                    logger.info(f"Found download/CUB link: {href}")
                
                if not href:
                    raise ValueError("Could not extract PDF download link from page")
                
                # Resolve relative URLs
                if not href.startswith("http"):
                    href = urljoin(self.base_url, href)
                
                logger.info(f"Final download URL: {href}")
                
                # Step B: Direct Download
                logger.info(f"Downloading PDF from: {href}")
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
        
        # Step C: Parse the PDF
        valor = self._parse_pdf(pdf_path)
        logger.info(f"Extracted R-8 Normal value: {valor}")
        
        # Create CUBValor object
        valor_obj = CUBValor(
            projeto="R-8",
            valor=valor,
            unidade="R$/m²"
        )
        
        return CUBData(
            estado="RJ",
            mes_referencia=month,
            ano_referencia=year,
            data_extracao=datetime.now(),
            valores=[valor_obj]
        )
    
    def _parse_pdf(self, pdf_path: Path) -> float:
        """
        Parse the PDF using Column Isolation Strategy (Crop).
        Target: PADRÃO NORMAL -> R-8.
        
        Strategy:
        1. Find X-coordinates of BAIXO, NORMAL, ALTO column headers
        2. Calculate horizontal bounds to isolate NORMAL column
        3. Crop the specific NORMAL column box
        4. Extract R-8 value from clean cropped text
        
        Args:
            pdf_path: Path to the downloaded PDF
        
        Returns:
            Float value for R-8 Normal
        """
        logger.info(f"Parsing PDF with Column Isolation: {pdf_path}")
        
        with pdfplumber.open(str(pdf_path)) as pdf:
            page = pdf.pages[0]
            words = page.extract_words(keep_blank_chars=True)
            
            # 1. Locate Header Geometry (BAIXO, NORMAL, ALTO)
            header_normal = None
            header_baixo = None
            header_alto = None
            
            # Scan for column headers (usually in top half of page)
            for word in words:
                text = word['text'].upper()
                
                # Skip footer content
                if word['top'] > page.height / 2:
                    continue
                
                if "NORMAL" in text and not header_normal:
                    header_normal = word
                    logger.info(f"Found NORMAL header at x={word['x0']:.1f}-{word['x1']:.1f}, y={word['top']:.1f}")
                elif ("BAIXO" in text or "BAΙΧΟ" in text) and not header_baixo:
                    header_baixo = word
                    logger.debug(f"Found BAIXO header at x={word['x0']:.1f}-{word['x1']:.1f}")
                elif "ALTO" in text and not header_alto:
                    header_alto = word
                    logger.debug(f"Found ALTO header at x={word['x0']:.1f}-{word['x1']:.1f}")
            
            if not header_normal:
                logger.warning("Could not find 'NORMAL' header. Trying fallback...")
                return self._find_r8_fallback(page)
            
            # 2. Calculate Crop Box with Geometry Validation
            norm_x0 = float(header_normal['x0'])
            norm_x1 = float(header_normal['x1'])
            
            # Y bounds: Start from NORMAL header, extend down for table rows
            y0 = float(header_normal['top'])
            y1 = y0 + 500  # Enough height for residential table rows
            
            # X bounds: Isolate the NORMAL column with validation
            
            # Left bound (x0): Validate BAIXO is actually to the left of NORMAL
            if header_baixo and float(header_baixo['x1']) < norm_x0:
                # Valid neighbor to the left
                x0 = (float(header_baixo['x1']) + norm_x0) / 2
                logger.debug(f"Using BAIXO midpoint for x0: {x0:.1f}")
            else:
                # Invalid or missing neighbor - use fixed margin
                if header_baixo:
                    logger.warning(f"Invalid BAIXO header position. Using fixed margin.")
                x0 = norm_x0 - 80  # Space to capture "R-8" label
            
            # Right bound (x1): Validate ALTO is actually to the right of NORMAL
            if header_alto and float(header_alto['x0']) > norm_x1:
                # Valid neighbor to the right
                x1 = (norm_x1 + float(header_alto['x0'])) / 2
                logger.debug(f"Using ALTO midpoint for x1: {x1:.1f}")
            else:
                # Invalid or missing neighbor - use fixed margin
                if header_alto:
                    logger.warning(f"Invalid ALTO header position. Using fixed margin.")
                x1 = norm_x1 + 80  # Space to capture value
            
            # Safety clamps
            x0 = max(0.0, x0)
            x1 = min(float(page.width), x1)
            y0 = max(0.0, y0)
            y1 = min(float(page.height), y1)
            
            # Final sanity check: ensure x0 < x1
            if x0 >= x1:
                logger.warning(f"Crop bounds invalid (x0={x0:.1f} >= x1={x1:.1f}). Resetting to wide column.")
                x0 = max(0.0, norm_x0 - 100)
                x1 = min(float(page.width), norm_x1 + 100)
            
            logger.info(f"Cropping Normal Column: x={x0:.1f}-{x1:.1f}, y={y0:.1f}-{y1:.1f}")
            
            # 3. Crop the specific column
            crop = page.crop((x0, y0, x1, y1))
            text = crop.extract_text(layout=True)
            
            if not text:
                logger.warning("No text in column crop. Trying fallback...")
                return self._find_r8_fallback(page)
            
            logger.debug(f"Cropped text preview: {text[:300]}...")
            
            # 4. Find R-8 and its Value
            for line in text.split('\n'):
                # Check for R-8 identifier
                if re.search(r'\bR[-\s]?8\b', line, re.IGNORECASE):
                    logger.info(f"Found R-8 line: '{line.strip()}'")
                    
                    # Pattern matches "R-8" or "R8" followed by any chars, then the currency value
                    match = re.search(r'R-?8.*?(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                    
                    if match:
                        val_str = match.group(1)
                        logger.info(f"Extracted R-8 value: {val_str}")
                        return self._parse_brl_currency(val_str)
                    else:
                        # Try just finding any currency value on the line
                        match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                        if match:
                            val_str = match.group(1)
                            logger.info(f"Extracted value from R-8 line: {val_str}")
                            return self._parse_brl_currency(val_str)
            
            # If R-8 not found in crop, try fallback
            logger.warning("R-8 not found in column crop. Using fallback...")
            return self._find_r8_fallback(page)
    
    def _find_r8_fallback(self, page) -> float:
        """
        Fallback: Find R-8 value using position-based extraction.
        
        Looks for R-8 lines and tries to identify the NORMAL column value
        based on position (usually the middle value in a 3-column layout).
        
        Args:
            page: PDFPlumber page object
        
        Returns:
            Float value or raises ValueError
        """
        logger.info("Using fallback extraction strategy...")
        
        text = page.extract_text(layout=True)
        if not text:
            raise ValueError("Could not extract text from PDF")
        
        lines = text.split('\n')
        
        for line in lines:
            # Look for R-8 pattern
            if re.search(r'\bR[-\s]?8\b', line, re.IGNORECASE):
                logger.info(f"Fallback: Found R-8 line: '{line.strip()[:100]}'")
                
                # First try: Value directly after R-8 label
                match = re.search(r'R-?8.*?(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                if match:
                    val_str = match.group(1)
                    valor = self._parse_brl_currency(val_str)
                    logger.info(f"Fallback extracted value after R-8: {valor}")
                    return valor
                
                # Second try: Position-based (index 1 = NORMAL column)
                matches = re.findall(r'(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                
                if matches:
                    # In a 3-column layout (BAIXO, NORMAL, ALTO):
                    # - Index 0 = BAIXO
                    # - Index 1 = NORMAL (what we want)
                    # - Index 2 = ALTO
                    if len(matches) >= 2:
                        val_str = matches[1]
                        valor = self._parse_brl_currency(val_str)
                        logger.info(f"Fallback extracted NORMAL value (index 1): {valor}")
                        return valor
                    elif len(matches) == 1:
                        val_str = matches[0]
                        valor = self._parse_brl_currency(val_str)
                        logger.info(f"Fallback extracted single value: {valor}")
                        return valor
        
        raise ValueError("Could not extract R-8 value from PDF")
    
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


# Test block for direct execution
if __name__ == "__main__":
    scraper = ScraperRJ(headless=False)
    
    # Test with December 2025
    month, year = 12, 2025
    
    print(f"\n{'='*50}")
    print(f"Testing RJ Scraper for {month}/{year}")
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

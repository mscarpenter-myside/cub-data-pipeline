"""
CUB Data Pipeline - Paraná (PR) Scraper

Concrete implementation of BaseScraper for extracting CUB data
from Sinduscon-PR website.

Target: https://sindusconpr.com.br/tabela-completa-370-p
Method: PDF Download + PDFPlumber extraction

Unlike SC/SP (HTML scraping), PR requires downloading a PDF file
and parsing its table structure to extract CUB values.
"""

import re
from datetime import datetime
from pathlib import Path

import pdfplumber
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from src.scrapers.base import BaseScraper
from src.core.models import CUBData, CUBValor
from src.utils.helpers import get_logger, get_data_path, month_name_pt

logger = get_logger(__name__)


class ScraperPR(BaseScraper):
    """
    Scraper for Paraná (PR) CUB data.
    
    Downloads PDF files from the Sinduscon-PR website and parses
    the table to extract CUB values.
    Target: R8-N (Residencial Multifamiliar - Padrão Normal)
    """
    
    def __init__(self, headless: bool = True):
        """Initialize the PR scraper with parent attributes."""
        super().__init__(
            estado="PR",
            base_url="https://sindusconpr.com.br/tabela-completa-370-p"
        )
        self.headless = headless
    
    def check_availability(self, month: int, year: int) -> bool:
        """
        Check if CUB data is available for the specified month.
        
        Looks for the download link with pattern:
        "{Month_Name} {Year} - Sem desoneração"
        
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
                
                # Look for the download link pattern
                # Pattern: "Dezembro 2025 - Sem desoneração"
                ref_pattern = re.compile(
                    rf"{month_name}\s+{year}\s*[-–]\s*Sem\s+desoneração",
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
        Extract CUB data by downloading and parsing the PDF.
        
        Steps:
        1. Find the download link for the specified month
        2. Download the PDF file
        3. Parse the PDF table to extract R8-N value
        
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
        pdf_filename = f"cub_pr_{year}_{month:02d}.pdf"
        pdf_path = raw_path / pdf_filename
        error_screenshot_path = raw_path.parent / "error_screenshot_pr.png"
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("networkidle")
                
                # Dismiss cookie banner
                self._dismiss_cookies(page)
                
                # Wait for content to load
                page.wait_for_timeout(2000)
                
                # Step A: Find the download link
                logger.info("Locating download link...")
                
                # Pattern: "Dezembro 2025 - Sem desoneração"
                ref_pattern = re.compile(
                    rf"{month_name}\s+{year}\s*[-–]\s*Sem\s+desoneração",
                    re.IGNORECASE
                )
                
                file_label = page.get_by_text(ref_pattern).first
                
                if not file_label.is_visible():
                    page.screenshot(path=str(error_screenshot_path))
                    raise ValueError(f"Could not find download link for {month_name}/{year}")
                
                logger.info(f"Found: {file_label.text_content()}")
                
                # Find the DOWNLOAD button in the same row
                # Strategy 1: Look for nearby download link/button
                row = file_label.locator("xpath=ancestor::*[contains(@class,'row') or contains(@class,'item') or self::tr or self::li][1]")
                
                if row.count() == 0:
                    # Fallback: parent container
                    row = file_label.locator("xpath=../..")
                
                # Find download button/link
                download_btn = None
                
                # Try various button patterns
                download_patterns = [
                    row.get_by_text("DOWNLOAD", exact=False),
                    row.get_by_text("Download", exact=False),
                    row.get_by_text("Baixar", exact=False),
                    row.locator("a[href*='.pdf']"),
                    row.locator("a[download]"),
                    row.locator("button"),
                ]
                
                for pattern in download_patterns:
                    if pattern.count() > 0:
                        download_btn = pattern.first
                        break
                
                # If not found in row, try clicking the label itself
                if not download_btn or download_btn.count() == 0:
                    logger.info("Download button not found in row, trying label click...")
                    download_btn = file_label
                
                # Step B: Download the file
                logger.info("Triggering download...")
                
                with page.expect_download(timeout=60000) as download_info:
                    download_btn.click()
                
                download = download_info.value
                download.save_as(str(pdf_path))
                logger.info(f"PDF saved to: {pdf_path}")
                
            except Exception as e:
                logger.error(f"Download error: {e}")
                try:
                    page.screenshot(path=str(error_screenshot_path))
                except:
                    pass
                raise
            finally:
                browser.close()
        
        # Step C: Parse the PDF
        valor = self._parse_pdf(pdf_path)
        logger.info(f"Extracted R8-N value: {valor}")
        
        # Create CUBValor object
        valor_obj = CUBValor(
            projeto="R8-N",
            valor=valor,
            unidade="R$/m²"
        )
        
        return CUBData(
            estado="PR",
            mes_referencia=month,
            ano_referencia=year,
            data_extracao=datetime.now(),
            valores=[valor_obj]
        )
    
    def _parse_pdf(self, pdf_path: Path) -> float:
        """
        Parse the PDF to extract R8-N CUB value using 2D column cropping.
        
        Strategy:
        1. Find X-coordinates of BAIXO, NORMAL, ALTO column headers
        2. Calculate horizontal bounds to isolate NORMAL column
        3. Find Y-coordinate of NORMAL header for vertical start
        4. Crop the specific NORMAL column box
        5. Extract R-8 value from clean cropped text
        
        Args:
            pdf_path: Path to the downloaded PDF
        
        Returns:
            Float value for R8-N
        """
        logger.info(f"Parsing PDF with Geometrically Validated Crop: {pdf_path}")
        
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
                    logger.info(f"Found BAIXO header at x={word['x0']:.1f}-{word['x1']:.1f}")
                elif "ALTO" in text and not header_alto:
                    header_alto = word
                    logger.info(f"Found ALTO header at x={word['x0']:.1f}-{word['x1']:.1f}")
            
            if not header_normal:
                logger.warning("Could not find 'NORMAL' header. Trying fallback...")
                return self._find_r8n_fallback(page)
            
            # 2. Calculate Crop Box with Geometry Validation
            norm_x0 = float(header_normal['x0'])
            norm_x1 = float(header_normal['x1'])
            
            # Y bounds: Start from NORMAL header, extend down for table rows
            y0 = float(header_normal['top'])
            y1 = y0 + 400  # Enough height for residential table rows
            
            # X bounds: Isolate the NORMAL column with validation
            
            # Left bound (x0): Validate BAIXO is actually to the left of NORMAL
            if header_baixo and float(header_baixo['x1']) < norm_x0:
                # Valid neighbor to the left
                x0 = (float(header_baixo['x1']) + norm_x0) / 2
                logger.info(f"Using BAIXO midpoint for x0: {x0:.1f}")
            else:
                # Invalid or missing neighbor - use fixed margin
                if header_baixo:
                    logger.warning(f"Invalid BAIXO header (x1={header_baixo['x1']:.1f} >= Normal x0={norm_x0:.1f}). Using fixed margin.")
                x0 = norm_x0 - 80  # Space to capture "R-8" label
            
            # Right bound (x1): Validate ALTO is actually to the right of NORMAL
            if header_alto and float(header_alto['x0']) > norm_x1:
                # Valid neighbor to the right
                x1 = (norm_x1 + float(header_alto['x0'])) / 2
                logger.info(f"Using ALTO midpoint for x1: {x1:.1f}")
            else:
                # Invalid or missing neighbor - use fixed margin
                if header_alto:
                    logger.warning(f"Invalid ALTO header (x0={header_alto['x0']:.1f} <= Normal x1={norm_x1:.1f}). Using fixed margin.")
                x1 = norm_x1 + 80  # Space to capture value
            
            # Safety clamps
            x0 = max(0.0, x0)
            x1 = min(float(page.width), x1)
            y0 = max(0.0, y0)
            y1 = min(float(page.height), y1)
            
            # Final sanity check: ensure x0 < x1
            if x0 >= x1:
                logger.warning(f"Crop bounds invalid (x0={x0:.1f} >= x1={x1:.1f}). Resetting to wide column around Normal.")
                x0 = max(0.0, norm_x0 - 100)
                x1 = min(float(page.width), norm_x1 + 100)
            
            logger.info(f"Final Crop Box: x={x0:.1f} to {x1:.1f}, y={y0:.1f} to {y1:.1f}")
            
            # 3. Crop the specific column
            crop = page.crop((x0, y0, x1, y1))
            text = crop.extract_text(layout=True)
            
            if not text:
                logger.warning("No text in column crop. Trying fallback...")
                return self._find_r8n_fallback(page)
            
            logger.info(f"Cropped text preview: {text[:200]}...")
            
            # 4. Find R-8 and its Value (Lookahead pattern)
            for line in text.split('\n'):
                # Check for R-8 identifier
                if re.search(r'\bR[-\s]?8\b', line, re.IGNORECASE):
                    logger.info(f"Analyzing line: '{line.strip()}'")
                    
                    # NEW STRATEGY: Enforce value AFTER label
                    # Pattern matches "R-8" or "R8" followed by any chars, then the currency value
                    # This ignores any artifacts (like 189,56) that appear BEFORE R-8
                    match = re.search(r'R-?8.*?(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                    
                    if match:
                        val_str = match.group(1)
                        logger.info(f"Targeted value match (after R-8): {val_str}")
                        
                        valor = self._parse_brl_currency(val_str)
                        logger.info(f"Extracted R8-N value: {valor}")
                        return valor
                    else:
                        logger.warning(f"R-8 found but no value followed it in line: {line}")
            
            # If R-8 not found in crop, try fallback
            logger.warning("R-8 not found in column crop. Dumping crop text for debug:")
            logger.warning(text)
            return self._find_r8n_fallback(page)
    
    def _find_r8n_fallback(self, page) -> float:
        """
        Fallback: Find R8-N value using position-based extraction.
        
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
        
        raise ValueError("Could not extract R8-N value from PDF")
    
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
    scraper = ScraperPR(headless=False)
    
    # Test availability
    available = scraper.check_availability(12, 2025)
    print(f"Available: {available}")
    
    if available:
        # Test extraction
        data = scraper.extract(12, 2025)
        print(f"Extracted: {data}")

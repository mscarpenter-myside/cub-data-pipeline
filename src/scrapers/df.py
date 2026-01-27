"""
CUB Data Pipeline - Distrito Federal (DF) Scraper

Concrete implementation of BaseScraper for extracting CUB data
from CBIC System (cub.org.br).

Target: http://cub.org.br/cub-m2-estadual/DF/
Method: Form cascade interaction -> PDF Download -> PDFPlumber extraction
Target Value: R-8 from PADRÃO NORMAL (middle column)

Note: This uses HTTP (insecure) which requires special browser args.
Same system as PE scraper.
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


class ScraperDF(BaseScraper):
    """
    Scraper for Distrito Federal (DF) CUB data.
    
    Target: R-8 (Residencial 8 pavimentos) from the PADRÃO NORMAL column.
    
    System: CBIC (cub.org.br) - HTTP/Insecure
    Requires security bypass args for downloads.
    Same logic as PE scraper with DF-specific dropdown values.
    
    Navigation:
    - Cascade of dropdowns: Estado -> Sinduscon -> Relatório -> Ano -> Mês -> Desoneração -> Variação
    - Click "Gerar Relatório em PDF"
    """
    
    # Security bypass args for HTTP downloads
    BROWSER_ARGS = [
        "--safebrowsing-disable-download-protection",
        "--unsafely-treat-insecure-origin-as-secure=http://cub.org.br",
        "--disable-web-security",
        "--allow-running-insecure-content",
    ]
    
    def __init__(self, headless: bool = True):
        """Initialize the DF scraper with parent attributes."""
        super().__init__(
            estado="DF",
            base_url="http://cub.org.br/cub-m2-estadual/DF/"
        )
        self.headless = headless
    
    def check_availability(self, month: int, year: int) -> bool:
        """
        Check if CUB data is available for the specified month/year.
        
        Validates that the dropdowns contain the requested year and month.
        
        Args:
            month: Reference month (1-12)
            year: Reference year
        
        Returns:
            True if the year and month are available in the dropdowns
        """
        month_name = month_name_pt(month)  # Capitalized
        logger.info(f"Checking availability for DF - {month_name}/{year}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.headless,
                args=self.BROWSER_ARGS
            )
            page = browser.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)
                
                # Check if year dropdown has the requested year
                year_dropdown = page.locator("select#ano")
                if year_dropdown.count() == 0:
                    year_dropdown = page.locator("select[name*='ano' i]")
                
                if year_dropdown.count() > 0:
                    year_options = year_dropdown.locator("option").all_text_contents()
                    year_available = str(year) in year_options
                    
                    if not year_available:
                        logger.warning(f"Year {year} not available in dropdown")
                        return False
                else:
                    logger.warning("Year dropdown not found")
                    return False
                
                # Check if month dropdown has the requested month
                month_dropdown = page.locator("select#mes")
                if month_dropdown.count() == 0:
                    month_dropdown = page.locator("select[name*='mes' i]")
                
                if month_dropdown.count() > 0:
                    month_options = month_dropdown.locator("option").all_text_contents()
                    # Check for month name (case insensitive)
                    month_available = any(
                        month_name.lower() in opt.lower() 
                        for opt in month_options
                    )
                    
                    if not month_available:
                        logger.warning(f"Month {month_name} not available in dropdown")
                        return False
                else:
                    logger.warning("Month dropdown not found")
                    return False
                
                logger.info(f"Data available for {month_name}/{year}")
                return True
                
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
        1. Setup browser with security bypass
        2. Navigate through cascade of dropdowns (DF specific)
        3. Click "Gerar Relatório em PDF"
        4. Download and parse PDF
        
        Args:
            month: Reference month (1-12)
            year: Reference year
        
        Returns:
            CUBData object with extracted R-8 value
        """
        month_name = month_name_pt(month)  # Capitalized
        logger.info(f"Extracting CUB data for DF - {month_name}/{year}")
        
        # Setup paths
        raw_path = get_data_path("raw")
        pdf_filename = f"cub_df_{year}_{month:02d}.pdf"
        pdf_path = raw_path / pdf_filename
        
        with sync_playwright() as p:
            # Step A: Browser Setup with Security Bypass
            browser = p.chromium.launch(
                headless=self.headless,
                args=self.BROWSER_ARGS
            )
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)
                
                # Step B: Form Interaction (The DF Cascade)
                logger.info("Filling form cascade for DF...")
                
                # Estado: DF - Distrito Federal (usually pre-selected from URL)
                estado_dropdown = page.locator("select#estado, select[name*='estado' i]").first
                if estado_dropdown.count() > 0:
                    try:
                        page.select_option(
                            "select#estado, select[name*='estado' i]",
                            label="DF - Distrito Federal"
                        )
                        page.wait_for_timeout(500)
                    except:
                        logger.debug("Estado might be pre-selected")
                
                # Sinduscon: Sinduscon-DF
                sinduscon_selectors = [
                    "select#sinduscon",
                    "select[name*='sinduscon' i]",
                    "select#entidade",
                    "select[name*='entidade' i]",
                ]
                for selector in sinduscon_selectors:
                    try:
                        dropdown = page.locator(selector)
                        if dropdown.count() > 0:
                            page.select_option(selector, label="Sinduscon-DF")
                            page.wait_for_timeout(500)
                            logger.debug("Selected Sinduscon-DF")
                            break
                    except:
                        continue
                
                # Relatório: Tabela do CUB/m² Valores em R$/m²
                relatorio_selectors = [
                    "select#relatorio",
                    "select[name*='relatorio' i]",
                    "select#tipo",
                    "select[name*='tipo' i]",
                ]
                for selector in relatorio_selectors:
                    try:
                        dropdown = page.locator(selector)
                        if dropdown.count() > 0:
                            labels_to_try = [
                                "Tabela do CUB/m² Valores em R$/m²",
                                "Tabela do CUB/m²",
                                "CUB/m² Valores",
                            ]
                            for label in labels_to_try:
                                try:
                                    page.select_option(selector, label=label)
                                    page.wait_for_timeout(500)
                                    logger.debug(f"Selected report: {label}")
                                    break
                                except:
                                    continue
                            break
                    except:
                        continue
                
                # Ano: Select year
                ano_selectors = ["select#ano", "select[name*='ano' i]"]
                for selector in ano_selectors:
                    try:
                        dropdown = page.locator(selector)
                        if dropdown.count() > 0:
                            page.select_option(selector, label=str(year))
                            page.wait_for_timeout(500)
                            logger.debug(f"Selected year: {year}")
                            break
                    except:
                        continue
                
                # Mês: Select month (Capitalized, e.g., "Dezembro")
                mes_selectors = ["select#mes", "select[name*='mes' i]"]
                for selector in mes_selectors:
                    try:
                        dropdown = page.locator(selector)
                        if dropdown.count() > 0:
                            page.select_option(selector, label=month_name)
                            page.wait_for_timeout(500)
                            logger.debug(f"Selected month: {month_name}")
                            break
                    except:
                        continue
                
                # Desoneração: Sem desoneração da mão de obra
                desoneracao_selectors = [
                    "select#cub",
                    "select[name*='cub' i]",
                    "select#desoneracao",
                    "select[name*='desoneracao' i]",
                ]
                for selector in desoneracao_selectors:
                    try:
                        dropdown = page.locator(selector)
                        if dropdown.count() > 0:
                            labels_to_try = [
                                "Sem desoneração da mão de obra",
                                "Sem desoneração",
                                "Normal",
                            ]
                            for label in labels_to_try:
                                try:
                                    page.select_option(selector, label=label)
                                    page.wait_for_timeout(500)
                                    logger.debug(f"Selected desoneracao: {label}")
                                    break
                                except:
                                    continue
                            break
                    except:
                        continue
                
                # Variação: Sem Variação Percentual
                variacao_selectors = [
                    "select#variacao",
                    "select[name*='variacao' i]",
                ]
                for selector in variacao_selectors:
                    try:
                        dropdown = page.locator(selector)
                        if dropdown.count() > 0:
                            labels_to_try = [
                                "Sem Variação Percentual",
                                "Sem variação",
                                "Sem Variação",
                            ]
                            for label in labels_to_try:
                                try:
                                    page.select_option(selector, label=label)
                                    page.wait_for_timeout(500)
                                    logger.debug(f"Selected variation: {label}")
                                    break
                                except:
                                    continue
                            break
                    except:
                        continue
                
                logger.info("Form filled. Generating PDF report...")
                
                # Step C: Download
                pdf_button_selectors = [
                    "button:has-text('Gerar Relatório em PDF')",
                    "input[value*='Gerar Relatório']",
                    "a:has-text('Gerar Relatório')",
                    "button:has-text('PDF')",
                    "input[type='submit'][value*='PDF']",
                    "#gerarPdf",
                    ".btn-pdf",
                ]
                
                pdf_button = None
                for selector in pdf_button_selectors:
                    try:
                        btn = page.locator(selector)
                        if btn.count() > 0:
                            pdf_button = btn.first
                            break
                    except:
                        continue
                
                if not pdf_button:
                    raise ValueError("Could not find 'Gerar Relatório em PDF' button")
                
                # Wait for download event
                with page.expect_download(timeout=60000) as download_info:
                    pdf_button.click()
                
                download = download_info.value
                download.save_as(str(pdf_path))
                logger.info(f"PDF saved to: {pdf_path}")
                
            except Exception as e:
                logger.error(f"Download error: {e}")
                raise
            finally:
                browser.close()
        
        # Step D: Parse the PDF
        valor = self._parse_pdf(pdf_path)
        logger.info(f"Extracted R-8 Normal value: {valor}")
        
        # Create CUBValor object
        valor_obj = CUBValor(
            projeto="R-8",
            valor=valor,
            unidade="R$/m²"
        )
        
        return CUBData(
            estado="DF",
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
            
            y0 = float(header_normal['top'])
            y1 = y0 + 500
            
            # Left bound (x0)
            if header_baixo and float(header_baixo['x1']) < norm_x0:
                x0 = (float(header_baixo['x1']) + norm_x0) / 2
                logger.debug(f"Using BAIXO midpoint for x0: {x0:.1f}")
            else:
                x0 = norm_x0 - 80
            
            # Right bound (x1)
            if header_alto and float(header_alto['x0']) > norm_x1:
                x1 = (norm_x1 + float(header_alto['x0'])) / 2
                logger.debug(f"Using ALTO midpoint for x1: {x1:.1f}")
            else:
                x1 = norm_x1 + 80
            
            # Safety clamps
            x0 = max(0.0, x0)
            x1 = min(float(page.width), x1)
            y0 = max(0.0, y0)
            y1 = min(float(page.height), y1)
            
            if x0 >= x1:
                logger.warning("Crop bounds invalid. Resetting to wide column.")
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
                if re.search(r'\bR[-\s]?8\b', line, re.IGNORECASE):
                    logger.info(f"Found R-8 line: '{line.strip()}'")
                    
                    match = re.search(r'R-?8.*?(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                    
                    if match:
                        val_str = match.group(1)
                        logger.info(f"Extracted R-8 value: {val_str}")
                        return self._parse_brl_currency(val_str)
                    else:
                        match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                        if match:
                            val_str = match.group(1)
                            logger.info(f"Extracted value from R-8 line: {val_str}")
                            return self._parse_brl_currency(val_str)
            
            logger.warning("R-8 not found in column crop. Using fallback...")
            return self._find_r8_fallback(page)
    
    def _find_r8_fallback(self, page) -> float:
        """
        Fallback: Find R-8 value using position-based extraction.
        
        Args:
            page: PDFPlumber page object
        
        Returns:
            Float value or raises ValueError
        """
        logger.info("Using fallback extraction strategy...")
        
        text = page.extract_text(layout=True)
        if not text:
            raise ValueError("Could not extract text from PDF")
        
        for line in text.split('\n'):
            if re.search(r'\bR[-\s]?8\b', line, re.IGNORECASE):
                logger.info(f"Fallback: Found R-8 line: '{line.strip()[:100]}'")
                
                match = re.search(r'R-?8.*?(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                if match:
                    val_str = match.group(1)
                    return self._parse_brl_currency(val_str)
                
                matches = re.findall(r'(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                
                if len(matches) >= 2:
                    val_str = matches[1]  # NORMAL is index 1
                    return self._parse_brl_currency(val_str)
                elif len(matches) == 1:
                    val_str = matches[0]
                    return self._parse_brl_currency(val_str)
        
        raise ValueError("Could not extract R-8 value from PDF")
    
    @staticmethod
    def _parse_brl_currency(value_str: str) -> float:
        """Parse Brazilian currency string to float."""
        if not value_str:
            raise ValueError("Empty currency string")
        
        cleaned = value_str.replace("R$", "").strip()
        cleaned = cleaned.replace(".", "").replace(",", ".")
        
        try:
            return float(cleaned)
        except ValueError:
            raise ValueError(f"Could not parse currency: '{value_str}'")


# Test block for direct execution
if __name__ == "__main__":
    scraper = ScraperDF(headless=False)
    
    month, year = 12, 2025
    
    print(f"\n{'='*50}")
    print(f"Testing DF Scraper for {month}/{year}")
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

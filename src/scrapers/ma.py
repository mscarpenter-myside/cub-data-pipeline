"""
CUB Data Pipeline - Maranhão (MA) Scraper

Concrete implementation of BaseScraper for extracting CUB data
from CBIC System (cub.org.br).

Target: http://cub.org.br/cub-m2-estadual/MA/
Method: Form cascade interaction -> PDF Download -> PDFPlumber extraction
Target Value: R-8 from PADRÃO NORMAL (middle column)

Note: This uses HTTP (insecure) which requires special browser args.
Same system as PE/DF/MT scrapers.
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


class ScraperMA(BaseScraper):
    """
    Scraper for Maranhão (MA) CUB data.
    
    Target: R-8 (Residencial 8 pavimentos) from the PADRÃO NORMAL column.
    
    System: CBIC (cub.org.br) - HTTP/Insecure
    Requires security bypass args for downloads.
    Same logic as PE/DF/MT scrapers with MA-specific dropdown values.
    """
    
    BROWSER_ARGS = [
        "--safebrowsing-disable-download-protection",
        "--unsafely-treat-insecure-origin-as-secure=http://cub.org.br",
        "--disable-web-security",
        "--allow-running-insecure-content",
    ]
    
    def __init__(self, headless: bool = True):
        """Initialize the MA scraper with parent attributes."""
        super().__init__(
            estado="MA",
            base_url="http://cub.org.br/cub-m2-estadual/MA/"
        )
        self.headless = headless
    
    def check_availability(self, month: int, year: int) -> bool:
        """Check if CUB data is available for the specified month/year."""
        month_name = month_name_pt(month)
        logger.info(f"Checking availability for MA - {month_name}/{year}")
        
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
                
                # Check year dropdown
                year_dropdown = page.locator("select#ano, select[name*='ano' i]").first
                if year_dropdown.count() > 0:
                    year_options = year_dropdown.locator("option").all_text_contents()
                    if str(year) not in year_options:
                        logger.warning(f"Year {year} not available")
                        return False
                else:
                    logger.warning("Year dropdown not found")
                    return False
                
                # Check month dropdown
                month_dropdown = page.locator("select#mes, select[name*='mes' i]").first
                if month_dropdown.count() > 0:
                    month_options = month_dropdown.locator("option").all_text_contents()
                    if not any(month_name.lower() in opt.lower() for opt in month_options):
                        logger.warning(f"Month {month_name} not available")
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
        """Extract CUB data (R-8 Normal) for the specified month/year."""
        month_name = month_name_pt(month)
        logger.info(f"Extracting CUB data for MA - {month_name}/{year}")
        
        raw_path = get_data_path("raw")
        pdf_filename = f"cub_ma_{year}_{month:02d}.pdf"
        pdf_path = raw_path / pdf_filename
        
        with sync_playwright() as p:
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
                
                logger.info("Filling form cascade for MA...")
                
                # Estado: MA - Maranhão
                try:
                    page.select_option(
                        "select#estado, select[name*='estado' i]",
                        label="MA - Maranhão"
                    )
                    page.wait_for_timeout(500)
                except:
                    logger.debug("Estado might be pre-selected")
                
                # Sinduscon: Sinduscon-MA
                for selector in ["select#sinduscon", "select[name*='sinduscon' i]", 
                                 "select#entidade", "select[name*='entidade' i]"]:
                    try:
                        if page.locator(selector).count() > 0:
                            try:
                                page.select_option(selector, label="Sinduscon-MA")
                            except:
                                page.select_option(selector, index=1)
                            page.wait_for_timeout(500)
                            break
                    except:
                        continue
                
                # Relatório
                for selector in ["select#relatorio", "select[name*='relatorio' i]",
                                 "select#tipo", "select[name*='tipo' i]"]:
                    try:
                        if page.locator(selector).count() > 0:
                            for label in ["Tabela do CUB/m² Valores em R$/m²", 
                                         "Tabela do CUB/m²", "CUB/m² Valores"]:
                                try:
                                    page.select_option(selector, label=label)
                                    page.wait_for_timeout(500)
                                    break
                                except:
                                    continue
                            break
                    except:
                        continue
                
                # Ano
                for selector in ["select#ano", "select[name*='ano' i]"]:
                    try:
                        if page.locator(selector).count() > 0:
                            page.select_option(selector, label=str(year))
                            page.wait_for_timeout(500)
                            break
                    except:
                        continue
                
                # Mês
                for selector in ["select#mes", "select[name*='mes' i]"]:
                    try:
                        if page.locator(selector).count() > 0:
                            page.select_option(selector, label=month_name)
                            page.wait_for_timeout(500)
                            break
                    except:
                        continue
                
                # Desoneração
                for selector in ["select#cub", "select[name*='cub' i]",
                                 "select#desoneracao", "select[name*='desoneracao' i]"]:
                    try:
                        if page.locator(selector).count() > 0:
                            for label in ["Sem desoneração da mão de obra", 
                                         "Sem desoneração", "Normal"]:
                                try:
                                    page.select_option(selector, label=label)
                                    page.wait_for_timeout(500)
                                    break
                                except:
                                    continue
                            break
                    except:
                        continue
                
                # Variação
                for selector in ["select#variacao", "select[name*='variacao' i]"]:
                    try:
                        if page.locator(selector).count() > 0:
                            for label in ["Sem Variação Percentual", "Sem variação", "Sem Variação"]:
                                try:
                                    page.select_option(selector, label=label)
                                    page.wait_for_timeout(500)
                                    break
                                except:
                                    continue
                            break
                    except:
                        continue
                
                logger.info("Form filled. Generating PDF report...")
                
                # Click PDF button
                pdf_button = None
                for selector in ["button:has-text('Gerar Relatório em PDF')",
                                 "input[value*='Gerar Relatório']",
                                 "a:has-text('Gerar Relatório')",
                                 "button:has-text('PDF')"]:
                    try:
                        btn = page.locator(selector)
                        if btn.count() > 0:
                            pdf_button = btn.first
                            break
                    except:
                        continue
                
                if not pdf_button:
                    raise ValueError("Could not find PDF button")
                
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
        
        valor = self._parse_pdf(pdf_path)
        logger.info(f"Extracted R-8 Normal value: {valor}")
        
        valor_obj = CUBValor(projeto="R-8", valor=valor, unidade="R$/m²")
        
        return CUBData(
            estado="MA",
            mes_referencia=month,
            ano_referencia=year,
            data_extracao=datetime.now(),
            valores=[valor_obj]
        )
    
    def _parse_pdf(self, pdf_path: Path) -> float:
        """Parse PDF using Column Isolation Strategy for R-8 Normal."""
        logger.info(f"Parsing PDF: {pdf_path}")
        
        with pdfplumber.open(str(pdf_path)) as pdf:
            page = pdf.pages[0]
            words = page.extract_words(keep_blank_chars=True)
            
            header_normal = header_baixo = header_alto = None
            
            for word in words:
                text = word['text'].upper()
                if word['top'] > page.height / 2:
                    continue
                
                if "NORMAL" in text and not header_normal:
                    header_normal = word
                    logger.info(f"Found NORMAL header at x={word['x0']:.1f}")
                elif "BAIXO" in text and not header_baixo:
                    header_baixo = word
                elif "ALTO" in text and not header_alto:
                    header_alto = word
            
            if not header_normal:
                return self._find_r8_fallback(page)
            
            norm_x0, norm_x1 = float(header_normal['x0']), float(header_normal['x1'])
            y0, y1 = float(header_normal['top']), float(header_normal['top']) + 500
            
            x0 = (float(header_baixo['x1']) + norm_x0) / 2 if header_baixo and float(header_baixo['x1']) < norm_x0 else norm_x0 - 80
            x1 = (norm_x1 + float(header_alto['x0'])) / 2 if header_alto and float(header_alto['x0']) > norm_x1 else norm_x1 + 80
            
            x0, x1 = max(0.0, x0), min(float(page.width), x1)
            y0, y1 = max(0.0, y0), min(float(page.height), y1)
            
            if x0 >= x1:
                x0, x1 = max(0.0, norm_x0 - 100), min(float(page.width), norm_x1 + 100)
            
            crop = page.crop((x0, y0, x1, y1))
            text = crop.extract_text(layout=True)
            
            if not text:
                return self._find_r8_fallback(page)
            
            for line in text.split('\n'):
                if re.search(r'\bR[-\s]?8\b', line, re.IGNORECASE):
                    match = re.search(r'R-?8.*?(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                    if match:
                        return self._parse_brl_currency(match.group(1))
                    match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                    if match:
                        return self._parse_brl_currency(match.group(1))
            
            return self._find_r8_fallback(page)
    
    def _find_r8_fallback(self, page) -> float:
        """Fallback extraction strategy."""
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
                    return self._parse_brl_currency(matches[1])
                elif matches:
                    return self._parse_brl_currency(matches[0])
        
        raise ValueError("Could not extract R-8 value from PDF")
    
    @staticmethod
    def _parse_brl_currency(value_str: str) -> float:
        """Parse Brazilian currency string to float."""
        cleaned = value_str.replace("R$", "").strip().replace(".", "").replace(",", ".")
        return float(cleaned)


if __name__ == "__main__":
    scraper = ScraperMA(headless=False)
    month, year = 12, 2025
    
    print(f"\n{'='*50}")
    print(f"Testing MA Scraper for {month}/{year}")
    print(f"{'='*50}\n")
    
    if scraper.check_availability(month, year):
        try:
            data = scraper.extract(month, year)
            print(f"SUCCESS! Value: {data.valores[0].valor}")
        except Exception as e:
            print(f"FAILED: {e}")
    else:
        print(f"Data not available")

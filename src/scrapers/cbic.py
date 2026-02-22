"""
Base scraper for CBIC System (cub.org.br) states.
Shared logic for form navigation and PDF parsing.
Used by DF, MA, MT, PE, etc.
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


class CBICScraper(BaseScraper):
    """
    Abstract Base Scraper for states using the CBIC system.
    
    Subclasses must set:
    - self.estado (e.g., "DF")
    - self.sinduscon_label (e.g., "Sinduscon-DF")
    """
    
    BROWSER_ARGS = [
        "--safebrowsing-disable-download-protection",
        "--unsafely-treat-insecure-origin-as-secure=http://cub.org.br",
        "--disable-web-security",
        "--allow-running-insecure-content",
    ]
    
    def __init__(self, estado: str, sinduscon_label: str, headless: bool = True):
        super().__init__(
            estado=estado,
            base_url=f"http://cub.org.br/cub-m2-estadual/{estado}/"
        )
        self.sinduscon_label = sinduscon_label
        self.headless = headless

    def _fill_cbic_form(self, page, month_name: str, year: int):
        """Fills the CBIC cascade form consistently."""
        # Estado
        estado_dropdown = page.locator("select#estado, select[name*='estado' i]").first
        if estado_dropdown.count() > 0:
            try:
                page.select_option("select#estado, select[name*='estado' i]", label=f"{self.estado} - ")
                page.wait_for_timeout(500)
            except:
                pass
        
        # Sinduscon
        for selector in ["select#sinduscon", "select[name*='sinduscon' i]", "select#entidade", "select[name*='entidade' i]"]:
            try:
                if page.locator(selector).count() > 0:
                    try:
                        page.select_option(selector, label=self.sinduscon_label)
                    except:
                        page.select_option(selector, index=1)
                    page.wait_for_timeout(500)
                    break
            except:
                continue
        
        # Relatório
        for selector in ["select#relatorio", "select[name*='relatorio' i]", "select#tipo", "select[name*='tipo' i]"]:
            try:
                if page.locator(selector).count() > 0:
                    for label in ["Tabela do CUB/m² Valores em R$/m²", "Tabela do CUB/m²", "CUB/m² Valores"]:
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
        for selector in ["select#cub", "select[name*='cub' i]", "select#desoneracao", "select[name*='desoneracao' i]"]:
            try:
                if page.locator(selector).count() > 0:
                    for label in ["Sem desoneração da mão de obra", "Sem desoneração", "Normal"]:
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

    def _get_pdf_button(self, page):
        for selector in [
            "button:has-text('Gerar Relatório em PDF')",
            "input[value*='Gerar Relatório']",
            "a:has-text('Gerar Relatório')",
            "button:has-text('PDF')",
            "input[type='submit'][value*='PDF']",
            "#gerarPdf",
            ".btn-pdf",
        ]:
            try:
                btn = page.locator(selector)
                if btn.count() > 0:
                    return btn.first
            except:
                continue
        return None

    def check_availability(self, month: int, year: int) -> bool:
        month_name = month_name_pt(month)
        logger.info(f"Checking availability for {self.estado} - {month_name}/{year}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless, args=self.BROWSER_ARGS)
            page = browser.new_page()
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)
                
                self._fill_cbic_form(page, month_name, year)
                
                pdf_button = self._get_pdf_button(page)
                if not pdf_button:
                    logger.warning("Could not find PDF button to check availability")
                    return False
                
                logger.info(f"Submitting form to check availability for {self.estado}...")
                with page.expect_response(lambda r: r.url == self.base_url and r.request.method == "POST", timeout=15000) as response_info:
                    pdf_button.click()
                
                res = response_info.value
                is_pdf = "application/pdf" in res.headers.get("content-type", "").lower()
                
                if not is_pdf:
                    logger.warning(f"Data NOT available for {month_name}/{year} (Returned HTML)")
                else:
                    logger.info(f"Data available for {month_name}/{year}")
                
                return is_pdf
                
            except PlaywrightTimeout:
                # If timeout happens here, either network or it didn't respond
                logger.warning(f"Timeout checking availability for {self.estado}.")
                return False
            except Exception as e:
                logger.error(f"Error checking availability: {e}")
                return False
            finally:
                browser.close()

    def extract(self, month: int, year: int) -> CUBData:
        month_name = month_name_pt(month)
        logger.info(f"Extracting CUB data for {self.estado} - {month_name}/{year}")
        
        raw_path = get_data_path("raw")
        pdf_filename = f"cub_{self.estado.lower()}_{year}_{month:02d}.pdf"
        pdf_path = raw_path / pdf_filename
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless, args=self.BROWSER_ARGS)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            
            try:
                page.goto(self.base_url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2000)
                
                logger.info(f"Filling form cascade for {self.estado}...")
                self._fill_cbic_form(page, month_name, year)
                
                pdf_button = self._get_pdf_button(page)
                if not pdf_button:
                    raise ValueError("Could not find PDF button")
                
                logger.info("Form filled. Generating PDF report...")
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
            estado=self.estado,
            mes_referencia=month,
            ano_referencia=year,
            data_extracao=datetime.now(),
            valores=[valor_obj]
        )
    
    def _parse_pdf(self, pdf_path: Path) -> float:
        logger.info(f"Parsing PDF with Column Isolation: {pdf_path}")
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
                    logger.info(f"Found NORMAL header at x={word['x0']:.1f}-{word['x1']:.1f}")
                elif ("BAIXO" in text or "BAΙΧΟ" in text) and not header_baixo:
                    header_baixo = word
                elif "ALTO" in text and not header_alto:
                    header_alto = word
            
            if not header_normal:
                logger.warning("Could not find 'NORMAL' header. Trying fallback...")
                return self._find_r8_fallback(page)
            
            norm_x0, norm_x1 = float(header_normal['x0']), float(header_normal['x1'])
            y0, y1 = float(header_normal['top']), float(header_normal['top']) + 500
            
            x0 = (float(header_baixo['x1']) + norm_x0) / 2 if header_baixo and float(header_baixo['x1']) < norm_x0 else norm_x0 - 80
            x1 = (norm_x1 + float(header_alto['x0'])) / 2 if header_alto and float(header_alto['x0']) > norm_x1 else norm_x1 + 80
            
            x0, x1 = max(0.0, x0), min(float(page.width), x1)
            y0, y1 = max(0.0, y0), min(float(page.height), y1)
            
            if x0 >= x1:
                x0, x1 = max(0.0, norm_x0 - 100), min(float(page.width), norm_x1 + 100)
            
            logger.info(f"Cropping Normal Column: x={x0:.1f}-{x1:.1f}, y={y0:.1f}-{y1:.1f}")
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
        cleaned = value_str.replace("R$", "").strip().replace(".", "").replace(",", ".")
        return float(cleaned)

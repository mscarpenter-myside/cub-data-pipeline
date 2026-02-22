from src.scrapers.cbic import CBICScraper

class ScraperPE(CBICScraper):
    def __init__(self, headless: bool = True):
        super().__init__(estado="PE", sinduscon_label="Sinduscon-PE", headless=headless)

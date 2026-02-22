from src.scrapers.cbic import CBICScraper

class ScraperMA(CBICScraper):
    def __init__(self, headless: bool = True):
        super().__init__(estado="MA", sinduscon_label="Sinduscon-MA", headless=headless)

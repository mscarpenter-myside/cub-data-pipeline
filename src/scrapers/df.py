from src.scrapers.cbic import CBICScraper

class ScraperDF(CBICScraper):
    def __init__(self, headless: bool = True):
        super().__init__(estado="DF", sinduscon_label="Sinduscon-DF", headless=headless)

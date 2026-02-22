from src.scrapers.cbic import CBICScraper

class ScraperMT(CBICScraper):
    def __init__(self, headless: bool = True):
        super().__init__(estado="MT", sinduscon_label="Sinduscon-MT", headless=headless)

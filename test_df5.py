import sys
from playwright.sync_api import sync_playwright

BROWSER_ARGS = [
    "--safebrowsing-disable-download-protection",
    "--unsafely-treat-insecure-origin-as-secure=http://cub.org.br",
    "--disable-web-security",
    "--allow-running-insecure-content",
]

def test_df():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=BROWSER_ARGS)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto('http://cub.org.br/cub-m2-estadual/DF/')
        page.wait_for_timeout(2000)
        
        print("Selecting year 2026, month Fevereiro")
        page.select_option("select#ano", label="2026")
        page.select_option("select#mes", label="Fevereiro")
        page.wait_for_timeout(500)
        
        with page.expect_response("http://cub.org.br/cub-m2-estadual/DF/") as response_info:
            page.click("input[value='Gerar Relatório em PDF']")
        
        res = response_info.value
        print("Response status:", res.status)
        print("Content-Type:", res.headers.get("content-type"))
        
        # If it's HTML, let's see the text.
        text = res.text()
        if "Nenhum" in text or "encontrado" in text or "alert" in text:
            import re
            alerts = re.findall(r'<div[^>]*alert[^>]*>(.*?)</div>', text, re.DOTALL | re.IGNORECASE)
            print("Alerts:", alerts)
            
        browser.close()

if __name__ == "__main__":
    test_df()

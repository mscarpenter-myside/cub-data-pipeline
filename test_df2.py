import sys
from playwright.sync_api import sync_playwright

def test_df():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--disable-web-security'])
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto('http://cub.org.br/cub-m2-estadual/DF/')
        page.wait_for_timeout(2000)
        
        print("Selecting year 2026")
        page.select_option("select#ano", label="2026")
        page.wait_for_timeout(500)
        
        print("Selecting month Fevereiro")
        page.select_option("select#mes", label="Fevereiro")
        page.wait_for_timeout(500)
        
        print("Clicking")
        try:
            with page.expect_download(timeout=10000) as download_info:
                page.click("input[value='Gerar Relatório em PDF']")
            dl = download_info.value
            print("Download started:", dl.url)
        except Exception as e:
            print("Failed download:", e)
        
        browser.close()

if __name__ == "__main__":
    test_df()

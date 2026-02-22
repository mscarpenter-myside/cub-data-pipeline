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
        
        print("Clicking with expect_download")
        try:
            with page.expect_download(timeout=10000) as download_info:
                page.click("input[value='Gerar Relatório em PDF']")
            dl = download_info.value
            print("Download started:", dl.url)
            dl.save_as("test_df.pdf")
            print("SAVED!")
        except Exception as e:
            print("Failed download:", e)
        
        browser.close()

if __name__ == "__main__":
    test_df()

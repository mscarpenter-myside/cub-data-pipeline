import sys
from playwright.sync_api import sync_playwright

BROWSER_ARGS = [
    "--safebrowsing-disable-download-protection",
    "--unsafely-treat-insecure-origin-as-secure=http://cub.org.br",
    "--disable-web-security",
    "--allow-running-insecure-content",
]

def check(month_name, year):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=BROWSER_ARGS)
        page = browser.new_page()
        page.goto('http://cub.org.br/cub-m2-estadual/DF/')
        
        page.select_option("select#ano", label=str(year))
        page.select_option("select#mes", label=month_name)
        
        try:
            with page.expect_response(lambda r: r.url == "http://cub.org.br/cub-m2-estadual/DF/" and r.request.method == "POST", timeout=10000) as response_info:
                page.click("input[value='Gerar Relatório em PDF']")
            res = response_info.value
            
            is_pdf = "application/pdf" in res.headers.get("content-type", "")
            return is_pdf
        except Exception as e:
            print("Error:", e)
            return False
        finally:
            browser.close()

if __name__ == "__main__":
    print("Janeiro:", check("Janeiro", 2026))
    print("Fevereiro:", check("Fevereiro", 2026))

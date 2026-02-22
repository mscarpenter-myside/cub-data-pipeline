import sys
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=['--disable-web-security'])
    page = browser.new_page()
    page.goto('http://cub.org.br/cub-m2-estadual/DF/')
    page.wait_for_timeout(2000)
    
    # Fill form minimally (using defaults or just clicking submit)
    # The default might be valid enough to generate a report
    try:
        with page.expect_response(lambda r: "pdf" in r.headers.get("content-type", "").lower(), timeout=30000) as response_info:
            page.click("input[value='Gerar Relatório em PDF']")
            
        res = response_info.value
        print("Success! Status:", res.status)
        print("Headers:", res.headers)
        with open("test_df.pdf", "wb") as f:
            f.write(res.body())
        print("Saved to test_df.pdf")
    except Exception as e:
        print("Failed:", e)
    browser.close()

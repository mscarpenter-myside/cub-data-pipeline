import sys
from playwright.sync_api import sync_playwright
import re

def dump_pa():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto('https://institucional.sindusconpa.com.br/cub-site.php', timeout=30000)
        page.wait_for_timeout(2000)
        
        # Get all links and their text
        links = page.locator("a").evaluate_all("""
            elements => elements.map(e => ({
                href: e.href,
                text: e.innerText.trim(),
                className: e.className
            }))
        """)
        
        print("--- LINKS ---")
        for l in links:
            if "pdf" in l["href"].lower() or "cub" in l["href"].lower():
                print(f"HREF: {l['href']} | TEXT: {l['text']} | CLASS: {l['className']}")
                
        # Get all box-boletim elements if any
        boxes = page.locator(".box-boletim, .boletim, .card").all_inner_texts()
        print(f"\n--- BOXES COUNT: {len(boxes)} ---")
        for b in boxes[:3]:
            print(b.replace('\n', ' '))
            
        print("\n--- YEAR SELECT ---")
        selects = page.locator("select").all_inner_texts()
        for s in selects:
            print(s.replace('\n', ' '))
            
        browser.close()

if __name__ == "__main__":
    dump_pa()

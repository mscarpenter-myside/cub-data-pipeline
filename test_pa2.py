import sys
from playwright.sync_api import sync_playwright

def dump_pa():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto('https://www.sindusconpa.org.br/cub', timeout=30000)
        page.wait_for_timeout(5000)
        
        page.screenshot(path="pa_screenshot.png")
        print("Captured pa_screenshot.png")
        
        # Also print iframe count
        frames = page.frames
        print(f"Frames count: {len(frames)}")
        for f in frames:
            print("Frame:", f.url)
            
        print("HTML sample:", page.content()[:500])
        
        browser.close()

if __name__ == "__main__":
    dump_pa()

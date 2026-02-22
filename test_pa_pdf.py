import pdfplumber

def test_pa_pdf():
    path = "/home/mateus/cub-vb/data/raw/cub_pa_2026_01.pdf"
    with pdfplumber.open(path) as pdf:
        print(f"Total pages: {len(pdf.pages)}")
        
        for i, page in enumerate(pdf.pages):
            text = page.extract_text(layout=True)
            text_raw = page.extract_text()
            
            print(f"--- PAGE {i} ---")
            print(f"Has layout text? {bool(text)}")
            print(f"Has raw text? {bool(text_raw)}")
            
            if text:
                lines = text.split('\n')
                print(f"First 5 lines: {lines[:5]}")
                for line in lines:
                    if "R-8" in line or "R8" in line:
                        print(f"Found R-8 on page {i}: {line.strip()}")

if __name__ == "__main__":
    test_pa_pdf()

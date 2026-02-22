import pdfplumber

def test_pa_pdf():
    path = "/home/mateus/cub-vb/data/raw/cub_pa_2026_01.pdf"
    with pdfplumber.open(path) as pdf:
        
        for i, page in enumerate(pdf.pages):
            text = page.extract_text(layout=True)
            if not text:
                continue
                
            lines = text.split('\n')
            for j, line in enumerate(lines):
                if "R-8" in line or "R8" in line or "NORMAL" in line:
                    print(f"[Page {i}] Line {j}: {line.strip()}")

if __name__ == "__main__":
    test_pa_pdf()

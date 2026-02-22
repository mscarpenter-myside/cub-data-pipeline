import pdfplumber

def test_pa_pdf():
    path = "/home/mateus/cub-vb/data/raw/cub_pa_2026_01.pdf"
    with pdfplumber.open(path) as pdf:
        
        print("PAGE 4 FULL TEXT:")
        text = pdf.pages[4].extract_text(layout=True)
        print(text)

        print("\n\nPAGE 3 FULL TEXT:")
        text = pdf.pages[3].extract_text(layout=True)
        print(text)

if __name__ == "__main__":
    test_pa_pdf()

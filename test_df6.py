import requests
import re

def test_requests_cbic(estado, month, year):
    url = f"http://cub.org.br/cub-m2-estadual/{estado}/"
    session = requests.Session()
    
    # Get CSRF
    response = session.get(url)
    match = re.search(r'name="csrfmiddlewaretoken" value="([^"]+)"', response.text)
    if not match:
        print("CSRF not found")
        return
    csrf = match.group(1)
    
    data = {
        "csrfmiddlewaretoken": csrf,
        "uf": estado,
        "sinduscon": 5, # Need to know the sinduscon ID? 
    }
    # Wait, the form has many fields.
    # In my test_df html:
    # <select id="sinduscon" name="sinduscon">
    #   <option value="5">Sinduscon-DF</option>
    # <select name="relatorio">
    #   <option value="1">Tabela do CUB/m² Valores em R$/m²</option>
    # <select name="ano"> ...
    # <select name="mes"> ...
    # <select name="desoneracao"> <option value="1">Sem desoneração da mão de obra</option>
    # <select name="porcentagem"> <option value="1">Sem Variação Percentual</option>
    
    # Let's extract all options from the HTML
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    
    payload = {"csrfmiddlewaretoken": csrf, "uf": estado}
    
    # 1. Sinduscon
    sinduscon_select = soup.find('select', {'id': 'sinduscon'}) or soup.find('select', {'name': re.compile('sinduscon', re.I)})
    if sinduscon_select:
        for opt in sinduscon_select.find_all('option'):
            if estado in opt.text:
                payload[sinduscon_select.get('name')] = opt['value']
                break
                
    # 2. Relatorio
    relatorio_select = soup.find('select', {'id': 'relatorio'}) or soup.find('select', {'name': re.compile('relatorio|tipo', re.I)})
    if relatorio_select:
        for opt in relatorio_select.find_all('option'):
            if "Valores em R$/m²" in opt.text or "Valores" in opt.text:
                payload[relatorio_select.get('name')] = opt['value']
                break
                
    # 3. Ano
    ano_select = soup.find('select', {'id': 'ano'}) or soup.find('select', {'name': re.compile('ano', re.I)})
    if ano_select:
        for opt in ano_select.find_all('option'):
            if str(year) in opt.text:
                payload[ano_select.get('name')] = opt['value']
                break
                
    # 4. Mes
    mes_select = soup.find('select', {'id': 'mes'}) or soup.find('select', {'name': re.compile('mes', re.I)})
    if mes_select:
        for opt in mes_select.find_all('option'):
            if str(month).lower() in opt.text.lower():
                payload[mes_select.get('name')] = opt['value']
                break
                
    # 5. Desoneracao
    des_select = soup.find('select', {'id': 'desoneracao'}) or soup.find('select', {'id': 'cub'})
    if des_select:
        for opt in des_select.find_all('option'):
            if "Sem desoneração" in opt.text or "Normal" in opt.text:
                payload[des_select.get('name')] = opt['value']
                break
                
    # 6. Variacao
    var_select = soup.find('select', {'id': 'variacao'}) or soup.find('select', {'name': re.compile('variacao', re.I)})
    if var_select:
        for opt in var_select.find_all('option'):
            if "Sem" in opt.text:
                payload[var_select.get('name')] = opt['value']
                break
    
    print("Payload:", payload)
    
    res = session.post(url, data=payload)
    print("Status:", res.status_code)
    print("Content-Type:", res.headers.get("Content-Type"))
    
    if "application/pdf" in res.headers.get("Content-Type", ""):
        print("Success! Got PDF.")
    else:
        print("Failed. Not a PDF.")

print("Testing Valid (Janeiro)")
test_requests_cbic("DF", "Janeiro", 2026)
print("\nTesting Invalid (Fevereiro)")
test_requests_cbic("DF", "Fevereiro", 2026)


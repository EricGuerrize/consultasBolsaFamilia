import os
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("CHAVE_API_DADOS")

url = "https://api.portaldatransparencia.gov.br/api-de-dados/novo-bolsa-familia-sacado-beneficiario-por-municipio"
params = {"mesAno": "202401", "codigoIbge": "5107602", "pagina": 1}
headers = {"chave-api-dados": api_key, "Accept": "application/json"}

print(f"Requesting {url}...")
r = requests.get(url, params=params, headers=headers, timeout=30)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    data = r.json()
    if data:
        import json
        print(json.dumps(data[0], indent=2, ensure_ascii=False))
    else:
        print("Empty data.")
else:
    print(r.text)

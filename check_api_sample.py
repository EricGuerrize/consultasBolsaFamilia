import os
from bolsa_familia_client import BolsaFamiliaAPI
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("CHAVE_API_DADOS")

api = BolsaFamiliaAPI(api_key)
# Busca apenas 1 página de Rondonópolis (5107602)
regs = api.buscar_sacados_municipio("202401", "5107602")
if regs:
    print("--- SAMPLE RECORD ---")
    import json
    print(json.dumps(regs[0], indent=2, ensure_ascii=False))
else:
    print("No records found.")

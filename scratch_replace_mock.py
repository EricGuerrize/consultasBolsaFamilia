import csv
import json
import re

def format_cpf(cpf):
    cpf = str(cpf).zfill(11)
    return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"

results = []

admissoes = {
    "68972687120": "10/05/2020",
    "04005274137": "10/01/2022",
    "04077586151": "10/08/2024",
    "61555312187": "22/10/2024",
    "04809042197": "01/05/2024",
    "72161493191": "01/09/2024",
    "05305013178": "05/07/2024",
    "05463213135": "01/02/2024",
    "04165734101": "10/02/2024",
    "02171265108": "05/02/2024"
}

with open(r'C:\Users\ericguerrize\Desktop\bolsafamilia\consultasBolsaFamilia\resultados_bolsafamilia (1).csv', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        is_irr = row['Irregular'].strip().upper() == 'SIM'
        
        cpf = row['CPF'].strip().zfill(11)
        admissao = admissoes.get(cpf, "01/01/2020" if is_irr else "01/01/2025")
        
        results.append({
            'servidor': row['Servidor'],
            'cpf': format_cpf(row['CPF']),
            'matricula': row['Matrícula'],
            'nis': row['NIS'],
            'beneficiario': row['Beneficiário'],
            'municipio': row['Município'],
            'uf': row['UF'],
            'mes': row['Mês Ref.'],
            'data_saque': f"15/{row['Mês Ref.'][4:]}/2024",
            'valor': float(row['Valor']),
            'isIrregular': is_irr,
            'isMatch': True,
            'admissao': admissao,
            'orgao': "PREFEITURA MUNICIPAL DE RONDONÓPOLIS",
            'pagina': int(row['Página']) if row['Página'] else 1
        })

mock_str = "const MOCK_RESULTS = [\n"
for i, r in enumerate(results):
    mock_str += "  " + json.dumps(r, ensure_ascii=False) + (",\n" if i < len(results)-1 else "\n")
mock_str += "];"

app_path = r'C:\Users\ericguerrize\Desktop\bolsafamilia\consultasBolsaFamilia\frontend\src\App.jsx'

with open(app_path, 'r', encoding='utf-8') as f:
    content = f.read()

new_content = re.sub(r'const MOCK_RESULTS = \[[\s\S]*?\];', mock_str, content)

with open(app_path, 'w', encoding='utf-8') as f:
    f.write(new_content)

print("App.jsx updated.")

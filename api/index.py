import os
import re
import json
import time
import pandas as pd
import requests
from io import BytesIO, StringIO
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Bolsa Família x Servidores API")

# Configuração de CORS para o Frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
#  CONSTANTES E AUXILIARES
# ─────────────────────────────────────────────
API_BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
API_SACADO = f"{API_BASE}/novo-bolsa-familia-sacado-beneficiario-por-municipio"
API_CPF = f"{API_BASE}/novo-bolsa-familia-disponivel-por-cpf-ou-nis"
PAGE_SIZE = 15

class CrossRequest(BaseModel):
    m_ini: str
    m_fim: str
    ibge: Optional[str] = ""
    modo: str # "cpf" ou "municipio"
    api_key: Optional[str] = ""

class CrossJsonRequest(BaseModel):
    records: List[dict]
    m_ini: str
    m_fim: str
    modo: str
    ibge: Optional[str] = ""
    api_key: Optional[str] = ""
    col_cpf: Optional[str] = "cpf"
    col_nome: Optional[str] = "nome"

def normalizar_cpf(cpf) -> str:
    if pd.isna(cpf) or cpf is None: return ""
    s = re.sub(r"\D", "", str(cpf))
    return s.zfill(11) if len(s) <= 11 else s[:11]

def meio_cpf(cpf_raw) -> str:
    """Extrai os 6 dígitos do meio visíveis no formato xxx.YYY.ZZZ-00.
    CPF local normalizado (11 dígitos): posições 3-8.
    CPF mascarado da API (xxx.123.456-00): remove x e não-dígitos → pega os 6 primeiros dígitos restantes.
    """
    raw = str(cpf_raw or "")
    # CPF completo normalizado (11 dígitos)
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11:
        return digits[3:9]
    # Formato mascarado: remove 'x/X' e não-dígitos
    digits = re.sub(r"\D", "", re.sub(r"[xX]", "", raw))
    return digits[:6] if len(digits) >= 6 else ""

def primeiro_nome(nome) -> str:
    """Retorna o primeiro nome em maiúsculas, sem acentos."""
    if not nome:
        return ""
    return str(nome).strip().split()[0].upper()

def chave_cruzamento(cpf_raw, nome) -> str:
    """Chave composta: 6 dígitos do meio do CPF + primeiro nome."""
    meio = meio_cpf(cpf_raw)
    pnome = primeiro_nome(nome)
    if not meio or not pnome:
        return ""
    return f"{meio}|{pnome}"

# ─────────────────────────────────────────────
#  CLIENTE API (Reuso da lógica do app.py)
# ─────────────────────────────────────────────
class BolsaFamiliaAPI:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "chave-api-dados": api_key,
            "Accept": "application/json",
        })

    def buscar_sacados_municipio(self, mes_ano: str, codigo_ibge: str) -> list[dict]:
        resultados = []
        pagina = 1
        while True:
            params = {"mesAno": mes_ano, "codigoIbge": codigo_ibge, "pagina": pagina}
            r = self.session.get(API_SACADO, params=params, timeout=30)
            if r.status_code == 429:
                time.sleep(1); continue
            r.raise_for_status()
            data = r.json()
            if not data: break
            resultados.extend(data)
            if len(data) < PAGE_SIZE: break
            pagina += 1
        return resultados

    def buscar_por_cpf(self, cpf: str) -> list[dict]:
        resultados = []
        pagina = 1
        while True:
            params = {"cpf": cpf, "pagina": pagina}
            r = self.session.get(API_CPF, params=params, timeout=20)
            if r.status_code == 429:
                time.sleep(1); continue
            r.raise_for_status()
            data = r.json()
            if not data: break
            resultados.extend(data)
            if len(data) < PAGE_SIZE: break
            pagina += 1
        return resultados

# ─────────────────────────────────────────────
#  ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/api/health")
async def health_check():
    return {"status": "ok", "service": "Bolsa Família API"}

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        suffix = Path(file.filename).suffix.lower()
        if suffix == ".csv":
            df = pd.read_csv(StringIO(content.decode("utf-8-sig")), dtype=str)
        else:
            df = pd.read_excel(BytesIO(content), dtype=str)

        df.columns = [c.strip().lower() for c in df.columns]
        return {"columns": list(df.columns), "filename": file.filename, "total": len(df)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao ler arquivo: {str(e)}")

@app.post("/api/cross")
async def start_cross(request: Request):
    content_type = request.headers.get("content-type", "")

    real_api_key = None

    if "application/json" in content_type:
        body = await request.json()
        real_api_key = body.get("api_key") or os.getenv("CHAVE_API_DADOS", "")
        if not real_api_key:
            raise HTTPException(status_code=401, detail="Chave de API não fornecida")
        records = body.get("records", [])
        content = json.dumps(records).encode("utf-8")
        result = do_cross(
            content, "input.json",
            body["m_ini"], body["m_fim"], body["modo"],
            body.get("ibge", ""), real_api_key,
            body.get("col_cpf", "cpf"), body.get("col_nome", "nome")
        )
    else:
        form = await request.form()
        real_api_key = form.get("api_key") or os.getenv("CHAVE_API_DADOS", "")
        if not real_api_key:
            raise HTTPException(status_code=401, detail="Chave de API não fornecida")
        file = form.get("file")
        if not file:
            raise HTTPException(status_code=400, detail="Nenhum arquivo enviado")
        content = await file.read()
        result = do_cross(
            content, file.filename,
            form["m_ini"], form["m_fim"], form["modo"],
            form.get("ibge", ""), real_api_key,
            form.get("col_cpf", "cpf"), form.get("col_nome", "nome")
        )

    return result

# ─────────────────────────────────────────────
#  TASK WORKER (Lógica de cruzamento)
# ─────────────────────────────────────────────

def get_meses_list(start, end):
    s_y, s_m = int(start[:4]), int(start[4:])
    e_y, e_m = int(end[:4]), int(end[4:])
    meses = []
    curr_y, curr_m = s_y, s_m
    while (curr_y < e_y) or (curr_y == e_y and curr_m <= e_m):
        meses.append(f"{curr_y}{curr_m:02d}")
        curr_m += 1
        if curr_m > 12: curr_m = 1; curr_y += 1
    return meses

def do_cross(content, filename, m_ini, m_fim, modo, ibge, api_key, col_cpf, col_nome):
    suffix = Path(filename).suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(StringIO(content.decode("utf-8-sig")), dtype=str)
    elif suffix == ".json":
        df = pd.DataFrame(json.loads(content.decode("utf-8")))
    else:
        df = pd.read_excel(BytesIO(content), dtype=str)

    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={col_cpf.lower(): "cpf", col_nome.lower(): "nome"})
    df["cpf"] = df["cpf"].apply(normalizar_cpf)
    df = df[df["cpf"] != ""].drop_duplicates(subset=["cpf"])

    api_client = BolsaFamiliaAPI(api_key)
    meses = get_meses_list(m_ini, m_fim)
    final_results = []

    if modo == "municipio":
        for mes in meses:
            regs = api_client.buscar_sacados_municipio(mes, ibge)
            api_por_chave = {}
            for r in regs:
                bf = r.get("beneficiarioNovoBolsaFamilia", {})
                chave = chave_cruzamento(bf.get("cpfFormatado", ""), bf.get("nome", ""))
                if chave:
                    api_por_chave.setdefault(chave, []).append(r)
            for _, srv in df.iterrows():
                chave_srv = chave_cruzamento(srv["cpf"], srv.get("nome", ""))
                if chave_srv and chave_srv in api_por_chave:
                    for reg in api_por_chave[chave_srv]:
                        final_results.append(format_result(srv, reg))
    else:
        meses_set = set(meses)
        for _, srv in df.iterrows():
            regs = api_client.buscar_por_cpf(srv["cpf"])
            for reg in regs:
                d_ref = reg.get("mesReferencia", "").replace("-", "")
                if d_ref not in meses_set:
                    continue
                bf = reg.get("beneficiarioNovoBolsaFamilia", {})
                if chave_cruzamento(bf.get("cpfFormatado", ""), bf.get("nome", "")) == \
                   chave_cruzamento(srv["cpf"], srv.get("nome", "")):
                    final_results.append(format_result(srv, reg))

    return {"status": "completed", "progress": 100, "result": final_results, "error": None}

def format_result(srv, reg):
    bf = reg.get("beneficiarioNovoBolsaFamilia", {})
    mun = reg.get("municipio", {})
    return {
        "servidor": srv.get("nome", ""),
        "cpf": srv.get("cpf", ""),
        "beneficiario": bf.get("nome", ""),
        "municipio": mun.get("nomeIBGE", ""),
        "uf": mun.get("uf", {}).get("sigla", ""),
        "mes": reg.get("dataMesReferencia", reg.get("mesReferencia", "")).replace("-", ""),
        "data_saque": reg.get("dataSaque", ""),
        "valor": reg.get("valorSaque", reg.get("valor", 0))
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

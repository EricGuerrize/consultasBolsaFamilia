import os
import re
import json
import time
import pandas as pd
import requests
from io import BytesIO, StringIO
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
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

# Estado global temporário (Simulação de DB em memória para este contexto)
# Em produção real, usar Redis para o status das tarefas
jobs = {}

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
async def start_cross(
    background_tasks: BackgroundTasks,
    m_ini: str = Form(...),
    m_fim: str = Form(...),
    modo: str = Form(...),
    ibge: str = Form(""),
    api_key: str = Form(""),
    col_cpf: str = Form("cpf"),
    col_nome: str = Form("nome"),
    file: Optional[UploadFile] = File(None),
    json_data: Optional[str] = Form(None)
):
    job_id = str(time.time())
    jobs[job_id] = {"status": "processing", "progress": 0, "result": None, "error": None}
    
    # Define a chave real (usa do env se não enviada)
    real_api_key = api_key or os.getenv("CHAVE_API_DADOS", "")
    if not real_api_key:
        raise HTTPException(status_code=401, detail="Chave de API não fornecida")

    content = None
    filename = "upload.json"
    if file:
        content = await file.read()
        filename = file.filename
    elif json_data:
        content = json_data.encode("utf-8")
        filename = "input.json"
    else:
        raise HTTPException(status_code=400, detail="Nenhum dado enviado (arquivo ou JSON)")

    background_tasks.add_task(
        run_cross_task, job_id, content, filename, m_ini, m_fim, modo, ibge, real_api_key, col_cpf, col_nome
    )
    
    return {"job_id": job_id}

@app.post("/api/cross/json")
async def start_cross_json(background_tasks: BackgroundTasks, req: CrossJsonRequest):
    job_id = str(time.time())
    jobs[job_id] = {"status": "processing", "progress": 0, "result": None, "error": None}

    real_api_key = req.api_key or os.getenv("CHAVE_API_DADOS", "")
    if not real_api_key:
        raise HTTPException(status_code=401, detail="Chave de API não fornecida")

    content = json.dumps(req.records).encode("utf-8")
    background_tasks.add_task(
        run_cross_task, job_id, content, "input.json",
        req.m_ini, req.m_fim, req.modo, req.ibge or "",
        real_api_key, req.col_cpf or "cpf", req.col_nome or "nome"
    )
    return {"job_id": job_id}

@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Tarefa não encontrada")
    return jobs[job_id]

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

def run_cross_task(job_id, content, filename, m_ini, m_fim, modo, ibge, api_key, col_cpf, col_nome):
    try:
        suffix = Path(filename).suffix.lower()
        from io import BytesIO, StringIO
        if suffix == ".csv":
            df = pd.read_csv(StringIO(content.decode("utf-8-sig")), dtype=str)
        elif suffix == ".json":
            data = json.loads(content.decode("utf-8"))
            df = pd.DataFrame(data)
        else:
            df = pd.read_excel(BytesIO(content), dtype=str)
        
        df.columns = [c.strip().lower() for c in df.columns]
        df = df.rename(columns={col_cpf.lower(): "cpf", col_nome.lower(): "nome"})
        df["cpf"] = df["cpf"].apply(normalizar_cpf)
        df = df[df["cpf"] != ""].drop_duplicates(subset=["cpf"])
        
        api = BolsaFamiliaAPI(api_key)
        meses = get_meses_list(m_ini, m_fim)
        final_results = []
        
        if modo == "municipio":
            for i, mes in enumerate(meses):
                regs = api.buscar_sacados_municipio(mes, ibge)
                # Indexa por chave: 6 dígitos do meio + primeiro nome
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

                jobs[job_id]["progress"] = int(((i+1)/len(meses)) * 100)

        else: # MODO CPF — busca por CPF completo, valida por chave ao retornar
            servidores = df.to_dict("records")
            for i, srv in enumerate(servidores):
                regs = api.buscar_por_cpf(srv["cpf"])
                for reg in regs:
                    d_ref = reg.get("mesReferencia", "").replace("-", "")
                    if d_ref not in meses:
                        continue
                    # Valida pelo mesmo critério: meio CPF + primeiro nome
                    bf = reg.get("beneficiarioNovoBolsaFamilia", {})
                    if chave_cruzamento(bf.get("cpfFormatado", ""), bf.get("nome", "")) == \
                       chave_cruzamento(srv["cpf"], srv.get("nome", "")):
                        final_results.append(format_result(srv, reg))
                jobs[job_id]["progress"] = int(((i+1)/len(servidores)) * 100)

        jobs[job_id]["status"] = "completed"
        jobs[job_id]["result"] = final_results
        
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)

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

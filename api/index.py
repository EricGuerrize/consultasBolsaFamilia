import os
import re
import sys
import requests
from io import BytesIO, StringIO
from pathlib import Path
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# Permite importar oracle_connector que está na pasta pai
sys.path.insert(0, str(Path(__file__).parent.parent))

load_dotenv()

app = FastAPI(title="Bolsa Família x Servidores API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

API_BASE   = "https://api.portaldatransparencia.gov.br/api-de-dados"
API_SACADO = f"{API_BASE}/novo-bolsa-familia-sacado-beneficiario-por-municipio"
API_CPF    = f"{API_BASE}/novo-bolsa-familia-disponivel-por-cpf-ou-nis"


def _normalizar_cpf(cpf) -> str:
    if not cpf:
        return ""
    d = re.sub(r"\D", "", str(cpf))
    return d.zfill(11)[:11] if len(d) <= 11 else d[:11]


# ─────────────────────────────────────────────
#  ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/api/health")
async def health_check():
    return {"status": "ok", "service": "Bolsa Família API"}


@app.post("/api/servidores")
async def get_servidores(request: Request):
    """
    Consulta o Oracle e retorna a lista de servidores (cpf + nome).
    Body: { "ent_codigo": "1118181", "exercicio": "2024" }
    """
    body = await request.json()
    ent_codigo = body.get("ent_codigo", "1118181")
    exercicio  = body.get("exercicio", "2024")

    try:
        from oracle_connector import OracleConnector
        oracle = OracleConnector()
        df = oracle.get_servidores_data(ent_codigo=ent_codigo, exercicio=exercicio)
        df.columns = [c.lower() for c in df.columns]

        # Normaliza CPF e remove duplicatas
        df["pess_cpf"] = df["pess_cpf"].apply(_normalizar_cpf)
        df = df[df["pess_cpf"] != ""].drop_duplicates(subset=["pess_cpf"])

        servidores = (
            df[["pess_cpf", "pess_nome"]]
            .rename(columns={"pess_cpf": "cpf", "pess_nome": "nome"})
            .to_dict(orient="records")
        )
        return {"servidores": servidores, "total": len(servidores)}

    except ImportError:
        raise HTTPException(status_code=503, detail="oracledb não instalado no servidor")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro Oracle: {str(e)}")


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """Lê colunas de arquivos Excel (CSV é processado direto no frontend)."""
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


@app.post("/api/proxy")
async def proxy_portal(request: Request):
    """Proxy leve: repassa UMA página da API do Portal da Transparência."""
    body = await request.json()
    real_api_key = body.get("api_key") or os.getenv("CHAVE_API_DADOS", "")
    if not real_api_key:
        raise HTTPException(status_code=401, detail="Chave de API não fornecida")

    endpoint = body.get("endpoint")
    params   = body.get("params", {})

    if endpoint == "municipio":
        url = API_SACADO
    elif endpoint == "cpf":
        url = API_CPF
    else:
        raise HTTPException(status_code=400, detail="endpoint inválido")

    try:
        r = requests.get(
            url, params=params,
            headers={"chave-api-dados": real_api_key, "Accept": "application/json"},
            timeout=55,
        )
        if r.status_code == 429:
            raise HTTPException(status_code=429, detail="Rate limit da API do Portal")
        if r.status_code >= 400:
            raise HTTPException(status_code=r.status_code, detail=f"API do Portal retornou {r.status_code}: {r.text[:200]}")
        try:
            return r.json() or []
        except Exception:
            raise HTTPException(status_code=502, detail="API do Portal retornou resposta inválida")
    except HTTPException:
        raise
    except requests.Timeout:
        raise HTTPException(status_code=504, detail="API do Portal não respondeu a tempo")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Erro ao contactar API do Portal: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

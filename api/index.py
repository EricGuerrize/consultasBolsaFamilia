import os
import requests
from io import BytesIO, StringIO
from pathlib import Path
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

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

# ─────────────────────────────────────────────
#  ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/api/health")
async def health_check():
    return {"status": "ok", "service": "Bolsa Família API"}

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
    """Proxy leve: repassa UMA página da API do Portal da Transparência.
    Todo o cruzamento é feito no frontend — isso evita o timeout de 10s do Vercel Hobby.
    """
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

#!/usr/bin/env python3
"""
Pipeline automatizado: Oracle (ou CSV) → API Bolsa Família → Cruzamento → Firebase

Uso:
  # Completo (Oracle + Firebase):
  python automated_pipeline.py --ibge 5107602 --mes-ini 202401 --mes-fim 202403

  # Só com CSV local e sem Firebase (para testes offline):
  python automated_pipeline.py --ibge 5107602 --mes-ini 202401 --mes-fim 202403 \\
      --sem-oracle --sem-firebase

  # Ajustar paralelismo:
  python automated_pipeline.py --ibge 5107602 --mes-ini 202401 --mes-fim 202403 \\
      --workers 6
"""

import argparse
import os
import re
import sys
import time
import csv as csv_mod
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from oracle_connector import OracleConnector
from bolsa_familia_client import BolsaFamiliaAPI, normalizar_cpf, formatar_cpf

# Mapeamento IBGE -> Código Entidade Oracle (conforme orgaos.json e APLIC)
IBGE_TO_ENTIDADE = {
    "5107602": "1118181",  # Rondonópolis
    "5100102": "1113216",  # Acorizal
    "5105101": "1120724",  # Jangada
    "5105259": "1111319",  # Lucas do Rio Verde
}


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get_meses(ini: str, fim: str) -> list[str]:
    y, m = int(ini[:4]), int(ini[4:])
    ey, em = int(fim[:4]), int(fim[4:])
    meses = []
    while (y < ey) or (y == ey and m <= em):
        meses.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return meses


def normalizar_nome(nome: str) -> str:
    """Remove acentos, converte para minúsculas e remove espaços extras."""
    if not nome: return ""
    import unicodedata
    nfkd = unicodedata.normalize('NFKD', nome)
    limpo = "".join([c for c in nfkd if not unicodedata.combining(c)])
    return limpo.lower().strip()


def mascarar_cpf(cpf_raw: str) -> str:
    """Converte CPF 12345678901 para formatado mascarado ***.456.789-**."""
    c = normalizar_cpf(cpf_raw)
    if len(c) != 11: return c
    return f"***.{c[3:6]}.{c[6:9]}-**"


def cruzar_registro(srv: dict, reg: dict) -> dict:
    bf  = reg.get("beneficiarioNovoBolsaFamilia", {})
    mun = reg.get("municipio", {})
    uf  = mun.get("uf", {})
    # O Portal da Transparência às vezes inverte sigla e nome.
    # Ex: sigla="MATO GROSSO", nome="MT". Queremos a sigla de 2 letras.
    uf_val = uf.get("nome", "")
    if len(uf_val) != 2:
        uf_val = uf.get("sigla", "")
    if len(uf_val) > 2:
        # Se ainda for longo, tenta extrair as iniciais ou um mapeamento simples
        if "MATO GROSSO" in uf_val.upper(): uf_val = "MT"
        else: uf_val = uf_val[:2].upper()

    data_ref = reg.get("dataMesReferencia", reg.get("mesReferencia", ""))
    if isinstance(data_ref, str):
        data_ref = data_ref.replace("-", "")[:6]

    return {
        "Nome Servidor":   srv.get("pess_nome", ""),
        "CPF":             formatar_cpf(normalizar_cpf(srv.get("pess_cpf", ""))),
        "Matrícula":       str(srv.get("pess_matricula", "")),
        "Cargo":           str(srv.get("cfpess_nome", "")),
        "Órgão":           str(srv.get("org_nome", "")),
        "Nome BF":         bf.get("nome", ""),
        "NIS":             str(bf.get("nis", "")),
        "Município":       mun.get("nomeIBGE", ""),
        "UF":              uf_val,
        "Mês Referência":  data_ref,
        "Data Saque":      reg.get("dataSaque", ""),
        "Valor Saque (R$)": float(reg.get("valorSaque", reg.get("valor", 0)) or 0),
    }


def cruzar_em_massa(df_serv: pd.DataFrame, registros_api: list[dict]) -> list[dict]:
    """
    Cruza lista de servidores com lista da API usando (NOME_NORMALIZADO, CPF_MASCARADO) como chave.
    """
    if not registros_api:
        return []
    
    # Indexa API por (nome_normalizado, cpf_mascarado)
    api_por_chave: dict[str, list[dict]] = {}
    for r in registros_api:
        bf = r.get("beneficiarioNovoBolsaFamilia", {})
        nome_api = normalizar_nome(bf.get("nome", ""))
        cpf_api  = bf.get("cpfFormatado", "") # Já vem mascarado da API: ***.XXX.XXX-**
        
        chave = f"{nome_api}|{cpf_api}"
        if chave not in api_por_chave:
            api_por_chave[chave] = []
        api_por_chave[chave].append(r)

    matches: list[dict] = []
    seen: set[str] = set()
    
    # Itera servidores
    for _, srv in df_serv.iterrows():
        nome_srv = normalizar_nome(srv.get("pess_nome", ""))
        cpf_srv_masc = mascarar_cpf(srv.get("pess_cpf", ""))
        
        chave_srv = f"{nome_srv}|{cpf_srv_masc}"
        
        if chave_srv in api_por_chave:
            for reg_api in api_por_chave[chave_srv]:
                # Evita duplicatas exatas se houver
                data_ref = reg_api.get("dataMesReferencia", reg_api.get("mesReferencia", "")).replace("-", "")[:6]
                valor    = reg_api.get("valorSaque", reg_api.get("valor", 0))
                unique_key = f"{srv.get('pess_cpf','')}|{data_ref}|{reg_api.get('dataSaque','')}|{valor}"
                
                if unique_key in seen:
                    continue
                seen.add(unique_key)
                matches.append(cruzar_registro(srv.to_dict(), reg_api))
    
    return matches


def salvar_csv_local(registros: list[dict], run_id: str) -> str:
    path = f"cruzamento_{run_id}.csv"
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv_mod.DictWriter(f, fieldnames=registros[0].keys())
        writer.writeheader()
        writer.writerows(registros)
    return path


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline Servidores × Bolsa Família → Firebase",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--ibge",      nargs="+", required=True, help="Lista de códigos IBGE dos municípios (ex: 5107602 5100102)")
    parser.add_argument("--mes-ini",   required=True, help="Mês início YYYYMM (ex: 202401)")
    parser.add_argument("--mes-fim",   required=True, help="Mês fim   YYYYMM (ex: 202412)")
    parser.add_argument("--exercicio", default="2024", help="Exercício para query Oracle (default: 2024)")
    parser.add_argument("--workers",   type=int, default=4, help="Threads paralelas para API (default: 4)")
    parser.add_argument("--sem-firebase", action="store_true", help="Pular upload Firebase — salva CSV local")
    parser.add_argument("--sem-oracle",   action="store_true", help="Usar servidores_2024.csv em vez do Oracle")
    args = parser.parse_args()

    # Validações
    if not re.match(r"^\d{6}$", args.mes_ini) or not re.match(r"^\d{6}$", args.mes_fim):
        log("ERRO: --mes-ini e --mes-fim devem ser YYYYMM (ex: 202401)")
        sys.exit(1)
    if int(args.mes_ini) > int(args.mes_fim):
        log("ERRO: --mes-ini não pode ser maior que --mes-fim")
        sys.exit(1)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    meses  = get_meses(args.mes_ini, args.mes_fim)
    t0     = time.time()

    log("=" * 60)
    log(f"Pipeline  run_id={run_id}")
    log(f"IBGE={args.ibge}  Periodo={args.mes_ini}->{args.mes_fim}  ({len(meses)} mes/meses)")
    log(f"Workers={args.workers}  Oracle={'não' if args.sem_oracle else 'sim'}  Firebase={'não' if args.sem_firebase else 'sim'}")
    log("=" * 60)

    # ── 1. Servidores ─────────────────────────────────────
    log("1/4 · Carregando servidores...")
    if args.sem_oracle:
        df_serv_full_csv = pd.read_csv("servidores_2024.csv", dtype=str, sep=";", encoding="utf-8-sig")
        df_serv_full_csv.columns = [c.strip().lower() for c in df_serv_full_csv.columns]
        # Normaliza colunas
        col_map = {}
        for col in df_serv_full_csv.columns:
            if "cpf" in col: col_map[col] = "pess_cpf"
            elif "nome" in col: col_map[col] = "pess_nome"
        df_serv_full_csv = df_serv_full_csv.rename(columns=col_map)
    else:
        oracle = OracleConnector()

    # ── LOOP POR CIDADE ───────────────────────────────────
    for idx_mun, ibge in enumerate(args.ibge):
        log("\n" + "=" * 60)
        log(f"PROCESSANDO MUNICIPIO {idx_mun+1}/{len(args.ibge)}: {ibge}")
        log("=" * 60)

        # ── 1. Carregar Servidores (Oracle ou CSV) ───────────
        if args.sem_oracle:
            df_serv = df_serv_full_csv.copy()
        else:
            ent_codigo = IBGE_TO_ENTIDADE.get(ibge)
            if not ent_codigo:
                log(f"   AVISO: Codigo entidade nao mapeado para IBGE {ibge}. Usando padrao Rondonopolis.")
                ent_codigo = "1118181"
            
            try:
                log(f"   Extraindo servidores do Oracle (entidade {ent_codigo})...")
                df_serv = oracle.get_servidores_data(ent_codigo=ent_codigo, exercicio=args.exercicio)
                df_serv.columns = [c.lower() for c in df_serv.columns]
            except Exception as e:
                log(f"   ERRO Oracle para {ibge}: {e}")
                continue
        df_serv["pess_cpf"] = df_serv["pess_cpf"].apply(normalizar_cpf)
        antes = len(df_serv)
        df_serv = df_serv[df_serv["pess_cpf"] != ""].drop_duplicates(subset=["pess_cpf"])
        log(f"   {len(df_serv)} servidores unicos.")

        # ── 2. API Bolsa Familia (paralelo) ───────────────────
        api_key = os.getenv("CHAVE_API_DADOS", "")
        log(f"2/4 · Buscando API - {len(meses)} mes(es) em paralelo...")
        api = BolsaFamiliaAPI(api_key)

        def on_mes_concluido(mes: str, total: int):
            log(f"   [OK] {mes}: {total} registro(s)")

        try:
            resultados_por_mes = api.buscar_sacados_municipio_paralelo(
                meses=meses,
                codigo_ibge=ibge,
                max_workers=args.workers,
                progress_cb=on_mes_concluido,
            )
        except Exception as e:
            log(f"   ERRO API para {ibge}: {e}")
            continue

        total_api = sum(len(v) for v in resultados_por_mes.values())
        log(f"   API concluída: {total_api} registro(s).")

        # ── 3. Cruzamento ─────────────────────────────────────
        log("3/4 · Cruzando dados...")
        cidade_resultados: list[dict] = []
        for mes in meses:
            regs = resultados_por_mes.get(mes, [])
            cruzados = cruzar_em_massa(df_serv, regs)
            cidade_resultados.extend(cruzados)

        log(f"   Total: {len(cidade_resultados)} correspondências.")

        if not cidade_resultados:
            log(f"   Nenhuma correspondência para {ibge} — pulando upload.")
            continue

        # ── 4. Firebase ou CSV ────────────────────────────────
        if args.sem_firebase:
            path = salvar_csv_local(cidade_resultados, f"{ibge}_{run_id}")
            log(f"   CSV salvo: {path}")
        else:
            log("4/4 · Upload para Firebase...")
            try:
                from firebase_connector import FirebaseConnector
                fb = FirebaseConnector()
                run_id_mun = f"{ibge}_{run_id}"
                fb.salvar_metadados_run(run_id_mun, {
                    "ibge":               ibge,
                    "mes_ini":            args.mes_ini,
                    "mes_fim":            args.mes_fim,
                    "total_servidores":   int(len(df_serv)),
                    "total_api":          int(total_api),
                    "total_cruzamentos":  int(len(cidade_resultados)),
                    "status":             "concluido",
                    "iniciado_em":        datetime.now().isoformat(),
                })
                fb.upload_cruzamento(cidade_resultados, run_id=run_id_mun)
                log(f"   Upload OK: {run_id_mun}")
            except Exception as e:
                log(f"   ERRO Firebase: {e}")
                path = salvar_csv_local(cidade_resultados, f"{ibge}_{run_id}")
                log(f"   Fallback CSV: {path}")

    elapsed = time.time() - t0
    log("=" * 60)
    log(f"Pipeline concluido em {elapsed:.1f}s")
    log("=" * 60)


if __name__ == "__main__":
    main()

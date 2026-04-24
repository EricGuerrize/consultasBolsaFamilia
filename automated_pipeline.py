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


def cruzar_registro(srv: dict, reg: dict) -> dict:
    bf  = reg.get("beneficiarioNovoBolsaFamilia", {})
    mun = reg.get("municipio", {})
    uf  = mun.get("uf", {})
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
        "UF":              uf.get("sigla", ""),
        "Mês Referência":  data_ref,
        "Data Saque":      reg.get("dataSaque", ""),
        "Valor Saque (R$)": float(reg.get("valorSaque", reg.get("valor", 0)) or 0),
    }


def cruzar_em_massa(df_serv: pd.DataFrame, registros_api: list[dict]) -> list[dict]:
    if not registros_api:
        return []

    # Índice da API por CPF normalizado
    api_por_cpf: dict[str, list[dict]] = {}
    for r in registros_api:
        bf = r.get("beneficiarioNovoBolsaFamilia", {})
        cpf_api = normalizar_cpf(bf.get("cpfFormatado", ""))
        if cpf_api:
            api_por_cpf.setdefault(cpf_api, []).append(r)

    linhas: list[dict] = []
    seen: set[str] = set()
    for _, srv in df_serv.iterrows():
        cpf_srv = normalizar_cpf(srv.get("pess_cpf", ""))
        if cpf_srv not in api_por_cpf:
            continue
        for reg in api_por_cpf[cpf_srv]:
            data_ref = reg.get("dataMesReferencia", reg.get("mesReferencia", "")).replace("-", "")[:6]
            valor    = reg.get("valorSaque", reg.get("valor", 0))
            key = f"{cpf_srv}|{data_ref}|{reg.get('dataSaque','')}|{valor}"
            if key in seen:
                continue
            seen.add(key)
            linhas.append(cruzar_registro(srv.to_dict(), reg))
    return linhas


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
    parser.add_argument("--ibge",      required=True, help="Código IBGE do município (ex: 5107602)")
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
    log(f"IBGE={args.ibge}  Período={args.mes_ini}→{args.mes_fim}  ({len(meses)} mês/meses)")
    log(f"Workers={args.workers}  Oracle={'não' if args.sem_oracle else 'sim'}  Firebase={'não' if args.sem_firebase else 'sim'}")
    log("=" * 60)

    # ── 1. Servidores ─────────────────────────────────────
    log("1/4 · Carregando servidores...")
    if args.sem_oracle:
        df_serv = pd.read_csv("servidores_2024.csv", dtype=str, sep=";", encoding="utf-8-sig")
        df_serv.columns = [c.strip().lower() for c in df_serv.columns]
        # Normaliza colunas para o padrão interno do pipeline
        col_map = {}
        for col in df_serv.columns:
            if "cpf" in col:
                col_map[col] = "pess_cpf"
            elif "nome" in col:
                col_map[col] = "pess_nome"
        df_serv = df_serv.rename(columns=col_map)
        if "pess_matricula" not in df_serv.columns:
            df_serv["pess_matricula"] = ""
        if "cfpess_nome" not in df_serv.columns:
            df_serv["cfpess_nome"] = ""
        if "org_nome" not in df_serv.columns:
            df_serv["org_nome"] = ""
    else:
        try:
            oracle = OracleConnector()
            df_serv = oracle.get_servidores_data(exercicio=args.exercicio)
            df_serv.columns = [c.lower() for c in df_serv.columns]
        except Exception as e:
            log(f"ERRO Oracle: {e}")
            sys.exit(1)

    df_serv["pess_cpf"] = df_serv["pess_cpf"].apply(normalizar_cpf)
    antes = len(df_serv)
    df_serv = df_serv[df_serv["pess_cpf"] != ""].drop_duplicates(subset=["pess_cpf"])
    log(f"   {len(df_serv)} servidores únicos ({antes - len(df_serv)} duplicatas removidas).")

    # ── 2. API Bolsa Família (paralelo) ───────────────────
    api_key = os.getenv("CHAVE_API_DADOS", "")
    if not api_key:
        log("ERRO: CHAVE_API_DADOS não definido no .env")
        sys.exit(1)

    log(f"2/4 · Buscando API — {len(meses)} mes(es) em paralelo ({args.workers} workers)...")
    api = BolsaFamiliaAPI(api_key)

    def on_mes_concluido(mes: str, total: int):
        log(f"   ✓ {mes}: {total} registro(s)")

    try:
        resultados_por_mes = api.buscar_sacados_municipio_paralelo(
            meses=meses,
            codigo_ibge=args.ibge,
            max_workers=args.workers,
            progress_cb=on_mes_concluido,
        )
    except Exception as e:
        log(f"ERRO API: {e}")
        sys.exit(1)

    total_api = sum(len(v) for v in resultados_por_mes.values())
    log(f"   API concluída: {total_api} registro(s) em {len(meses)} mes(es).")

    # ── 3. Cruzamento ─────────────────────────────────────
    log("3/4 · Cruzando servidores × Bolsa Família...")
    todos: list[dict] = []
    for mes in meses:
        regs = resultados_por_mes.get(mes, [])
        cruzados = cruzar_em_massa(df_serv, regs)
        if cruzados:
            log(f"   {mes}: {len(cruzados)} correspondência(s)")
        todos.extend(cruzados)

    log(f"   Total: {len(todos)} correspondência(s) encontrada(s).")

    if not todos:
        log("Nenhuma correspondência — encerrando.")
        sys.exit(0)

    # ── 4. Firebase ou CSV ────────────────────────────────
    if args.sem_firebase:
        log("4/4 · Salvando resultado em CSV local (--sem-firebase)...")
        path = salvar_csv_local(todos, run_id)
        log(f"   Arquivo: {path}")
    else:
        log("4/4 · Fazendo upload para Firebase...")
        try:
            from firebase_connector import FirebaseConnector
            fb = FirebaseConnector()
            fb.salvar_metadados_run(run_id, {
                "ibge":               args.ibge,
                "mes_ini":            args.mes_ini,
                "mes_fim":            args.mes_fim,
                "total_servidores":   int(len(df_serv)),
                "total_api":          int(total_api),
                "total_cruzamentos":  int(len(todos)),
                "status":             "em_andamento",
                "iniciado_em":        datetime.now().isoformat(),
            })
            fb.upload_cruzamento(todos, run_id=run_id)
            fb.salvar_metadados_run(run_id, {"status": "concluido"})
            log(f"   Upload OK! Firestore: cruzamentos/{run_id}")
        except Exception as e:
            log(f"ERRO Firebase: {e}")
            # Fallback: salva CSV para não perder os dados
            path = salvar_csv_local(todos, run_id)
            log(f"   Fallback CSV salvo em: {path}")
            sys.exit(1)

    elapsed = time.time() - t0
    log("=" * 60)
    log(f"Pipeline concluído em {elapsed:.1f}s")
    log("=" * 60)


if __name__ == "__main__":
    main()

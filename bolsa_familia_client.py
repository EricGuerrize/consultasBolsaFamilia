"""
Cliente da API do Portal da Transparência — Bolsa Família.
Suporta busca sequencial e paralela (por mês) via ThreadPoolExecutor.
"""

import re
import time
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

API_BASE   = "https://api.portaldatransparencia.gov.br/api-de-dados"
API_SACADO = f"{API_BASE}/novo-bolsa-familia-sacado-beneficiario-por-municipio"
API_CPF    = f"{API_BASE}/novo-bolsa-familia-disponivel-por-cpf-ou-nis"
PAGE_SIZE  = 15


def normalizar_cpf(cpf) -> str:
    try:
        if pd.isna(cpf):
            return ""
    except (TypeError, ValueError):
        pass
    if cpf is None:
        return ""
    s = re.sub(r"\D", "", str(cpf))
    return s.zfill(11) if len(s) <= 11 else s[:11]


def formatar_cpf(cpf_raw: str) -> str:
    s = normalizar_cpf(cpf_raw)
    if len(s) == 11:
        return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}"
    return s


class BolsaFamiliaAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "chave-api-dados": api_key,
            "Accept": "application/json",
        })

    def buscar_sacados_municipio(
        self,
        mes_ano: str,
        codigo_ibge: str,
        progress_cb=None,
        cancel_flag=None,
    ) -> list[dict]:
        """Busca todos os saques de um município em um mês (sequencial, página a página)."""
        resultados = []
        pagina = 1
        while True:
            if cancel_flag and cancel_flag():
                break
            params = {"mesAno": mes_ano, "codigoIbge": codigo_ibge, "pagina": pagina}
            try:
                r = self.session.get(API_SACADO, params=params, timeout=30)
                if r.status_code == 429:
                    time.sleep(2)
                    continue
                r.raise_for_status()
                data = r.json()
                if not data:
                    break
                resultados.extend(data)
                if progress_cb:
                    progress_cb(len(resultados), pagina)
                if len(data) < PAGE_SIZE:
                    break
                pagina += 1
            except Exception as e:
                raise RuntimeError(f"Erro na API (Município {mes_ano}): {e}")
        return resultados

    def buscar_sacados_municipio_paralelo(
        self,
        meses: list[str],
        codigo_ibge: str,
        max_workers: int = 4,
        progress_cb=None,
    ) -> dict[str, list[dict]]:
        """
        Busca múltiplos meses em paralelo usando ThreadPoolExecutor.
        Retorna {mes_ano: [registros]}.
        """
        resultados: dict[str, list[dict]] = {}

        def fetch_mes(mes: str):
            regs = self.buscar_sacados_municipio(mes, codigo_ibge)
            if progress_cb:
                progress_cb(mes, len(regs))
            return mes, regs

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_mes, mes): mes for mes in meses}
            for future in as_completed(futures):
                try:
                    mes, regs = future.result()
                    resultados[mes] = regs
                except Exception as e:
                    mes = futures[future]
                    raise RuntimeError(f"Falha ao buscar mês {mes}: {e}")

        return resultados

    def buscar_por_cpf(self, cpf: str) -> list[dict]:
        """Busca histórico completo de parcelas por CPF."""
        resultados = []
        pagina = 1
        while True:
            params = {"cpf": cpf, "pagina": pagina}
            try:
                r = self.session.get(API_CPF, params=params, timeout=20)
                if r.status_code == 429:
                    time.sleep(1)
                    continue
                r.raise_for_status()
                data = r.json()
                if not data:
                    break
                resultados.extend(data)
                if len(data) < PAGE_SIZE:
                    break
                pagina += 1
            except Exception:
                return []
        return resultados

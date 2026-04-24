"""
Cruzamento de Servidores x Bolsa Família
Consulta a API do Portal da Transparência e cruza com base local de servidores.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import time
import re
import json
import csv
import os
from datetime import datetime
from pathlib import Path
import concurrent.futures

import pandas as pd
import requests
from oracle_connector import OracleConnector
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
#  CONSTANTES
# ─────────────────────────────────────────────
API_BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
API_SACADO = f"{API_BASE}/bolsa-familia-sacado-por-municipio"
API_CPF = f"{API_BASE}/bolsa-familia-disponivel-por-cpf-ou-nis"
PAGE_SIZE = 15  # Máximo por página no portal da transparência
CONFIG_FILE = Path("config_bf.json")
CACHE_FILE = Path("cache_bolsafamilia.json")

# ─────────────────────────────────────────────
#  UTILITÁRIOS DE CPF
# ─────────────────────────────────────────────

def normalizar_cpf(cpf) -> str:
    """Remove qualquer não-dígito e retorna string de 11 chars ou ''."""
    if pd.isna(cpf) or cpf is None:
        return ""
    s = re.sub(r"\D", "", str(cpf))
    return s.zfill(11) if len(s) <= 11 else s[:11]


def formatar_cpf(cpf_raw: str) -> str:
    s = normalizar_cpf(cpf_raw)
    if len(s) == 11:
        return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}"
    return s


# ─────────────────────────────────────────────
#  CLIENTE DA API
# ─────────────────────────────────────────────

class BolsaFamiliaAPI:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "chave-api-dados": api_key,
            "Accept": "application/json",
        })

    def buscar_sacados_municipio(self, mes_ano: str, codigo_ibge: str,
                                progress_cb=None, cancel_flag=None) -> list[dict]:
        """Busca todos os saques de um município em um período."""
        resultados = []
        pagina = 1
        while True:
            if cancel_flag and cancel_flag():
                break
            params = {
                "mesAno": mes_ano,
                "codigoIbge": codigo_ibge,
                "pagina": pagina,
            }
            try:
                r = self.session.get(API_SACADO, params=params, timeout=30)
                if r.status_code == 429:
                    time.sleep(2)  # Rate limit hit, wait
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
                raise RuntimeError(f"Erro na API (Município): {e}")
        return resultados

    def buscar_por_cpf(self, cpf: str, progress_cb=None) -> list[dict]:
        """Busca parcelas por CPF (Histórico completo)."""
        resultados = []
        pagina = 1
        while True:
            params = {"cpf": cpf, "pagina": pagina}
            try:
                r = self.session.get(API_CPF, params=params, timeout=20)
                if r.status_code == 429:
                    time.sleep(1)  # Rate limit hit
                    continue
                r.raise_for_status()
                data = r.json()
                if not data:
                    break
                resultados.extend(data)
                if len(data) < PAGE_SIZE:
                    break
                pagina += 1
            except Exception as e:
                # Se der erro num CPF específico, ignoramos e seguimos
                return []
        return resultados


def cruzar_registro(srv_row, reg) -> dict:
    """Helper para formatar uma linha de cruzamento."""
    bf = reg.get("beneficiarioBolsaFamilia", {})
    mun = reg.get("municipio", {})
    uf = mun.get("uf", {})
    
    # Campo de data pode variar conforme o endpoint
    data_ref = reg.get("dataMesReferencia", reg.get("mesReferencia", ""))
    if isinstance(data_ref, str) and len(data_ref) == 7 and "-" in data_ref: # YYYY-MM
        data_ref = data_ref.replace("-", "")
        
    return {
        "Nome Servidor": srv_row.get("nome", ""),
        "CPF": formatar_cpf(normalizar_cpf(srv_row.get("cpf", ""))),
        "Matrícula": srv_row.get("matricula", srv_row.get("matrícula", srv_row.get("pess_matricula", ""))),
        "Cargo": srv_row.get("cargo", srv_row.get("cfpessug_descricao", "")),
        "Órgão": srv_row.get("orgao", srv_row.get("órgão", srv_row.get("orgão", srv_row.get("org_nome", "")))),
        "Data Admissão": srv_row.get("data_admissao", srv_row.get("admissao", srv_row.get("data_ingresso", ""))),
        "Nome BF": bf.get("nome", ""),
        "NIS": bf.get("nis", ""),
        "Município": mun.get("nomeIBGE", ""),
        "UF": uf.get("sigla", ""),
        "Mês Referência": data_ref,
        "Data Saque": reg.get("dataSaque", "N/A"),
        "Valor Saque (R$)": reg.get("valorSaque", reg.get("valor", 0)),
    }

def cruzar_em_massa(df_servidores: pd.DataFrame, registros_api: list[dict]) -> pd.DataFrame:
    """Cruzamento clássico por município."""
    if not registros_api: return pd.DataFrame()
    api_por_cpf = {}
    for r in registros_api:
        bf = r.get("beneficiarioBolsaFamilia", {})
        cpf_api = normalizar_cpf(bf.get("cpfFormatado", ""))
        if cpf_api:
            api_por_cpf.setdefault(cpf_api, []).append(r)
    
    linhas = []
    for _, srv in df_servidores.iterrows():
        cpf_srv = normalizar_cpf(srv.get("cpf", ""))
        if cpf_srv in api_por_cpf:
            for reg in api_por_cpf[cpf_srv]:
                linhas.append(cruzar_registro(srv, reg))
    return pd.DataFrame(linhas)


# ─────────────────────────────────────────────
#  INTERFACE GRÁFICA
# ─────────────────────────────────────────────

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Cruzamento Servidores × Bolsa Família")
        self.geometry("1100x720")
        self.minsize(900, 600)
        self.configure(bg="#0f172a")

        # Estado interno
        self.df_servidores: pd.DataFrame | None = None
        self.df_resultado: pd.DataFrame | None = None
        self.registros_api: list[dict] = []
        self.cache: dict = {}
        self.config: dict = {}
        self._cancel = False
        
        self._load_config()
        self._load_cache()

        self._build_ui()
        self._apply_config()

    # ── Config e Cache ──────────────────────────
    def _load_config(self):
        if CONFIG_FILE.exists():
            try: self.config = json.loads(CONFIG_FILE.read_text())
            except: self.config = {}

    def _save_config(self):
        self.config["api_key"] = self.var_apikey.get().strip()
        self.config["col_cpf"] = self.var_col_cpf.get().strip()
        self.config["col_nome"] = self.var_col_nome.get().strip()
        self.config["save_key"] = self.var_save_key.get()
        if not self.var_save_key.get():
            self.config["api_key"] = ""
        CONFIG_FILE.write_text(json.dumps(self.config))

    def _apply_config(self):
        if self.config.get("save_key"):
            self.var_apikey.set(self.config.get("api_key", ""))
            self.var_save_key.set(True)
        self.var_col_cpf.set(self.config.get("col_cpf", "cpf"))
        self.var_col_nome.set(self.config.get("col_nome", "nome"))

    # ── Cache ──────────────────────────────────
    def _load_cache(self):
        if CACHE_FILE.exists():
            try:
                self.cache = json.loads(CACHE_FILE.read_text())
            except Exception:
                self.cache = {}

    def _save_cache(self):
        CACHE_FILE.write_text(json.dumps(self.cache, ensure_ascii=False))

    # ── Construção da UI ───────────────────────
    def _build_ui(self):
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure(".", background="#0f172a", foreground="#e2e8f0",
                        fieldbackground="#1e293b", bordercolor="#334155",
                        troughcolor="#1e293b", selectbackground="#3b82f6",
                        selectforeground="white")
        style.configure("TLabel", background="#0f172a", foreground="#e2e8f0",
                        font=("Segoe UI", 10))
        style.configure("Header.TLabel", font=("Segoe UI", 11, "bold"),
                        foreground="#94a3b8")
        style.configure("TButton", font=("Segoe UI", 10, "bold"),
                        padding=6, background="#1e293b", foreground="#e2e8f0",
                        bordercolor="#334155")
        style.map("TButton", background=[("active", "#334155")])
        style.configure("Accent.TButton", background="#3b82f6", foreground="white",
                        bordercolor="#2563eb")
        style.map("Accent.TButton", background=[("active", "#2563eb")])
        style.configure("TEntry", fieldbackground="#1e293b", foreground="#f1f5f9",
                        bordercolor="#475569", insertcolor="#94a3b8")
        style.configure("TCombobox", fieldbackground="#1e293b", foreground="#f1f5f9",
                        bordercolor="#475569")
        style.configure("Treeview", background="#1e293b", foreground="#e2e8f0",
                        fieldbackground="#1e293b", rowheight=24,
                        font=("Segoe UI", 9))
        style.configure("Treeview.Heading", background="#0f172a", foreground="#94a3b8",
                        font=("Segoe UI", 9, "bold"), bordercolor="#334155")
        style.map("Treeview", background=[("selected", "#3b82f6")])
        style.configure("TNotebook", background="#0f172a", bordercolor="#334155")
        style.configure("TNotebook.Tab", background="#1e293b", foreground="#94a3b8",
                        padding=[12, 5], font=("Segoe UI", 10))
        style.map("TNotebook.Tab",
                  background=[("selected", "#0f172a")],
                  foreground=[("selected", "#f1f5f9")])
        style.configure("TFrame", background="#0f172a")
        style.configure("Card.TFrame", background="#1e293b", relief="flat")
        style.configure("TProgressbar", troughcolor="#1e293b",
                        background="#3b82f6", bordercolor="#0f172a")
        style.configure("TCheckbutton", background="#0f172a", foreground="#e2e8f0")

        # ── Header ──
        header = tk.Frame(self, bg="#1e293b", height=56)
        header.pack(fill="x", side="top")
        tk.Label(header, text="⚖  Cruzamento Servidores × Bolsa Família",
                 bg="#1e293b", fg="#f1f5f9",
                 font=("Segoe UI", 14, "bold")).pack(side="left", padx=20, pady=12)
        tk.Label(header, text="Portal da Transparência — Gov.br",
                 bg="#1e293b", fg="#64748b",
                 font=("Segoe UI", 9)).pack(side="right", padx=20, pady=16)

        # ── Notebook ──
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=10, pady=(8, 0))

        self._tab_config()
        self._tab_resultados()
        self._tab_relatorio()

        # ── Status bar ──
        bar = tk.Frame(self, bg="#020617", height=28)
        bar.pack(fill="x", side="bottom")
        self.lbl_status = tk.Label(bar, text="Pronto.", bg="#020617", fg="#475569",
                                   font=("Segoe UI", 9), anchor="w")
        self.lbl_status.pack(side="left", padx=12, pady=4)
        self.lbl_timer = tk.Label(bar, text="", bg="#020617", fg="#475569",
                                  font=("Segoe UI", 9))
        self.lbl_timer.pack(side="right", padx=12)

    # ── Aba Configuração ─────────────────────────
    def _tab_config(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="  ⚙  Configuração  ")

        # Dois painéis lado a lado
        left = ttk.Frame(tab, style="Card.TFrame", padding=20)
        left.grid(row=0, column=0, sticky="nsew", padx=(12, 6), pady=12)
        right = ttk.Frame(tab, style="Card.TFrame", padding=20)
        right.grid(row=0, column=1, sticky="nsew", padx=(6, 12), pady=12)
        tab.columnconfigure(0, weight=1)
        tab.columnconfigure(1, weight=1)
        tab.rowconfigure(0, weight=1)

        # ─ Painel Esquerdo: Arquivo de Servidores ─
        ttk.Label(left, text="BASE DE SERVIDORES", style="Header.TLabel").grid(
            row=0, column=0, columnspan=3, sticky="w", pady=(0, 12))

        ttk.Label(left, text="Arquivo (CSV ou Excel):").grid(row=1, column=0, sticky="w", pady=4)
        self.var_arquivo = tk.StringVar()
        ttk.Entry(left, textvariable=self.var_arquivo, width=32).grid(
            row=2, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        ttk.Button(left, text="Selecionar…", command=self._selecionar_arquivo).grid(
            row=2, column=2, padx=(6, 0))

        ttk.Label(left, text="Coluna CPF:").grid(row=3, column=0, sticky="w", pady=4)
        self.var_col_cpf = tk.StringVar(value="cpf")
        ttk.Entry(left, textvariable=self.var_col_cpf, width=18).grid(row=3, column=1, sticky="w")

        ttk.Label(left, text="Coluna Nome:").grid(row=4, column=0, sticky="w", pady=4)
        self.var_col_nome = tk.StringVar(value="nome")
        ttk.Entry(left, textvariable=self.var_col_nome, width=18).grid(row=4, column=1, sticky="w")

        self.btn_carregar = ttk.Button(left, text="⬆  Carregar Arquivo",
                                       command=self._carregar_arquivo)
        self.btn_carregar.grid(row=5, column=0, columnspan=1, pady=(14, 0), sticky="ew")

        self.btn_oracle = ttk.Button(left, text="⚡  Carga Oracle (Thin)",
                                     command=self._carregar_oracle)
        self.btn_oracle.grid(row=5, column=1, columnspan=2, pady=(14, 0), padx=(6,0), sticky="ew")

        self.lbl_serv_info = ttk.Label(left, text="Nenhum arquivo carregado.",
                                       foreground="#64748b")
        self.lbl_serv_info.grid(row=6, column=0, columnspan=3, pady=(8, 0), sticky="w")

        left.columnconfigure(1, weight=1)

        # ─ Painel Direito: API ─
        ttk.Label(right, text="CONSULTA À API", style="Header.TLabel").grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 12))

        ttk.Label(right, text="Chave de API (Portal da Transparência):").grid(
            row=1, column=0, columnspan=2, sticky="w")
        self.var_apikey = tk.StringVar()
        ttk.Entry(right, textvariable=self.var_apikey, show="•", width=38).grid(
            row=2, column=0, columnspan=2, sticky="ew", pady=(2, 2))
        
        self.var_save_key = tk.BooleanVar(value=False)
        ttk.Checkbutton(right, text="Salvar chave localmente", variable=self.var_save_key).grid(
            row=3, column=0, columnspan=2, sticky="w", pady=(0, 6))

        # Período
        p_frame = ttk.Frame(right)
        p_frame.grid(row=4, column=0, columnspan=2, sticky="ew", pady=4)
        
        ttk.Label(p_frame, text="Mês Início:").pack(side="left")
        self.var_mes_ini = tk.StringVar(value=datetime.now().strftime("%Y%m"))
        ttk.Entry(p_frame, textvariable=self.var_mes_ini, width=8).pack(side="left", padx=4)
        
        ttk.Label(p_frame, text=" Mês Fim:").pack(side="left")
        self.var_mes_fim = tk.StringVar(value=datetime.now().strftime("%Y%m"))
        ttk.Entry(p_frame, textvariable=self.var_mes_fim, width=8).pack(side="left", padx=4)

        # Modo de Busca
        ttk.Label(right, text="Modo de Busca:").grid(row=5, column=0, sticky="w", pady=(8, 0))
        self.var_modo = tk.StringVar(value="municipio")
        m_frame = ttk.Frame(right)
        m_frame.grid(row=6, column=0, columnspan=2, sticky="w")
        ttk.Radiobutton(m_frame, text="Por Município (Lote)", variable=self.var_modo, value="municipio").pack(side="left")
        ttk.Radiobutton(m_frame, text="Por CPF (Individual)", variable=self.var_modo, value="cpf").pack(side="left", padx=10)

        # IBGE
        self.lbl_ibge = ttk.Label(right, text="Código IBGE:")
        self.lbl_ibge.grid(row=7, column=0, sticky="w", pady=(8, 0))
        self.var_ibge = tk.StringVar()
        self.ent_ibge = ttk.Entry(right, textvariable=self.var_ibge, width=12)
        self.ent_ibge.grid(row=7, column=1, sticky="w", pady=(8, 0))

        self.var_usar_cache = tk.BooleanVar(value=True)
        ttk.Checkbutton(right, text="Usar cache local", variable=self.var_usar_cache).grid(
            row=8, column=0, columnspan=2, sticky="w", pady=(8, 0))

        # Progresso
        self.progress = ttk.Progressbar(right, mode="determinate", length=280)
        self.progress.grid(row=9, column=0, columnspan=2, pady=(14, 4), sticky="ew")

        self.lbl_api_info = ttk.Label(right, text="Pronto para iniciar.", foreground="#64748b", font=("Segoe UI", 8))
        self.lbl_api_info.grid(row=10, column=0, columnspan=2, sticky="w")

        btn_frame = ttk.Frame(right)
        btn_frame.grid(row=9, column=0, columnspan=2, pady=(14, 0), sticky="ew")
        self.btn_consultar = ttk.Button(btn_frame, text="🔍  Consultar e Cruzar",
                                        style="Accent.TButton",
                                        command=self._iniciar_consulta)
        self.btn_consultar.pack(side="left", fill="x", expand=True)
        self.btn_cancelar = ttk.Button(btn_frame, text="✕ Cancelar",
                                       command=self._cancelar, state="disabled")
        self.btn_cancelar.pack(side="left", padx=(6, 0))

        right.columnconfigure(1, weight=1)

    # ── Aba Resultados ──────────────────────────
    def _tab_resultados(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="  📋  Resultados  ")

        # Filtros
        filt = ttk.Frame(tab, style="Card.TFrame", padding=(12, 8))
        filt.pack(fill="x", padx=12, pady=(10, 0))

        ttk.Label(filt, text="Filtrar:").pack(side="left")

        ttk.Label(filt, text="  Nome/CPF:").pack(side="left")
        self.var_filtro_nome = tk.StringVar()
        self.var_filtro_nome.trace_add("write", lambda *_: self._aplicar_filtros())
        ttk.Entry(filt, textvariable=self.var_filtro_nome, width=20).pack(side="left", padx=4)

        ttk.Label(filt, text="  Município:").pack(side="left")
        self.var_filtro_mun = tk.StringVar()
        self.var_filtro_mun.trace_add("write", lambda *_: self._aplicar_filtros())
        ttk.Entry(filt, textvariable=self.var_filtro_mun, width=18).pack(side="left", padx=4)

        ttk.Label(filt, text="  UF:").pack(side="left")
        self.var_filtro_uf = tk.StringVar()
        self.var_filtro_uf.trace_add("write", lambda *_: self._aplicar_filtros())
        ttk.Entry(filt, textvariable=self.var_filtro_uf, width=4).pack(side="left", padx=4)

        ttk.Button(filt, text="Limpar filtros", command=self._limpar_filtros).pack(
            side="right", padx=6)

        self.lbl_contagem = ttk.Label(filt, text="", foreground="#3b82f6",
                                      font=("Segoe UI", 10, "bold"))
        self.lbl_contagem.pack(side="right", padx=12)

        # Tabela
        cols = ("Nome Servidor", "CPF", "Matrícula", "Cargo", "Órgão", "Data Admissão", "Nome BF", "Município", "UF",
                 "Mês Ref.", "Data Saque", "Valor (R$)")
        frame_tree = ttk.Frame(tab)
        frame_tree.pack(fill="both", expand=True, padx=12, pady=8)

        self.tree = ttk.Treeview(frame_tree, columns=cols, show="headings",
                                  selectmode="browse")
        widths = [160, 110, 80, 110, 130, 90, 160, 110, 30, 70, 90, 80]
        for c, w in zip(cols, widths):
            self.tree.heading(c, text=c, command=lambda _c=c: self._sort_col(_c))
            self.tree.column(c, width=w, anchor="w")

        vsb = ttk.Scrollbar(frame_tree, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(frame_tree, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")
        self.tree.pack(fill="both", expand=True)

        # Botões exportação
        exp = ttk.Frame(tab)
        exp.pack(fill="x", padx=12, pady=(0, 10))
        ttk.Button(exp, text="⬇  Exportar CSV", command=lambda: self._exportar("csv")).pack(
            side="left", padx=(0, 6))
        ttk.Button(exp, text="⬇  Exportar Excel", command=lambda: self._exportar("xlsx")).pack(
            side="left")

    # ── Aba Relatório ───────────────────────────
    def _tab_relatorio(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="  📊  Relatório  ")

        self.txt_relatorio = tk.Text(tab, bg="#1e293b", fg="#e2e8f0",
                                     font=("Consolas", 10), relief="flat",
                                     state="disabled", wrap="word",
                                     insertbackground="white")
        sb = ttk.Scrollbar(tab, command=self.txt_relatorio.yview)
        self.txt_relatorio.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y", padx=(0, 12), pady=12)
        self.txt_relatorio.pack(fill="both", expand=True, padx=(12, 0), pady=12)

    # ── Ações ───────────────────────────────────
    def _selecionar_arquivo(self):
        path = filedialog.askopenfilename(
            title="Selecionar arquivo de servidores",
            filetypes=[("CSV / Excel", "*.csv *.xlsx *.xls"), ("Todos", "*.*")])
        if path:
            self.var_arquivo.set(path)

    def _carregar_arquivo(self):
        path = self.var_arquivo.get().strip()
        if not path:
            messagebox.showwarning("Atenção", "Selecione um arquivo primeiro.")
            return
        try:
            if path.lower().endswith(".csv"):
                df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
            else:
                df = pd.read_excel(path, dtype=str)

            # Normaliza colunas
            df.columns = [c.strip().lower() for c in df.columns]

            col_cpf = self.var_col_cpf.get().strip().lower()
            col_nome = self.var_col_nome.get().strip().lower()

            if col_cpf not in df.columns:
                messagebox.showerror("Erro", f"Coluna '{col_cpf}' não encontrada.\n"
                                     f"Colunas disponíveis: {list(df.columns)}")
                return
            if col_nome not in df.columns:
                messagebox.showerror("Erro", f"Coluna '{col_nome}' não encontrada.\n"
                                     f"Colunas disponíveis: {list(df.columns)}")
                return

            # Renomeia para padrão interno
            df = df.rename(columns={col_cpf: "cpf", col_nome: "nome"})
            df["cpf"] = df["cpf"].apply(normalizar_cpf)
            df = df[df["cpf"] != ""]

            # Remove duplicatas de CPF
            antes = len(df)
            df = df.drop_duplicates(subset=["cpf"])
            dupls = antes - len(df)

            self.df_servidores = df
            info = (f"✔  {len(df)} servidores carregados"
                    + (f" ({dupls} duplicatas removidas)" if dupls else ""))
            self.lbl_serv_info.configure(text=info, foreground="#22c55e")
            self._status(info)
    def _carregar_oracle(self):
        self._status("Conectando ao Oracle...")
        self.btn_oracle.configure(state="disabled")
        
        def run_oracle():
            try:
                connector = OracleConnector()
                # Usando os valores padrão ou poderíamos adicionar campos na UI
                df = connector.get_servidores_data()
                
                # Normaliza colunas para o padrão do app
                # A query já retorna pess_cpf, pess_nome, org_nome, etc.
                df.columns = [c.strip().lower() for c in df.columns]
                
                # Mapeamento automático já que conhecemos a query do Oracle
                df = df.rename(columns={
                    "pess_cpf": "cpf", 
                    "pess_nome": "nome",
                    "pess_matricula": "matricula",
                    "cfpessug_descricao": "cargo"
                })
                
                df["cpf"] = df["cpf"].apply(normalizar_cpf)
                df = df[df["cpf"] != ""]
                
                antes = len(df)
                df = df.drop_duplicates(subset=["cpf"])
                dupls = antes - len(df)
                
                self.df_servidores = df
                info = (f"✔  {len(df)} servidores carregados via Oracle"
                        + (f" ({dupls} duplicatas removidas)" if dupls else ""))
                
                self.after(0, lambda: self.lbl_serv_info.configure(text=info, foreground="#3b82f6"))
                self.after(0, lambda: self._status(info))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Erro Oracle", str(e)))
                self.after(0, lambda: self._status("Falha na carga Oracle."))
            finally:
                self.after(0, lambda: self.btn_oracle.configure(state="normal"))

        threading.Thread(target=run_oracle, daemon=True).start()

    def _iniciar_consulta(self):
        if self.df_servidores is None or self.df_servidores.empty:
            messagebox.showwarning("Atenção", "Carregue o arquivo de servidores primeiro.")
            return
        api_key = self.var_apikey.get().strip()
        if not api_key:
            messagebox.showwarning("Atenção", "Informe a chave de API.")
            return
        
        m_ini = self.var_mes_ini.get().strip()
        m_fim = self.var_mes_fim.get().strip()
        
        if not (re.match(r"^\d{6}$", m_ini) and re.match(r"^\d{6}$", m_fim)):
            messagebox.showwarning("Atenção", "Períodos devem ser AAAAMM.")
            return
        
        if int(m_ini) > int(m_fim):
            messagebox.showwarning("Atenção", "Mês inicial não pode ser superior ao final.")
            return

        ibge = self.var_ibge.get().strip()
        if self.var_modo.get() == "municipio" and not ibge:
            messagebox.showwarning("Atenção", "Informe o Código IBGE para busca em lote.")
            return

        self._save_config()
        self._cancel = False
        self.btn_consultar.configure(state="disabled")
        self.btn_cancelar.configure(state="normal")
        self.progress.configure(mode="determinate")
        self.progress["value"] = 0
        
        self._status("Iniciando cruzamento…")
        self._start_time = time.time()
        self._update_timer()

        threading.Thread(target=self._worker_consulta,
                         args=(api_key, m_ini, m_fim, ibge), daemon=True).start()

    def _get_meses(self, start, end):
        s_y, s_m = int(start[:4]), int(start[4:])
        e_y, e_m = int(end[:4]), int(end[4:])
        meses = []
        curr_y, curr_m = s_y, s_m
        while (curr_y < e_y) or (curr_y == e_y and curr_m <= e_m):
            meses.append(f"{curr_y}{curr_m:02d}")
            curr_m += 1
            if curr_m > 12:
                curr_m = 1
                curr_y += 1
        return meses

    def _worker_consulta(self, api_key, m_ini, m_fim, ibge):
        try:
            api = BolsaFamiliaAPI(api_key)
            meses = self._get_meses(m_ini, m_fim)
            modo = self.var_modo.get()
            
            todos_resultados = []
            
            if modo == "municipio":
                total_etapas = len(meses)
                for i, mes in enumerate(meses):
                    if self._cancel: break
                    self._status_async(f"Consultando Município: {mes} ({i+1}/{total_etapas})")
                    
                    cache_key = f"{mes}_{ibge}"
                    if self.var_usar_cache.get() and cache_key in self.cache:
                        regs = self.cache[cache_key]
                    else:
                        regs = api.buscar_sacados_municipio(mes, ibge, cancel_flag=lambda: self._cancel)
                        if not self._cancel:
                            self.cache[cache_key] = regs
                            self._save_cache()
                    
                    df_mes = cruzar_em_massa(self.df_servidores, regs)
                    todos_resultados.append(df_mes)
                    self.progress["value"] = ((i + 1) / total_etapas) * 100
            
            else: # modo CPF
                servidores = self.df_servidores.to_dict("records")
                total_srv = len(servidores)
                concluidos = 0

                def processa_cpf(srv):
                    if self._cancel: return srv, []
                    cpf_cln = normalizar_cpf(srv["cpf"])
                    key = f"cpf_{cpf_cln}"
                    
                    # Leitura em cache é segura (dictionary lookup on CPython)
                    if self.var_usar_cache.get() and key in self.cache:
                        res = self.cache[key]
                    else:
                        res = api.buscar_por_cpf(cpf_cln)
                        if not self._cancel:
                            self.cache[key] = res  # Escrita em dict no modo multi-thread é "safe" o suficiente pelo GIL
                    return srv, res

                # Executando até 8 consultas simultâneas na API (Isso simula o conceito de dividir tarefas)
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                    futures = {executor.submit(processa_cpf, s): s for s in servidores}
                    for future in concurrent.futures.as_completed(futures):
                        if self._cancel:
                            executor.shutdown(wait=False, cancel_futures=True)
                            break
                        
                        srv, regs = future.result()
                        concluidos += 1
                        
                        # Atualiza barra de progresso em tempo real a cada conclusão
                        self._status_async(f"Consultando CPF {concluidos}/{total_srv}: {srv.get('nome', '')[:20]}…")
                        self.after(0, lambda v=(concluidos / total_srv) * 100: self.progress.configure(value=v))
                        
                        # Aplica o filtro de mês/Ano igual antes
                        for r in regs:
                            d_ref = r.get("mesReferencia", "").replace("-", "")
                            if d_ref in meses:
                                todos_resultados.append(pd.DataFrame([cruzar_registro(srv, r)]))
                
                # Salvar o cache fisicamente no SSD só acontece depois de buscar todos, para evitar problemas de concorrência com arquivos.
                if self.var_usar_cache.get() and not self._cancel:
                    self._save_cache()

            if self._cancel:
                self._status_async("Consulta cancelada.")
                return

            if todos_resultados:
                self.df_resultado = pd.concat(todos_resultados, ignore_index=True).drop_duplicates()
            else:
                self.df_resultado = pd.DataFrame()
                
            self.after(0, self._exibir_resultados)
        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Erro", str(e)))
            self._status_async(f"Erro: {e}")
        finally:
            self.after(0, self._finalizar_consulta)

    def _finalizar_consulta(self):
        self.progress.stop()
        self.btn_consultar.configure(state="normal")
        self.btn_cancelar.configure(state="disabled")
        self._timer_running = False

    def _cancelar(self):
        self._cancel = True
        self._status("Cancelando…")

    def _exibir_resultados(self):
        df = self.df_resultado
        if df is None or df.empty:
            self._status("Cruzamento concluído — nenhuma correspondência encontrada.")
            self._gerar_relatorio(0, 0)
            return

        self._popular_tree(df)
        elapsed = time.time() - self._start_time
        total_val = df["Valor Saque (R$)"].astype(float).sum()
        msg = (f"✔  {len(df)} correspondência(s) encontrada(s) "
               f"em {elapsed:.1f}s  |  Total: R$ {total_val:,.2f}")
        self._status(msg)
        self._gerar_relatorio(len(df), total_val)
        self.nb.select(1)  # vai para aba resultados

    def _popular_tree(self, df: pd.DataFrame):
        self.tree.delete(*self.tree.get_children())
        for _, row in df.iterrows():
            self.tree.insert("", "end", values=(
                row.get("Nome Servidor", ""),
                row.get("CPF", ""),
                row.get("Matrícula", ""),
                row.get("Cargo", ""),
                row.get("Órgão", ""),
                row.get("Data Admissão", ""),
                row.get("Nome BF", ""),
                row.get("Município", ""),
                row.get("UF", ""),
                row.get("Mês Referência", ""),
                row.get("Data Saque", ""),
                f"R$ {float(row.get('Valor Saque (R$)', 0)):,.2f}",
            ))
        total = len(df)
        self.lbl_contagem.configure(text=f"{total} registro(s)")

    def _aplicar_filtros(self):
        if self.df_resultado is None:
            return
        df = self.df_resultado.copy()
        filtro_nome = self.var_filtro_nome.get().strip().lower()
        filtro_mun = self.var_filtro_mun.get().strip().lower()
        filtro_uf = self.var_filtro_uf.get().strip().upper()

        if filtro_nome:
            mask = (df["Nome Servidor"].str.lower().str.contains(filtro_nome, na=False) |
                    df["CPF"].str.contains(filtro_nome, na=False) |
                    df["Nome BF"].str.lower().str.contains(filtro_nome, na=False))
            df = df[mask]
        if filtro_mun:
            df = df[df["Município"].str.lower().str.contains(filtro_mun, na=False)]
        if filtro_uf:
            df = df[df["UF"].str.upper() == filtro_uf]

        self._popular_tree(df)

    def _limpar_filtros(self):
        self.var_filtro_nome.set("")
        self.var_filtro_mun.set("")
        self.var_filtro_uf.set("")

    def _sort_col(self, col):
        if self.df_resultado is None:
            return
        if not hasattr(self, "_sort_state"):
            self._sort_state = {}
        asc = not self._sort_state.get(col, False)
        self._sort_state[col] = asc
        df = self.df_resultado.sort_values(col, ascending=asc)
        self._popular_tree(df)

    def _exportar(self, fmt: str):
        if self.df_resultado is None or self.df_resultado.empty:
            messagebox.showinfo("Exportar", "Não há resultados para exportar.")
            return
        ext = ".csv" if fmt == "csv" else ".xlsx"
        path = filedialog.asksaveasfilename(
            defaultextension=ext,
            filetypes=[("CSV", "*.csv"), ("Excel", "*.xlsx")] if fmt == "csv"
                       else [("Excel", "*.xlsx"), ("CSV", "*.csv")],
            initialfile=f"cruzamento_bf_{datetime.now().strftime('%Y%m%d_%H%M')}{ext}",
        )
        if not path:
            return
        try:
            if fmt == "csv":
                self.df_resultado.to_csv(path, index=False, encoding="utf-8-sig")
            else:
                self.df_resultado.to_excel(path, index=False)
            messagebox.showinfo("Exportar", f"Arquivo salvo em:\n{path}")
        except Exception as e:
            messagebox.showerror("Erro ao exportar", str(e))

    def _gerar_relatorio(self, total_matches: int, total_valor: float):
        df_srv = self.df_servidores
        n_srv = len(df_srv) if df_srv is not None else 0
        m_ini = self.var_mes_ini.get()
        m_fim = self.var_mes_fim.get()
        modo = "Individual (CPF)" if self.var_modo.get() == "cpf" else "Lote (Município)"
        ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        linhas = [
            "=" * 64,
            "   RELATÓRIO DE CRUZAMENTO — BOLSA FAMÍLIA × SERVIDORES",
            "=" * 64,
            f"  Gerado em         : {ts}",
            f"  Período           : {m_ini} até {m_fim}",
            f"  Modo de Busca     : {modo}",
            "-" * 64,
            f"  Servidores Únicos : {n_srv:>8,}",
            f"  Ocorrências totais: {total_matches:>8,}",
            f"  TOTAL IDENTIFICADO: R$ {total_valor:>15,.2f}",
            "=" * 64,
        ]

        if self.df_resultado is not None and not self.df_resultado.empty:
            df = self.df_resultado
            linhas.append("")
            linhas.append("  TOP 10 — Maiores valores de saque:")
            linhas.append("-" * 62)
            top = (df[["Nome Servidor", "CPF", "Valor Saque (R$)"]]
                   .copy()
                   .assign(**{"Valor Saque (R$)": df["Valor Saque (R$)"].astype(float)})
                   .sort_values("Valor Saque (R$)", ascending=False)
                   .head(10))
            for _, r in top.iterrows():
                linhas.append(f"  {r['Nome Servidor'][:30]:<30}  {r['CPF']}  "
                               f"R$ {float(r['Valor Saque (R$)']):>10,.2f}")
            linhas.append("=" * 62)

        texto = "\n".join(linhas)
        self.txt_relatorio.configure(state="normal")
        self.txt_relatorio.delete("1.0", "end")
        self.txt_relatorio.insert("1.0", texto)
        self.txt_relatorio.configure(state="disabled")

    # ── Helpers ─────────────────────────────────
    def _status(self, msg: str):
        self.lbl_status.configure(text=msg)

    def _status_async(self, msg: str):
        self.after(0, lambda: self._status(msg))
        self.after(0, lambda: self.lbl_api_info.configure(text=msg[:80]))

    def _update_timer(self):
        self._timer_running = True

        def tick():
            if not self._timer_running:
                return
            elapsed = time.time() - self._start_time
            self.lbl_timer.configure(text=f"⏱ {elapsed:.1f}s")
            self.after(500, tick)

        tick()


if __name__ == "__main__":
    app = App()
    app.mainloop()
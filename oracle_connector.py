import os
import oracledb
import pandas as pd
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

class OracleConnector:
    def __init__(self):
        self.user = os.getenv("ORACLE_USER")
        self.password = os.getenv("ORACLE_PASSWORD")
        self.dsn = os.getenv("ORACLE_DSN")
        self.config_dir = os.getenv("ORACLE_CONFIG_DIR")
        
    def _get_connection(self):
        """Retorna uma conexão no modo Thin, limpando variáveis que podem causar conflito em serverless."""
        # Em alguns ambientes serverless (Vercel/Lambda), LD_LIBRARY_PATH pode causar conflito com o modo Thin
        if "LD_LIBRARY_PATH" in os.environ:
            del os.environ["LD_LIBRARY_PATH"]

        conn_params = {
            "user": self.user,
            "password": self.password,
            "dsn": self.dsn,
            "expire_time": 2, 
            "tcp_connect_timeout": 15, # 15 segundos para conectar
        }
        # Só usa config_dir se o diretório realmente existir (ex: tnsnames.ora/wallet)
        if self.config_dir and os.path.isdir(self.config_dir):
            conn_params["config_dir"] = self.config_dir
            
        # Força explicitamente o modo thin
        return oracledb.connect(**conn_params)

    def get_servidores_data(self, ent_codigo='1118181', exercicio='2024'):
        """
        Executa a query no Oracle e retorna um DataFrame do Pandas.
        Utiliza o modo Thin do oracledb com retentativas e tratamento de erro EBUSY.
        """
        if not all([self.user, self.password, self.dsn]):
            raise ValueError("Credenciais do Oracle não encontradas no arquivo .env")

        query = f"""
        SELECT /*+ FIRST_ROWS(100) */
               p.pess_matricula,
               e.pess_cpf,
               TRANSLATE(e.pess_nome, 'ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÄËÏÖÜÇáéíóúàèìòùâêîôûãõäëïöüç', 'AEIOUAEIOUAEIOUAOAEIOUCaeiouaeiouaeiouaoaeiouc') AS pess_nome,
               TRANSLATE(COALESCE(cfug.cfpessug_descricao, cn.cnat_descricao), 'ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÄËÏÖÜÇáéíóúàèìòùâêîôûãõäëïöüç', 'AEIOUAEIOUAEIOUAOAEIOUCaeiouaeiouaeiouaoaeiouc') AS cargo,
               TO_CHAR(ap.atop_datadocumento, 'DD/MM/YYYY') AS pess_data_admissao,
               ap.tatop_descricao AS tipo_ato,
               p.fpgto_mesreferencia,
               p.fpgto_anoreferencia
        FROM (
            SELECT DISTINCT ent_codigo, exercicio, pess_matricula, fpgto_mesreferencia, fpgto_anoreferencia, cfpessug_codigo
            FROM aplic2008.pessoal_folha_pagamento@conectprod
            WHERE ent_codigo = '{ent_codigo}' AND exercicio = '{exercicio}'
        ) p
        INNER JOIN aplic2008.pessoal@conectprod e
            ON p.pess_matricula = e.pess_matricula
           AND p.ent_codigo = e.ent_codigo
        LEFT JOIN aplic2008.cargo_funcao_pessoal_ug@conectprod cfug
            ON p.cfpessug_codigo = cfug.cfpessug_codigo
        LEFT JOIN aplic2008.cargo_natureza@conectprod cn
            ON cfug.cfpessug_naturezacargo = cn.cnat_codigo
        LEFT JOIN (
            SELECT t.ent_codigo, t.pess_matricula, t.atop_datadocumento, tap.tatop_descricao
            FROM (
                SELECT ent_codigo, pess_matricula, atop_datadocumento, tatop_codigo,
                       ROW_NUMBER() OVER (PARTITION BY ent_codigo, pess_matricula ORDER BY atop_datadocumento ASC) as rn
                FROM aplic2008.ato_pessoal@conectprod
                WHERE tatop_codigo IN (1, 2)
            ) t
            INNER JOIN aplic2008.tipo_ato_pessoal@conectprod tap ON t.tatop_codigo = tap.tatop_codigo
            WHERE t.rn = 1
        ) ap
            ON p.ent_codigo = ap.ent_codigo
           AND p.pess_matricula = ap.pess_matricula
        ORDER BY p.fpgto_anoreferencia, p.fpgto_mesreferencia
        """

        import time
        max_retries = 5
        last_error = None
        
        for attempt in range(max_retries):
            try:
                with self._get_connection() as connection:
                    # Uso do cursor direto para evitar problemas de locking do pandas/sqlalchemy
                    with connection.cursor() as cursor:
                        cursor.execute(query)
                        columns = [col[0].lower() for col in cursor.description]
                        data = cursor.fetchall()
                        return pd.DataFrame(data, columns=columns)
            except Exception as e:
                last_error = e
                # Se for erro de recurso ocupado (EBUSY / Errno 16), espera e tenta de novo com backoff
                err_str = str(e)
                if "Device or resource busy" in err_str or "Errno 16" in err_str or "DPI-1047" in err_str:
                    wait_time = (attempt + 1) * 3
                    time.sleep(wait_time)
                    continue
                break
        
        raise RuntimeError(f"Erro Oracle (após {max_retries} tentativas): {last_error}")

    def test_connection(self):
        """Testa a conexão com o Oracle."""
        try:
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM DUAL")
                    return True, "Conexão bem-sucedida!"
        except Exception as e:
            return False, f"Erro ao conectar: {e}"

    def execute_query(self, query):
        """Executa uma query arbitrária e retorna um DataFrame."""
        try:
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    columns = [col[0].lower() for col in cursor.description]
                    data = cursor.fetchall()
                    return pd.DataFrame(data, columns=columns)
        except Exception as e:
            raise RuntimeError(f"Erro ao executar query: {e}")

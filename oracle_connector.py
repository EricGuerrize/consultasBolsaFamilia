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
        
    def get_servidores_data(self, ent_codigo='1118181', exercicio='2024'):
        """
        Executa a query no Oracle e retorna um DataFrame do Pandas.
        Utiliza o modo Thin do oracledb.
        """
        if not all([self.user, self.password, self.dsn]):
            raise ValueError("Credenciais do Oracle não encontradas no arquivo .env")

        # Query baseada na fornecida pelo usuário, com sanitização de caracteres (TRANSLATE)
        query = f"""
        SELECT sub.*
        FROM (
            SELECT DISTINCT
                   NULL AS tipocargo,
                   p.org_codigo,
                   p.unor_codigo,
                   p.fpgto_anoreferencia,
                   p.fpgto_mesreferencia,
                   p.pfg_valliquido,
                   p.lei_numero,
                   p.cfpessugr_classenivel,
                   TRANSLATE(cfug.cfpessug_descricao, 'ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÄËÏÖÜÇáéíóúàèìòùâêîôûãõäëïöüç', 'AEIOUAEIOUAEIOUAOAEIOUCAEIOUAEIOUAEIOUAOAEIOUC') AS cfpessug_descricao,
                   p.pfg_valorbase AS pfg_valbruto,
                   p.pfg_valordescontos AS pfg_valdesconto,
                   p.pfg_valorgratificacoes AS pfg_valgratificacao,
                   p.pfg_valorbeneficios AS pfg_valbeneficio,
                   (SELECT MAX(pdfp.pdfp_valor)
                      FROM aplic2008.pessoal_desconto_folha_pagto@conectprod pdfp
                     WHERE pdfp.ent_codigo = p.ent_codigo
                       AND pdfp.exercicio = p.exercicio
                       AND pdfp.org_codigo = p.org_codigo
                       AND pdfp.unor_codigo = p.unor_codigo
                       AND pdfp.fpgto_anoreferencia = p.fpgto_anoreferencia
                       AND pdfp.fpgto_mesreferencia = p.fpgto_mesreferencia
                       AND pdfp.fpgto_numfolhapagto = p.fpgto_numfolhapagto
                       AND pdfp.pess_matricula = p.pess_matricula
                       AND pdfp.tdbg_codigo = '05') AS desconto_inss,
                   (SELECT MAX(pdfp.pdfp_valor)
                      FROM aplic2008.pessoal_desconto_folha_pagto@conectprod pdfp
                     WHERE pdfp.ent_codigo = p.ent_codigo
                       AND pdfp.exercicio = p.exercicio
                       AND pdfp.org_codigo = p.org_codigo
                       AND pdfp.unor_codigo = p.unor_codigo
                       AND pdfp.fpgto_anoreferencia = p.fpgto_anoreferencia
                       AND pdfp.fpgto_mesreferencia = p.fpgto_mesreferencia
                       AND pdfp.fpgto_numfolhapagto = p.fpgto_numfolhapagto
                       AND pdfp.pess_matricula = p.pess_matricula
                       AND pdfp.tdbg_codigo = '06') AS desconto_irrf,
                   (SELECT MAX(pdfp.pdfp_valor)
                      FROM aplic2008.pessoal_desconto_folha_pagto@conectprod pdfp
                     WHERE pdfp.ent_codigo = p.ent_codigo
                       AND pdfp.exercicio = p.exercicio
                       AND pdfp.org_codigo = p.org_codigo
                       AND pdfp.unor_codigo = p.unor_codigo
                       AND pdfp.fpgto_anoreferencia = p.fpgto_anoreferencia
                       AND pdfp.fpgto_mesreferencia = p.fpgto_mesreferencia
                       AND pdfp.fpgto_numfolhapagto = p.fpgto_numfolhapagto
                       AND pdfp.pess_matricula = p.pess_matricula
                       AND pdfp.tdbg_codigo = '10') AS desconto_rpps,
                   p.pfg_rescisao,
                   p.fpgto_numfolhapagto,
                   p.pess_matricula,
                   e.pess_cpf,
                   TRANSLATE(e.pess_nome, 'ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÄËÏÖÜÇáéíóúàèìòùâêîôûãõäëïöüç', 'AEIOUAEIOUAEIOUAOAEIOUCAEIOUAEIOUAEIOUAOAEIOUC') AS pess_nome,
                   TRANSLATE(o.org_nome, 'ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÄËÏÖÜÇáéíóúàèìòùâêîôûãõäëïöüç', 'AEIOUAEIOUAEIOUAOAEIOUCAEIOUAEIOUAEIOUAOAEIOUC') AS org_nome,
                   TRANSLATE(u.unor_nome, 'ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÄËÏÖÜÇáéíóúàèìòùâêîôûãõäëïöüç', 'AEIOUAEIOUAEIOUAOAEIOUCAEIOUAEIOUAEIOUAOAEIOUC') AS unor_nome,
                   TRANSLATE(c.cfpess_nome, 'ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÄËÏÖÜÇáéíóúàèìòùâêîôûãõäëïöüç', 'AEIOUAEIOUAEIOUAOAEIOUCAEIOUAEIOUAEIOUAOAEIOUC') AS cfpess_nome,
                   NULL AS chfpess_tipocargofuncao,
                   cn.cnat_descricao AS natureza_cargo,
                   DECODE(p.fpgto_mesreferencia,
                          '01','Janeiro','02','Fevereiro','03','Marco','04','Abril',
                          '05','Maio','06','Junho','07','Julho','08','Agosto',
                          '09','Setembro','10','Outubro','11','Novembro','12','Dezembro'
                   ) AS mesreferencia
            FROM aplic2008.pessoal_folha_pagamento@conectprod p
            INNER JOIN aplic2008.unidade_orcamentaria@conectprod u
                ON p.unor_codigo = u.unor_codigo
               AND p.org_codigo = u.org_codigo
               AND p.exercicio = u.exercicio
               AND p.ent_codigo = u.ent_codigo
            INNER JOIN aplic2008.pessoal@conectprod e
                ON p.pess_matricula = e.pess_matricula
               AND p.ent_codigo = e.ent_codigo
               AND (
                    (p.exercicio < '2015' AND p.exercicio = e.exercicio)
                 OR (p.exercicio >= '2015' AND e.exercicio >= '2015')
                   )
            INNER JOIN aplic2008.orgao@conectprod o
                ON u.org_codigo = o.org_codigo
               AND u.exercicio = o.exercicio
               AND u.ent_codigo = o.ent_codigo
            LEFT JOIN aplic2008.cargo_func_pess_ug_remuneracao@conectprod r
                ON p.ent_codigo = r.ent_codigo
               AND p.cfpess_codigo = r.cfpess_codigo
               AND p.cfpessug_codigo = r.cfpessug_codigo
               AND p.lei_numero = r.lei_numero
               AND p.cfpessugr_classenivel = r.cfpessugr_classenivel
            LEFT JOIN aplic2008.cargo_funcao_pessoal_ug@conectprod cfug
                ON r.cfpessug_codigo = cfug.cfpessug_codigo
            LEFT JOIN aplic2008.cargo_funcao_pessoal@conectprod c
                ON r.cfpess_codigo = c.cfpess_codigo
            LEFT JOIN aplic2008.cargo_natureza@conectprod cn
                ON cfug.cfpessug_naturezacargo = cn.cnat_codigo
            WHERE p.ent_codigo = '{ent_codigo}'
              AND p.exercicio = '{exercicio}'
        ) sub
        ORDER BY sub.fpgto_anoreferencia, sub.fpgto_mesreferencia
        """

        try:
            # Conexão no modo Thin
            with oracledb.connect(user=self.user, password=self.password, dsn=self.dsn) as connection:
                df = pd.read_sql(query, connection)
                return df
        except Exception as e:
            raise RuntimeError(f"Erro ao conectar ou executar query no Oracle: {e}")

from oracle_connector import OracleConnector
import pandas as pd

def main():
    query = """
SELECT DISTINCT
       e.pess_nome          AS nome,
       e.pess_cpf           AS cpf,
       p.pess_matricula     AS matricula,
       o.org_nome           AS orgao,
       c.cfpess_nome        AS cargo,
       p.exercicio          AS exercicio,
       ap.tipo_ato          AS tipo_ato,
       ap.cod_tip_ato       AS codigo_tipo_ato,
       ap.data_doc          AS data_doc
FROM aplic2008.pessoal_folha_pagamento@conectprod p
INNER JOIN aplic2008.pessoal@conectprod e
        ON p.ent_codigo = e.ent_codigo
       AND p.pess_matricula = e.pess_matricula
       AND (
             (p.exercicio < '2015' AND p.exercicio = e.exercicio)
          OR (p.exercicio >= '2015' AND e.exercicio >= '2015')
           )
INNER JOIN aplic2008.unidade_orcamentaria@conectprod u
        ON p.ent_codigo = u.ent_codigo
       AND p.exercicio = u.exercicio
       AND p.org_codigo = u.org_codigo
       AND p.unor_codigo = u.unor_codigo
INNER JOIN aplic2008.orgao@conectprod o
        ON u.ent_codigo = o.ent_codigo
       AND u.exercicio = o.exercicio
       AND u.org_codigo = o.org_codigo
LEFT JOIN aplic2008.cargo_funcao_pessoal@conectprod c
       ON p.cfpess_codigo = c.cfpess_codigo
LEFT JOIN (
    SELECT pess_matricula,
           tatop_codigo AS cod_tip_ato,
           tatop_descricao AS tipo_ato,
           atop_datadocumento AS data_doc
    FROM (
        SELECT a.pess_matricula,
               a.tatop_codigo,
               a.atop_datadocumento,
               ta.tatop_descricao,
               ROW_NUMBER() OVER (
                   PARTITION BY a.pess_matricula
                   ORDER BY a.atop_datadocumento
               ) AS rn
        FROM aplic2008.ato_pessoal@conectprod a
        INNER JOIN aplic2008.tipo_ato_pessoal@conectprod ta
                ON ta.tatop_codigo = a.tatop_codigo
    )
    WHERE rn = 1
) ap
       ON p.pess_matricula = ap.pess_matricula
WHERE p.ent_codigo = '1118181'
ORDER BY e.pess_nome, p.exercicio
"""
    
    print("Executando query customizada no Oracle...")
    connector = OracleConnector()
    
    try:
        df = connector.execute_query(query)
        if not df.empty:
            print(f"[SUCCESS] Query executada com sucesso! {len(df)} registros encontrados.")
            output_file = "resultado_query.csv"
            df.to_csv(output_file, index=False, encoding="utf-8-sig")
            print(f"[SUCCESS] Resultados salvos em: {output_file}")
            
            print("\nPrimeiros 10 registros:")
            print(df.head(10))
        else:
            print("[INFO] Query executada com sucesso, mas nenhum registro foi encontrado.")
            
    except Exception as e:
        print(f"[ERROR] Erro ao executar a query: {e}")

if __name__ == "__main__":
    main()

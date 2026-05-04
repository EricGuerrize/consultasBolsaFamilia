import os
from oracle_connector import OracleConnector

def list_columns():
    connector = OracleConnector()
    query = "SELECT column_name FROM all_tab_columns WHERE table_name = 'PESSOAL' AND owner = 'APLIC2008' AND ROWNUM <= 50"
    
    try:
        with connector._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                cols = [row[0] for row in cursor.fetchall()]
                print("Colunas encontradas na tabela PESSOAL:")
                for c in cols:
                    print(f" - {c}")
    except Exception as e:
        print(f"Erro ao listar colunas: {e}")

if __name__ == "__main__":
    list_columns()

from oracle_connector import OracleConnector

def main():
    print("Iniciando teste de conexão com o Oracle...")
    connector = OracleConnector()
    
    success, message = connector.test_connection()
    
    if success:
        print(f"[SUCCESS] {message}")
    else:
        print(f"[FAILURE] {message}")
        print("\nVerifique se as seguintes variáveis estão corretas no seu arquivo .env:")
        print(f"ORACLE_USER: {connector.user}")
        print(f"ORACLE_PASSWORD: {'****' if connector.password else 'Não definida'}")
        print(f"ORACLE_DSN: {connector.dsn}")

if __name__ == "__main__":
    main()

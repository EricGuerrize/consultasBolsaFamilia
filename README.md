# ⚖️ Consultas Bolsa Família × Servidores Públicos

Este projeto automatiza o cruzamento de informações entre uma base de dados local de servidores públicos (Oracle ou CSV/Excel) e a API oficial do **Portal da Transparência** para identificar recebimentos do programa Bolsa Família.

## 🚀 Como Iniciar

Siga os passos abaixo para configurar e rodar o projeto em sua máquina.

### 1. Clonar / Atualizar o Repositório
```powershell
git clone https://github.com/EricGuerrize/consultasBolsaFamilia.git
# ou, se já tiver clonado:
git pull
```

### 2. Instalar Dependências Python
Na raiz do projeto:
```powershell
pip install -r requirements.txt
```

### 3. Instalar Dependências do Frontend
```powershell
cd frontend
npm install
cd ..
```

### 4. Configurar Variáveis de Ambiente (.env)
Crie um arquivo `.env` na raiz do projeto com o seguinte conteúdo:
```env
CHAVE_API_DADOS=f54c8fde4ed675ea08ce5ae37e43e3f1
ORACLE_USER=Harriman
ORACLE_PASSWORD=tce2026!
ORACLE_DSN=dbdesenv02.tcemt.gov.br:1521/dbsie.tcemt.gov.br
```

### 5. Rodar o Projeto
Você precisará de dois terminais abertos.

**Terminal 1 — Backend:**
```powershell
python api/index.py
# ou: uvicorn api.index:app --reload --port 8000
```

**Terminal 2 — Frontend:**
```powershell
cd frontend
npm run dev
```

### 6. Acessar no Navegador
Abra seu navegador em: [http://localhost:5173](http://localhost:5173)

## 🧪 Testes e Verificação

Antes de iniciar, verifique se a conexão com o banco Oracle está funcionando:
```powershell
python test_db.py
```
Se o `test_db.py` passar (Retornar "[SUCCESS]"), o endpoint `/api/servidores` funcionará corretamente.

## 🎯 Instruções de Uso
1. A fonte de dados já vem selecionada como **Oracle**.
2. **Código da Entidade**: 1118181.
3. **Exercício**: 2024.
4. Selecione a **UF MT** e busque o município desejado.
5. Clique em **Iniciar Cruzamento**.

---
*Desenvolvido para auditoria, transparência e controle social.*

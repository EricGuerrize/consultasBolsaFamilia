---
description: Como configurar e rodar o projeto do zero
---

Siga estes passos para configurar o ambiente e executar o cruzamento de dados.

1. Instalar dependências do Backend
```powershell
pip install -r requirements.txt
```

2. Instalar dependências do Frontend
```powershell
cd frontend
npm install
cd ..
```

3. Verificar conexão com o Banco de Dados
// turbo
```powershell
python test_db.py
```

4. Executar o Backend
```powershell
python api/index.py
```

5. Executar o Frontend (em outro terminal)
```powershell
cd frontend
npm run dev
```
